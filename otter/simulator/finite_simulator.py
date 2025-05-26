from typing import Dict, Deque, Tuple, List, Set, Protocol, Optional, Union
from itertools import chain
from collections import deque, defaultdict

import otter.log
from otter.log import Loggable
from otter.definitions import TaskID, TaskSyncMode
from otter.db import ReadConnection
from otter.db.protocols import TaskActionCallback, TaskSuspendMetaCallback, CriticalTaskCallback
from otter.db.types import TaskSchedulingState, Task, TaskAction, SourceLocation, Event


class TaskSchedulingPolicy(Protocol):
    """
    Define signatures of callbacks to be called to determine scheduling decisions at various scheduling points
    """

    def on_task_create(self, global_ts: int, created: TaskID, encountering_thread: int, thread: int, thread_available: bool, ready_tasks: Set[TaskID]) -> Optional[TaskID]: ...

    def on_task_complete(self, global_ts: int, completed: TaskID, encountering_thread: int, thread: int, thread_available: bool, ready_tasks: Set[TaskID]) -> Optional[TaskID]: ...

    def on_task_suspend(self, global_ts: int, suspended: TaskID, mode: TaskSyncMode, dependencies: Set[TaskID], encountering_thread: int, thread: int, thread_available: bool, ready_tasks: Set[TaskID]) -> Optional[TaskID]: ...


class TaskEventListener(Protocol):

    def notify_task_create(self, task: TaskID): ...
    def notify_task_suspend(self, task: TaskID, mode: TaskSyncMode): ...
    def notify_task_complete(self, completed: TaskID): ...

class TaskPool(Loggable):
    """Encapsulates the connection to the tasks database, responsible for
    traversing the database to spawn tasks

    When a task is scheduled, generates data for the task-scheduling states
    which constitute the execution of the task. The task scheduling points
    encountered during each task scheduling state can be queued for evaluation.
    """

    def __init__(
        self,
        reader: ReadConnection,
    ) -> None:
        self.reader = reader

        # The set of tasks ready to be scheduled i.e. tasks with no outstanding dependencies.
        self._ready_tasks = {reader.get_root_task()}

        # The set of suspended tasks with their outstanding dependencies
        self._waiting_tasks: Dict[TaskID, Set[TaskID]] = {}

        # The set of currently running tasks, used to track where we're up to
        # Tasks are added here the first time they are scheduled
        # Tasks remain in here until they are completed at which point they are removed
        # Tasks in here may be waiting, ready or scheduled i.e. this does not
        # track the exact execution state of a task
        self._running_tasks: Dict[TaskID, Tuple[Task, Deque[Tuple[TaskSchedulingState, Union[Tuple[None, None], Tuple[int, TaskSyncMode]]]]]] = {}

        #! Note: the set of all tasks currently suspended is self._ready_tasks + self._waiting_tasks

    def count_ready_tasks(self):
        """Count tasks ready and yet to be scheduled"""
        return len(self._ready_tasks)

    def count_waiting_tasks(self):
        """Count suspended tasks waiting for dependencies"""
        return len(self._waiting_tasks)

    def count_running_tasks(self):
        """Count running tasks, which may be either scheduled or waiting"""
        return len(self._running_tasks)

    def get_task(self, task_id: TaskID) -> Task:
        if task_id in self._running_tasks:
            return self._running_tasks[task_id][0]
        return self.reader.get_task(task_id)

    def get_ready_tasks(self):
        """Returns a copy of the set of ready tasks"""
        return self._ready_tasks.copy()

    def get_task_create_events(self, parent: TaskID, start_ts: int, end_ts: int, relative: bool = False) -> List[Event]:
        children = [c for (c, _) in self.reader.get_children_created_between(parent, start_ts, end_ts)]
        return self.reader.get_task_create_events(children, reference_ts=start_ts if relative else 0)

    def schedule_task(self, task_id: TaskID) -> Tuple[Task, TaskSchedulingState, Optional[TaskSyncMode]]: 
        assert task_id in self._ready_tasks and task_id not in self._waiting_tasks
        if task_id in self._running_tasks:
            task, scheduled_state, _, sync_mode = self._continue_task(task_id)
        else:
            task, scheduled_state, _, sync_mode = self._start_task(task_id)
        self.log_debug(f"schedule state: {scheduled_state}")
        self._ready_tasks.remove(task_id)
        assert scheduled_state.is_active
        return task, scheduled_state, sync_mode

    def _start_task(self, task_id: TaskID) -> Tuple[Task, TaskSchedulingState, Optional[int], Optional[TaskSyncMode]]:
        self.log_debug(f"start task {task_id}")
        task = self.reader.get_task(task_id)
        active_states = self.reader.get_task_scheduling_states((task_id,), cond=lambda s: s.is_active)
        task_suspend_meta = self.reader.get_task_suspend_meta(task_id) + [(None, None)]
        assert len(task_suspend_meta) == len(active_states)
        task_scheduling_data = deque(zip(active_states, task_suspend_meta))
        if otter.log.is_debug_enabled():
            self.log_debug("active task states:")
            for state in active_states:
                self.log_debug(f"  {state}")
            self.log_debug("task suspend meta:")
            for (suspend_ts, mode) in task_suspend_meta:
                self.log_debug(f"  {suspend_ts}: {mode}")
        self._running_tasks[task.id] = (task, task_scheduling_data)
        next_state, (suspend_ts, next_sync_mode) = task_scheduling_data.popleft()
        if next_state.action_end == TaskAction.SUSPEND:
            assert suspend_ts == next_state.end_ts
        return task, next_state, suspend_ts, next_sync_mode

    def _continue_task(self, task_id: TaskID) -> Tuple[Task, TaskSchedulingState, Optional[int], Optional[TaskSyncMode]]:
        self.log_debug(f"continue task {task_id}")
        task, task_scheduling_data = self._running_tasks[task_id]
        next_state, (suspend_ts, next_sync_mode) = task_scheduling_data.popleft()
        assert next_state.is_active
        if next_state.action_end == TaskAction.SUSPEND:
            assert suspend_ts == next_state.end_ts
        return task, next_state, suspend_ts, next_sync_mode

    def notify_task_create(self, task: TaskID):
        self.log_debug(f"{task=} created")
        self._add_ready_task(task)
        assert task in self._ready_tasks

    def notify_task_suspend(self, task: TaskID, mode: TaskSyncMode):
        self.log_debug(f"suspend {task=}, {mode=}")
        dependencies = self.get_pending_dependencies(task, mode)
        self.log_debug(f"{dependencies=}")
        if not dependencies:
            self.log_debug(f"task {task} suspended (ready)")
            self._add_ready_task(task)
        else:
            self.log_debug(f"task {task} suspended (waiting for tasks {dependencies})")
            self._add_waiting_task(task, dependencies)
        assert task in self._ready_tasks or task in self._waiting_tasks

    def notify_task_complete(self, completed: TaskID):
        assert completed in self._running_tasks
        del self._running_tasks[completed]
        ready_tasks: List[TaskID] = []
        for waiting_task, dependencies in self._waiting_tasks.items():
            if completed in dependencies:
                self.log_debug(f"dependency satisfied for task {waiting_task}")
                dependencies.remove(completed)
            if not dependencies:
                ready_tasks.append(waiting_task)
        #! NOTE: take care to update self._waiting_tasks only AFTER we have iterated over it
        for ready_task in ready_tasks:
            self.log_debug(f"task now ready: {ready_task}")
            del self._waiting_tasks[ready_task]
            self._add_ready_task(ready_task)
        assert completed not in self._ready_tasks and completed not in self._waiting_tasks

    def task_is_pending(self, task: TaskID):
        return task in self._ready_tasks or task in self._waiting_tasks or task in self._running_tasks
    
    def task_is_ready(self, task: TaskID):
        return task in self._ready_tasks

    def get_pending_children(self, task: TaskID) -> List[TaskID]:
        return self.reader.get_related_tasks(
            task,
            relation="children",
            cond=self.task_is_pending
        )

    def get_pending_dependencies(self, task: TaskID, mode: TaskSyncMode) -> Set[TaskID]:
        if mode == TaskSyncMode.YIELD:
            return set()
        elif mode == TaskSyncMode.CHILDREN:
            return set(self.get_pending_children(task))
        elif mode == TaskSyncMode.DESCENDANTS:
            pending_children = self.get_pending_children(task)
            return set(chain(*(self.reader.get_related_tasks(t, relation="descendants") for t in pending_children)))
        else:
            raise ValueError("unhandled task sync mode")

    def filter_descendants_of(self, tasks: Set[TaskID], branch: TaskID) -> Set[TaskID]:
        return tasks.intersection(self.reader.get_related_tasks(branch, relation="descendants"))

    def _add_ready_task(self, task: TaskID):
        assert not (task in self._ready_tasks or task in self._waiting_tasks)
        self.log_debug(f"self._ready_tasks.add({task=})")
        self._ready_tasks.add(task)

    def _add_waiting_task(self, task: TaskID, dependencies: Set[TaskID]):
        assert task not in self._ready_tasks and task not in self._waiting_tasks
        self._waiting_tasks[task] = dependencies


class TaskScheduler(Loggable):

    def __init__(
        self,
        task_pool: TaskPool,
        crit_task_callback: CriticalTaskCallback,
        task_action_callback: TaskActionCallback,
        task_suspend_callback: TaskSuspendMetaCallback,
        scheduling_policy: TaskSchedulingPolicy,
        task_event_listeners: List[TaskEventListener],
        num_threads: int = 1,
        t_schedule: int = 0,
        t_pending: int = 0,
    ) -> None:
        self.log_debug("CALLBACKS:")
        self.log_debug("CALLBACKS: %s", crit_task_callback)
        self.log_debug("CALLBACKS: %s", task_action_callback)
        self.log_debug("CALLBACKS: %s", task_suspend_callback)
        self.task_pool = task_pool
        self.crit_task_callback = crit_task_callback
        self.task_action_callback = task_action_callback
        self.task_suspend_callback = task_suspend_callback
        self.task_event_listeners = task_event_listeners
        self.policy = scheduling_policy
        self.global_clock = 0

        # Each thread will send its next-available timestamp here
        self.next_available_ts = [0] * num_threads

        # The time-ordered queue of task scheduling events to be evaluated
        # Contains task-create, task-suspend and task-complete events
        self._scheduling_event_queue: Deque[Event] = deque()

        # Scheduling cost model parameters (in ns)
        # the cost of scheduling (i.e. starting or resuming) a task
        self.t_schedule = t_schedule
        # the cost per currently pending task
        self.t_pending = t_pending

        self.log_debug(f"scheduling cost model: {t_schedule=}, {t_pending=}")

    def task_created(self, task: TaskID):
        for listener in self.task_event_listeners:
            listener.notify_task_create(task)

    def task_suspended(self, task: TaskID, mode: TaskSyncMode):
        for listener in self.task_event_listeners:
            listener.notify_task_suspend(task, mode)

    def task_completed(self, task: TaskID):
        for listener in self.task_event_listeners:
            listener.notify_task_complete(task)

    def start(self):
        """
        Start the scheduler with whatever tasks are ready in the task pool
        """
        assert self.global_clock == 0
        ready_tasks = self.task_pool.get_ready_tasks()
        num_ready_tasks = len(ready_tasks)
        self.log_debug(f"=== START SCHEDULER ({num_ready_tasks} ready tasks: {ready_tasks}) ===")
        for task in map(self.task_pool.get_task, ready_tasks):
            self.task_action_callback(task.id, TaskAction.CREATE, self.global_clock, task.attr.create_location, cpu=-1, tid=0)
        for thread_id, next_avail in enumerate(self.next_available_ts):
            assert next_avail == self.global_clock
            if ready_tasks:
                task = ready_tasks.pop()
                self.log_debug(f"schedule {task=} on {thread_id=} at {self.global_clock=}")
                self.schedule_task(self.global_clock, task, thread_id, 0)

        self.log_debug("threads next available:")
        for thread, next_avail in enumerate(self.next_available_ts):
            self.log_debug(f"  thread {thread} next available: {next_avail}")
        self.dump_scheduling_events()
        # self.dump_task_statistics()
        self.dump_thread_state(self.global_clock)

    def step(self):
        """
        Process the next pending scheduling event, updating the global clock and
        notifying the task pool. Invoke the callbacks for the given event to
        decide what (if anything) to do.
        """
        self.log_debug("=== STEP SCHEDULER ===")
        if otter.log.is_debug_enabled():
            self.dump_task_statistics()
        event = self._scheduling_event_queue.popleft()
        self.global_clock = event.time
        self.log_debug(f"{self.global_clock=}")
        self.log_debug(f"{event=}")
        self.task_action_callback(event.task, event.action, self.global_clock, event.location, cpu=-1, tid=event.thread)
        if event.action == TaskAction.CREATE:
            self.task_created(event.task)
        elif event.action == TaskAction.SUSPEND:
            assert event.sync_mode is not None
            self.task_suspend_callback(event.task, self.global_clock, False, event.sync_mode)
            self.task_suspended(event.task, event.sync_mode)
        elif event.action == TaskAction.END:
            self.task_completed(event.task)
        else:
            raise ValueError(f"unkown scheduling event: {event}")

        ready_tasks = self.task_pool.get_ready_tasks()
        self.log_debug(f"ready tasks: {ready_tasks}")

        if event.action == TaskAction.END:
            selected_task = self.policy.on_task_complete(self.global_clock, event.task, event.thread, event.thread, True, ready_tasks)
            if selected_task is not None:
                assert self.task_pool.task_is_ready(selected_task)
                self.schedule_task(self.global_clock, selected_task, event.thread, 0)
        elif event.action == TaskAction.SUSPEND:
            assert event.sync_mode is not None
            dependencies = self.task_pool.get_pending_dependencies(event.task, event.sync_mode)
            selected_task = self.policy.on_task_suspend(self.global_clock, event.task, event.sync_mode, dependencies, event.thread, event.thread, True, ready_tasks)
            if selected_task is not None:
                assert self.task_pool.task_is_ready(selected_task)
                self.schedule_task(self.global_clock, selected_task, event.thread, 0)
        elif event.action == TaskAction.CREATE:
            # ask all threads what to do in turn
            self.log_debug("schedule on task-create")
            for thread, next_avail in self.iter_threads():
                self.log_debug(f"{thread=}, {next_avail=}")
                thread_available = next_avail <= self.global_clock
                selected_task = self.policy.on_task_create(self.global_clock, event.task, event.thread, thread, thread_available, ready_tasks)
                if selected_task is not None:
                    assert self.task_pool.task_is_ready(selected_task)
                    assert thread_available
                    self.schedule_task(self.global_clock, selected_task, thread, 0)
        else:
            raise ValueError(event.action)

        self.dump_scheduling_events()
        self.dump_thread_state(self.global_clock)

    def schedule_task(self, global_ts: int, task_id: TaskID, thread: int, depth: int):
        """
        TODO: does this function correctly handle both new and resumed tasks???

        Start or resume the given task on the given thread at the given global
        time. Enqueue the TSPs encountered by the scheduled task segment. Mark
        the given thread as busy until this task segmet is complete.

        Requires that the given thread is available at this time.

        Depth is the depth of this task in the task tree.
        """
        self.log_debug(f"schedule {task_id=} on {thread=} at {global_ts=}")

        ready_tasks = self.task_pool.count_ready_tasks()
        scheduling_ts = global_ts + self.t_schedule + (self.t_pending * ready_tasks)

        # tell the task pool that this task was scheduled, generating the task's tsp data
        task, scheduled_state, sync_mode = self.task_pool.schedule_task(task_id)
        assert scheduled_state.is_active

        if scheduled_state.action_end == TaskAction.SUSPEND:
            assert sync_mode is not None

        # fire the callback for this action
        self.task_action_callback(task.id, scheduled_state.action_start, scheduling_ts, scheduled_state.start_location, cpu=-1, tid=thread)

        # build the task-create events which this task will encounter
        task_create_events = self.task_pool.get_task_create_events(task.id, scheduled_state.start_ts, scheduled_state.end_ts, relative=True)
        self.log_debug(f"{task_create_events=}")

        # sanity check - relative creation timestamps should fall within duration of this part of the task
        assert all((0 <= e.time <= scheduled_state.duration for e in task_create_events))

        events: List[Event] = [
            Event(scheduling_ts + evt.time, evt.task, evt.action, evt.location, -1, thread, None)
            for evt in task_create_events
        ]
        state_end_ts = scheduling_ts + scheduled_state.duration
        if scheduled_state.action_end in [TaskAction.SUSPEND, TaskAction.END]:
            events.append(Event(state_end_ts, task.id, scheduled_state.action_end, scheduled_state.end_location, -1, thread, sync_mode))
        else:
            self.log_error(f"unhandled: {scheduled_state.action_end=}")
            assert False

        self.log_debug(f"simulated events: {events}")
        self.append_scheduling_events(events)

        # the thread is next available at the end of the scheduled state
        if task_id != self.task_pool.reader.get_root_task():
            self.set_next_available_ts(thread, state_end_ts)

    def available_threads(self, time: int):
        """Return the threads available at the given time"""
        self.log_debug(f"get threads available at {time=}")
        for thread_id, next_avail in enumerate(self.next_available_ts):
            if next_avail <= time:
                self.log_debug(f"thread {thread_id} available ({next_avail=})")
                yield thread_id

    def iter_threads(self):
        # for thread, next_avail in enumerate(self.next_available_ts):
        #     yield thread, next_avail
        return enumerate(self.next_available_ts)

    def dump_thread_state(self, time: int):
        self.log_debug(f"thread states at time {time}:")
        for thread_id, next_avail in enumerate(self.next_available_ts):
            if next_avail <= time:
                self.log_debug(f"  t{thread_id} IDLE {next_avail=}")
            else:
                self.log_debug(f"  t{thread_id} BUSY {next_avail=}")

    def dump_scheduling_events(self):
        if not otter.log.is_debug_enabled():
            return
        self.log_debug(f"{len(self._scheduling_event_queue)} scheduling events queued:")
        for evt in self._scheduling_event_queue:
            self.log_debug(f"   {evt}")

    def dump_task_statistics(self):
        ready = self.task_pool.count_ready_tasks()
        waiting = self.task_pool.count_waiting_tasks()
        running = self.task_pool.count_running_tasks()
        self.log_debug("task statistics:")
        self.log_debug(f"  ready:     {ready:>6d}")
        self.log_debug(f"  waiting:   {waiting:>6d}")
        self.log_debug(f"  running:   {running:>6d}")

    def set_next_available_ts(self, thread_id: int, time: int):
        self.next_available_ts[thread_id] = time

    def append_scheduling_events(
        self, events: List[Event]
    ):
        self.log_debug(f"append {len(events)} scheduling events")
        self._scheduling_event_queue.extend(events)
        self._scheduling_event_queue = deque(
            sorted(self._scheduling_event_queue, key=lambda evt: evt.time)
        )

    def events_pending(self):
        return len(self._scheduling_event_queue) > 0


class FIFOTaskSchedule(Loggable):

    def __init__(self) -> None:
        self.task_queue: Deque[TaskID] = deque()

    def dump_queue_state(self):
        self.log_debug("queue state:")
        self.log_debug(f" {list(self.task_queue)}")

    def get_task(self) -> Optional[TaskID]:
        if self.tasks_queued():
            task = self.task_queue.popleft()
        else:
            task = None
        self.log_debug(f"popleft {task=}")
        return task

    def iter_tasks(self):
        return iter(self.task_queue)

    def tasks_queued(self) -> int:
        return len(self.task_queue)

    def notify_task_create(self, task: TaskID):
        self.log_debug(f"append {task=}")
        self.task_queue.append(task)
        self.dump_queue_state()

    def notify_task_suspend(self, task: TaskID, mode: TaskSyncMode):
        return

    def notify_task_complete(self, completed: TaskID):
        return

    def on_task_create(self, global_ts: int, created: TaskID, encountering_thread: int, thread: int, thread_available: bool, ready_tasks: Set[TaskID]) -> Optional[TaskID]:
        if thread_available:
            selected = self.get_task()
            if selected is not None:
                assert selected in ready_tasks
                ready_tasks.remove(selected)
                self.log_debug(f"run {selected=} from queue")
                return selected
            selected = ready_tasks.pop() if ready_tasks else None
            self.log_debug(f"run {selected=} ready task")
            return selected

    def on_task_complete(self, global_ts: int, completed: TaskID, encountering_thread: int, thread: int, thread_available: bool, ready_tasks: Set[TaskID]) -> Optional[TaskID]:
        selected = self.get_task()
        if selected is not None:
            assert selected in ready_tasks
            ready_tasks.remove(selected)
            self.log_debug(f"run {selected=} from queue")
            return selected
        selected = ready_tasks.pop() if ready_tasks else None
        self.log_debug(f"run {selected=} ready task")
        return selected

    def on_task_suspend(self, global_ts: int, suspended: TaskID, mode: TaskSyncMode, dependencies: Set[TaskID], encountering_thread: int, thread: int, thread_available: bool, ready_tasks: Set[TaskID]) -> Optional[TaskID]:
        if mode == TaskSyncMode.YIELD:
            ready_tasks.remove(suspended)
            self.log_debug(f"resume {suspended=} due to yield")
            return suspended
        ready_dependencies = dependencies & ready_tasks
        if ready_dependencies:
            self.log_debug(f"choose from {ready_dependencies=}")
            self.dump_queue_state()
            selected = next(filter(lambda t: t in ready_dependencies, self.iter_tasks()), None)
            if selected is not None:
                self.task_queue.remove(selected)
            else:
                selected = ready_dependencies.pop()
            self.log_debug(f"chose {selected=}")
            return selected
        if suspended in ready_tasks:
            ready_tasks.remove(suspended)
            self.log_debug(f"resume {suspended=} as ready")
            return suspended
        selected = self.get_task()
        if selected is not None:
            assert selected in ready_tasks
            ready_tasks.remove(selected)
            self.log_debug(f"run {selected=} from queue")
            return selected
        selected = ready_tasks.pop() if ready_tasks else None
        self.log_debug(f"run {selected=} ready task")
        return selected


def simulate_finite(
    reader: ReadConnection,
    crit_task_callback: CriticalTaskCallback,
    task_action_callback: TaskActionCallback,
    task_suspend_callback: TaskSuspendMetaCallback,
    num_threads: int = 1,
    t_schedule: int = 0,
    t_pending: int = 0,
):
    action_count = defaultdict(int)
    def count_action(task: TaskID,
        action: TaskAction,
        time: int,
        source_location: SourceLocation,
        /,
        *,
        location_ref: Optional[int] = None,
        location_count: Optional[int] = None,
        cpu: int,
        tid: int):
        action_count[action] += 1
        return task_action_callback(task, action, time, source_location, location_ref=location_ref, location_count=location_count, cpu=cpu, tid=tid)

    otter.log.info("run finite simulator")
    otter.log.info(f"{reader}")
    task_pool = TaskPool(reader)
    fifo = FIFOTaskSchedule()
    scheduler = TaskScheduler(
        task_pool,
        crit_task_callback,
        count_action,
        task_suspend_callback,
        scheduling_policy=fifo,
        task_event_listeners=[task_pool, fifo],
        num_threads=num_threads,
        t_schedule=t_schedule,
        t_pending=t_pending,
    )
    scheduler.start()
    while scheduler.events_pending():
        scheduler.step()
    fifo.dump_queue_state()
    scheduler.dump_scheduling_events()
    scheduler.dump_task_statistics()

    assert scheduler.task_pool.count_ready_tasks() == 0
    assert scheduler.task_pool.count_waiting_tasks() == 0
    assert scheduler.task_pool.count_running_tasks() == 0

    print("ACTIONS:")
    for key, value in sorted(action_count.items(), key=lambda item: item[0].value):
        print(f"    {str(key):<26s} {value:>1d}")
