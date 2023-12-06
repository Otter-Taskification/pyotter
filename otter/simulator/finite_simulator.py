import argparse
from collections import deque
from contextlib import ExitStack
from enum import Enum, auto
from typing import Deque, Dict, Iterator, List, Literal, Optional, Set, Tuple, Union

import otf2_ext

import otter
from otter import log
from otter.core.chunk_reader import ChunkReaderProtocol, DBChunkReader
from otter.core.events import Event
from otter.definitions import EventType


class TSP(Enum):
    """Represents the set of modelled task-scheduling points"""

    CREATE = auto()
    SUSPEND = auto()
    COMPLETE = auto()


TaskSchedulingPoint = Union[
    Tuple[
        Literal[TSP.CREATE], int, int, int, int
    ],  # (TSP.CREATE, time, thread, task, child_task)
    Tuple[Literal[TSP.SUSPEND], int, int, int],  # (TSP.SUSPEND, time, thread, task)
    Tuple[
        Literal[TSP.COMPLETE], int, int, int, int
    ],  # (TSP.COMPLETE, time, thread, task, parent_task)
]

"""
Each TSP occurs as part of a segment of task. These segments start with either
a task-enter or task-resume event and end at the very next task-suspend or task-leave
event.

For example, this task chunk:

task-enter
task-create
task-sync-enter
task-sync-leave
task-create
task-create
task-create
task-sync-enter
task-sync-leave
task-leave

Has these task segments:

1:
    task-enter
    task-create
    task-sync-enter

2:
    task-sync-leave
    task-create
    task-create
    task-create
    task-sync-enter

3:
    task-sync-leave
    task-leave

Each TSP occurs at a known duration into the task segment.
"""


class TaskPool:
    """Encapsulates the connection to the tasks database, responsible for
    traversing the database to spawn tasks

    If no set of initial tasks waiting to be scheduled is given, take these from
    `con`.

    When a task is scheduled, generates data for the task-scheduling points
    the task encounters up to & including a task-suspend or task-complete TSP.
    The timestamps of these TSPs are given as offsets from the task start ts.
    """

    def __init__(
        self,
        con: otter.db.Connection,
        chunk_reader: ChunkReaderProtocol,
        initial_tasks: Optional[Set[int]] = None,
    ) -> None:
        self.debug = log.log_with_prefix("[TaskPool]", log.debug)

        self.con = con
        self.chunk_reader = chunk_reader

        # The set of tasks ready to be scheduled on a thread i.e. with no outstanding dependencies
        self._ready_tasks = initial_tasks or set(con.root_tasks())

        # The set of suspended tasks with a count of outstanding dependencies
        self._waiting_tasks: Dict[int, int] = {}

        # The set of currently running tasks - used to track where we're up to
        # Tasks are added here the first time they are scheduled
        # Tasks remain in here until they are completed at which point they are removed
        # Tasks in here may be either scheduled or waiting.
        # Tasks in here may not also be in self._ready_tasks.
        self._running_tasks: Dict[int, Tuple[int, int, str, str, Iterator[Event]]] = {}

        #! Note: the set of all tasks currently suspended is self._ready_tasks + self._waiting_tasks

    def get_ready_tasks(self):
        """Returns a copy of the set of ready tasks"""
        return self._ready_tasks.copy()

    def count_ready_tasks(self):
        """Count tasks ready and yet to be scheduled"""
        return len(self._ready_tasks)

    def count_waiting_tasks(self):
        """Count suspended tasks waiting for dependencies"""
        return len(self._waiting_tasks)

    def count_running_tasks(self):
        """Count running tasks, which may be either scheduled or waiting"""
        return len(self._running_tasks)

    def schedule_task(self, task: int):
        """
        Schedule a task from the set of ready tasks. This requires that the
        given task is not waiting. It may be in the set of running tasks if it
        was previously scheduled and then suspended e.g. at a taskwait

        Remove the given task from the set of ready tasks.

        Return data sufficient to construct the TSPs that result from scheduling
        this task (& in particular this task chunk)

        Algorithm:

        - if not in running tasks:
            - store pending TSPs in self._running_tasks
        - return the next set of TSPs pending for this task
        """

        assert task in self._ready_tasks and task not in self._waiting_tasks

        if task in self._running_tasks:
            self.debug(f"resume task {task}")
        else:
            self.debug(f"schedule new task {task}")

        self._ready_tasks.remove(task)

        task, parent, num_children, start_ts, end_ts, attr = self.con.task_attributes(
            task
        )[0]

        chunk = self.chunk_reader.get_chunk(task)

        if task not in self._running_tasks:
            self._running_tasks[task] = (
                parent,
                num_children,
                start_ts,
                end_ts,
                iter(chunk.events),
            )

        return self._running_tasks[task]

    def count_outstanding_children(self, task: int):
        """
        Return the number of outstanding previously-created children of this task.
        """
        pending_children = sum(
            1
            for child in self.con.children_of(task)
            if (
                child in self._ready_tasks
                or child in self._waiting_tasks
                or child in self._running_tasks
            )
        )
        return pending_children

    def notify_task_create(self, task: int):
        """
        Notify that a task was created
        """
        self.debug(f"task {task} created")
        self._add_ready_task(task)
        assert task in self._ready_tasks

    def notify_task_complete(self, task: int, parent_task: int):
        """
        Notify a scheduled task that it was completed and notify any waiting parent
        """
        if task in self._running_tasks:
            del self._running_tasks[task]
        if parent_task in self._waiting_tasks:
            self._waiting_tasks[parent_task] -= 1
            if self._waiting_tasks[parent_task] == 0:
                del self._waiting_tasks[parent_task]
                self._add_ready_task(parent_task)
        assert not (task in self._ready_tasks or task in self._waiting_tasks)

    def notify_task_suspend(self, task: int):
        """
        Notify a scheduled task that it is suspended i.e. waiting for any
        outstanding dependencies
        """
        deps = self.count_outstanding_children(task)
        if deps == 0:
            self.debug(f"task {task} suspended (ready)")
            self._add_ready_task(task)
        else:
            self.debug(f"task {task} suspended (waiting for {deps} tasks)")
            self._add_waiting_task(task, deps)
        assert task in self._ready_tasks or task in self._waiting_tasks

    def _add_ready_task(self, task: int):
        """Add a task to the pool of tasks ready to be scheduled"""
        assert not (task in self._ready_tasks or task in self._waiting_tasks)
        self._ready_tasks.add(task)

    def _add_waiting_task(self, task: int, num_dependencies: int):
        """Record that this task is not available to be scheduled until its
        outstanding dependencies are met"""
        assert not (task in self._ready_tasks or task in self._waiting_tasks)
        self._waiting_tasks[task] = num_dependencies


class ThreadAgent:
    """A thread which can request and execute work"""

    def __init__(self, thread_id: int, scheduler: "TaskScheduler") -> None:
        self.id = thread_id
        self.scheduler = scheduler
        self._current_task_duration = 0
        self._current_task_start_ts = 0

    def activate(self):
        """Activate this thread. If not busy, request work from the scheduler.
        Consume the task for a given duration. Note the time at which the thread
        is next available to be activated."""
        print(f"[thread={self.id}] thread activated")

    def notify_next_available_ts(self):
        self.scheduler.set_next_available_ts(
            self.id, self._current_task_start_ts + self._current_task_duration
        )


class TaskScheduler:
    """Manages the task pool.

    Resolves TSPs in the global order in which they are spawned by the scheduled
    tasks. At each TSP, update the global time and fire the requisite handler
    for the type of TSP. Add any new TSPs to the queue and maintain the correct
    TSP order.

    At a given TSP, each available thread has the opportunity to make a task-
    scheduling decision.

    Possible TSPs are:
        - CREATE
        - SUSPEND
        - COMPLETE

    Possible choices are:
        - schedule a new tied/untied task
        - resume a suspended tied/untied task

    The choice made must satisfy the task-scheduling constraints for the particular
    thread
    """

    class Action(Enum):
        """Represents the actions that can be taken at a task-scheduling point"""

        START = auto()  # start a new task to execute on a thread
        RESUME = auto()  # resume a previously suspended task
        NONE = auto()  # take no action

    def __init__(
        self, task_pool: TaskPool, num_threads: int = 1, global_clock: int = 0
    ) -> None:
        self.debug = log.log_with_prefix(f"[Sched]", log.debug)

        self.task_pool = task_pool
        self.global_clock = global_clock

        # Each thread will send its next-available timestamp here
        self.next_available_ts = [0] * num_threads

        # The time-ordered list of task-scheduling points to be evaluated
        self._task_scheduling_points: Deque[TaskSchedulingPoint] = deque()

        # Map thread num to a set of tasks tied to that thread.
        self._tied_tasks = [set() for _ in range(num_threads)]

        self.threads = [ThreadAgent(n, self) for n in range(num_threads)]

    def start(self):
        """
        Start the scheduler with whatever tasks are ready in the task pool
        """
        assert self.global_clock == 0
        self.debug("* * * S T A R T * * *")
        ready_tasks = self.task_pool.get_ready_tasks()
        num_ready_tasks = len(ready_tasks)
        self.debug(f"starting scheduler: {num_ready_tasks} tasks ready ({ready_tasks})")
        for thread_id, next_avail in enumerate(self.next_available_ts):
            assert next_avail == 0
            if ready_tasks:
                task = ready_tasks.pop()
                self.debug(f"start: schedule {task=} on {thread_id=}")
                self.schedule_task(self.global_clock, task, thread_id)

        for thread, next_avail in enumerate(self.next_available_ts):
            self.debug(f"thread {thread} next available: {next_avail}")

        self.dump_task_scheduling_points()
        self.dump_task_statistics()
        self.dump_thread_state(self.global_clock)

        self.debug("started")

    def step(self):
        """
        Process the next task-scheduling point in the queue, updating the global
        clock and notifying the task pool. Invoke the callbacks for the given
        type of task-scheduling point to decide what (if anything) to do.
        """
        self.debug("* * * S T E P * * *")
        mode, *data = self._task_scheduling_points.popleft()
        self.global_clock = data[0]
        self.debug(f"global clock advanced: {self.global_clock=}")
        self.debug(f"got tsp: {mode=}, {data=}")
        if mode == TSP.CREATE:
            assert len(data) == 4
            global_ts, thread, task, child_task = data
            self.task_pool.notify_task_create(child_task)
        elif mode == TSP.SUSPEND:
            assert len(data) == 3
            global_ts, thread, task = data
            self.task_pool.notify_task_suspend(task)
        elif mode == TSP.COMPLETE:
            assert len(data) == 4
            global_ts, thread, task, parent_task = data
            self.task_pool.notify_task_complete(task, parent_task)
            self._tied_tasks[thread].remove(task)
        else:
            raise ValueError(f"unkown task scheduling point: {mode=}, {data=}")

        # After handling TSP:

        # For each available thread:
        #   - determine tasks eligible to be scheduled on this thread
        #   - call a handler to decide what to do (start/resume/do nothing)
        #   - if a task was scheduled (either started/resumed) enqueue the generated TSPs
        #   - remove any scheduled task from the set of ready tasks
        #   - ensure TSPs remain sorted in temporal order

        self.debug(f"ready tasks: {self.task_pool.get_ready_tasks()}")

        for thread_id in self.available_threads(self.global_clock):
            self.debug(f"call TSP handler for {mode=} at {global_ts=}, {thread_id=}")

        self.dump_task_scheduling_points()
        self.dump_task_statistics()
        self.dump_thread_state(self.global_clock)

    def schedule_task(self, global_ts: int, task: int, thread: int):
        """
        Start or resume the given task on the given thread at the given global
        time. Enqueue the TSPs encountered by the scheduled task segment. Mark
        the given thread as busy until this task segmet is complete.

        Requires that the given thread is available at this time.
        """
        self.debug(f"schedule {task=} on {thread=} at {global_ts=}")

        # tell the task pool that this task was scheduled, to generate the task's
        # tsp data
        tsp_data = self.task_pool.schedule_task(task)
        parent, num_children, start_ts, end_ts, event_iter = tsp_data
        first_event = next(event_iter)

        # the event types which are valid at the start of a new task section
        assert first_event.event_type in [EventType.task_enter, EventType.sync_end]

        # the native start ts of this part of the task
        schedule_start_ts = int(first_event.time)

        # build the task-scheduling points which this task will encounter
        task_scheduling_points: List[TaskSchedulingPoint] = []
        while True:
            event = next(event_iter)
            assert event.encountering_task_id == task

            # the offset of this event into this part of the tasks
            event_dt = int(event.time) - schedule_start_ts

            # the simulated global time at which this tsp will be encountered
            event_ts = global_ts + event_dt

            if event.event_type == EventType.task_create:
                task_scheduling_points.append(
                    (
                        TSP.CREATE,
                        event_ts,
                        thread,
                        task,
                        event.unique_id,
                    )
                )
            elif event.event_type == EventType.sync_begin:
                task_scheduling_points.append((TSP.SUSPEND, event_ts, thread, task))
                break
            elif event.event_type == EventType.task_leave:
                task_scheduling_points.append(
                    (TSP.COMPLETE, event_ts, thread, task, parent)
                )
                break

        if log.is_debug_enabled():
            self.debug(f" -- {num_children=}")
            self.debug(f" -- {start_ts=}")
            self.debug(f" -- {end_ts=}")
            self.debug(f" -- task-scheduling points from this task:")
            for tsp in task_scheduling_points:
                self.debug(f" ---- {tsp=}]")

        # the thread is next available when it encounters the last tsp in this part of the task
        self.set_next_available_ts(thread, event_ts)

        # all tasks are assumed to be tied
        self.tie_task_to_thread(task, thread)

        self.append_task_scheduling_points(task_scheduling_points)

    def append_task_scheduling_points(
        self, task_scheduling_points: List[TaskSchedulingPoint]
    ):
        self.debug(f"append {len(task_scheduling_points)} task-scheduling points")
        self._task_scheduling_points.extend(task_scheduling_points)
        self._task_scheduling_points = deque(
            sorted(self._task_scheduling_points, key=lambda tsp: tsp[1])
        )
        self.debug(f"{len(task_scheduling_points)} task-scheduling points pending")

    def task_scheduling_points_pending(self):
        return len(self._task_scheduling_points) > 0

    def set_next_available_ts(self, thread_id: int, time: int):
        self.next_available_ts[thread_id] = time

    def available_threads(self, time: int):
        """Return the threads available at the given time"""
        self.debug(f"get threads available at {time=}")
        for thread_id, next_avail in enumerate(self.next_available_ts):
            if next_avail <= time:
                self.debug(f"thread {thread_id} available at {time} {next_avail=}")
                yield thread_id
            else:
                self.debug(f"thread {thread_id} next available at {next_avail}")

    def tie_task_to_thread(self, task: int, thread: int):
        """
        Record that a task is tied to a thread. The task must not already be tied
        to any other thread.
        """
        assert task not in self._tied_tasks[thread]
        self._tied_tasks[thread].add(task)
        self.debug(f"task {task} tied to thread {thread}")

    def dump_task_scheduling_points(self):
        if log.is_debug_enabled():
            self.debug(f"TSPs outstanding at global time {self.global_clock}:")
            for tsp in self._task_scheduling_points:
                self.debug(f"  {tsp}")

    def dump_task_statistics(self):
        if log.is_debug_enabled():
            ready = self.task_pool.count_ready_tasks()
            waiting = self.task_pool.count_waiting_tasks()
            running = self.task_pool.count_running_tasks()
            self.debug("task statistics:")
            self.debug(f"  ready:     {ready:>6d}")
            self.debug(f"  waiting:   {waiting:>6d}")
            self.debug(f"  running:   {running:>6d}")

    def dump_thread_state(self, time: int):
        if log.is_debug_enabled():
            self.debug("thread states at time {time}:")
            for thread_id, next_avail in enumerate(self.next_available_ts):
                if next_avail <= time:
                    self.debug(f"  t{thread_id} IDLE {next_avail=}")
                else:
                    self.debug(f"  t{thread_id} BUSY {next_avail=}")


class Model:
    """Creates the scheduler with the given number of threads"""

    def __init__(self, task_pool: TaskPool, num_threads: int = 1) -> None:
        self.scheduler = TaskScheduler(task_pool, num_threads)

    def run(self, max_steps: Optional[int] = None):
        initial_tasks = self.scheduler.task_pool.get_ready_tasks()
        print(f"task pool contains {len(initial_tasks)} initial tasks: {initial_tasks}")
        steps = 0
        self.scheduler.start()
        while self.scheduler.task_scheduling_points_pending():
            self.scheduler.step()
            steps += 1
            if max_steps is not None and steps > max_steps:
                break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("anchorfile", help="the Otter OTF2 anchorfile to use")
    otter.args.add_common_arguments(parser)
    args = parser.parse_args()
    otter.log.initialise(args)
    project = otter.project.BuildGraphFromDB(args.anchorfile)
    print(f"simulating trace {args.anchorfile}")
    with ExitStack() as ctx:
        con = ctx.enter_context(project.connection())
        reader = ctx.enter_context(otf2_ext.open_trace(args.anchorfile))
        seek_events = ctx.enter_context(reader.seek_events())
        chunk_reader = DBChunkReader(reader.attributes, seek_events, con)
        model = Model(TaskPool(con, chunk_reader), num_threads=4)
        model.run(max_steps=3)
