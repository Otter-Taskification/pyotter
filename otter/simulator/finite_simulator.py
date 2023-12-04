import argparse
from collections import deque
from enum import Enum, auto
from typing import Deque, Literal, Optional, Set, Tuple, Union

import otter
from otter import log


class TSP(Enum):
    """Represents the set of modelled task-scheduling points"""

    CREATE = auto()
    SUSPEND = auto()
    COMPLETE = auto()


TaskSchedulingPoint = Union[
    Tuple[Literal[TSP.CREATE], int, int, int, int],
    Tuple[Literal[TSP.SUSPEND], int, int, int],
    Tuple[Literal[TSP.COMPLETE], int, int, int],
]  # (TSP.CREATE, time, thread, task, child_task) | (TSP, time, thread, task)


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
        self, con: otter.db.Connection, initial_tasks: Optional[Set[int]] = None
    ) -> None:
        self.con = con

        # The set of tasks ready to be scheduled on a thread i.e. with no outstanding dependencies
        self._ready_tasks = initial_tasks or set(con.root_tasks())

        # The set of suspended tasks with a count of outstanding dependencies
        self._waiting_tasks = {}

        # The set of currently running tasks - used to track which task chunk we're up to
        # Tasks in here are those which have multiple chunks to schedule (because they contain at least 1 task-sync point)
        # Tasks are added here the first time they are scheduled
        # Tasks remain in here until they are completed at which point they are removed
        self._running_tasks = {}

        #! Note: the set of all tasks currently suspended is self._ready_tasks + self._waiting_tasks

    def get_ready_tasks(self):
        return self._ready_tasks.copy()

    def tasks_pending(self):
        return bool(self._ready_tasks)

    def schedule_new_task(self, task: int):
        """Start a new task. This requires that the task is in the set of ready
        tasks and is not in the set of running tasks. Raise an error if this isn't
        true.

        Remove the given task from the set of ready tasks

        Return data sufficient to construct the TSPs that result from scheduling
        this task (& in particular this task chunk)
        """

        msg = None
        if task in self._running_tasks:
            msg = f"task {task} already in set of running tasks"
        elif task not in self._ready_tasks:
            msg = f"task {task} not in set of ready tasks"

        if msg:
            log.error(msg)
            raise ValueError(msg)

        self._ready_tasks.remove(task)

        task, parent, num_children, start_ts, end_ts, attr = self.con.task_attributes(
            task
        )[0]

        if num_children == 0:
            # If the given task is a leaf task, the only TSP data is from the task-complete event
            tsp_data = (TSP.COMPLETE, int(start_ts), int(end_ts))
        else:
            # get the list of task sync groups i.e. taskwait constructs encountered and the created tasks
            # this list is possibly terminated by a batch of unsynchronised tasks
            task_sync_groups = list(self.con.task_synchronisation_groups(task))

            # get the list of times (if any) at which this task was suspended & resumed
            # a pair of timestamps is given here for each taskwait construct encountered
            _, task_suspend_times = next(self.con.task_suspend_ts((task,)))

            if len(task_suspend_times) < len(task_sync_groups):
                msg = f"{task=} fewer task suspend times than task sync groups"
                log.error(msg)
                raise ValueError(msg)

            if len(task_sync_groups) == len(task_suspend_times):
                log.debug(
                    f"{task=}: number of task sync groups matches number of suspend/resume timestamps"
                )
            elif len(task_sync_groups) == 1 + len(task_suspend_times):
                log.debug(
                    f"{task=}: one more task sync groups than suspend/resume timestamps"
                )

            # construct the list of times this task is started/suspended and the tasks created during those times
            # make sure the last element indicates the end of the task
            data_to_generate_TSPs = []
            chunk_start_ts = int(start_ts)
            for (sequence, rows, sync_ts, sync_descendants), (suspend, resume) in zip(
                task_sync_groups, task_suspend_times
            ):
                assert (
                    sync_ts == suspend
                )  # the sync timestamp should also be the time the task was suspended
                created_tasks = tuple(
                    (int(r["child_id"]), int(r["child_crt_ts"])) for r in rows
                )
                log.debug(f"{task=}: {sequence=} start={chunk_start_ts} end={suspend}")

                # Record the star of the subeqeuent chunk
                chunk_start_ts = resume

            log.debug(f"{task=}: last chunk start={chunk_start_ts} end={int(end_ts)}")

            if len(task_sync_groups) == 1 + len(task_suspend_times):
                log.debug(
                    "there is a final group of un-synchronised tasks to handle!!!"
                )

            # get the first batch of TSP data
            sequence, rows, sync_ts, sync_descendants = next(self._running_tasks[task])
            if sequence is None:
                log.warning(f"sequence was None for {task=}")
            created_tasks = tuple(
                (int(r["child_id"]), int(r["child_crt_ts"])) for r in rows
            )
            tsp_data = (
                TSP.SUSPEND,
                int(start_ts),  # the time this task started
                int(sync_ts),  # the time this task chunk is finished
                created_tasks,  # the generated children
                sync_descendants,  # whether descendants are also synchronised
            )

        return tsp_data

    def resume_suspended_task(self, task: int):
        """Resume a previously suspended task. This requires that the task is
        in the set of ready tasks and also in the set of running tasks.

        !!!
        NOTE: the data model needs more thought for this. I can't easily get the last "chunk" after the final time a task is resumed, since there are no further tasks created and no further synchronisation!
        !!!
        """

        msg = None
        if not (task in self._running_tasks and task in self._ready_tasks):
            msg = f"task {task} not in set of running tasks"

        if msg:
            log.error(msg)
            raise ValueError(msg)

    def suspend_task(self, task: int, ndeps: Optional[int] = 0):
        ...


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
        self.task_pool = task_pool
        self.global_clock = global_clock

        # Each thread will send its next-available timestamp here
        self.next_available_ts = [0] * num_threads

        # The time-ordered list of task-scheduling points to be evaluated
        self._task_scheduling_points: Deque[TaskSchedulingPoint] = deque()

        self.threads = [ThreadAgent(n, self) for n in range(num_threads)]

    def step(self):
        """
        Get the time each thread is next available.
        Advance the global clock to the minimum such time.
        Activate all threads now available.

        What are the possible effects of scheduling a task on a thread?
            - Child tasks are spawned, and may be synchronised at a barrier.
                (tasks can have distinct creation & execution times!)
            - The thread is busy for the duration of the task i.e. unavailable
                for scheduling another task.

        When a task (chunk) is scheduled on a thread we can immediately get:
            - the crt_ts of tasks created during that task chunk
            - the id of tasks created during that task chunk
            - the duration of that task chunk i.e. the time the thread is busy
            - whether a task-sync occurs at the end of this task chunk

        When are the possible times we could then make a scheduling decision?
            - at task-create (i.e. could choose to schedule immediately on an available thread, or suspend the current task and schedule immediately on the current thread, or just add to task pool)
            - at task-sync-enter i.e. when a task is suspended (i.e. can choose to take any available task)
            - at task-complete i.e. when a task is completed (i.e. must notify task pool that the task is completed, and can then choose any available task)

        ## OpenMP 5.1 sec. 2.12.6 Task Scheduling:

        At a task-scheduling point, any of the following are permitted by OpenMP (subject to certain constraints):
            - start a new (tied or untied) task
            - resume any suspended (tied or untied) task

        Those constraints are:
            1. Scheduling of new tied tasks is constrained by the set of {task regions currently tied to the thread and not suspended in a barrier region}.
            - If there are any {task regions currently tied to the thread and not suspended in a barrier region}:
                - a new tied task may be scheduled on this thread only if it is a descendant of every task in the set.
            - Otherwise:
                - any new tied task may be scheduled on this thread.

            ==> this constraint basically enforces a depth-first scheduling of descendant tied tasks!!

            2. Task dependences must be satisfied

            3. Can't overlap mutually exclusive tasks

            4. Tasks with a false `if` clause are executed immediately after task-creation

        2 ways to process the effects of scheduling a task on a thread:
            1. immediately add effects to a queue of some sort
            2. resolve effects when task next suspended/completed.

        ==> Most sensible approach seems to be to replay the task-scheduling points that would be encountered in native execution, in order, making sure the set of ready/suspended tasks is kept up to date between TSPs.

        """
        mode, *data = self._task_scheduling_points.popleft()
        if mode == TSP.CREATE:
            # handle task-create TSP
            assert len(data) == 4
            global_ts, thread, task, child = data
        elif mode == TSP.SUSPEND:
            # handle task-suspend TSP i.e. encountered a taskwait
            assert len(data) == 3
            global_ts, thread, task = data
        elif mode == TSP.COMPLETE:
            # handle task-complete TSP
            assert len(data) == 3
            global_ts, thread, task = data
        else:
            raise ValueError(f"unkown task scheduling point: {mode=}, {data=}")

        # Ensure TSPs remain sorted in temporal order
        self._task_scheduling_points = deque(
            sorted(self._task_scheduling_points, key=lambda tsp: tsp[1])
        )

    def tasks_pending(self):
        return self.task_pool.tasks_pending()

    def task_scheduling_points_pending(self):
        return len(self._task_scheduling_points) > 0

    def set_next_available_ts(self, thread_id: int, time: int):
        self.next_available_ts[thread_id] = time


class Model:
    """Creates the scheduler with the given number of threads"""

    def __init__(self, task_pool: TaskPool, num_threads: int = 1) -> None:
        self.scheduler = TaskScheduler(task_pool, num_threads)

    def run(self, max_steps: Optional[int] = None):
        initial_tasks = self.scheduler.task_pool.get_ready_tasks()
        print(f"task pool contains {len(initial_tasks)} initial tasks: {initial_tasks}")
        steps = 0
        while self.scheduler.task_scheduling_points_pending():
            print("[STEP]")
            self.scheduler.step()
            steps = steps + 1
            if max_steps is not None and steps > max_steps:
                break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("anchorfile", help="the Otter OTF2 anchorfile to use")
    otter.args.add_common_arguments(parser)
    args = parser.parse_args()
    otter.log.initialise(args)
    project = otter.project.BuildGraphFromDB(args.anchorfile)
    with project.connection() as con:
        print(f"simulating trace {args.anchorfile}")
        model = Model(TaskPool(con), num_threads=4)
        model.run(max_steps=3)
