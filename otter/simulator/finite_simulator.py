import argparse
from typing import Optional, Set

import otter


class TaskPool:
    """Encapsulates the connection to the tasks database, responsible for
    traversing the database to spawn tasks"""

    def __init__(
        self, con: otter.db.Connection, initial_tasks: Optional[Set[int]] = None
    ) -> None:
        self.con = con
        self._ready_tasks = initial_tasks or set(con.root_tasks())

    def get_ready_tasks(self):
        return self._ready_tasks.copy()

    def tasks_pending(self):
        return bool(self._ready_tasks)


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
    """Manages the task pool, and activates threads so they can request work."""

    def __init__(
        self, task_pool: TaskPool, num_threads: int = 1, global_clock: int = 0
    ) -> None:
        self.task_pool = task_pool
        self.global_clock = global_clock

        # Each thread will send its next-available timestamp here
        self.next_available_ts = [0] * num_threads

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

        2 ways to process the effects of scheduling a task on a thread:
            1. immediately add effects to a queue of some sort
            2. resolve effects when task next suspended/completed.
        """
        next_ts = min(self.next_available_ts)
        print(f"{next_ts=}")
        self.global_clock = next_ts
        for thread in self.threads:
            thread.activate()

    def tasks_pending(self):
        return self.task_pool.tasks_pending()

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
        while self.scheduler.tasks_pending():
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
