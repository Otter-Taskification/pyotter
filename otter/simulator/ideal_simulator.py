from typing import Optional, Sequence, Protocol

import otter.log
import otter.db


class ScheduleWriterProtocol(Protocol):

    def insert(self, task: int, start_ts: int, duration: int, /, *args): ...


class CriticalTaskWriterProtocol(Protocol):

    def insert(self, task: int, sequence: int, critical_child: int, /, *args): ...


class TaskScheduler:

    def __init__(
        self,
        con: otter.db.Connection,
        schedule_writer: ScheduleWriterProtocol,
        crit_task_writer: CriticalTaskWriterProtocol,
        initial_tasks: Optional[Sequence[int]] = None,
    ) -> None:
        self.con = con
        self.schedule_writer = schedule_writer
        self.crit_task_writer = crit_task_writer
        self._root_tasks = initial_tasks or con.root_tasks()
        otter.log.debug("found %d root tasks", len(self._root_tasks))

    def run(self) -> None:
        max_root_task_dt = 0
        global_ts = 0
        root_task_attributes = self.con.task_attributes(self._root_tasks)
        otter.log.info("simulate %d tasks", self.con.num_tasks())
        for task, *_, create_ts, start_ts, end_ts, attr in root_task_attributes:
            print(f"Simulate root task {task}")
            print(f"    Start: {attr.start_location}")
            print(f"    End:   {attr.end_location}")
            duration_observed = int(end_ts) - int(start_ts)
            duration = self.descend(
                task,
                start_ts,
                end_ts,
                0,
                global_ts,
            )
            speedup = duration_observed / duration
            max_root_task_dt = max(max_root_task_dt, duration)
            print("    Duration:")
            print(f"      observed:  {duration_observed:>12d}")
            print(f"      simulated: {duration:>12d}")
            print(f"      speedup:   {speedup:>12.2f}")
            print(f"      relative:  {1/speedup:12.2%}")

    def descend(
        self, task: int, start_ts: str, end_ts: str, depth: int, global_start_ts: int
    ):
        """Descend into the children of task. At the root, handle leaf tasks"""
        if self.con.num_children(task) > 0:
            duration = self.branch_task(task, start_ts, end_ts, depth, global_start_ts)
        else:
            duration = self.leaf_task(task, start_ts, end_ts, depth, global_start_ts)
        self.schedule_writer.insert(task, global_start_ts, duration)
        return duration

    def branch_task(
        self,
        task: int,
        start_ts: str,
        end_ts: str,
        depth: int,
        global_start_ts: int,
    ):
        """Returns duration elapsed from task-start to task-end, including time spent
        waiting for children/descendants, but not including the duration of any
        children which are not synchronised

        Think of this as the "taskwait-inclusive duration", which is not the same as
        the "inclusive duration" i.e. this task + all descendants.add

        The taskwait-inclusive duration is composed of two parts:
        - time spent executing the task itself (recorded in the trace)
        - time in which the task is suspended at a barrier (but modelled for an
        infinite machine)

        In our idealised infinite scheduler, we get execution time directly from
        the trace (start_ts, suspended_ts, resume_ts and end_ts).

        However, the suspended duration as recorded depends on the number of tasks
        that could be executed in parallel. For our infinite machine, this is
        unlimited so the idealised suspended duration should just be the maximum
        "taskwait-inclusive duration" among the children synchronised by a barrier
        """

        (_, execution_native_dt, _), *_ = self.con.time_active((task,))

        #! NOTE: could improve the SQL behind this as there's a lot of duplicated info returned when getting task sync groups.
        #! could just return 1 row per sequence with the relevant times/durations, then 1 table per sequence with the synchronised child task IDs
        suspended_ideal_dt = 0  # idealised suspended duration
        sync_groups = list(self.con.task_synchronisation_groups(task))
        for (
            sequence,
            rows,
            sync_start_ts,
            sync_descendants,
            chunk_duration,
        ) in sync_groups:
            child_crt_dt = [(r["child_id"], r["child_crt_dt"]) for r in rows]
            sync_children_attr = self.con.task_attributes([r["child_id"] for r in rows])
            barrier_duration = 0
            critical_task = None
            for (child, *_, create_ts, start_ts, end_ts, _), (
                _child,
                crt_dt,
            ) in zip(sync_children_attr, child_crt_dt, strict=True):
                assert child == _child
                child_duration = self.descend(
                    child, start_ts, end_ts, depth + 1, global_start_ts + crt_dt
                )
                duration_into_barrier = child_duration - (
                    int(sync_start_ts) - int(create_ts)
                )
                if duration_into_barrier > barrier_duration:
                    barrier_duration = duration_into_barrier
                    critical_task = child
            if critical_task is not None:
                self.crit_task_writer.insert(task, sequence, critical_task)
            suspended_ideal_dt = suspended_ideal_dt + barrier_duration
            global_start_ts = global_start_ts + chunk_duration + barrier_duration

        taskwait_inclusive_dt = execution_native_dt + suspended_ideal_dt

        return taskwait_inclusive_dt

    def leaf_task(
        self,
        task: int,
        start_ts: str,
        end_ts: str,
        depth: int,
        global_start_ts: int,
    ):
        # Returns the duration of a leaf task. Assumes a leaf task is executed in one go i.e. never suspended.
        pre = "+" * depth
        start = int(start_ts)
        end = int(end_ts)
        duration = end - start
        return duration


def simulate_ideal(con: otter.db.Connection):
    schedule_writer = otter.db.ScheduleWriter(con)
    crit_task_writer = otter.db.CritTaskWriter(con)
    con.on_close(schedule_writer.close)
    con.on_close(crit_task_writer.close)
    scheduler = TaskScheduler(con, schedule_writer, crit_task_writer)
    scheduler.run()
