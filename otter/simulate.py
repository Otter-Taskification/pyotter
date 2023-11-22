import argparse
import sys
from typing import Tuple

import otter


def descend(
    con: otter.db.Connection,
    task: int,
    start_ts: str,
    end_ts: str,
    task_attr: otter.TaskAttributes,
    depth: int,
):
    """Descend into the children of task. At the root, handle leaf tasks"""
    children = con.children_of(task)
    if children:
        duration = branch_task(con, task, start_ts, end_ts, children, depth)
    else:
        duration = leaf_task(task, start_ts, end_ts, task_attr, depth)
    return duration


def branch_task(
    con: otter.db.Connection,
    task: int,
    start_ts: str,
    end_ts: str,
    children: Tuple[int],
    depth: int,
):
    """Returns duration elapsed from task-start to task-end, including time spent
    waiting for children/descendants, but not including the duration of any
    children which are not synchronised

    Think of this as the "taskwait-inclusive duration", which is not the same as
    the "inclusive duration" i.e. this task + all descendants.

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

    task_native_dt = int(end_ts) - int(start_ts)
    suspended_native_dt = 0  # suspended duration as measured
    for _, timestamps in con.task_suspend_ts((task,)):
        if timestamps:
            for suspend, resume in timestamps:
                suspended_native_dt = suspended_native_dt + (resume - suspend)
    execution_native_dt = task_native_dt - suspended_native_dt

    suspended_ideal_dt = 0  # idealised suspended duration
    sync_groups = list(con.task_synchronisation_groups(task))
    for sequence, rows, sync_ts, sync_descendants in sync_groups:
        sync_children_attr = con.task_attributes(r["child_id"] for r in rows)
        max_child_duration = 0
        for child, *_, start_ts, end_ts, child_attr in sync_children_attr:
            duration = descend(con, child, start_ts, end_ts, child_attr, depth + 1)
            max_child_duration = max(max_child_duration, duration)
        suspended_ideal_dt = suspended_ideal_dt + max_child_duration

    taskwait_inclusive_dt = execution_native_dt + suspended_ideal_dt
    if otter.log.is_debug_enabled():
        print_task_durations(
            task,
            task_native_dt,
            suspended_native_dt,
            execution_native_dt,
            suspended_ideal_dt,
            taskwait_inclusive_dt,
        )
    return taskwait_inclusive_dt


def leaf_task(
    task: int, start_ts: str, end_ts: str, task_attr: otter.TaskAttributes, depth: int
):
    # Returns the duration of a leaf task. Assumes a leaf task is executed in one go i.e. never suspended.
    pre = "+" * depth
    start = int(start_ts)
    end = int(end_ts)
    duration = end - start
    otter.log.info(f"{pre} {task=} is a leaf task (dur = {duration}ns)")
    return duration


def print_task_durations(
    task,
    native_total,
    native_suspended,
    native_exec,
    ideal_suspended,
    ideal_taskwait_inclusive,
):
    print()
    print(f"{task=}")
    print(f"  native_total             {native_total:>8d}")
    print(f"  native_suspended         {native_suspended:>8d}")
    print(f"  native_exec              {native_exec:>8d}")
    print(f"  ideal_suspended          {ideal_suspended:>8d}")
    print(f"  ideal_taskwait_inclusive {ideal_taskwait_inclusive:>8d}")
    print(
        f"  relative error           {(ideal_taskwait_inclusive-native_total)/native_total}"
    )
    print()


def main(anchorfile):
    project = otter.project.BuildGraphFromDB(anchorfile)
    with project.connection() as con:
        print(f"simulating trace {anchorfile}")
        # con.print_summary()
        root_task_attrs = con.task_attributes(con.root_tasks())
        max_root_task_dt = 0
        for task, *_, start_ts, end_ts, attr in root_task_attrs:
            max_root_task_dt = max(
                max_root_task_dt, descend(con, task, start_ts, end_ts, attr, 0)
            )
            print(
                f"{anchorfile},{max_root_task_dt},{int(end_ts) - int(start_ts)}",
                file=sys.stderr,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("anchorfile", help="the Otter OTF2 anchorfile to use")
    otter.args.add_common_arguments(parser)
    args = parser.parse_args()
    otter.log.initialise(args)
    main(args.anchorfile)
