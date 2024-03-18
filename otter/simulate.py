import argparse
from typing import TextIO

import otter
import igraph


def descend(
    con: otter.db.Connection,
    task: int,
    start_ts: str,
    end_ts: str,
    depth: int,
    global_start_ts: int,
    sched: TextIO,
    crit: TextIO,
):
    """Descend into the children of task. At the root, handle leaf tasks"""
    if con.num_children(task) > 0:
        duration = branch_task(
            con,
            task,
            start_ts,
            end_ts,
            depth,
            global_start_ts,
            sched,
            crit,
        )
    else:
        duration = leaf_task(task, start_ts, end_ts, depth, global_start_ts, sched)
    return duration


def branch_task(
    con: otter.db.Connection,
    task: int,
    start_ts: str,
    end_ts: str,
    depth: int,
    global_start_ts: int,
    sched: TextIO,
    crit: TextIO,
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

    task_start_ts = global_start_ts

    otter.log.debug(f"  {task=}")
    (_, execution_native_dt, suspended_native_dt), *_ = con.time_active((task,))
    otter.log.debug(f"  time active:   {execution_native_dt:>9d} ns")
    otter.log.debug(f"  time inactive: {suspended_native_dt:>9d} ns")
    task_native_dt = execution_native_dt + suspended_native_dt

    #! NOTE: could improve the SQL behind this as there's a lot of duplicated info returned when getting task sync groups.
    #! could just return 1 row per sequence with the relevant times/durations, then 1 table per sequence with the synchronised child task IDs
    suspended_ideal_dt = 0  # idealised suspended duration
    sync_groups = list(con.task_synchronisation_groups(task))
    for sequence, rows, sync_start_ts, sync_descendants, chunk_duration in sync_groups:
        child_crt_dt = [(r["child_id"], r["child_crt_dt"]) for r in rows]
        sync_children_attr = con.task_attributes([r["child_id"] for r in rows])
        barrier_duration = 0
        critical_task = None
        for (child, *_, create_ts, start_ts, end_ts, _), (
            _child,
            crt_dt,
        ) in zip(sync_children_attr, child_crt_dt, strict=True):
            assert child == _child
            otter.log.debug(
                f"{crt_dt=:>9d} {child=:>4d} {chunk_duration=:>9d} {chunk_duration>=crt_dt}"
            )
            child_duration = descend(
                con,
                child,
                start_ts,
                end_ts,
                depth + 1,
                global_start_ts + crt_dt,
                sched,
                crit,
            )
            duration_into_barrier = child_duration - (
                int(sync_start_ts) - int(create_ts)
            )
            if duration_into_barrier > barrier_duration:
                barrier_duration = duration_into_barrier
                critical_task = child
        if critical_task is not None:
            crit.write(f"{task},{sequence},{critical_task}\n")
        suspended_ideal_dt = suspended_ideal_dt + barrier_duration
        global_start_ts = global_start_ts + chunk_duration + barrier_duration

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
    sched.write(f"{task},{task_start_ts},{taskwait_inclusive_dt}\n")
    return taskwait_inclusive_dt


def leaf_task(
    task: int,
    start_ts: str,
    end_ts: str,
    depth: int,
    global_start_ts: int,
    sched: TextIO,
):
    # Returns the duration of a leaf task. Assumes a leaf task is executed in one go i.e. never suspended.
    pre = "+" * depth
    start = int(start_ts)
    end = int(end_ts)
    duration = end - start
    sched.write(f"{task},{global_start_ts},{duration}\n")
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
        f"  relative error           {(ideal_taskwait_inclusive-native_total)/native_total:>8.2%}"
    )
    print()


def main(anchorfile, sched: TextIO, crit: TextIO):
    project = otter.project.BuildGraphFromDB(anchorfile)
    with project.connection() as con:
        otter.log.info(f"simulating trace {anchorfile}")
        # con.print_summary()
        root_task_attrs = con.task_attributes(con.root_tasks())
        max_root_task_dt = 0
        global_ts = 0
        for task, *_, create_ts, start_ts, end_ts, attr in root_task_attrs:
            print(f"Simulate root task {task}")
            print(f"    Start: {attr.start_location}")
            print(f"    End:   {attr.end_location}")
            duration_observed = int(end_ts) - int(start_ts)
            duration = descend(
                con,
                task,
                start_ts,
                end_ts,
                0,
                global_ts,
                sched,
                crit,
            )
            speedup = duration_observed / duration
            max_root_task_dt = max(max_root_task_dt, duration)
            print("    Duration:")
            print(f"      observed:  {duration_observed:>12d}")
            print(f"      simulated: {duration:>12d}")
            print(f"      speedup:   {speedup:>12.2f}")
            print(f"      relative:  {1/speedup:12.2%}")


def detect_critical_tasks(sched: TextIO):
    header = sched.readline()
    otter.log.info(f"{header=}")
    graph = igraph.Graph(directed=True)
    graph.vs["name"] = None
    for line in sched:
        task, _, critical_child = line.strip().split(",")
        otter.log.info(f"{task=}, {critical_child=}")
        if task not in graph.vs["name"]:
            v1 = graph.add_vertex(name=task)
        if critical_child not in graph.vs["name"]:
            v2 = graph.add_vertex(name=critical_child)
        graph.add_edge(task, critical_child)
    otter.log.info(f"num vertices: {len(graph.vs)}")
    root_node = graph.vs.find(name="0")
    neighbours = graph.neighborhood_size(vertices=root_node.index, order=99)
    neighborhood = graph.neighborhood(vertices=root_node.index, order=neighbours)
    critical_subgraph = graph.induced_subgraph(neighborhood)
    otter.log.info(f"neighbours: {neighbours}")
    otter.log.info(f"neighborhood: {neighborhood}")
    print("Critical task sequence: ", end="")
    print(", ".join(critical_subgraph.vs["name"]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--sched",
        dest="sched",
        metavar="file",
        help="where to save the task scheduling data",
        default="sched.txt",
    )
    parser.add_argument(
        "-c",
        "--crit",
        dest="crit",
        metavar="file",
        help="where to save data about critical tasks",
        default="crit.txt",
    )
    parser.add_argument(
        "--critical-tasks",
        dest="detect_critical",
        help="detect the sequence of critical tasks",
        action="store_true",
        default=False,
    )
    parser.add_argument("anchorfile", help="the Otter OTF2 anchorfile to use")
    otter.args.add_common_arguments(parser)
    args = parser.parse_args()
    otter.log.initialise(args)
    with open(args.sched, "w", encoding="utf-8") as sched_file, open(
        args.crit, "w", encoding="utf-8"
    ) as crit_file:
        sched_file.write("task,type,global_start_ts,duration\n")
        crit_file.write("task,sequence,critical_child\n")
        main(args.anchorfile, sched_file, crit_file)
    otter.log.info(f"task scheduling data written to {args.sched}")
    otter.log.info(f"critical sub-tasks written to {args.crit}")
    if args.detect_critical:
        otter.log.info("finding critical tasks")
        with open(args.crit, "r", encoding="utf-8") as crit_file:
            detect_critical_tasks(crit_file)
