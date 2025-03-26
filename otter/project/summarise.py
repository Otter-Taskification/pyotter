from typing import List

import otter.log

from otter.definitions import TaskAction, TaskID
from otter.db import ReadConnection
from otter.db.types import TaskSchedulingState
from otter.args import Summarise
from otter.utils.demangle import demangle as demangle_tokens

from .project import ReadTraceData


def print_phase_scheduling_data(reader: ReadConnection):
    phase_tasks = reader.get_related_tasks(reader.get_root_task(), relation="children")
    phase_sched = reader.get_task_scheduling_states(phase_tasks)
    for s in phase_sched:
        if s.action_start == TaskAction.CREATE:
            continue
        if s.action_start == TaskAction.START:
            print(f"  PHASE ID: {s.task:>8d} [label: {reader.get_task_label(s.task)}]")
            print(
                f"  {'TIME':<15s} | {' DURATION':<15s} | {' CHILDREN':<9s} | {' DESC.':<9s} | {' ACTION':<9s} | {' LOCATION':<14s}"
            )
            print("=" * 80)
        children = reader.get_children_created_between(s.task, s.start_ts, s.end_ts)
        num_children = len(children)
        num_descendants = 0
        for child, _ in children:
            desc = reader.get_related_tasks(child, relation="descendants")
            num_descendants += len(desc)
        print(
            f"{s.start_ts:>17,d} | {s.duration:>15,d} | {num_children:>9,d} | {num_descendants:>9,d} | {s.action_start.name:<9s} | {s.start_location}"
        )
        if s.action_end == TaskAction.END:
            print(
                f"{s.end_ts:>17,d} | {'-':>15s} | {'-':>9s} | {'-':>9s} | {s.action_end.name:<9s} | {s.end_location}"
            )
            print()


def summarise_tasks_db(
    anchorfile: str,
    summarise: Summarise,
    demangle: bool = False,
    debug: bool = False,
) -> None:
    """Print summary information about a tasks database"""

    project = ReadTraceData(anchorfile)

    with project.connect() as con:

        if summarise == Summarise.ROWCOUNT:
            for items in con.count_rows():
                print(*items, sep="\t")

        elif summarise == Summarise.SOURCE:
            for location_id, location in con.get_all_source_locations():
                print(location_id, location.file, location.line, location.func, sep="\t")

        elif summarise == Summarise.STRINGS:
            for ref, string in con.get_all_strings():
                print(ref, string, sep="\t")

        elif summarise == Summarise.TASKS:
            for attr, num_tasks in con.iter_all_task_types():
                print(
                    num_tasks,
                    demangle_tokens([attr.label])[0] if demangle else attr.label,
                    f"{attr.create_location.file}:{attr.create_location.line}",
                    f"{attr.start_location.file}:{attr.start_location.line}",
                    f"{attr.end_location.file}:{attr.end_location.line}",
                    sep="\t",
                )

        elif summarise == Summarise.SIMS:
            for sim_id, rows in con.count_simulation_rows():
                print(sim_id, rows, sep="\t")

        elif summarise == Summarise.PHASES:
            print_phase_scheduling_data(otter.project.ReadTraceData(anchorfile).connect())

        elif summarise == Summarise.TIME:
            simulations = con.get_sim_ids()
            root_task_id = TaskID(1) #!!! HACK!!!
            native_schedule = con.get_task_scheduling_states((root_task_id,), cond=lambda s: s.is_active)
            simulated_schedules = {sim_id: con.get_task_scheduling_states((root_task_id,), cond=lambda s: s.is_active, sim_id=sim_id) for sim_id in simulations}
            start, end = native_schedule[0], native_schedule[-1]
            duration = end.end_ts - start.start_ts
            print(anchorfile, "native", start.start_ts, end.end_ts, duration, start.action_start.name, end.action_end.name, sep=",")

            for sim_id, states in simulated_schedules.items():
                start, end = states[0], states[-1]
                duration = end.end_ts - start.start_ts
                # print(f"{sim_id=}")
                # for state in states:
                #     print(state)
                print(anchorfile, sim_id, start.start_ts, end.end_ts, duration, start.action_start.name, end.action_end.name, sep=",")
                # print()

        else:
            otter.log.error("don't know how to summarise %s", summarise)
