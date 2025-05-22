from __future__ import annotations

from typing import Optional, List, Dict, Tuple, NamedTuple
from itertools import count

import otter.log
import otter.db

from otter.log import Loggable
from otter.db import ReadConnection
from otter.db.protocols import TaskActionCallback, TaskSuspendMetaCallback, CriticalTaskCallback
from otter.db.types import Task
from otter.definitions import TaskAction, TaskID, TaskSyncMode


class Timings(NamedTuple):
    task: TaskID
    start_ts: int
    duration: int
    end_ts: int

    def __gt__(self, value) -> bool:
        return self.end_ts > value.end_ts


TaskTimingData = Dict[TaskID, Timings]


class TaskScheduler(Loggable):

    def __init__(
        self,
        reader: ReadConnection,
        crit_task_callback: CriticalTaskCallback,
        task_action_callback: TaskActionCallback,
        task_suspend_callback: TaskSuspendMetaCallback,
        initial_task: Optional[TaskID] = None,
    ) -> None:
        self.log_debug("CALLBACKS:")
        self.log_debug("CALLBACKS: %s", crit_task_callback)
        self.log_debug("CALLBACKS: %s", task_action_callback)
        self.log_debug("CALLBACKS: %s", task_suspend_callback)
        self.reader = reader
        self.crit_task_callback = crit_task_callback
        self.task_action_callback = task_action_callback
        self.task_suspend_callback = task_suspend_callback
        self._root_task = initial_task or reader.get_root_task()
        otter.log.debug("found root task: %d", self._root_task)

    def run(self) -> None:
        global_ts = 0
        root_task = self.reader.get_task(self.reader.get_root_task())
        phase_states = self.get_phase_states()
        assert len(phase_states) == root_task.children
        for mode, (phase,) in phase_states:
            global_ts = self.simulate_phase(global_ts, phase)

    def get_phase_states(self):
        root = self.reader.get_root_task()
        root_suspend_mode = dict(self.reader.get_task_suspend_meta(root))
        phase_data: List[Tuple[TaskSyncMode, List[TaskID]]] = []
        children: List[TaskID]
        for root_state in self.reader.get_task_scheduling_states((root,)):
            if root_state.is_active:
                children = [
                    item[0]
                    for item in self.reader.get_children_created_between(
                        root, root_state.start_ts, root_state.end_ts
                    )
                ]
                if not children:
                    continue
                assert len(children) == 1, "invalid root children"
            elif root_state.action_start == TaskAction.SUSPEND:
                mode = root_suspend_mode[root_state.start_ts]
                phase_data.append((mode, children))
            elif root_state.action_start == TaskAction.END:
                ...
            elif root_state.action_start == TaskAction.CREATE:
                ...
            else:
                self.log_error(f"unhandled root state: {root_state}")

        return phase_data

    def simulate_phase(self, global_ts: int, phase_id: TaskID):
        phase = self.reader.get_task(phase_id)
        self.log_info(f'simulate phase task {phase_id} {global_ts=} "{phase.attr.label}"')
        suspend_mode = dict(self.reader.get_task_suspend_meta(phase_id))
        barrier_counter = count()
        children_visited = 0
        for state in self.reader.get_task_scheduling_states((phase_id,)):
            if state.is_active:
                # Spawn children created during this state, assume this state
                # has no intrinsic duration of its own. Assume all children
                # during this state created at t=global_ts

                # Get the children created during this state
                children = [
                    pair[0]
                    for pair in self.reader.get_children_created_between(
                        phase_id, state.start_ts, state.end_ts
                    )
                ]

                # TaskID -> (start, duration, end)
                timing_data: TaskTimingData = dict()

                # Descend into each child to record their simulated start & end
                for child in self.reader.get_tasks(children):
                    self.descend(child, 0, global_ts, timing_data)
                    children_visited += 1

                if state.action_end == TaskAction.SUSPEND:
                    mode = suspend_mode[state.end_ts]
                    self.task_suspend_callback(phase_id, global_ts, False, mode.value)
                    if children:
                        if mode == TaskSyncMode.CHILDREN:
                            latest_child = max(timing_data[child] for child in children)
                            global_ts = max(global_ts, latest_child.end_ts)
                            self.crit_task_callback(
                                phase_id, next(barrier_counter), latest_child.task
                            )
                        elif mode == TaskSyncMode.DESCENDANTS:
                            latest_descendant = max(timing_data.values())
                            global_ts = max(global_ts, latest_descendant.end_ts)
                            self.crit_task_callback(
                                phase_id, next(barrier_counter), latest_descendant.task
                            )
                        else:
                            self.log_error(f"unhandled task sync mode {mode} for state:")
                            self.log_error(f"    {state=}")
                    else:
                        global_ts += state.duration
                elif state.action_end == TaskAction.END:
                    assert (
                        not children
                    ), "there seem to be unsynchronised children at the end of an active part of a phase"
                else:
                    self.log_error(f"invalid end to active state in phase task {phase_id}")
                    self.log_error(
                        f"  start: {state.start_location.file}:{state.start_location.line}"
                    )
                    self.log_error(f"  end  : {state.end_location.file}:{state.end_location.line}")
                    self.log_error(f"  {state=}")
                    raise RuntimeError(f"invalid end to active state in phase task {phase_id}")

            elif state.action_start in (TaskAction.CREATE, TaskAction.SUSPEND):
                continue
            else:
                self.log_error(f"unhandled phase state: {state}")

        assert children_visited == phase.children
        return global_ts

    def descend(
        self,
        task: Task,
        depth: int,
        global_start_ts: int,
        timing_data: TaskTimingData,
    ):
        """Descend into the children of task. At the lowest level, handle leaf tasks"""
        self.task_action_callback(
            task.id, TaskAction.CREATE, global_start_ts, task.attr.create_location, cpu=0, tid=0
        )
        self.task_action_callback(
            task.id, TaskAction.START, global_start_ts, task.attr.start_location, cpu=0, tid=0
        )
        if task.children > 0:
            duration = self.branch_task(task, depth, global_start_ts, timing_data)
        else:
            duration = self.leaf_task(task, depth, global_start_ts)
        timing_data[task.id] = Timings(
            task.id, global_start_ts, duration, global_start_ts + duration
        )
        self.task_action_callback(
            task.id,
            TaskAction.END,
            global_start_ts + duration,
            task.attr.end_location,
            cpu=0,
            tid=0,
        )
        return duration

    def branch_task(
        self,
        task: Task,
        depth: int,
        global_start_ts: int,
        timing_data: TaskTimingData,
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

        self.log_debug(f"BRANCH: {'+'*depth} {task.id=} {task.children=}")

        children_visited = 0
        execution_native_dt, suspended_ideal_dt = 0, 0

        task_states = self.reader.get_task_scheduling_states((task.id,))
        task_suspend_meta = dict(self.reader.get_task_suspend_meta(task.id))
        otter.log.debug("got %d task scheduling states", len(task_states))
        barrier_counter = count()
        children_pending: List[TaskID] = []
        descendants_timing_data: TaskTimingData = dict()
        for state in task_states:
            self.log_debug(f"BRANCH: {'|'*depth} {task.id=} {TaskAction(state.action_start).name}")
            if state.is_active:

                #! 1. record native execution dt
                execution_native_dt += state.duration

                #! 2. descend into all tasks created during this state
                children_created = [
                    pair[0]
                    for pair in self.reader.get_children_created_between(
                        task.id, state.start_ts, state.end_ts
                    )
                ]
                children_pending.extend(children_created)

                local_timing_data: TaskTimingData = dict()
                for child in self.reader.get_tasks(children_created):
                    child_crt_dt = child.create_ts - state.start_ts
                    assert child_crt_dt > 0, "child_crt_dt must not be negative"
                    #! 3. EXTEND simulated timing data for descendants created during this state
                    self.descend(
                        child, depth + 1, global_start_ts + child_crt_dt, local_timing_data
                    )
                    children_visited += 1
                    descendants_timing_data.update(local_timing_data)

                #! 4. advance global time by state.duration
                global_start_ts += state.duration

                #! 5. continue to next state
                ...

            elif state.action_start == TaskAction.SUSPEND:

                #! 1. get task sync mode
                mode = task_suspend_meta[state.start_ts]

                #! 2. calculate the global time the sync mode is satisfied from simulated timing data
                barrier_duration = 0
                if mode == TaskSyncMode.CHILDREN:
                    latest_child = max(descendants_timing_data[child] for child in children_pending)
                    if latest_child.end_ts >= global_start_ts:
                        barrier_duration = latest_child.end_ts - global_start_ts
                    children_pending.clear()
                elif mode == TaskSyncMode.DESCENDANTS:
                    latest_descendant = max(descendants_timing_data.values())
                    if latest_descendant.end_ts >= global_start_ts:
                        barrier_duration = latest_descendant.end_ts - global_start_ts
                        self.crit_task_callback(
                            task.id, next(barrier_counter), latest_descendant.task
                        )
                    children_pending.clear()
                elif mode == TaskSyncMode.YIELD:
                    ...  # do nothing, no sync constraints apply here so can immediately resume
                else:
                    self.log_error(f"unhandled task sync mode: {mode}")

                #! 3. record the simulated barrier start & end times
                self.task_suspend_callback(task.id, global_start_ts, False, mode.value)
                self.task_action_callback(
                    task.id, TaskAction.SUSPEND, global_start_ts, state.start_location, cpu=0, tid=0
                )
                self.task_action_callback(
                    task.id,
                    TaskAction.RESUME,
                    global_start_ts + barrier_duration,
                    state.end_location,
                    cpu=0,
                    tid=0,
                )

                #! 4. advance global time to the barrier end time
                global_start_ts += barrier_duration
                suspended_ideal_dt += barrier_duration

                #! 5. DO NOT CLEAR SIMULATED TIMING DATA
                #! 6. continue to next state

            elif state.action_start == TaskAction.CREATE:
                # task is created and not yet started
                pass
            else:
                self.log_error("unhandled state: %s", state)

        timing_data.update(descendants_timing_data)

        assert children_visited == task.children

        return execution_native_dt + suspended_ideal_dt

    def leaf_task(self, task: Task, depth: int, global_start_ts: int):
        # Returns the duration of a leaf task. Assumes a leaf task is executed in one go i.e. never suspended.
        return task.end_ts - task.start_ts


def simulate_ideal(
    reader: ReadConnection,
    crit_task_callback: CriticalTaskCallback,
    task_action_callback: TaskActionCallback,
    task_suspend_callback: TaskSuspendMetaCallback,
):
    TaskScheduler(reader, crit_task_callback, task_action_callback, task_suspend_callback).run()
