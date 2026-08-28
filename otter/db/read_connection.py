from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Generator, List, Tuple, Sequence, Optional, Literal, Callable

from otter.definitions import TaskSyncMode, TaskID

from .connect_base import Mode, ConnectionBase
from .scripts import scripts
from .types import SourceLocation, TaskAttributes, Task, TaskSchedulingState, Event, TaskAction


class ReadConnection(ConnectionBase):
    """Implements all logic for querying an otter database"""

    def __init__(self, root_path: Path) -> None:
        super().__init__(root_path, mode=Mode.ro)
        # Check for simulations and try to read all simulation IDs
        sim_register = self.get_uri("_WriteSimParallelConnection.db", Mode.ro)
        try:
            con = sim_register.connect()
        except FileNotFoundError:
            self.log_debug("no simulation register found at %s", sim_register)
            sim_ids: List[int] = []
        else:
            self.log_debug("connected to simulation register at '%s'", sim_register)
            cur = con.execute("select id from simulations order by id;")
            sim_ids: List[int] = [sim_id for (sim_id,) in cur]
        self.log_debug("found %d simulations", len(sim_ids))
        self.simulations = {
            sim_id: self.get_uri(f"sim_{sim_id}.db", Mode.ro).connect() for sim_id in sim_ids
        }

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex, tb):
        pass

    def count_rows(self):

        self.log_debug("try to read from sqlite_master")
        rows = self._con.execute(scripts["select_names_from_sqlite_master"]).fetchall()

        counts: List[Tuple[str, str, int]]
        counts = [
            (
                table_or_view,
                name,
                self._con.execute(f"select count(*) from {name}").fetchone()[0],
            )
            for (table_or_view, name) in rows
        ]
        return counts

    def count_tasks(self) -> int:
        (count,) = self._con.execute("select count(*) from task").fetchone()
        return count

    def count_simulations(self) -> int:
        return len(self.simulations)

    def count_simulation_rows(self) -> List[Tuple[int, int]]:
        rows = [
            con.execute(scripts["count_simulation_rows"]).fetchone()
            for con in self.simulations.values()
        ]
        return rows

    def get_root_task(self):
        return TaskID(0)

    def get_num_children(self, task: TaskID) -> int:
        query = "select count(*) from task_relation where parent_id in (?)"
        (count,) = self._con.execute(query, (task,)).fetchone()
        return count

    def get_related_tasks(self, task: TaskID, relation: Literal["children", "descendants", "ancestors"], cond: Optional[Callable[[TaskID], bool]] = None) -> List[TaskID]:
        query = "select child_id from task_relation where parent_id in (?)"
        if relation == "descendants":
            query = scripts["get_descendants"]
        elif relation == "ancestors":
            query = scripts["get_ancestors"]
        tasks = (task for (task,) in self._con.execute(query, (task,)))
        return list(tasks if cond is None else filter(cond, tasks))

    def get_all_parent_child_attributes(
        self,
    ) -> List[Tuple[TaskAttributes, TaskAttributes, int]]:
        """Return tuples of task attributes for each parent-child link and the number of such links"""

        cur = self._con.execute(scripts["count_children_by_parent_attributes"])
        results = [
            (self._make_task_attr(*row[0:4]), self._make_task_attr(*row[4:8]), row[8])
            for row in cur
        ]
        return results

    def get_tasks(self, tasks: Sequence[TaskID]) -> List[Task]:
        placeholder = ",".join("?" for _ in tasks)
        query = scripts["get_task_attributes"].format(placeholder=placeholder)
        cur = self._con.execute(query, tuple(tasks))
        return list(map(self._make_task, cur))

    def get_task(self, task: TaskID):
        return self.get_tasks((task,))[0]

    def iter_all_tasks(self):
        """An iterator over all tasks in the db"""
        return map(self._make_task, self._con.execute(scripts["get_all_task_attributes"]))

    def iter_all_task_ids(self) -> Generator[TaskID, None, None]:
        query = "select id from task order by id"
        cur = self._con.execute(query)
        return (n for (n,) in cur)

    def get_thread_ids(self) -> List[int]:
        query = "select distinct tid from task_history order by tid"
        cur = self._con.execute(query)
        return list(n for (n,) in cur)

    @lru_cache(maxsize=1000)
    def get_task_label(self, task: TaskID):
        query = "select user_label from task where id = ?"
        (label_id,) = self._con.execute(query, (task,)).fetchone()
        return self.get_string(label_id)

    @lru_cache(maxsize=1000)
    def get_string(self, string_id: int) -> str:
        (string,) = self._con.execute(scripts["get_string"], (string_id,)).fetchone()
        return string

    @lru_cache(maxsize=1000)
    def get_source_location(self, location_id: int) -> SourceLocation:
        """Construct a source location from its id"""
        self.log_debug(f"get source location for {location_id=}")
        row = self._con.execute(scripts["get_source_location"], (location_id,)).fetchone()
        if not row:
            self.log_error(f"no source location data found for {location_id=}")
        return SourceLocation(*row)

    def get_all_source_locations(self) -> List[Tuple[int, SourceLocation]]:
        """Get all the source locations defined in the trace"""

        results = [
            (location_id, SourceLocation(*row))
            for (location_id, *row) in self._con.execute(
                "select src_loc_id, file_name, func_name, line from source_location order by file_name, line"
            )
        ]
        return results

    def get_all_strings(self) -> List[Tuple[int, str]]:
        return list(self._con.execute("select id, text from string order by id;"))

    def iter_all_task_types(self) -> Generator[Tuple[TaskAttributes, int], None, None]:
        """Return task attributes for each distinct set of task attributes and the number of such records"""

        cur = self._con.execute(scripts["count_tasks_by_attributes"])
        return ((self._make_task_attr(*row[0:4]), row[4]) for row in cur)

    def get_task_scheduling_states(
        self,
        tasks: Sequence[TaskID],
        *,
        cond: Optional[Callable[[TaskSchedulingState], bool]] = None,
        sim_id: Optional[int] = None,
    ):
        """Return 1 row per task scheduling state during the task's lifetime"""
        return list(filter(cond, self.iter_task_scheduling_states(tasks, sim_id=sim_id)))

    def iter_task_scheduling_states(
        self,
        tasks: Sequence[TaskID],
        *,
        sim_id: Optional[int] = None,
    ) -> Generator[TaskSchedulingState, None, None]:
        """Yield 1 row per task scheduling state during the task's lifetime"""
        if sim_id is None:
            yield from self._iter_task_scheduling_states(tasks)
        else:
            # Think we need to open a reader to the actual trace data here, since a reader
            yield from self._iter_simulated_task_scheduling_states(tasks, sim_id)

    def _iter_simulated_task_scheduling_states(self, tasks: Sequence[TaskID], sim_id: int):
        """Yield 1 row per task scheduling state during the task's lifetime for a specific simulation"""
        query = scripts["get_simulated_scheduling_states"].format(
            sim_id=sim_id,
            placeholder=",".join("?" for task in tasks),
        )
        cur = self.simulations[sim_id].execute(query, tasks)
        for row in cur:
            task_id, start_id, end_id, action_start, action_end, *rest = row
            self.log_debug(f"get source location for {start_id=}")
            self.log_debug(f"get source location for {end_id=}")
            start = self.get_source_location(start_id)
            end = self.get_source_location(end_id)
            data = [ task_id, action_start, action_end, *start, *end, *rest ]
            self.log_debug(f"yield simulated scheduling state ({len(row)} items): {row}")
            yield TaskSchedulingState(*data)

    def _iter_task_scheduling_states(self, tasks: Sequence[TaskID]):
        """Yield 1 row per task scheduling state during the task's lifetime"""
        query = scripts["get_task_scheduling_states"].format(
            placeholder=",".join("?" for task in tasks)
        )
        cur = self._con.execute(query, tasks)
        yield from (TaskSchedulingState(*row) for row in cur)

    def get_task_create_events(self, tasks: List[TaskID], reference_ts: int = 0) -> List[Event]:
        placeholder = ",".join("?" for _ in tasks)
        query = scripts["get_task_create_events"].format(placeholder=placeholder, since=reference_ts)
        cur = self._con.execute(query, tasks).fetchall()
        return list(map(lambda row: self._make_task_create_event(*row), cur))

    def get_task_event_positions(self, task: TaskID) -> List[Tuple[int, int]]:
        return list(self._con.execute(scripts["get_task_events"], (task,)))

    def get_task_suspend_meta(self, task: TaskID) -> List[Tuple[int, TaskSyncMode]]:
        """Return the metadata for each suspend event encountered by a task"""

        query = "select time, sync_mode from task_suspend_meta where id in (?) order by time"
        cur = self._con.execute(query, (task,))
        return list((time, TaskSyncMode(sync_mode)) for (time, sync_mode) in cur)

    def get_children_created_between(
        self, task: TaskID, start_ts: int, end_ts: int, relative: bool=False,
    ) -> List[Tuple[TaskID, int]]:
        """Return the children created between the given start & end times"""

        if relative:
            query = scripts["get_children_created_between_relative"].format(start_ts=start_ts, end_ts=end_ts)
        else:
            query = scripts["get_children_created_between"].format(start_ts=start_ts, end_ts=end_ts)

        cur = self._con.execute(query, (task,))
        return list(cur)

    def get_sim_ids(self) -> List[int]:
        return list(self.simulations.keys())

    def get_critical_tasks(self, /, *, sim_id: int) -> List[TaskID]:
        # TODO! doesn't work with separate databases for each simulation
        cur = self._con.execute(scripts["get_critical_tasks"].format(root_task=0), (sim_id,))
        return [task for (task,) in cur]

    # Row factories

    def _make_task(self, row) -> Task:
        """Make a task from its attributes and source location refs"""
        return Task(*row[0:7], *map(self.get_source_location, row[7:]))

    def _make_task_attr(self, label: str, create: int, start: int, end: int) -> TaskAttributes:
        return TaskAttributes(
            label,
            self.get_source_location(create),
            self.get_source_location(start),
            self.get_source_location(end),
        )

    def _make_task_create_event(self, task_id: int, action: int, file: str, func: str, line: int, time: int, cpu: int, tid: int) -> Event:
        return Event(time, TaskID(task_id), TaskAction(action), SourceLocation(file, func, line), cpu, tid, None)
