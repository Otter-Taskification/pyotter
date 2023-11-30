from __future__ import annotations

import sqlite3
from collections import defaultdict
from itertools import groupby
from typing import Any, Generator, Iterable, List, Optional, Tuple

from .. import log
from ..utils import batched
from ..definitions import TaskAttributes, SourceLocation, TaskAction
from . import scripts


class Row(sqlite3.Row):
    """A wrapper around sqlite3.Row with nicer printing"""

    def __repr__(self) -> str:
        values = ", ".join([f"{key}={self[key]}" for key in self.keys()])
        return f"Row({values})"

    def as_dict(self) -> dict:
        """Return a row as a dict"""

        return {key: self[key] for key in self.keys()}


class Connection(sqlite3.Connection):
    """Implements the connection to and operations on an Otter task database"""

    def __init__(self, db: str, **kwargs):
        super().__init__(db, **kwargs)
        self.db = db
        self.default_row_factory = Row
        self.row_factory = self.default_row_factory

    def print_summary(self) -> None:
        """Print summary information about the connected tasks database"""

        print(f"=== Summary of {self.db} ===")

        row_format = "{0:<8s} {1:20s} ({2} rows)"

        rows = self.execute(
            "select name, type from sqlite_schema where type in ('table', 'view')"
        ).fetchall()

        for row in rows:
            query_count_rows = f"select count(*) as rows from {row['name']}"
            log.debug(query_count_rows)
            count = self.execute(query_count_rows).fetchone()
            print(row_format.format(row["type"], row["name"], count["rows"]))

    def task_ids(self) -> Iterable[int]:
        cur = self.cursor()
        cur.execute("select id from task order by rowid")
        while True:
            row = cur.fetchone()
            if row is None:
                break
            yield row["id"]

    def root_tasks(self) -> Tuple[int]:
        return (0,)

    def children_of(self, parent: int) -> Tuple[int]:
        cur = self.cursor()
        query = "select child_id from task_relation where parent_id in (?)"
        return [r["child_id"] for r in cur.execute(query, (parent,)).fetchall()]

    def attributes_of(self, tasks: Iterable[int]) -> Tuple[Any, ...]:
        # TODO: consider returning Tuple[TaskAttributes] instead
        tasks = tuple(tasks)
        placeholder = ",".join("?" for _ in tasks)
        query_str = (
            f"select * from task_attributes where id in ({placeholder}) order by id\n"
        )
        cur = self.execute(query_str, tasks)
        return tuple(cur.fetchall())

    def task_attributes(self, tasks: Iterable[int]) -> List[Tuple[int, int, int, str, str, TaskAttributes]]:
        tasks = tuple(tasks)
        placeholder = ",".join("?" for _ in tasks)
        query_str = (
            f"select * from task_attributes where id in ({placeholder}) order by id\n"
        )
        self.row_factory = self._task_attributes_row_factory
        cur = self.execute(query_str, tasks)
        self.row_factory = Row
        return cur.fetchall()
    
    def task_suspend_ts(self, tasks: Iterable[int]) -> Generator[tuple[int, list[tuple[int, int]]], Any, None]:
        tasks = tuple(tasks)
        placeholder = ",".join("?" for _ in tasks)
        query = f"""select *
        from task_history
        where id in ({placeholder})
        and action in ({TaskAction.SUSPEND.value}, {TaskAction.RESUME.value})
        order by id, time"""
        cur = self.execute(query, tasks)
        rows = cur.fetchall()
        grouper = groupby(rows, key = lambda row: row["id"])
        for task_id, task_suspend_iter in grouper:
            timestamps = []
            for suspended, resumed in batched(task_suspend_iter, 2):
                assert suspended["action"] == TaskAction.SUSPEND
                assert resumed["action"] == TaskAction.RESUME
                timestamps.append((int(suspended["time"]), int(resumed["time"])))
            yield task_id, timestamps

    @staticmethod
    def _parent_child_attributes_row_factory(
        _, values: Tuple[Any, ...]
    ) -> Tuple[TaskAttributes, TaskAttributes, int]:
        parent_attr, child_attr = values[0:11], values[11:22]
        parent = TaskAttributes(*parent_attr)
        child = TaskAttributes(*child_attr)
        total = values[22]
        return parent, child, total

    @staticmethod
    def _task_count_by_attributes_row_factory(
        _, values: Tuple[Any, ...]
    ) -> Tuple[TaskAttributes, int]:
        task_attr = (values[0], 0, *values[1:10])
        count: int = values[10]
        return TaskAttributes(*task_attr), count

    @staticmethod
    def _source_location_row_factory(_, values: tuple[Any, ...]) -> SourceLocation:
        return SourceLocation(*values)

    @staticmethod
    def _task_attributes_row_factory(_, values: tuple[Any, ...]):
        task_id, parent_id, num_children, flavour, label, start_ts, end_ts, *locations = values
        return task_id, parent_id, num_children, start_ts, end_ts, TaskAttributes(label, flavour, *locations)

    def parent_child_attributes(
        self,
    ) -> List[Tuple[TaskAttributes, TaskAttributes, int]]:
        """Return tuples of task attributes for each parent-child link and the number of such links"""

        self.row_factory = self._parent_child_attributes_row_factory
        cur = self.execute(scripts.count_children_by_parent_attributes)
        results = cur.fetchall()
        log.debug("got %d rows", len(results))
        return results

    def child_sync_points(self, task: int, debug: bool = False) -> Tuple[Any]:
        """Get the sequences of child tasks synchronised during a task."""

        cur = self.cursor()
        cur.execute(scripts.get_child_sync_points, (task,))
        results = tuple(cur.fetchall())
        if debug:
            log.debug("child_sync_points: got %d results", len(results))
        return results

    def sync_groups(
        self, task: int, debug: bool = False
    ) -> Generator[Tuple[Optional[int], List[Row]], None, None]:
        """Get the sequences of child tasks synchronised during a task.

        For each sequence (group of synchronised tasks), yield a sequence
        identifier and the rows representing the synchronised tasks.
        """

        records = self.child_sync_points(task, debug=debug)
        sequences = defaultdict(list)
        for row in records:
            sequences[row["sequence"]].append(row)
        for seq, rows in sequences.items():
            if debug:
                log.debug(
                    "sync_groups: sequence %s yielding %d records", seq, len(rows)
                )
            yield seq, rows

    def task_synchronisation_groups(self, task: int):
        records = self.child_sync_points(task)
        sequences = defaultdict(list)
        time = {}
        sync_descendants = {}
        for row in records:
            s = row["sequence"]
            sequences[s].append(row)
            if s not in time:
                time[s] = int(row["sync_ts"])
            if s not in sync_descendants:
                sync_descendants[s] = bool(row["sync_descendants"])
        for seq, rows in sequences.items():
            yield seq, rows, time[seq], sync_descendants[seq]

    def source_locations(self):
        """Get all the source locations defined in the trace"""

        self.row_factory = self._source_location_row_factory
        cur = self.execute("select * from source_location")
        results: List[SourceLocation] = cur.fetchall()
        log.debug("got %d source locations", len(results))
        return results

    def task_types(self) -> List[Tuple[TaskAttributes, int]]:
        """Return task attributes for each distinct set of task attributes and the number of such records"""

        self.row_factory = self._task_count_by_attributes_row_factory
        cur = self.execute(scripts.count_tasks_by_attributes)
        results = cur.fetchall()
        log.debug("got %d task definitions", len(results))
        return results
