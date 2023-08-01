import sqlite3
from collections import defaultdict
from typing import Any, Iterable, Tuple, Generator, Optional, List

from ..definitions import TaskAttributes, SourceLocation
from .. import log
from . import query, scripts


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
        self.row_factory = Row

    def children_of(self, parent: int) -> Tuple[int]:
        cur = self.cursor()
        cur.execute(
            "select child_id from task_relation where parent_id in (?)", (parent,)
        )
        return tuple(cur.fetchall())

    def attributes_of(self, tasks: Iterable[int]) -> Tuple[Any]:
        # TODO: consider returning Tuple[TaskAttributes] instead
        cur = self.cursor()
        tasks = tuple(tasks)
        placeholder = ",".join("?" for _ in tasks)
        q = f"select * from task_attributes where id in ({placeholder}) order by id\n"
        cur.execute(q, tasks)
        return tuple(cur.fetchall())

    def parent_child_attributes(
        self,
    ) -> List[Tuple[TaskAttributes, TaskAttributes, int]]:
        """Return tuples of task attributes for each parent-child link and the number of such links"""

        def row_factory(_, values) -> Tuple[TaskAttributes, TaskAttributes, int]:
            parent = TaskAttributes(
                values[0],
                values[1],
                SourceLocation(file=values[2], line=values[3], func=values[4]),
                SourceLocation(file=values[5], line=values[6], func=values[7]),
                SourceLocation(file=values[8], line=values[9], func=values[10]),
            )
            child = TaskAttributes(
                values[11],
                values[12],
                SourceLocation(file=values[13], line=values[14], func=values[15]),
                SourceLocation(file=values[16], line=values[17], func=values[18]),
                SourceLocation(file=values[19], line=values[20], func=values[21]),
            )
            total = values[22]
            return parent, child, total

        cur = self.cursor()
        cur.row_factory = row_factory
        results = cur.execute(scripts.count_children_by_parent_attributes)
        return results.fetchall()

    def child_sync_points(self, task: int, debug: bool = False) -> Tuple[Any]:
        """Get the sequences of child tasks synchronised during a task."""

        cur = self.cursor()
        cur.execute(query.CHILD_SYNC_POINTS, (task,))
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
