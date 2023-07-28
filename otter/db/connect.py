import sqlite3
from collections import defaultdict
from typing import Any, Iterable, Tuple, Generator, Optional

from .. import log
from . import query


class Row(sqlite3.Row):
    def __repr__(self) -> str:
        return "Row({})".format(
            ", ".join([f"{key}={self[key]}" for key in self.keys()])
        )

    def as_dict(self) -> dict:
        return {key: self[key] for key in self.keys()}


class Connection(sqlite3.Connection):
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
        cur = self.cursor()
        tasks = tuple(tasks)
        placeholder = ",".join("?" for _ in tasks)
        q = f"select * from task_attributes where id in ({placeholder}) order by id\n"
        cur.execute(q, tasks)
        return tuple(cur.fetchall())

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
    ) -> Generator[Tuple[Optional[int], Iterable[Row]], None, None]:
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
