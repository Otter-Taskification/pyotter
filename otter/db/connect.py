import sqlite3
from typing import Any, Iterable, Tuple, Generator, Optional

from .. import log
from . import query


class Row(sqlite3.Row):
    def __repr__(self) -> str:
        return "Row({})".format(
            ", ".join([f"{key}={self[key]}" for key in self.keys()])
        )


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
        placeholder = ",".join("?" for _ in tasks)
        q = f"select * from task_attributes where id in ({placeholder}) order by id\n"
        cur.execute(q, tuple(tasks))
        return tuple(cur.fetchall())

    def child_sync_points(self, task: int) -> Tuple[Any]:
        cur = self.cursor()
        cur.execute(query.CHILD_SYNC_POINTS, (task,))
        return tuple(cur.fetchall())

    def sync_groups(
        self, task: int, debug: bool = False
    ) -> Generator[Tuple[Optional[int], Iterable[Row]], None, None]:
        """Get the sequences of child tasks synchronised during a task.

        For each sequence (group of synchronised tasks), yield a sequence
        identifier and the rows representing the synchronised tasks.
        """

        iter_sequence = iter(self.sequences(task))
        records = self.child_sync_points(task)
        if debug:
            log.debug("%d records", len(records))
        seq = next(iter_sequence, None)
        rows = []
        for row in records:
            if row["sequence"] == seq:
                rows.append(row)
            else:
                if debug:
                    log.debug("  yield %d rows", len(rows))
                yield seq, rows
                rows = []
                seq = next(iter_sequence, None)
        if rows:
            if debug:
                log.debug("  final yield %d rows", len(rows))
            yield seq, rows
        assert next(iter_sequence, "empty") == "empty"

    def sequences(self, task: int) -> Generator[Optional[int], None, None]:
        result = self.cursor().execute(query.DISTINCT_SYNC_GROUPS, (task,))
        while (row := result.fetchone()) is not None:
            yield row["sequence"]

    def count_sync_groups(self, task: int) -> int:
        result = self.cursor().execute(query.DISTINCT_SYNC_GROUPS, (task,))
        return len(result.fetchall())
