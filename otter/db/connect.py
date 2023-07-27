import sqlite3
from typing import Any, Iterable, Tuple

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
        query = (
            f"select * from task_attributes where id in ({placeholder}) order by id\n"
        )
        cur.execute(query, tuple(tasks))
        return tuple(cur.fetchall())

    def child_sync_points(self, task: int) -> Tuple[Any]:
        cur = self.cursor()
        cur.execute(query.child_sync_points, (task,))
        return tuple(cur.fetchall())
