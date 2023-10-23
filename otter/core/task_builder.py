from __future__ import annotations

from typing import List, Tuple
import sqlite3

from otter import db


class DBTaskBuilder:
    """Builds a DB representation of the tasks in a trace"""

    def __init__(self, con: sqlite3.Connection, bufsize: int = 1000) -> None:
        self.con = con
        self.bufsize = bufsize
        self._task_meta: List[Tuple[int, int, int]] = []
        self._task_links: List[Tuple[int, int]] = []
        self._task_actions: List[Tuple[int, int, str]] = []

    def add_task_metadata(self, task: int, label: int, flavour: int = -1) -> None:
        self._task_meta.append((task, flavour, label))
        if self._size >= self.bufsize:
            self._flush()

    def add_parent_child(self, parent: int, child: int) -> None:
        self._task_links.append((parent, child))
        if self._size >= self.bufsize:
            self._flush()

    def add_task_action(self, task: int, action: int, time: str) -> None:
        self._task_actions.append((task, action, time))
        if self._size >= self.bufsize:
            self._flush()

    @property
    def _size(self):
        return len(self._task_meta) + len(self._task_links) + len(self._task_actions)

    def _flush(self):
        if self._task_meta:
            self.con.executemany(db.scripts.insert_tasks, self._task_meta)
            self._task_meta.clear()
        if self._task_links:
            self.con.executemany(db.scripts.insert_task_relations, self._task_links)
            self._task_links.clear()
        if self._task_actions:
            self.con.executemany(db.scripts.insert_task_history, self._task_actions)
            self._task_actions.clear()
        self.con.commit()

    def close(self) -> None:
        self._flush()
