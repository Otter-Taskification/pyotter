from __future__ import annotations

from typing import List, Tuple, Dict, Optional
import sqlite3

from otter import db
from otter.definitions import SourceLocation, TaskAction

class DBTaskBuilder:
    """Builds a DB representation of the tasks in a trace"""

    def __init__(self, con: sqlite3.Connection, source_location_id: Dict[SourceLocation, int], string_id: Dict[str, int], bufsize: int = 1000) -> None:
        self.con = con
        self.bufsize = bufsize
        self._source_location_id = source_location_id
        self._string_id = string_id
        self._task_meta: List[Tuple[int, int, int]] = []
        self._task_links: List[Tuple[int, int]] = []
        self._task_actions: List[Tuple[int, int, str, int]] = []
        self._task_actions_unique: List[Tuple[int, int, str, int]] = []

    def add_task_metadata(self, task: int, parent: Optional[int], label: str, flavour: int = -1) -> None:
        self._task_meta.append((task, flavour, self._string_id[label]))
        if parent is not None:
            self._task_links.append((parent, task))
        if self._size >= self.bufsize:
            self._flush()

    def add_task_action(self, task: int, action: TaskAction, time: str, location: SourceLocation, unique: bool = False) -> None:
        self._task_actions.append((task, action, time, self._source_location_id[location]))
        if self._size >= self.bufsize:
            self._flush()

    @property
    def _size(self):
        return len(self._task_meta) + len(self._task_links) + len(self._task_actions) + len(self._task_actions_unique)

    def _flush(self):
        if self._task_meta:
            self.con.executemany(db.scripts.insert_tasks, self._task_meta)
            self._task_meta.clear()
        if self._task_links:
            self.con.executemany(db.scripts.insert_task_relations, self._task_links)
            self._task_links.clear()
        if self._task_actions:
            self.con.executemany(
                "insert into task_history_multi values(?,?,?,?);",
                self._task_actions,
            )
            self._task_actions.clear()
        if self._task_actions_unique:
            self.con.executemany(
                "insert into task_history_unique values(?,?,?,?);",
                self._task_actions_unique,
            )
            self._task_actions_unique.clear()
        self.con.commit()

    def close(self) -> None:
        self._flush()
