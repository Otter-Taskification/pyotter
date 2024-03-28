from typing import Dict, Optional

from otter.definitions import SourceLocation, TaskAction

from .connect import Connection
from .buffered_writers import BufferedDBWriter


class DBTaskMetaWriter:

    def __init__(
        self,
        con: Connection,
        string_id_lookup: Dict[str, int],
        bufsize: int = 1000,
        overwrite: bool = True,
    ) -> None:
        self._string_id_lookup = string_id_lookup
        self._task_meta = BufferedDBWriter(
            con, "task", 3, bufsize=bufsize, overwrite=overwrite
        )
        self._task_links = BufferedDBWriter(
            con, "task_relation", 2, bufsize=bufsize, overwrite=overwrite
        )

    def add_task_metadata(
        self, task: int, parent: Optional[int], label: str, flavour: int = -1
    ) -> None:
        self._task_meta.insert(task, flavour, self._string_id_lookup[label])
        if parent is not None:
            self._task_links.insert(parent, task)

    def close(self):
        self._task_meta.close()
        self._task_links.close()


class DBTaskActionWriter:

    def __init__(
        self,
        con: Connection,
        source_location_id: Dict[SourceLocation, int],
        bufsize: int = 1000,
        overwrite: bool = True,
    ) -> None:
        self._source_location_id = source_location_id
        self._task_actions = BufferedDBWriter(
            con, "task_history_multi", 4, bufsize=bufsize, overwrite=overwrite
        )
        self._task_actions_unique = BufferedDBWriter(
            con, "task_history_unique", 4, bufsize=bufsize, overwrite=overwrite
        )

    def add_task_action(
        self,
        task: int,
        action: TaskAction,
        time: str,
        location: SourceLocation,
        unique: bool = False,
    ) -> None:
        writer = self._task_actions_unique if unique else self._task_actions
        writer.insert(task, action, time, self._source_location_id[location])

    def close(self):
        self._task_actions.close()
        self._task_actions_unique.close()
