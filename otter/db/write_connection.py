from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
from typing import Tuple, Dict

from otter.utils import LabellingDict

from .connect_base import Mode, ConnectionBase
from .scripts import scripts
from .types import SourceLocation
from .protocols import (
    TaskMetaCallback,
    TaskActionCallback,
    TaskSuspendMetaCallback,
)
from .writers import TaskActionWriter, TaskMetaWriter, SourceLocationWriter, StringDefinitionWriter


class WriteConnection(ConnectionBase):

    def __init__(self, root_path: Path, /, *, views: bool = False, overwrite: bool = False, memory: bool = False) -> None:
        if memory:
            super().__init__(root_path, mode=Mode.memory)
        else:
            super().__init__(root_path, mode=Mode.wo, overwrite=overwrite)
        self.views = views
        source_location_id: Dict[SourceLocation, int] = LabellingDict()
        string_id: Dict[str, int] = LabellingDict()
        self._task_meta_writer = TaskMetaWriter(self._con, string_id, bufsize=1000000)
        self._action_writer = TaskActionWriter(
            self._con, source=source_location_id, bufsize=1000000
        )
        self._exit = ExitStack()
        # Must push string writer before source writer so all strings have been seen when string writer closed
        self._exit.enter_context(StringDefinitionWriter(self._con, string_id))
        self._exit.enter_context(SourceLocationWriter(self._con, string_id, source_location_id))
        self._exit.enter_context(self._task_meta_writer)
        self._exit.enter_context(self._action_writer)

    def __enter__(self) -> Tuple[TaskMetaCallback, TaskActionCallback, TaskSuspendMetaCallback]:
        self.log_info(" -- create tables")
        self._con.executescript(scripts["create_tables"])
        self.log_info(" -- create indexes")
        self._con.executescript(scripts["create_indexes"])
        if self.views:
            self.log_info(" -- create views")
            self._con.executescript(scripts["create_views"])
        return (
            self._task_meta_writer.add_task_metadata,
            self._action_writer.add_task_action,
            self._action_writer.add_task_suspend_meta,
        )

    def __exit__(self, ex_type, ex, tb):
        if ex_type is None:
            self.log_info(" -- close writers")
            self._exit.close()
            self.log_info(" -- update task locations and timestamps")
            self._con.executescript(scripts["update_task_locations_times"])
            self.log_info(" -- count children")
            self._con.executescript(scripts["update_task_num_children"])
            self.log_info(" -- update source location definitions")
            self._con.executescript(scripts["update_source_info"])
            return True
        else:
            self.log_error(f"database not finalised due to unhandled {ex_type.__name__} exception")
            return False
