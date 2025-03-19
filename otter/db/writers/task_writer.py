from typing import Dict, Optional
from sqlite3 import Connection

from otter.log import is_debug_enabled
from otter.definitions import TaskAction

from ..types import SourceLocation
from .writer_base import WriterBase
from .buffered_writers import BufferedDBWriter


class TaskMetaWriter(WriterBase):

    def __init__(
        self,
        con: Connection,
        string_id_lookup: Dict[str, int],
        bufsize: int = 1000,
    ) -> None:
        self._string_id_lookup = string_id_lookup
        self._task_meta = BufferedDBWriter(con, "task", 10, bufsize=bufsize)
        self._task_links = BufferedDBWriter(con, "task_relation", 2, bufsize=bufsize)

    def add_task_metadata(self, task: int, parent: Optional[int], label: str) -> None:
        self._task_meta.insert(
            task,
            parent,
            None,
            self._string_id_lookup[label],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        if parent is not None:
            self._task_links.insert(parent, task)

    def close(self):
        self.log_debug("closing...")
        self._task_meta.close()
        self._task_links.close()


class TaskActionWriter(WriterBase):

    def __init__(
        self, con: Connection, /, *, source: Dict[SourceLocation, int], bufsize: int = 1000
    ) -> None:
        self._source = source
        self._task_actions = BufferedDBWriter(con, "task_history", 8, bufsize=bufsize)
        self._task_suspend_meta = BufferedDBWriter(con, "task_suspend_meta", 4, bufsize=bufsize)

    def add_task_action(
        self,
        task: int,
        action: TaskAction,
        time: int,
        source_location: SourceLocation,
        /,
        *,
        location_ref: Optional[int] = None,
        location_count: Optional[int] = None,
        cpu: int,
        tid: int,
    ) -> None:
        self._task_actions.insert(
            task,
            action,
            time,
            self._source[source_location],
            location_ref,
            location_count,
            cpu,
            tid,
        )

    def add_task_suspend_meta(
        self, task: int, time: int, sync_descendants: bool, sync_mode: int
    ) -> None:
        self._task_suspend_meta.insert(
            task,
            time,
            int(sync_descendants),
            sync_mode,
        )

    def close(self):
        self.log_debug("closing...")
        self._task_actions.close()
        self._task_suspend_meta.close()


class SimTaskActionWriter(WriterBase):

    def __init__(
        self,
        con: Connection,
        /,
        *,
        sim_id: int,
        source: Dict[SourceLocation, int],
        bufsize: int = 1000,
    ) -> None:
        self._source = source
        self._sim_id = sim_id  # simulation ID
        self._task_actions = BufferedDBWriter(con, "sim_task_history", 7, bufsize=bufsize)
        self._task_suspend_meta = BufferedDBWriter(con, "sim_task_suspend_meta", 5, bufsize=bufsize)

    def add_task_action(
        self,
        task: int,
        action: TaskAction,
        time: int,
        source_location: SourceLocation,
        /,
        *,
        location_ref: Optional[int] = None,
        location_count: Optional[int] = None,
        cpu: int,
        tid: int,
    ) -> None:
        self._task_actions.insert(
            self._sim_id,
            task,
            action,
            time,
            self._source[source_location],
            cpu,
            tid,
        )

    def add_task_suspend_meta(
        self, task: int, time: int, sync_descendants: bool, sync_mode: int
    ) -> None:
        self._task_suspend_meta.insert(
            self._sim_id,
            task,
            time,
            int(sync_descendants),
            sync_mode,
        )

    def close(self):
        self.log_debug("closing...")
        self._task_actions.close()
        self._task_suspend_meta.close()

    @classmethod
    def clear_sim(cls, con: Connection, sim_id: int):
        if is_debug_enabled():
            (rows_deleted_hist,) = con.execute(
                "select count(*) from sim_task_history where sim_id = ?;", (sim_id,)
            )
            cls.log_debug(
                "deleting %d rows from sim_task_history where sim_id = %d",
                rows_deleted_hist,
                sim_id,
            )
        con.execute("delete from sim_task_history where sim_id = ?;", (sim_id,))
        if is_debug_enabled():
            (rows_deleted_meta,) = con.execute(
                "select count(*) from sim_task_suspend_meta where sim_id = ?;", (sim_id,)
            )
            cls.log_debug(
                "deleting %d rows from sim_task_suspend_meta where sim_id = %d",
                rows_deleted_meta,
                sim_id,
            )
        con.execute("delete from sim_task_suspend_meta where sim_id = ?;", (sim_id,))
        con.commit()


class SimTaskActionDummyWriter(WriterBase):

    def add_task_action(
        self,
        task: int,
        action: TaskAction,
        time: int,
        source_location: SourceLocation,
        /,
        *,
        location_ref: Optional[int] = None,
        location_count: Optional[int] = None,
        cpu: int,
        tid: int,
    ) -> None:
        print(
            task,
            action,
            time,
            source_location,
            cpu,
            tid,
            sep=","
        )

    def add_task_suspend_meta(
        self, task: int, time: int, sync_descendants: bool, sync_mode: int
    ) -> None:
        print(
            task,
            time,
            int(sync_descendants),
            sync_mode,
            sep=","
        )

    def close(self):
        pass
