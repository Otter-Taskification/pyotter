from typing import List, Tuple, Any

import otter.log

from otter.definitions import TaskAction

from .connect import Connection


class BufferedDBWriter:

    def __init__(
        self,
        con: Connection,
        table: str,
        nargs: int,
        bufsize: int,
        overwrite: bool,
    ) -> None:
        placeholder = ",".join("?" * nargs)
        self.sql_insert_row = f"insert into {table} values({placeholder});"
        self.sql_count_rows = f"select count(*) as rows from {table};"
        prefix = f"[{self.__class__.__name__}({table=})]"
        self.debug = otter.log.log_with_prefix(prefix, otter.log.debug)
        self.info = otter.log.log_with_prefix(prefix, otter.log.info)
        self.con = con
        self._bufsize = bufsize
        self._buffer: List[Tuple[Any]] = []
        if overwrite:
            self.debug(f"delete from {table}")
            self.con.execute(f"delete from {table};")

    def insert(self, *args: Any):
        self._buffer.append(args)
        if len(self._buffer) >= self._bufsize:
            self._flush()

    def close(self):
        self._flush()
        if otter.log.is_debug_enabled():
            rows = self.con.execute(self.sql_count_rows).fetchone()["rows"]
            self.debug("contains %d rows", rows)

    def _flush(self):
        self.debug(f"write {len(self._buffer)} records")
        self.con.executemany(self.sql_insert_row, self._buffer)
        self.con.commit()
        self._buffer.clear()


class CritTaskWriter(BufferedDBWriter):

    def __init__(
        self, con: Connection, bufsize: int = 1000, overwrite: bool = True
    ) -> None:
        super().__init__(con, "critical_task", 3, bufsize, overwrite)

    def insert(self, task: int, sequence: int, critical_child: int, /, *args):
        return super().insert(task, sequence, critical_child)


class ScheduleWriter:

    def __init__(
        self, con: Connection, bufsize: int = 1000, overwrite: bool = True
    ) -> None:
        self._writer_unique = BufferedDBWriter(
            con, "_sim_task_history_unique", 4, bufsize=1000, overwrite=True
        )
        self._writer_multi = BufferedDBWriter(
            con, "_sim_task_history_multi", 4, bufsize=1000, overwrite=True
        )

    def insert(self, task: int, action: TaskAction, event_ts: int, /, *args):
        if action in (TaskAction.CREATE, TaskAction.START, TaskAction.END):
            self._writer_unique.insert(task, action.value, event_ts, -1)
        else:
            self._writer_multi.insert(task, action.value, event_ts, -1)

    def close(self):
        self._writer_multi.close()
        self._writer_unique.close()
