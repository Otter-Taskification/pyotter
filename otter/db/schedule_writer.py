from typing import List, Tuple

import sqlite3

import otter.log

__TABLE__ = "task_schedule"

TABLE_EXISTS = f"select name from sqlite_master where name = '{__TABLE__}';"

CREATE_TABLE = f"""
create table {__TABLE__}(
    id int unique not null,
    start_ts int not null,
    duration int not null,
    primary key (id)
    foreign key (id) references task (id)
)
"""

COUNT_ROWS = f"select count(*) as rows from {__TABLE__};"


class ScheduleWriter:

    def __init__(self, con: sqlite3.Connection, bufsize: int = 1000) -> None:
        prefix = f"[{self.__class__.__name__}]"
        self.debug = otter.log.log_with_prefix(prefix, otter.log.debug)
        self.info = otter.log.log_with_prefix(prefix, otter.log.info)
        self.con = con
        self._bufsize = bufsize
        self._buffer: List[Tuple[int, int, int]] = []
        self.debug("prepare to write task schedule data")
        if self.con.execute(TABLE_EXISTS).fetchone():
            otter.log.warning("overwriting schedule data")
            self.con.execute(f"drop table {__TABLE__}")
        self.con.execute(CREATE_TABLE)

    def shedule_task(self, task: int, start_ts: int, duration: int):
        self.debug(f"got: {task=}, {start_ts=}, {duration=}")
        self._buffer.append((task, start_ts, duration))
        if len(self._buffer) >= self._bufsize:
            self._flush()

    def close(self):
        self._flush()
        if otter.log.is_info_enabled():
            rows = self.con.execute(COUNT_ROWS).fetchone()["rows"]
            self.info(f"{__TABLE__} contains %d rows", rows)

    def _flush(self):
        self.debug(f"write {len(self._buffer)} task schedule records")
        self.con.executemany("insert into task_schedule values(?,?,?);", self._buffer)
        self.con.commit()
        self._buffer.clear()
