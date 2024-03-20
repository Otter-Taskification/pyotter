from typing import List, Tuple

import sqlite3

import otter.log

__TABLE__ = "critical_task"

TABLE_EXISTS = f"select name from sqlite_master where name = '{__TABLE__}';"

CREATE_TABLE = f"""
create table {__TABLE__}(
    id int unique not null,
    sequence int not null,
    critical_child int not null,
    primary key (id, sequence)
    foreign key (id) references task (id)
)
"""

COUNT_ROWS = f"select count(*) as rows from {__TABLE__};"


class CritTaskWriter:

    def __init__(self, con: sqlite3.Connection, bufsize: int = 1000) -> None:
        prefix = f"[{self.__class__.__name__}]"
        self.debug = otter.log.log_with_prefix(prefix, otter.log.debug)
        self.info = otter.log.log_with_prefix(prefix, otter.log.info)
        self.con = con
        self._bufsize = bufsize
        self._buffer: List[Tuple[int, int, int]] = []
        self.debug("prepare to write critical task data")
        if self.con.execute(TABLE_EXISTS).fetchone():
            otter.log.warning("overwriting critical task data")
            self.con.execute(f"drop table {__TABLE__}")
        self.con.execute(CREATE_TABLE)

    def record_critical_task(self, task: int, sequence: int, critical_child: int):
        self.debug(f"got: {task=}, {sequence=}, {critical_child=}")
        self._buffer.append((task, sequence, critical_child))
        if len(self._buffer) >= self._bufsize:
            self._flush()

    def close(self):
        self._flush()
        if otter.log.is_info_enabled():
            rows = self.con.execute(COUNT_ROWS).fetchone()["rows"]
            self.info(f"{__TABLE__} contains %d rows", rows)

    def _flush(self):
        self.debug(f"write {len(self._buffer)} records")
        self.con.executemany(f"insert into {__TABLE__} values(?,?,?);", self._buffer)
        self.con.commit()
        self._buffer.clear()
