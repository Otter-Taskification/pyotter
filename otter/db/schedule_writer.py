from typing import List, Tuple

import sqlite3

import otter.log


class ScheduleWriter:

    def __init__(self, con: sqlite3.Connection, bufsize: int = 1000) -> None:
        self.debug = otter.log.log_with_prefix(
            f"[{self.__class__.__name__}]", otter.log.debug
        )
        self.con = con
        self._bufsize = bufsize
        self._buffer: List[Tuple[int, int, int]] = []
        self.debug("prepare to write task schedule data")
        exists = self.con.execute(
            "select name from sqlite_master where name = 'task_schedule';"
        ).fetchone()
        if exists:
            otter.log.warning("overwriting schedule data")
            self.con.execute("drop table task_schedule")
        self.con.execute(
            """
            create table task_schedule(
                id int unique not null,
                start_ts int not null,
                duration int not null,
            primary key (id)
            foreign key (id) references task (id)
            )
        """
        )

    def shedule_task(self, task: int, start_ts: int, duration: int):
        self.debug(f"schedule task: {task=}, {start_ts=}, {duration=}")
        self._buffer.append((task, start_ts, duration))
        if len(self._buffer) >= self._bufsize:
            self._flush()

    def close(self):
        self._flush()
        if otter.log.is_debug_enabled():
            rows = self.con.execute(
                "select count(*) as rows from task_schedule;"
            ).fetchone()["rows"]
            self.debug(f"task_schedule contains %d rows", rows)

    def _flush(self):
        self.debug(f"write {len(self._buffer)} task schedule records")
        self.con.executemany("insert into task_schedule values(?,?,?);", self._buffer)
        self.con.commit()
        self._buffer.clear()
