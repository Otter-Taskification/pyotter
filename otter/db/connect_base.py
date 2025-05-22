from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from enum import Enum, auto
from pathlib import Path

from otter.log import Loggable


class Mode(Enum):
    wo = auto()  #  write-only, fail if exists
    ro = auto()  #  read-only
    rw = auto()  #  read-write, fail if not exists
    rwc = auto()  #  read-write, create if not exists


class ConnectionBase(ABC, Loggable):

    def __init__(
        self,
        root_path: Path,
        /,
        *,
        mode: Mode,
        overwrite: bool = False,
        name: str = "tasks.db",
        **kwargs,
    ) -> None:
        super().__init__()
        dbpath = root_path / "aux" / name
        mode_s = Mode.rwc.name if mode is Mode.wo else mode.name
        self._uri = f"file:{dbpath}?mode={mode_s}"
        if mode is Mode.wo and dbpath.exists():
            if not overwrite:
                raise FileExistsError(dbpath)
            else:
                self.log_warning("overwriting database: %s", dbpath)
                dbpath.unlink()
        self.log_debug("connect: %r", self)
        try:
            self._con = sqlite3.connect(self._uri, uri=True)
        except sqlite3.OperationalError as err:
            self.log_error(str(err))
            if mode in [Mode.ro, Mode.rw] and not dbpath.exists():
                raise FileNotFoundError(dbpath) from None
            else:
                raise err

    def __repr__(self) -> str:
        return f"{type(self).__name__}(uri={self._uri})"

    @abstractmethod
    def __enter__(self): ...

    @abstractmethod
    def __exit__(self, ex_type, ex, tb): ...
