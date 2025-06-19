from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from enum import Enum, auto
from pathlib import Path
from os import PathLike

from otter.log import Loggable


class Mode(Enum):
    wo = auto()  #  write-only, fail if exists
    ro = auto()  #  read-only
    rw = auto()  #  read-write, fail if not exists
    rwc = auto()  #  read-write, create if not exists


class ConnectionURI:

    def __init__(self, mode: Mode, path: str | PathLike[str]) -> None:
        self.mode = mode
        self.path = Path(path)

    def str(self) -> str:
        mode_s = Mode.rwc.name if self.mode is Mode.wo else self.mode.name
        return f"file:{self.path.as_posix()}?mode={mode_s}"

    def connect(self) -> sqlite3.Connection:
        """Return a sqlite3 connection to this URI"""
        try:
            con = sqlite3.connect(self.str(), uri=True)
        except sqlite3.OperationalError as err:
            if self.mode in [Mode.ro, Mode.rw] and not self.path.exists():
                raise FileNotFoundError(self) from None
            else:
                raise err
        return con

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
        self.root_path = root_path
        self.uri = self.get_uri(name, mode)
        if mode is Mode.wo and self.uri.path.exists():
            if not overwrite:
                raise FileExistsError(self.uri)
            else:
                self.log_warning("overwriting database: %s", self.uri)
                self.uri.path.unlink()
        self.log_debug("connect: %r", self)
        self._con = self.uri.connect()

    def get_uri(self, name: str, mode: Mode) -> ConnectionURI:
        """Return the URI of a database"""
        return ConnectionURI(mode=mode, path=self.root_path / "aux" / name)

    @property
    def tasks(self):
        return self._con

    def __repr__(self) -> str:
        return f"{type(self).__name__}(uri={self.uri.str()})"

    @abstractmethod
    def __enter__(self): ...

    @abstractmethod
    def __exit__(self, ex_type, ex, tb): ...
