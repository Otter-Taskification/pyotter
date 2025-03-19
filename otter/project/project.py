from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path

from otter.log import Loggable
import otter.db
import otter.simulator


class ProjectBase(ABC, Loggable):
    """Prepare to use an anchorfile as input"""

    def __init__(self, anchorfile: str, /) -> None:
        self.log_debug("using project: %s", self)
        self._anchorfile = Path(anchorfile).expanduser().resolve()
        aux_dir = self.project_root / "aux"
        maps_file = aux_dir / "maps"

        if not self.project_root.is_dir():
            self.log_error("project directory not found: %s", self.project_root)
            raise NotADirectoryError(str(self.project_root))
        if not aux_dir.is_dir():
            self.log_error("directory not found: %s", aux_dir)
            raise NotADirectoryError(str(aux_dir))
        if not maps_file.is_file():
            self.log_error("no such file: %s", maps_file)
            raise FileNotFoundError(str(maps_file))

        self.log_info("project root:  %s", self.project_root)
        self.log_info("anchorfile:    %s", self._anchorfile)

    @property
    def anchorfile(self):
        return self._anchorfile

    @property
    def project_root(self):
        return self._anchorfile.parent

    def abspath(self, relname: str):
        """Get the absolute path of an internal folder"""
        return os.path.abspath(os.path.join(self.project_root, relname))

    @abstractmethod
    def connect(self, /, **kwargs): ...


class UnpackTraceData(ProjectBase):

    def connect(self, /, overwrite: bool):
        return otter.db.WriteConnection(self.project_root, overwrite=overwrite)


class ReadTraceData(ProjectBase):
    """Read data from an unpacked trace"""

    def connect(self, /):
        """Return a connection to this project's tasks db"""
        return otter.db.ReadConnection(self.project_root)


class SimulateTrace(ProjectBase):
    """Read native trace data and write a simulated schedule"""

    def __init__(self, anchorfile: str) -> None:
        super().__init__(anchorfile)
        self._reader = otter.db.ReadConnection(self.project_root)

    @property
    def reader(self):
        return self._reader

    def connect(self, /, dummy: bool = False):
        return otter.db.WriteSimConnection(Path(self.project_root), dummy=dummy)
