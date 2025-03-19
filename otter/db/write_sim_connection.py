from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
from typing import Tuple

from .connect_base import Mode, ConnectionBase
from .read_connection import ReadConnection
from .protocols import TaskActionCallback, TaskSuspendMetaCallback, CriticalTaskCallback
from .writers import SimTaskActionWriter, CritTaskWriter, SimTaskActionDummyWriter


class WriteSimConnection(ConnectionBase):
    """Manages writing a simulated schedule to a database"""

    def __init__(self, root_path: Path, /, *args, dummy: bool = False, **kwargs) -> None:
        super().__init__(root_path, mode=Mode.rw)  # rw: fail if not found
        self._root_path = root_path
        self._exit = ExitStack()
        self._dummy = dummy

    def clear_sim(self, sim_id: int):
        SimTaskActionWriter.clear_sim(self._con, sim_id)

    def __enter__(self) -> Tuple[CriticalTaskCallback, TaskActionCallback, TaskSuspendMetaCallback]:
        # Construct writers using data from the native trace data
        # Do this lazily in case simulations were read/deleted since __init__
        reader = ReadConnection(self._root_path)
        source_location_id = {src: src_id for src_id, src in reader.get_all_source_locations()}
        num_simulations: int = reader.count_simulations()
        self._sim_id = num_simulations
        crit_task_writer = CritTaskWriter(self._con, sim_id=self._sim_id, bufsize=1000000)
        if self._dummy:
            action_writer = SimTaskActionDummyWriter()
        else:
            action_writer = SimTaskActionWriter(
                self._con, sim_id=self._sim_id, source=source_location_id, bufsize=1000000
            )
        self.log_debug(f"{action_writer=}")
        self._exit.enter_context(action_writer)
        self._exit.enter_context(crit_task_writer)
        return (
            crit_task_writer.insert,
            action_writer.add_task_action,
            action_writer.add_task_suspend_meta,
        )

    def __exit__(self, ex_type, ex, tb):
        if ex_type is None:
            self.log_info(" -- close writers")
            self._exit.close()
            return True
        else:
            self.log_error(f"database not finalised due to unhandled {ex_type.__name__} exception")
            return False
