from __future__ import annotations

from collections import deque
from typing import Deque, Iterable, Optional

from loggingdecorators import on_init

from .. import definitions as defn
from ..log import DEBUG, logger_getter
from .events import Event

get_module_logger = logger_getter("chunks")


class Chunk:
    # @on_init(logger=logger_getter("init_logger"), level=DEBUG)
    def __init__(self, chunk_type: defn.RegionType, task_id: int):
        self.log = get_module_logger()
        self._events: Deque[Event] = deque()
        self._type = chunk_type
        self._task_id = task_id

    def __len__(self):
        return len(self._events)

    @property
    def _base_repr(self) -> str:
        return f"{self.__class__.__name__}({len(self._events)} events, self.task_id={self._task_id}, self.type={self.type})"

    @property
    def _data_repr(self) -> str:
        return "\n".join(f" - {e.__repr__()}" for e in self._events)

    def __repr__(self) -> str:
        return f"{self._base_repr}\n{self._data_repr}"

    def to_text(self) -> list[str]:
        content = [self._base_repr]
        content.extend([f" - {e}" for e in self._events])
        return content

    @property
    def header(self) -> str:
        return self._base_repr

    @property
    def task_id(self) -> int:
        return self._task_id

    @property
    def first(self) -> Optional[Event]:
        return None if len(self._events) == 0 else self._events[0]

    @property
    def last(self) -> Optional[Event]:
        return None if len(self._events) == 0 else self._events[-1]

    @property
    def events(self) -> Iterable[Event]:
        yield from self._events

    @property
    def type(self) -> defn.RegionType:
        return self._type

    def append_event(self, event):
        self.log.debug(
            "append event %s to chunk: %s",
            event,
            self._base_repr,
        )
        self._events.append(event)
