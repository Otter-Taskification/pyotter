from __future__ import annotations

from collections import deque
from typing import Deque, Dict, Iterable, Optional, Tuple, Protocol

from otf2_ext.events import EventType

import otter.log
from .events import Event

ChunkDict = Dict[int, "Chunk"]


class EventSeeker(Protocol):

    def __call__(self, positions: Iterable[Tuple[int, int]], batch_size: int) -> Iterable[Tuple[int, Tuple[int, EventType]]]:
        ...


class Chunk:
    def __init__(self):
        self._events: Deque[Event] = deque()

    def __len__(self):
        return len(self._events)

    @property
    def _base_repr(self) -> str:
        return f"{self.__class__.__name__}({len(self._events)} events)"

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
    def first(self) -> Optional[Event]:
        return None if len(self._events) == 0 else self._events[0]

    @property
    def last(self) -> Optional[Event]:
        return None if len(self._events) == 0 else self._events[-1]

    @property
    def events(self) -> Iterable[Event]:
        yield from self._events

    def append_event(self, event):
        otter.log.debug(
            "append event %s to chunk: %s",
            event,
            self._base_repr,
        )
        self._events.append(event)
