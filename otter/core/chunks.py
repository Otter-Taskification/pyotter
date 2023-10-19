from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from typing import Deque, Dict, Iterable, Optional, List, Tuple, Generator
import sqlite3

from otf2_ext import SeekingEventReader

import otter.db
from otter import log

from ..log import logger_getter
from .events import Event

get_module_logger = logger_getter("chunks")


class Chunk:
    def __init__(self):
        self.log = get_module_logger()
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
        self.log.debug(
            "append event %s to chunk: %s",
            event,
            self._base_repr,
        )
        self._events.append(event)


ChunkDict = Dict[int, Chunk]


class AbstractChunkManager(ABC):
    """Responsible for maintaining the set of chunks built from a trace"""

    @abstractmethod
    def __iter__(self) -> Generator[int, None, None]:
        ...

    @abstractmethod
    def __len__(self) -> int:
        ...

    @abstractmethod
    def new_chunk(self, key: int, event: Event, location_ref: int, location_count: int):
        ...

    @abstractmethod
    def append_to_chunk(
        self, key: int, event: Event, location_ref: int, location_count: int
    ) -> None:
        ...

    @abstractmethod
    def get_chunk(self, key: int) -> Chunk:
        ...

    @abstractmethod
    def contains(self, key: int) -> bool:
        ...

    @abstractmethod
    def close(self):
        ...


class MemoryChunkManger(AbstractChunkManager):
    """Maintains in-memory the set of chunks built from a trace"""

    def __init__(self) -> None:
        self._chunk_dict: ChunkDict = {}

    def __iter__(self) -> Generator[int, None, None]:
        yield from self._chunk_dict

    def __len__(self) -> int:
        return len(self._chunk_dict)

    def new_chunk(self, key: int, event: Event, location_ref: int, location_count: int):
        chunk = Chunk()
        self._chunk_dict[key] = chunk
        chunk.append_event(event)

    def append_to_chunk(
        self, key: int, event: Event, location_ref: int, location_count: int
    ) -> None:
        chunk = self._chunk_dict[key]
        chunk.append_event(event)

    def get_chunk(self, key: int) -> Chunk:
        return self._chunk_dict[key]

    def contains(self, key: int) -> bool:
        return key in self._chunk_dict
    
    def close(self):
        pass


class DBChunkManager(AbstractChunkManager):
    """Maintains a db-backed set of chunks built from a trace"""

    def __init__(
        self, reader: SeekingEventReader, con: sqlite3.Connection, bufsize: int = 100
    ) -> None:
        self.con = con
        self.bufsize = bufsize
        self._reader = reader
        self._buffer: List[Tuple[int, int, int]] = []

    def __iter__(self) -> Generator[int, None, None]:
        self._flush()
        rows = self.con.execute(otter.db.scripts.get_chunk_ids).fetchall()
        for row in rows:
            yield row["chunk_key"]

    def __len__(self) -> int:
        self._flush()
        row = self.con.execute(otter.db.scripts.count_chunks).fetchone()
        return row["num_chunks"]

    def new_chunk(self, key: int, event: Event, location_ref: int, location_count: int):
        self.append_to_chunk(key, event, location_ref, location_count)

    def append_to_chunk(
        self, key: int, event: Event, location_ref: int, location_count: int
    ) -> None:
        # append (key, location_ref, location_count) to the appropriate list
        self._buffer.append((key, location_ref, location_count))
        # if list is long enough, flush to DB
        if len(self._buffer) > self.bufsize:
            self._flush()

    def get_chunk(self, key: int) -> Chunk:
        # build the Chunk corresponding to the sequence of events given by a list of (location_ref, location_count) stored for the given key
        self._flush()
        rows: Iterable[Tuple[int, int]] = self.con.execute(
            otter.db.scripts.get_chunk_events, (key,)
        ).fetchall()
        chunk = Chunk()
        event_iter = (
            Event(event, self._reader.attributes) for pos, (location, event) in self._reader.events(rows)
        )
        for event in event_iter:
            chunk.append_event(event)
        return chunk

    def contains(self, key: int) -> bool:
        for k, *_ in self._buffer:
            if k == key:
                return True
        rows = self.con.execute(otter.db.scripts.get_chunk_events, (key,)).fetchall()
        return len(rows) > 0
    
    def close(self):
        self._flush()

    def _flush(self):
        # flush internal buffer to db
        self.con.executemany(otter.db.scripts.insert_chunk_events, self._buffer)
        self.con.commit()
        self._buffer.clear()
