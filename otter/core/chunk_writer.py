from __future__ import annotations

from typing import Protocol, Dict, List, Tuple
import sqlite3

from otter import db

from .events import Event
from .chunks import Chunk, ChunkDict


class ChunkWriterProtocol(Protocol):
    """Capable of building the set of chunks from a trace"""

    def __len__(self) -> int: ...

    def new_chunk(self, key: int, event: Event, location_ref: int, location_count: int): ...

    def append_to_chunk(self, key: int, event: Event, location_ref: int, location_count: int) -> None: ...

    def contains(self, key: int) -> bool: ...

    def close(self): ...


class MemoryChunkWriter:
    """Builds an in-memory set of the chunks in a trace"""

    def __init__(self) -> None:
        self._chunk_dict: ChunkDict = {}

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

    def contains(self, key: int) -> bool:
        return key in self._chunk_dict
    
    def move(self):
        chunks = self._chunk_dict
        self._chunk_dict.clear()
        return chunks
    
    def close(self):
        pass


class DBChunkWriter:
    """Builds a database representation of the chunks in a trace"""

    def __init__(
        self, attributes, con: sqlite3.Connection, bufsize: int = 100
    ) -> None:
        self.con = con
        self.bufsize = bufsize
        self._attributes = attributes
        self._buffer: List[Tuple[int, int, int]] = []

    def __len__(self) -> int:
        self._flush()
        row = self.con.execute(db.scripts.count_chunks).fetchone()
        return row["num_chunks"]

    def new_chunk(self, key: int, event: Event, location_ref: int, location_count: int):
        self.append_to_chunk(key, event, location_ref, location_count)

    def append_to_chunk(
        self, key: int, event: Event, location_ref: int, location_count: int
    ) -> None:
        self._buffer.append((key, location_ref, location_count))
        if len(self._buffer) > self.bufsize:
            self._flush()

    def contains(self, key: int) -> bool:
        for k, *_ in self._buffer:
            if k == key:
                return True
        rows = self.con.execute(db.scripts.get_chunk_events, (key,)).fetchall()
        return len(rows) > 0
    
    def close(self):
        self._flush()

    def _flush(self):
        self.con.executemany(db.scripts.insert_chunk_events, self._buffer)
        self.con.commit()
        self._buffer.clear()
