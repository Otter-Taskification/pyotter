from __future__ import annotations

from typing import Protocol, Tuple, Generator, Iterable
import sqlite3

from otf2_ext.events import EventType

from otter import db

from .events import Event
from .chunks import Chunk


class SeekEventsCallback(Protocol):

    def __call__(self, positions: Iterable[Tuple[int, int]], batch_size: int = 100) -> Iterable[Tuple[int, Tuple[int, EventType]]]:
        ...



class ChunkReaderProtocol(Protocol):
    """Responsible for reading or re-constructing the chunks of a trace"""

    def __iter__(self) -> Generator[int, None, None]:
        ...

    def __len__(self) -> int:
        ...

    def items(self) -> Iterable[Tuple[int, Chunk]]:
        ...

    @property
    def chunks(self) -> Iterable[Chunk]:
        ...

    def get_chunk(self, key: int) -> Chunk:
        ...

    def contains(self, key: int) -> bool:
        ...


class DBChunkReader:
    """Read chunks from a database"""

    def __init__(
        self, attributes, seek_events: SeekEventsCallback, con: sqlite3.Connection, bufsize: int = 100
    ) -> None:
        self.con = con
        self.bufsize = bufsize
        self._attributes = attributes
        self._seek_events = seek_events

    def __iter__(self) -> Generator[int, None, None]:
        rows = self.con.execute(db.scripts.get_chunk_ids).fetchall()
        for row in rows:
            yield row["chunk_key"]

    def __len__(self) -> int:
        row = self.con.execute(db.scripts.count_chunks).fetchone()
        return row["num_chunks"]

    def items(self) -> Iterable[Tuple[int, Chunk]]:
        for key in iter(self):
            yield key, self.get_chunk(key)

    @property
    def chunks(self) -> Iterable[Chunk]:
        for _, chunk in self.items():
            yield chunk

    def get_chunk(self, key: int) -> Chunk:
        rows: Iterable[Tuple[int, int]] = self.con.execute(db.scripts.get_chunk_events, (key,)).fetchall()
        chunk = Chunk()
        for pos, (location, event) in self._seek_events(rows):
            chunk.append_event(Event(event, self._attributes))
        return chunk

    def contains(self, key: int) -> bool:
        rows = self.con.execute(db.scripts.get_chunk_events, (key,)).fetchall()
        return len(rows) > 0
    