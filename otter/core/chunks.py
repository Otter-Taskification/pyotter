from collections import defaultdict, deque
from functools import cached_property
from typing import List, Iterable, Deque, Optional
from itertools import islice
import igraph as ig
from loggingdecorators import on_init

import otter
# from .. import log
from ..log import logger_getter, DEBUG
from .. import definitions as defn
from .tasks import TaskRegistry, TaskSynchronisationContext, NullTask
from .events import is_event, Event
from ..utils.decorators import warn_deprecated

get_module_logger = logger_getter("chunks")


class Chunk:

    @on_init(logger=logger_getter("init_logger"), level=DEBUG)
    def __init__(self, chunk_type: defn.RegionType):
        self.log = get_module_logger()
        self._events: Deque[Event] = deque()
        self._type = chunk_type

    def __len__(self):
        return len(self._events)

    @property
    def _base_repr(self):
        return f"{self.__class__.__name__}({len(self._events)} events, self.type={self.type})"

    @property
    def _data_repr(self):
        return "\n".join(f" - {e.__repr__()}" for e in self._events)

    def __repr__(self):
        return f"{self._base_repr}\n{self._data_repr}"

    def to_text(self):
        content = [self._base_repr]
        content.extend([f" - {e}" for e in self._events])
        return content

    @property
    def first(self) -> Optional[Event]:
        return None if len(self._events) == 0 else self._events[0]

    @property
    def last(self) -> Optional[Event]:
        return None if len(self._events) == 0 else self._events[-1]

    @property
    def type(self) -> defn.RegionType:
        return self._type

    def append_event(self, event):
        self.log.debug(f"{self.__class__.__name__}.append_event {event._base_repr} to chunk: {self._base_repr}")
        self._events.append(event)
