from collections import deque
from ..Logging import get_logger

class Chunk:

    def __init__(self):
        self.log = get_logger(f"{self.__class__.__name__}")
        self._events = deque()
        self.log.debug(f"initialised {self}")

    @property
    def _base_repr(self):
        return f"{self.__class__.__name__}({len(self._events)} events)"

    @property
    def _data_repr(self):
        return "\n - ".join(e.__repr__() for e in self._events)

    def __repr__(self):
        return f"{self._base_repr}\n - {self._data_repr}"

    def append_event(self, event):
        self.log.debug(f"Add event {event._base_repr} to chunk: {self}")
        self._events.append(event)
