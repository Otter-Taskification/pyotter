from collections import deque
from ..Logging import get_logger

class Chunk:

    def __init__(self):
        self.log = get_logger(f"{self.__class__.__name__}")
        self._events = deque()
        self.log.debug(f"initialised {self}")

    def append_event(self, event):
        self.log.debug(f"Add event {event._base_repr} to chunk: {self}")
        self._events.append(event)
