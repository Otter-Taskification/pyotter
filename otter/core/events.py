from __future__ import annotations

from collections import deque
from typing import Dict

from loggingdecorators import on_init
from otf2 import LocationType as OTF2Location
from otf2.definitions import Attribute as OTF2Attribute
from otf2.events import _Event as OTF2Event
from otf2.events import BufferFlush

from .. import log
from ..definitions import Attr

get_module_logger = log.logger_getter("events")


class Location:
    # NOTE: Responsible for recording its traversal into & out of parallel regions

    @on_init(logger=log.logger_getter("init_logger"), level=log.DEBUG)
    def __init__(self, location: OTF2Location):
        self.log = log.get_logger(self.__class__.__name__)
        self._loc = location
        self.parallel_region_deque = deque()

    def __repr__(self):
        return f"{self.__class__.__name__}(location={self._loc.name})"

    @property
    def name(self):
        return self._loc.name

    @property
    def current_parallel_region(self):
        return self.parallel_region_deque[-1]

    def enter_parallel_region(self, id: int):
        self.log.debug(f"{self} entered parallel region {id}")
        self.parallel_region_deque.append(id)

    def leave_parallel_region(self):
        self.log.debug(f"{self} exited parallel region {self.current_parallel_region}")
        self.parallel_region_deque.pop()


class Event:
    """A basic wrapper for OTF2 events"""

    def __init__(
        self, otf2_event: OTF2Event, attribute_lookup: Dict[str, OTF2Attribute]
    ) -> None:
        self._event = otf2_event
        self._attribute_lookup = attribute_lookup
        if self._event.attributes is None:
            log.warning(
                "%s event attributes is None: %s",
                str(type(otf2_event)),
                str(otf2_event),
            )

    def __repr__(self) -> str:
        data = {}
        if self._event.attributes is not None:
            for attr_name, attr in self._attribute_lookup.items():
                if attr in self._event.attributes:
                    data[attr_name] = self._event.attributes[attr]
        return (
            f"{type(self).__name__}"
            + f"(time={self.time}, "
            + f"{', '.join(f'{name}={value}' for name, value in data.items())})"
        )

    def __getattr__(self, attr_name: str):
        if attr_name == Attr.time:
            return self._event.time
        if self._event.attributes is None:
            raise RuntimeError("otf2 event attributes not found")
        attr = self._attribute_lookup[attr_name]
        try:
            return self._event.attributes[attr]
        except KeyError:
            raise AttributeError(f"attribute '{attr_name}' not found") from None

    def get(self, item, default=None):
        try:
            return getattr(self, item)
        except AttributeError:
            return default

    def is_buffer_flush_event(self) -> bool:
        return isinstance(self._event, BufferFlush)
