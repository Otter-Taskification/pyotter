from abc import ABC, abstractmethod
from collections import deque
from typing import List, Union, Dict
from itertools import chain
from loggingdecorators import on_init
from ..utils import transpose_list_to_dict
from ..utils.decorators import warn_deprecated
from .. import log
from .. import definitions as defn
from ..reader import OTF2Reader
from ..definitions import Attr
from otf2.definitions import Attribute as OTF2Attribute
from otf2 import LocationType as OTF2Location
from otf2.events import _Event as OTF2Event

get_module_logger = log.logger_getter("events")

is_event = lambda item: isinstance(item, Event)
all_events = lambda args: all(map(is_event, args))
any_events = lambda args: any(map(is_event, args))
is_event_list = lambda args: isinstance(args, list) and all_events(args)


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


# TODO: could I remove this wrapper class entirely and replace it with a set of functions for querying an event's attributes?
# TODO: need to understand how expensive this class is to create once for each OTF2 event.
class Event:
    """A basic wrapper for OTF2 events"""

    def __init__(self, otf2_event: OTF2Event, attribute_lookup: Dict[str, OTF2Attribute]) -> None:
        self._event = otf2_event
        self._attribute_lookup = attribute_lookup

    def __repr__(self) -> str:
        # return f"{type(self).__name__}(time={self.time}, endpoint={self._event.attributes[self._attribute_lookup['endpoint']]}, type={type(self._event).__name__})"
        return f"{type(self).__name__}(time={self.time}, {', '.join(f'{name}={getattr(self, name)}' for name in self._attribute_lookup if name in self)})"

    @property
    def _base_repr(self):
        return f"{type(self).__name__}(time={self.time})"

    # TODO: could it be more efficient to setattr for all the event attributes in __init__? Need to test
    def __getattr__(self, attr: Attr):
        if attr == defn.Attr.time:
            return self._event.time
        try:
            return self._event.attributes[self._attribute_lookup[attr]]
        except KeyError:
            raise AttributeError(f"attribute '{attr}' not found") from None

    def __contains__(self, attr: Attr):
        if attr == defn.Attr.time:
            return True
        return self._attribute_lookup[attr] in self._event.attributes

    def get(self, item, default=None):
        try:
            return getattr(self, item)
        except AttributeError:
            return default

    def to_dict(self) -> Dict:
        as_dict = {name: getattr(self, name) for name in self._attribute_lookup if name in self}
        as_dict['time'] = self.time
        return as_dict

    @property
    def vertex_label(self):
        return self.unique_id

    @property
    def vertex_color_key(self):
        return self.region_type

    @property
    def vertex_shape_key(self):
        return self.vertex_color_key
