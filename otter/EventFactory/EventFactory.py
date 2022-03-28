from collections import defaultdict, deque
from .events import _Event
from . import events
from ..utils import PrettyCounter
from ..types import OTF2Reader, OTF2Event, AttrDict
from ..definitions import EventType, RegionType, Attr
from ..Logging import get_logger

log = get_logger(f"{__name__}")

class_map = {
    EventType.thread_begin:    events.ThreadBegin,
    EventType.thread_end:      events.ThreadEnd,
    EventType.parallel_begin:  events.ParallelBegin,
    EventType.parallel_end:    events.ParallelEnd,
    EventType.workshare_begin: events.WorkshareBegin,
    EventType.workshare_end:   events.WorkshareEnd,
    EventType.sync_begin:      events.Sync,
    EventType.sync_end:        events.Sync,
    EventType.master_begin:    events.Master,
    EventType.master_end:      events.Master,
    EventType.task_enter:      events.TaskEnter,
    EventType.task_leave:      events.TaskLeave,
    EventType.task_create:     events.TaskCreate,
    EventType.task_schedule:   events.TaskSchedule,
    EventType.task_switch:     events.TaskSwitch
}

class Location:

    def __init__(self, location):
        self.log = get_logger(f"{self.__class__.__name__}")
        self._loc = location
        self.parallel_region_deque = deque()
        self.log.debug(f"initialised {self}")

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


class EventFactory:

    def __init__(self, r: OTF2Reader, default_cls: type=None):
        if default_cls is not None and not issubclass(default_cls, _Event):
            raise TypeError(f"arg {default_cls=} is not subclass of events._Event")
        self.log = get_logger(f"{self.__class__.__name__}")
        self.default_cls = default_cls
        self.attr = {attr.name: attr for attr in r.definitions.attributes}
        self.location_registry = {location: Location(location) for location in r.definitions.locations}
        self.events = r.events
        self.log.debug(f"initialised {self}")

    def __repr__(self):
        return f"{self.__class__.__name__}(default_cls={self.default_cls})"

    def __iter__(self) -> events._Event:
        self.log.debug(f"iterating over events")
        for k, (location, event) in enumerate(self.events):
            constructor = self.get_class(
                event.attributes[self.attr[Attr.event_type]],
                event.attributes.get(self.attr[Attr.region_type], None)
            )
            self.log.debug(f"got event {k}:{event} with {constructor=}")
            yield constructor(event, self.location_registry[location], self.attr)

    def get_class(self, event_type: EventType, region_type: RegionType) -> type:
        if event_type == EventType.task_switch:
            return events.TaskSwitch
        elif region_type == RegionType.initial_task:
            if event_type == EventType.task_enter:
                return events.InitialTaskEnter
            elif event_type == EventType.task_leave:
                return events.InitialTaskLeave
            else:
                pass
        elif region_type == RegionType.implicit_task:
            if event_type == EventType.task_enter:
                return events.ImplicitTaskEnter
            elif event_type == EventType.task_leave:
                return events.ImplicitTaskLeave
            else:
                pass
        elif region_type in [RegionType.single_executor, RegionType.master]:
            if event_type == EventType.workshare_begin:
                return events.SingleBegin if region_type == RegionType.single_executor else events.MasterBegin
            elif event_type == EventType.workshare_end:
                return events.SingleEnd if region_type == RegionType.single_executor else events.MasterEnd
            else:
                pass
        elif event_type in class_map:
            return class_map[event_type]

        if self.default_cls is not None:
            return self.default_cls
        else:
            raise TypeError(
                f"{self.__class__.__name__} can't construct event of type '{event_type}' for {region_type} region")
