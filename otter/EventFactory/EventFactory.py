from collections import deque
from .. import log
from ..log.levels import DEBUG, INFO, WARN, ERROR
from .events import _Event
from . import events
from ..types import OTF2Reader, OTF2Event, AttrDict
from ..definitions import EventType, RegionType, Attr
from loggingdecorators import on_init

get_module_logger = log.logger_getter("events")

event_class_lookup = {
    EventType.thread_begin:    events.ThreadBegin,
    EventType.thread_end:      events.ThreadEnd,
    EventType.parallel_begin:  events.ParallelBegin,
    EventType.parallel_end:    events.ParallelEnd,
    EventType.workshare_begin: events.WorkshareBegin,
    EventType.workshare_end:   events.WorkshareEnd,
    EventType.sync_begin:      events.SyncBegin,
    EventType.sync_end:        events.SyncEnd,
    EventType.master_begin:    events.Master,
    EventType.master_end:      events.Master,
    EventType.task_enter:      events.TaskEnter,
    EventType.task_leave:      events.TaskLeave,
    EventType.task_create:     events.TaskCreate,
    EventType.task_schedule:   events.TaskSchedule,
    EventType.task_switch:     events.TaskSwitch
}

region_event_class_lookup = {
    (RegionType.initial_task,    EventType.task_enter):       events.InitialTaskEnter,
    (RegionType.initial_task,    EventType.task_leave):       events.InitialTaskLeave,
    (RegionType.implicit_task,   EventType.task_enter):       events.ImplicitTaskEnter,
    (RegionType.implicit_task,   EventType.task_leave):       events.ImplicitTaskLeave,
    (RegionType.single_executor, EventType.workshare_begin):  events.SingleBegin,
    (RegionType.single_executor, EventType.workshare_end):    events.SingleEnd,
    (RegionType.master,          EventType.master_begin):     events.MasterBegin,
    (RegionType.master,          EventType.master_end):       events.MasterEnd,
    (RegionType.taskgroup,       EventType.sync_begin):       events.TaskgroupBegin,
    (RegionType.taskgroup,       EventType.sync_end):         events.TaskgroupEnd
}

class Location:

    @on_init(logger=log.logger_getter("init_logger"), level=DEBUG)
    def __init__(self, location):
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


class EventFactory:

    @on_init(logger=log.logger_getter("init_logger"))
    def __init__(self, r: OTF2Reader, default_cls: type=None, log_event_construction=DEBUG):
        if default_cls is not None and not issubclass(default_cls, _Event):
            raise TypeError(f"arg {default_cls=} is not subclass of events._Event")
        self.log = get_module_logger()
        self.default_cls = default_cls
        self.attr = {attr.name: attr for attr in r.definitions.attributes}
        self.location_registry = dict() #{location: Location(location) for location in r.definitions.locations}
        for location in r.definitions.locations:
            self.location_registry[location] = Location(location)
            self.log.debug(f"got location: {location.name}(events={location.number_of_events}, type={location.type})")
        self.events = r.events

    def __repr__(self):
        return f"{self.__class__.__name__}(default_cls={self.default_cls})"

    def __iter__(self) -> events._Event:
        self.log.debug(f"generating events from {self.events}")
        for k, (location, event) in enumerate(self.events):
            cls = self.get_class(
                event.attributes[self.attr[Attr.event_type]],
                event.attributes.get(self.attr[Attr.region_type],None)
            )
            self.log.debug(f"making event {k} {cls=}")
            yield cls(event, self.location_registry[location], self.attr)

    def get_class(self, event_type: EventType, region_type: RegionType) -> type:
        try:
            return region_event_class_lookup[(region_type, event_type)]
        except KeyError:
            pass # fallback to event_class_lookup
        try:
            return event_class_lookup[event_type]
        except KeyError:
            # no class found in either dict
            if self.default_cls is not None:
                return self.default_cls
            else:
                raise TypeError(
                    f"{self.__class__.__name__} can't construct event of type '{event_type}' for {region_type} region")
