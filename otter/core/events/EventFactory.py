from collections import deque
import loggingdecorators as logdec
from ... import log
from ... import definitions as defn
from . import events

get_module_logger = log.logger_getter("events")

event_class_lookup = {
    defn.EventType.thread_begin:    events.ThreadBegin,
    defn.EventType.thread_end:      events.ThreadEnd,
    defn.EventType.parallel_begin:  events.ParallelBegin,
    defn.EventType.parallel_end:    events.ParallelEnd,
    defn.EventType.workshare_begin: events.WorkshareBegin,
    defn.EventType.workshare_end:   events.WorkshareEnd,
    defn.EventType.sync_begin:      events.SyncBegin,
    defn.EventType.sync_end:        events.SyncEnd,
    defn.EventType.master_begin:    events.Master,
    defn.EventType.master_end:      events.Master,
    defn.EventType.task_enter:      events.TaskEnter,
    defn.EventType.task_leave:      events.TaskLeave,
    defn.EventType.task_create:     events.TaskCreate,
    defn.EventType.task_schedule:   events.TaskSchedule,
    defn.EventType.task_switch:     events.TaskSwitch
}

region_event_class_lookup = {
    (defn.RegionType.initial_task,    defn.EventType.task_enter):       events.InitialTaskEnter,
    (defn.RegionType.initial_task,    defn.EventType.task_leave):       events.InitialTaskLeave,
    (defn.RegionType.implicit_task,   defn.EventType.task_enter):       events.ImplicitTaskEnter,
    (defn.RegionType.implicit_task,   defn.EventType.task_leave):       events.ImplicitTaskLeave,
    (defn.RegionType.single_executor, defn.EventType.workshare_begin):  events.SingleBegin,
    (defn.RegionType.single_executor, defn.EventType.workshare_end):    events.SingleEnd,
    (defn.RegionType.master,          defn.EventType.master_begin):     events.MasterBegin,
    (defn.RegionType.master,          defn.EventType.master_end):       events.MasterEnd,
    (defn.RegionType.taskgroup,       defn.EventType.sync_begin):       events.TaskgroupBegin,
    (defn.RegionType.taskgroup,       defn.EventType.sync_end):         events.TaskgroupEnd
}

class Location:

    @logdec.on_init(logger=log.logger_getter("init_logger"), level=log.DEBUG)
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

    @logdec.on_init(logger=log.logger_getter("init_logger"))
    def __init__(self, reader, default_cls: type=None):
        if default_cls is not None and not issubclass(default_cls, _Event):
            raise TypeError(f"arg {default_cls=} is not subclass of events._Event")
        self.log = get_module_logger()
        self.default_cls = default_cls
        self.attr = {attr.name: attr for attr in reader.definitions.attributes}
        self.location_registry = dict()
        for location in reader.definitions.locations:
            self.location_registry[location] = Location(location)
            self.log.debug(f"got location: {location.name}(events={location.number_of_events}, type={location.type})")
        self.events = reader.events

    def __repr__(self):
        return f"{self.__class__.__name__}(default_cls={self.default_cls})"

    def __iter__(self) -> events._Event:
        self.log.debug(f"generating events from {self.events}")
        for k, (location, event) in enumerate(self.events):
            cls = self.get_class(
                event.attributes[self.attr[defn.Attr.event_type]],
                event.attributes.get(self.attr[defn.Attr.region_type],None)
            )
            self.log.debug(f"making event {k} {cls=}")
            yield cls(event, self.location_registry[location], self.attr)

    def get_class(self, event_type: defn.EventType, region_type: defn.RegionType) -> type:
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
