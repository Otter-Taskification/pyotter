from abc import ABC, abstractmethod
from collections import defaultdict, deque
from typing import Union, List, NewType
import itertools as it
import loggingdecorators as logdec
from .. import log
from .. import utils
from .. import definitions as defn

get_module_logger = log.logger_getter("events")

is_event = lambda item: isinstance(item, _Event)
all_events = lambda args: all(map(is_event, args))
any_events = lambda args: any(map(is_event, args))
is_event_list = lambda args: isinstance(args, list) and all_events(args)


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
            raise TypeError(f"arg {default_cls=} is not subclass of _Event")
        self.log = get_module_logger()
        self.default_cls = default_cls
        self.attr = {attr.name: attr for attr in reader.definitions.attributes}
        for key, value in self.attr.items():
            self.log.debug(f"got attribute: {key}={value}")
        self.location_registry = dict()
        for location in reader.definitions.locations:
            self.location_registry[location] = Location(location)
            self.log.debug(f"got location: {location.name}(events={location.number_of_events}, type={location.type})")
        self.events = reader.events

    def __repr__(self):
        return f"{self.__class__.__name__}(default_cls={self.default_cls})"

    def __iter__(self):
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
            return self.get_region_event_class(region_type, event_type)
        except KeyError:
            pass # fallback to event_class_lookup
        try:
            return self.get_event_class(event_type)
        except KeyError:
            # no class found in either dict
            if self.default_cls is not None:
                return self.default_cls
            else:
                raise TypeError(f"{self.__class__.__name__} can't construct event of type '{event_type}' for {region_type} region") from None

    @staticmethod
    def get_event_class(event_type):

        lookup = {
            defn.EventType.thread_begin: ThreadBegin,
            defn.EventType.thread_end: ThreadEnd,
            defn.EventType.parallel_begin: ParallelBegin,
            defn.EventType.parallel_end: ParallelEnd,
            defn.EventType.workshare_begin: WorkshareBegin,
            defn.EventType.workshare_end: WorkshareEnd,
            defn.EventType.sync_begin: SyncBegin,
            defn.EventType.sync_end: SyncEnd,
            defn.EventType.master_begin: Master,
            defn.EventType.master_end: Master,
            defn.EventType.task_enter: TaskEnter,
            defn.EventType.task_leave: TaskLeave,
            defn.EventType.task_create: TaskCreate,
            defn.EventType.task_schedule: TaskSchedule,
            defn.EventType.task_switch: TaskSwitch,
            defn.EventType.phase_begin: PhaseBegin,
            defn.EventType.phase_end: PhaseEnd,
        }

        return lookup[event_type]

    @staticmethod
    def get_region_event_class(region_type, event_type):

        lookup = {
            (defn.RegionType.initial_task, defn.EventType.task_enter): InitialTaskEnter,
            (defn.RegionType.initial_task, defn.EventType.task_leave): InitialTaskLeave,
            (defn.RegionType.implicit_task, defn.EventType.task_enter): ImplicitTaskEnter,
            (defn.RegionType.implicit_task, defn.EventType.task_leave): ImplicitTaskLeave,
            (defn.RegionType.single_executor, defn.EventType.workshare_begin): SingleBegin,
            (defn.RegionType.single_executor, defn.EventType.workshare_end): SingleEnd,
            (defn.RegionType.master, defn.EventType.master_begin): MasterBegin,
            (defn.RegionType.master, defn.EventType.master_end): MasterEnd,
            (defn.RegionType.taskgroup, defn.EventType.sync_begin): TaskgroupBegin,
            (defn.RegionType.taskgroup, defn.EventType.sync_end): TaskgroupEnd
        }

        return lookup[(region_type, event_type)]


class _Event(ABC):

    is_enter_event = False
    is_leave_event = False
    is_task_enter_event = False
    is_task_create_event = False
    is_task_register_event = False
    is_task_complete_event = False
    is_task_leave_event = False
    is_task_switch_event = False
    is_task_switch_complete_event = False
    is_chunk_switch_event = False
    is_task_group_end_event = False

    _additional_attributes = [
        "vertex_label",
        "vertex_color_key",
        "vertex_shape_key"
    ]

    @logdec.on_init(logger=log.logger_getter("init_logger"))
    def __init__(self, event, location, attr):
        self.log = get_module_logger()
        self._event = event
        self._location = location
        self.attr = attr
        self.time = self._event.time

    def __getattr__(self, item):
        try:
            return self._event.attributes[self.attr[item]]
        except KeyError:
            raise AttributeError(f"attribute '{item}' not found in {self._base_repr} object") from None

    def get(self, item, default=None):
        try:
            return getattr(self, item)
        except AttributeError:
            return default

    @property
    def attributes(self):
        return ((attr.name, self._event.attributes[attr]) for attr in self._event.attributes)

    @property
    def _base_repr(self):
        return f"{type(self).__name__}(time={self.time}, loc={self._location.name})"

    @property
    def _attr_repr(self):
        return ", ".join([f"{name}:{value}" for name, value in self.attributes if name != 'time'])

    def get_task_data(self):
        raise NotImplementedError(f"method not implemented for {type(self)}")

    @abstractmethod
    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        raise NotImplementedError(f"method not implemented for {type(self)}")

    def __repr__(self):
        return f"{self._base_repr} {self._attr_repr}"

    def as_row(self, fmt="{type:<22} {time} {region_type:<16} {endpoint}"):
        return fmt.format(type=self.__class__.__name__, time=self.time, region_type=getattr(self, "region_type", None), endpoint=self.endpoint)

    def yield_attributes(self):
        yield "time", str(self.time)
        for name in it.chain(self.attr, self._additional_attributes):
            try:
                yield name, getattr(self, name)
            except AttributeError:
                continue

    def get_task_completed(self):
        raise NotImplementedError()

    @property
    def is_update_duration_event(self):
        return self.is_task_switch_event or self.is_task_enter_event or self.is_task_leave_event

    @property
    def is_update_task_start_ts_event(self):
        return (self.is_task_switch_event and not self.is_task_complete_event) or self.is_task_enter_event

    def get_tasks_switched(self):
        raise NotImplementedError("only implemented if event.is_update_duration_event == True")

    def get_task_entered(self):
        raise NotImplementedError("only implemented if event.is_update_task_start_ts_event == True")

    def get_task_created(self):
        raise NotImplementedError("only implemented if event.is_task_create_event == True")

    @property
    def task_synchronisation_cache(self):
        raise NotImplementedError()

    def clear_task_synchronisation_cache(self):
        raise NotImplementedError()

    @property
    def vertex_label(self):
        return self.unique_id

    @property
    def vertex_color_key(self):
        return self.region_type

    @property
    def vertex_shape_key(self):
        return self.vertex_color_key

# mixin
class ClassNotImplementedMixin(ABC):

    @logdec.on_init(logger=log.logger_getter("init_logger"), level=log.ERROR)
    def __init__(self, *args, **kwargs):
        raise NotImplementedError(f"{self.__class__.__name__}")


# mixin
class DefaultUpdateChunksMixin(ABC):

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        encountering_task_id = self.encountering_task_id
        chunk = chunk_dict[encountering_task_id]
        chunk.append_event(self)


# mixin
class EnterMixin(ABC):
    is_enter_event = True
    is_leave_event = False

# mixin
class LeaveMixin(ABC):
    is_enter_event = False
    is_leave_event = True


# mixin
class RegisterTaskDataMixin(ABC):
    is_task_register_event = True

    def get_task_data(self):
        data = {
            defn.Attr.unique_id:         self.unique_id,
            defn.Attr.task_type:         self.task_type,
            defn.Attr.parent_task_id:    self.parent_task_id,
            defn.Attr.time:              self._event.time
        }

        try:
            data[defn.Attr.source_file_name] = self.source_file_name
            data[defn.Attr.source_func_name] = self.source_func_name
            data[defn.Attr.source_line_number] = self.source_line_number
        except AttributeError as e:
            self.log.debug("couldn't return source file data")

        return data


# mixin
class ChunkSwitchEventMixin(ABC):
    is_chunk_switch_event = True


class GenericEvent(_Event):
    pass


class ThreadBegin(ChunkSwitchEventMixin, EnterMixin, _Event):

    def update_chunks(self, chunk_dict, chunk_stack):
        self.log.debug(f"{self.__class__.__name__}.update_chunks: pass")
        yield None


class ThreadEnd(ChunkSwitchEventMixin, LeaveMixin, _Event):

    def update_chunks(self, chunk_dict, chunk_stack):
        self.log.debug(f"{self.__class__.__name__}.update_chunks: pass")
        yield None


class ParallelBegin(ChunkSwitchEventMixin, EnterMixin, _Event):

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        task_chunk_key = (self._location.name, self.encountering_task_id, defn.RegionType.task)
        parallel_chunk_key = (self._location.name, self.unique_id, defn.RegionType.parallel)
        self.log.debug(f"{self.__class__.__name__}.update_chunks: {task_chunk_key=}, {parallel_chunk_key=}")
        self._location.enter_parallel_region(self.unique_id)
        if task_chunk_key in chunk_dict.keys():
            # The master thread will already have recorded an event in the task which encountered this parallel
            # region, so update the chunk which was previously created before creating a nested chunk
            task_chunk = chunk_dict[task_chunk_key]
            task_chunk.append_event(self)
            # record enclosing chunk before creating the nested chunk
            chunk_stack[parallel_chunk_key].append(task_chunk)
        chunk_dict[parallel_chunk_key].append_event(self)
        yield None


class ParallelEnd(ChunkSwitchEventMixin, LeaveMixin, _Event):

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        task_chunk_key = (self._location.name, self.encountering_task_id, defn.RegionType.task)
        self.log.debug(f"{self.__class__.__name__}.update_chunks: {task_chunk_key=}")
        # append to parallel region chunk
        parallel_chunk_key = (self._location.name, self.unique_id, defn.RegionType.parallel)
        parallel_chunk = chunk_dict[parallel_chunk_key]
        parallel_chunk.append_event(self)
        # yield parallel region chunk
        yield parallel_chunk
        self._location.leave_parallel_region()
        if task_chunk_key in chunk_dict.keys():
            # master thread: restore and update the enclosing chunk
            chunk_dict[parallel_chunk_key] = chunk_stack[parallel_chunk_key].pop()
            chunk_dict[parallel_chunk_key].append_event(self)
            chunk_dict[task_chunk_key] = chunk_dict[parallel_chunk_key]


class Sync(DefaultUpdateChunksMixin, _Event):

    def __repr__(self):
        return "{} ({})".format(self._base_repr, ", ".join([str(self.__getattr__(attr)) for attr in [defn.Attr.encountering_task_id, defn.Attr.region_type, defn.Attr.endpoint]]))


class SyncBegin(EnterMixin, Sync):
    pass


class SyncEnd(LeaveMixin, Sync):
    pass


class TaskgroupBegin(SyncBegin):
    pass


class TaskgroupEnd(SyncEnd):
    is_task_group_end_event = True


class WorkshareBegin(EnterMixin, DefaultUpdateChunksMixin, _Event):
    pass


class WorkshareEnd(LeaveMixin, DefaultUpdateChunksMixin, _Event):
    pass


class SingleBegin(ChunkSwitchEventMixin, WorkshareBegin):

    @logdec.on_init(logger=log.logger_getter("init_logger"))
    def __init__(self, event, location, attr):
        self._task_sync_cache = list()
        super().__init__(event, location, attr)

    @property
    def task_synchronisation_cache(self):
        return self._task_sync_cache

    def clear_task_synchronisation_cache(self):
        self._task_sync_cache.clear()

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        # Nested region - append to task chunk, push onto stack, create nested chunk
        task_chunk_key = self.encountering_task_id
        self.log.debug(f"{self.__class__.__name__}.update_chunks: {task_chunk_key=}")
        task_chunk = chunk_dict.pop(task_chunk_key)
        task_chunk.append_event(self)
        # store the enclosing chunk
        chunk_stack[task_chunk_key].append(task_chunk)
        # Create a new nested Chunk for the single region
        chunk_dict[task_chunk_key].append_event(self)
        yield None


class SingleEnd(ChunkSwitchEventMixin, WorkshareEnd):

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        # Nested region - append to inner chunk, yield, then pop enclosing chunk & append to that chunk
        task_chunk_key = self.encountering_task_id
        self.log.debug(f"{self.__class__.__name__}.update_chunks: {task_chunk_key=}")
        task_chunk = chunk_dict[task_chunk_key]
        task_chunk.append_event(self)
        yield task_chunk
        chunk_dict[task_chunk_key] = chunk_stack[task_chunk_key].pop()
        chunk_dict[task_chunk_key].append_event(self)


class MasterBegin(SingleBegin):
    pass


class MasterEnd(SingleEnd):
    pass


class Master(_Event):
    pass


class Task(_Event):
    pass


class TaskEnter(RegisterTaskDataMixin, Task):
    is_task_enter_event = True

    def get_tasks_switched(self):
        return self.encountering_task_id, self.unique_id

    def get_task_entered(self):
        return self.unique_id


class InitialTaskEnter(ChunkSwitchEventMixin, TaskEnter):

    def update_chunks(self, chunk_dict, chunk_stack):
        # For initial-task-begin, chunk key is (thread ID, initial task unique_id)
        chunk_key = self._location.name, self.unique_id, defn.RegionType.task
        self.log.debug(f"{self.__class__.__name__}.update_chunks: {chunk_key=}")
        chunk = chunk_dict[chunk_key]
        chunk.append_event(self)

        # Ensure the initial task's ID gives the correct chunk so that events
        # nested between it and an enclosed parallel region can get the correct
        # chunk for the initial task
        task_chunk_key = self.unique_id
        chunk_dict[task_chunk_key] = chunk
        
        yield None


class ImplicitTaskEnter(ChunkSwitchEventMixin, TaskEnter):

    def update_chunks(self, chunk_dict, chunk_stack):
        # (location name, current parallel ID, defn.RegionType.parallel)
        chunk_key = self._location.name, self._location.current_parallel_region, defn.RegionType.parallel
        self.log.debug(f"{self.__class__.__name__}.update_chunks: {chunk_key=}")
        chunk = chunk_dict[chunk_key]
        chunk.append_event(self)
        # Ensure implicit-task-id points to the same chunk for later events in this task
        chunk_dict[self.unique_id] = chunk

        # Allow nested parallel regions to find the chunk for this implicit task
        implicit_task_chunk_key = self._location.name, self.unique_id, defn.RegionType.task
        chunk_dict[implicit_task_chunk_key] = chunk

        yield None


class TaskLeave(Task):
    is_task_complete_event = True
    is_task_leave_event = True

    def get_task_completed(self):
        return self.unique_id

    def get_tasks_switched(self):
        return self.encountering_task_id, self.unique_id


class InitialTaskLeave(ChunkSwitchEventMixin, TaskLeave):

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        chunk_key = self._location.name, self.unique_id, defn.RegionType.task
        self.log.debug(f"{self.__class__.__name__}.update_chunks: {chunk_key=}")
        chunk = chunk_dict[chunk_key]
        chunk.append_event(self)
        yield chunk


class ImplicitTaskLeave(ChunkSwitchEventMixin, TaskLeave):

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        # don't yield until parallel-end
        chunk_key = self.unique_id
        self.log.debug(f"{self.__class__.__name__}.update_chunks: {chunk_key=}")
        chunk = chunk_dict[chunk_key]
        chunk.append_event(self)
        yield None


class TaskCreate(RegisterTaskDataMixin, DefaultUpdateChunksMixin, Task):

    is_task_create_event = True

    def __repr__(self):
        return f"{self._base_repr} {self._attr_repr}"

    def get_task_created(self):
        return self.unique_id


class TaskSchedule(Task):
    pass


class TaskSwitch(ChunkSwitchEventMixin, Task):
    is_task_switch_event = True

    @property
    def is_task_complete_event(self):
        return self.is_task_switch_complete_event

    @property
    def is_task_switch_complete_event(self):
        return self.prior_task_status in [defn.TaskStatus.complete, defn.TaskStatus.cancel]

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        this_chunk_key = self.encountering_task_id
        next_chunk_key = self.next_task_id
        self.log.debug(f"{self.__class__.__name__}.update_chunks: {this_chunk_key=}, {next_chunk_key=}")
        this_chunk = chunk_dict[this_chunk_key]
        if self.prior_task_status != defn.TaskStatus.switch: # only update the prior task's chunk if it wasn't a regular switch event
            self.log.debug(f"{self.__class__.__name__}.update_chunks: {self} updating chunk key={this_chunk_key} for {self.region_type} with status {self.prior_task_status}")
            this_chunk.append_event(self)
            yield this_chunk if self.prior_task_status == defn.TaskStatus.complete else None
        else:
            self.log.debug(f"{self.__class__.__name__}.update_chunks: {self} skipped updating chunk key={this_chunk_key} for {self.region_type} with status {self.prior_task_status}")
        self.log.debug(f"{self.__class__.__name__}.update_chunks: {self} updating chunk key={next_chunk_key}")
        next_chunk = chunk_dict[next_chunk_key]
        next_chunk.append_event(self)

        # Allow nested parallel regions to append to next_chunk where a task
        # creates a nested parallel region?
        nested_parallel_region_chunk_key = (self._location.name, self.unique_id, defn.RegionType.task)
        chunk_dict[nested_parallel_region_chunk_key] = next_chunk

    def get_task_completed(self):
        if not self.is_task_complete_event:
            raise RuntimeError("not a task-complete event: {self}")
        return self.encountering_task_id

    def get_tasks_switched(self):
        return self.encountering_task_id, self.next_task_id

    def get_task_entered(self):
        return self.next_task_id

    def __repr__(self):
        return f"{self._base_repr} [{self.prior_task_id} ({self.prior_task_status}) -> {self.next_task_id}]"

    @property
    def vertex_label(self):
        if self.prior_task_status in [defn.TaskStatus.complete, defn.TaskStatus.cancel]:
            return self.prior_task_id
        else:
            return self.next_task_id

    @property
    def vertex_color_key(self):
        if self.prior_task_status in [defn.TaskStatus.complete, defn.TaskStatus.cancel]:
            return self.region_type
        else:
            return self.next_task_region_type


class PhaseBegin(EnterMixin, DefaultUpdateChunksMixin, _Event):
    
    @property
    def vertex_label(self):
        return self.phase_name


class PhaseEnd(LeaveMixin, DefaultUpdateChunksMixin, _Event):
    
    @property
    def vertex_label(self):
        return self.phase_name


def unpack(event: List[_Event]) -> dict:
    assert is_event_list(event)
    if is_event(event):
        return dict(event.yield_attributes())
    elif is_event_list(event):
        return utils.transpose_list_to_dict([dict(e.yield_attributes()) for e in event])
    else:
        raise TypeError(f"{type(event)}")
