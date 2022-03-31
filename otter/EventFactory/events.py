from logging import DEBUG, INFO
from abc import ABC, abstractmethod
from ..definitions import Attr, RegionType, TaskStatus
from ..types import OTF2Event, OTF2Location, AttrDict
from ..logging import get_logger
from ..utils.decorate import log_init


is_event = lambda item : isinstance(item, _Event)
all_events = lambda args : all(map(is_event, args))
any_events = lambda args : any(map(is_event, args))


class _Event(ABC):

    is_enter_event = False
    is_leave_event = False
    is_task_register_event = False
    is_chunk_switch_event = False

    @log_init(DEBUG)
    def __init__(self, event: OTF2Event, location: OTF2Location, attr: AttrDict):
        self.log = get_logger(f"{self.__class__.__name__}")
        self._event = event
        self._location = location
        self.attr = attr

    def __getattr__(self, item):
        if item == "time":
            return self._event.time
        elif item not in self.attr:
            raise AttributeError(f"attribute '{item}' is not defined")
        elif self.attr[item] not in self._event.attributes:
            raise AttributeError(f"attribute '{item}' not found in {self._base_repr} object")
        return self._event.attributes[self.attr[item]]

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
        return " ".join([self._base_repr, self._attr_repr])

# mixin
class ClassNotImplementedMixin(ABC):

    @log_init
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
        return {
            Attr.unique_id:         self.unique_id,
            Attr.task_type:         self.task_type,
            Attr.parent_task_id:    self.parent_task_id,
            Attr.time:              self._event.time
        }


# mixin
class ChunkSwitchEventMixin(ABC):
    is_chunk_switch_event = True


class GenericEvent(_Event):
    pass


class ThreadBegin(ChunkSwitchEventMixin, EnterMixin, _Event):

    def update_chunks(self, chunk_dict, chunk_stack):
        self.log.debug(f"{self.__class__.__name__}.update_chunks called - IGNORED")
        yield None


class ThreadEnd(ChunkSwitchEventMixin, LeaveMixin, _Event):

    def update_chunks(self, chunk_dict, chunk_stack):
        self.log.debug(f"{self.__class__.__name__}.update_chunks called - IGNORED")
        yield None


class ParallelBegin(ChunkSwitchEventMixin, EnterMixin, _Event):

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        task_chunk_key = (self._location.name, self.encountering_task_id, RegionType.task)
        parallel_chunk_key = (self._location.name, self.unique_id, RegionType.parallel)
        self.log.debug(f"{self.__class__.__name__}.update_chunks called ({task_chunk_key=}, {parallel_chunk_key=})")
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
        task_chunk_key = (self._location.name, self.encountering_task_id, RegionType.task)
        self.log.debug(f"{self.__class__.__name__}.update_chunks called {task_chunk_key=}")
        # append to parallel region chunk
        parallel_chunk_key = (self._location.name, self.unique_id, RegionType.parallel)
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
    pass


class WorkshareBegin(EnterMixin, DefaultUpdateChunksMixin, _Event):
    pass


class WorkshareEnd(LeaveMixin, DefaultUpdateChunksMixin, _Event):
    pass


class SingleBegin(ChunkSwitchEventMixin, WorkshareBegin):

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        # Nested region - append to task chunk, push onto stack, create nested chunk
        task_chunk_key = self.encountering_task_id
        self.log.debug(f"{self.__class__.__name__}.update_chunks called {task_chunk_key=}")
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
        self.log.debug(f"{self.__class__.__name__}.update_chunks called {task_chunk_key=}")
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
    pass


class InitialTaskEnter(ChunkSwitchEventMixin, TaskEnter):

    def update_chunks(self, chunk_dict, chunk_stack):
        # For initial-task-begin, chunk key is (thread ID, initial task unique_id)
        chunk_key = self._location.name, self.unique_id, RegionType.task
        self.log.debug(f"{self.__class__.__name__}.update_chunks called {chunk_key=}")
        chunk = chunk_dict[chunk_key]
        chunk.append_event(self)
        yield None


class ImplicitTaskEnter(ChunkSwitchEventMixin, TaskEnter):

    def update_chunks(self, chunk_dict, chunk_stack):
        # (location name, current parallel ID, RegionType.parallel)
        chunk_key = self._location.name, self._location.current_parallel_region, RegionType.parallel
        self.log.debug(f"{self.__class__.__name__}.update_chunks called {chunk_key=}")
        chunk = chunk_dict[chunk_key]
        chunk.append_event(self)
        # Ensure implicit-task-id points to the same chunk for later events in this task
        chunk_dict[self.unique_id] = chunk_dict[chunk_key]
        yield None


class TaskLeave(Task):
    pass


class InitialTaskLeave(ChunkSwitchEventMixin, TaskLeave):

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        chunk_key = self._location.name, self.unique_id, RegionType.task
        self.log.debug(f"{self.__class__.__name__}.update_chunks called {chunk_key=}")
        chunk = chunk_dict[chunk_key]
        chunk.append_event(self)
        yield chunk


class ImplicitTaskLeave(ChunkSwitchEventMixin, TaskLeave):

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        # don't yield until parallel-end
        chunk_key = self.unique_id
        self.log.debug(f"{self.__class__.__name__}.update_chunks called {chunk_key=}")
        chunk = chunk_dict[chunk_key]
        chunk.append_event(self)
        yield None


class TaskCreate(RegisterTaskDataMixin, DefaultUpdateChunksMixin, Task):

    def __repr__(self):
        return f"{self._base_repr} {self.parent_task_id} created {self.unique_id}"


class TaskSchedule(Task):
    pass


class TaskSwitch(ChunkSwitchEventMixin, Task):

    def update_chunks(self, chunk_dict, chunk_stack) -> None:
        this_chunk_key = self.encountering_task_id
        next_chunk_key = self.next_task_id
        self.log.debug(f"{self.__class__.__name__}.update_chunks called {this_chunk_key=} {next_chunk_key=}")
        this_chunk = chunk_dict[this_chunk_key]
        this_chunk.append_event(self)
        yield this_chunk if self.prior_task_status == TaskStatus.complete else None
        next_chunk = chunk_dict[next_chunk_key]
        next_chunk.append_event(self)

    def __repr__(self):
        return f"{self._base_repr} {self.prior_task_id} ({self.prior_task_status}) -> {self.next_task_id}"
