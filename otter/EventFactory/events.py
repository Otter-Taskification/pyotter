from abc import ABC, abstractmethod
from ..definitions import Attr, Endpoint
from ..types import OTF2Event, AttrDict


class _Event(ABC):

    is_enter_event = False
    is_leave_event = False
    is_task_register_event = False

    def __init__(self, event: OTF2Event, attr: AttrDict):
        self._event = event
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

    def get_task_data(self):
        raise NotImplementedError(f"task_data attribute not implemented for {self.__class__}")

    @property
    def _base_repr(self):
        return f"{type(self).__name__}(time={self.time})"

    @property
    def _attr_repr(self):
        return ", ".join([f"{name}:{value}" for name, value in self.attributes if name != 'time'])

    def __repr__(self):
        return " ".join([self._base_repr, self._attr_repr])

# mixin
class ClassNotImplementedMixin(ABC):

    def __init__(self, *args, **kwargs):
        raise NotImplementedError(f"{self.__class__.__name__}")

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


class Generic(_Event):
    pass


class ThreadBegin(EnterMixin, _Event):
    pass


class ThreadEnd(LeaveMixin, _Event):
    pass


class ParallelBegin(EnterMixin, _Event):
    pass


class ParallelEnd(LeaveMixin, _Event):
    pass


class Sync(_Event):
    pass


class Workshare(_Event):
    pass


class Master(_Event):
    pass


class Task(_Event):
    pass


class TaskEnter(RegisterTaskDataMixin, Task):
    pass


class TaskLeave(Task):
    pass


class TaskCreate(RegisterTaskDataMixin, Task):

    def __repr__(self):
        return f"{self._base_repr} {self.parent_task_id} created {self.unique_id}"


class TaskSchedule(ClassNotImplementedMixin, Task):
    pass


class TaskSwitch(Task):

    def __repr__(self):
        return f"{self._base_repr} {self.prior_task_id} ({self.prior_task_status}) -> {self.next_task_id}"
