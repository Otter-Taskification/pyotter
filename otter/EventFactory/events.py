from abc import ABC, abstractmethod
from ..types import OTF2Event, AttrDict


class _Event(ABC):

    def __init__(self, event: OTF2Event, attr: AttrDict):
        self._event = event
        self.attr = attr

    def __getattr__(self, item):
        return self._event.time if item == 'time' else self._event.attributes[self.attr[item]]

    @property
    def attributes(self):
        return ((attr.name, self._event.attributes[attr]) for attr in self._event.attributes)

    def __repr__(self):
        return f"{type(self).__name__}(time={self.time}) " + ", ".join([f"{name}:{value}" for name, value in self.attributes if name != 'time'])


class Generic(_Event):
    pass


class Thread(_Event):
    pass


class Parallel(_Event):
    pass


class Sync(_Event):
    pass


class Workshare(_Event):
    pass


class Master(_Event):
    pass


class Task(_Event):
    pass


class TaskEnter(Task):
    pass


class TaskLeave(Task):
    pass


class TaskCreate(Task):
    pass


class TaskSchedule(Task):
    pass


class TaskSwitch(Task):
    pass