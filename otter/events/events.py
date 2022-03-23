from abc import ABC, abstractmethod
from .. import definitions as defn
from ..utils import PrettyCounter
import os
import otf2
import pathlib
import typing

# Define some type aliases
Path = typing.Union[str, pathlib.Path]
OTF2Event = otf2.events._Event

class Event(ABC):

    def __init__(self, event: OTF2Event):
        discard = (defn.Attr.event_type,)
        self.time = event.time
        for name, value in EventStream.get_event_attributes(event).items():
            if name not in discard:
                self.__setattr__(name, value)

    def __repr__(self):
        return f"{type(self).__name__}(time={self.time}) " + ", ".join([f"{name}:{value}" for name, value in self.__dict__.items() if name != 'time'])


class Generic(Event):
    pass


class Thread(Event):
    pass


class Parallel(Event):
    pass


class Sync(Event):
    pass


class Workshare(Event):
    pass


class Master(Event):
    pass


class Task(Event):
    """Should also instantiate single representations of each task entered"""

    @abstractmethod
    def register_task(self):
        pass


class TaskEnter(Task):
    pass


class TaskLeave(Task):
    pass


class TaskCreate(Task):
    """Should also instantiate single representations of each task created"""
    pass


class TaskSchedule(Task):
    pass


class TaskSwitch(Task):
    pass


class EventStream:
    """
    A container for lazily generating the events contained in a trace:
        for event in EventStream(...):
          print(event)
    """

    class_lookup = {
        defn.EventType.thread_begin: Thread,
        defn.EventType.thread_end: Thread,
        defn.EventType.parallel_begin: Parallel,
        defn.EventType.parallel_end: Parallel,
        defn.EventType.workshare_begin: Workshare,
        defn.EventType.workshare_end: Workshare,
        defn.EventType.sync_begin: Sync,
        defn.EventType.sync_end: Sync,
        defn.EventType.master_begin: Master,
        defn.EventType.master_end: Master,
        defn.EventType.task_enter: TaskEnter,
        defn.EventType.task_leave: TaskLeave,
        defn.EventType.task_create: TaskCreate,
        defn.EventType.task_schedule: TaskSchedule,
        defn.EventType.task_switch: TaskSwitch
    }

    def __init__(self, p: Path):
        if not os.path.isfile(p):
            raise FileNotFoundError(p)
        self.path = os.path.abspath(os.path.normpath(p))
        self.counter = PrettyCounter()

    def __iter__(self):
        self.counter.clear()
        with otf2.reader.open(self.path) as t:
            for location, event in t.events:
                e = self.make_event(event)
                self.counter[type(e)] += 1
                yield e

    @staticmethod
    def get_event_attributes(e: OTF2Event) -> dict:
        return {attr.name: e.attributes[attr] for attr in e.attributes}

    @classmethod
    def make_event(cls, e: OTF2Event) -> Event:
        event_type = cls.get_event_attributes(e)[defn.Attr.event_type]
        return cls.class_lookup[event_type](e)
