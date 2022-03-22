from abc import ABC, abstractmethod
from collections import Counter
from .. import definitions as defn
import otf2

class Event(ABC):

    def __init__(self, time, **attr):
        discard = (defn.Attr.event_type,)
        self.time = time
        for name in attr:
            if name not in discard:
                self.__setattr__(name, attr[name])

    def __repr__(self):
        return f"{type(self).__name__}(time={self.time}) " + ", ".join([f"{name}:{value}" for name, value in self.__dict__.items() if name != 'time'])


class GenericEvent(Event):
    pass


class ThreadEvent(Event):
    pass


class ParallelEvent(Event):
    pass


class SyncEvent(Event):
    pass


class WorkshareEvent(Event):
    pass


class TaskEvent(Event):
    pass


class TaskEnterEvent(Event):
    pass


class TaskLeaveEvent(Event):
    pass


class TaskCreateEvent(Event):
    pass


class TaskScheduleEvent(Event):
    pass


class TaskSwitchEvent(Event):
    pass


def make_event(time, **attr):

    event_type = attr[defn.Attr.event_type]

    if event_type in defn.ThreadEvents:
        cls = ThreadEvent
    elif event_type in defn.ParallelEvents:
        cls = ParallelEvent
    elif event_type in defn.WorkshareEvents:
        cls = WorkshareEvent
    elif event_type in defn.SyncEvents:
        cls = SyncEvent
    elif event_type in defn.MasterEvents:
        raise NotImplementedError(event_type)
    elif event_type in defn.TaskEvents:
        if event_type == defn.TaskEvent.CREATE:
            cls = TaskCreateEvent
        elif event_type == defn.TaskEvent.SWITCH:
            cls = TaskSwitchEvent
        else:
            cls = TaskEvent
    else:
        raise ValueError(event_type)

    return cls(time, **attr)


def read_trace(t):
    c = Counter()
    with otf2.reader.open(t) as tr:
        for location, event in tr.events:
            e = make_event(event.time, **get_event_attributes(event))
            print(location.name, e)
            c[type(e)] += 1
    return e, c


def get_event_attributes(e):
    return {attr.name: e.attributes[attr] for attr in e.attributes}
