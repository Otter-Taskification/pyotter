from collections import defaultdict
from typing import Union, List, NewType
from .EventFactory import EventFactory
from . import events
from ..utils import flatten

Event = NewType("Event", events._Event)
EventList = List[Event]

def unpack(event: Union[Event, EventList]) -> Union[dict, List[dict]]:
    if events.is_event(event):
        return dict(event.yield_attributes())
    elif events.is_event_list(event):
        l = [dict(e.yield_attributes()) for e in event]
        return transpose_list_to_dict(l)
    else:
        raise TypeError(f"{type()}")


def transpose_list_to_dict(list_of_dicts):
    D = defaultdict(list)
    all_keys = set(flatten(d.keys() for d in list_of_dicts))
    for d in list_of_dicts:
        for key in all_keys:
            D[key].append(d.get(key, None))
    return D
