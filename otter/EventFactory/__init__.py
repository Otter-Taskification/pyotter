from .EventFactory import EventFactory
from . import events
from typing import Union, List, NewType

Event = NewType("Event", events._Event)
EventList = List[Event]

def unpack(event: Union[Event, EventList]) -> Union[dict, List[dict]]:
    if events.is_event(event):
        return dict(event.yield_attributes())
    elif events.is_event_list(event):
        return [dict(e.yield_attributes()) for e in event]
    else:
        raise TypeError(f"{type()}")
