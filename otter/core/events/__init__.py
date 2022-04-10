from collections import defaultdict
from typing import Union, List, NewType
from ... import utils
from . import events, EventFactory
from .events import is_event, is_event_list, all_events, any_events

def unpack(event: Union[events._Event, List[events._Event]]) -> Union[dict, List[dict]]:
    if events.is_event(event):
        return dict(event.yield_attributes())
    elif events.is_event_list(event):
        l = [dict(e.yield_attributes()) for e in event]
        return transpose_list_to_dict(l)
    else:
        raise TypeError(f"{type()}")

# TODO: move to utils
def transpose_list_to_dict(list_of_dicts):
    D = defaultdict(list)
    all_keys = set(utils.flatten(d.keys() for d in list_of_dicts))
    for d in list_of_dicts:
        for key in all_keys:
            D[key].append(d.get(key, None))
    return D
