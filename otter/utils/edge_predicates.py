from ..definitions import RegionType, EventType
from ..EventFactory import events
from .. import log
from ..log import get_logger
from ..log.levels import DEBUG, INFO, WARN, ERROR
from typing import List, Callable

module_logger = get_logger("edges")

def edge_connects_same_type(edge, match_list: List[RegionType]) -> bool:
    event_pair = (vertex['event'] for vertex in (edge.source_vertex, edge.target_vertex))
    region_types = set()
    for event in event_pair:
        if events.is_event(event):
            region_types.add(event.region_type)
        elif isinstance(event, list):
            region_types.update([item.region_type for item in event])
        else:
            raise TypeError(f"unexpected type: {event=}")
    if len(region_types) > 1:
        return False
    return True if region_types.pop() in match_list else False
