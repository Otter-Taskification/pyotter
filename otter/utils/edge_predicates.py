from typing import List, Callable
from ..definitions import RegionType, EventType
from ..core import events
from .. import log

get_module_logger = log.logger_getter("edges")

def edge_connects_same_type(edge, match_list: List[RegionType]) -> bool:
    region_types = set()
    for vertex in (edge.source_vertex, edge.target_vertex):
        assert events.is_event_list(vertex['event'])
        for event in vertex['event']:
            region_types.add(event.region_type)
    if len(region_types) > 1:
        return False
    return True if region_types.pop() in match_list else False
