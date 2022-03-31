from ..definitions import RegionType, EventType
from ..EventFactory import events
from ..logging import get_logger
from typing import Callable

module_logger = get_logger(f"{__name__}")

def key_is_not_none(key) -> Callable:
    return lambda vertex: vertex[key] is not None

def _is_event_region_type(event, region_type) -> bool:
    assert events.is_event(event)
    return (event.is_enter_event or event.is_leave_event) and event.region_type == region_type

def is_region_type(region_type: RegionType) -> Callable:
    """Return a function which checks whether the event attribute of a vertex is of the given region type"""
    def check_region_type(vertex):
        """
        Check the region_type of the event attribute of some vertex. If the event attribute is an Event, check whether
        it has the corresponding region_type. If it is a list of Events, check whether all Events have the corresponding
        region_type. Raise a RuntimeError otherwise, as the type of the event attribute must be either Event or List[Event].
        """
        event_attribute = vertex['event']
        if events.is_event(event_attribute):
            return _is_event_region_type(event_attribute, region_type)
        elif isinstance(event_attribute, list) and events.all_events(event_attribute):
            return all(_is_event_region_type(e, region_type) for e in event_attribute)
        else:
            module_logger.debug(f"expected {events._Event} or List[{events._Event}], got {type(event_attribute)} ({event_attribute})", stack_info=True)
            raise RuntimeError(f"expected {events._Event}, got {type(event)} ({event})")
    return check_region_type

def is_empty_task_region(vertex) -> bool:
    # Return True if vertex is a task-enter (-leave) node with no outgoing (incoming) edges
    if vertex['_task_cluster_id'] is None:
        return False
    if vertex['_is_task_enter_node'] or vertex['_is_task_leave_node']:
        return ((vertex['_is_task_leave_node'] and vertex.indegree() == 0) or
                (vertex['_is_task_enter_node'] and vertex.outdegree() == 0))
    if type(vertex['event']) is list and set(map(type, vertex['event'])) in [{EventType.task_switch}]:
        return ((all(vertex['_is_task_leave_node']) and vertex.indegree() == 0) or
                (all(vertex['_is_task_enter_node']) and vertex.outdegree() == 0))
