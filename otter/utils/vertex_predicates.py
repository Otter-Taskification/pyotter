from ..definitions import RegionType, EventType
from ..core import events
from .. import log
from typing import Callable

get_module_logger = log.logger_getter("vertex.pred")

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
            logger = get_module_logger()
            logger.debug(f"expected {core.events.events._Event} or List[{core.events.events._Event}], got {type(event_attribute)} ({event_attribute})", stack_info=True)
            for item in event_attribute:
                logger.debug(f"is an event: {isinstance(item, core.events.events._Event)} {item=}")
            raise RuntimeError(f"expected {core.events.events._Event}, got {type(event_attribute)} ({event_attribute})")
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

def is_terminal_task_vertex(vertex) -> bool:
    event = vertex['event']
    if isinstance(event, list):
        assert(all(events.is_event(item) for item in event))
        return any(e.is_task_switch_complete_event for e in event)
    else:
        assert(events.is_event(event))
        return event.is_task_switch_complete_event

def is_task_group_end_vertex(vertex) -> bool:
    event = vertex['event']
    if isinstance(event, list):
        assert(all(events.is_event(item) for item in event))
        return any(e.is_task_group_end_event for e in event)
    else:
        assert(events.is_event(event))
        return event.is_task_group_end_event
