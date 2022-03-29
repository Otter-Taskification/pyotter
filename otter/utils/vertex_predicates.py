from ..definitions import RegionType, EventType
from ..EventFactory import events
from ..logging import get_logger
from typing import Callable

module_logger = get_logger(f"{__name__}")

def key_is_not_none(key) -> Callable:
    return lambda vertex: vertex[key] is not None

def is_region_type(region_type: RegionType) -> Callable:
    def check(vertex):
        event = vertex['event']
        try:
            assert isinstance(event, events._Event)
        except AssertionError as e:
            module_logger.exception(f"expected type {events._Event}, got type {type(event)}", stack_info=True)
            raise
        return (event.is_enter_event or event.is_leave_event) and event.region_type == region_type
    return check

def is_empty_task_region(v) -> bool:
    # Return True if v is a task-enter (-leave) node with no outgoing (incoming) edges
    if v['_task_cluster_id'] is None:
        return False
    if v['_is_task_enter_node'] or v['_is_task_leave_node']:
        return ((v['_is_task_leave_node'] and v.indegree() == 0) or
                (v['_is_task_enter_node'] and v.outdegree() == 0))
    if type(v['event']) is list and set(map(type, v['event'])) in [{EventType.task_switch}]:
        return ((all(v['_is_task_leave_node']) and v.indegree() == 0) or
                (all(v['_is_task_enter_node']) and v.outdegree() == 0))
