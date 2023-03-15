from .event_model import EventModelFactory, BaseEventModel, EventList, ChunkDict, ChunkStackDict, ChunkUpdateHandlerKey, ChunkUpdateHandlerFn
from typing import Iterable, Tuple, Optional
from igraph import Graph, disjoint_union, Vertex
from otter.definitions import EventModel, EventType, TaskStatus, Endpoint, NullTaskID, Attr
from otter.core.chunks import Chunk
from otter.core.events import Event, Location
from otter.core.tasks import TaskData
from otter.log import logger_getter

get_module_logger = logger_getter("task_graph_event_model")


@EventModelFactory.register(EventModel.TASKGRAPH)
class TaskGraphEventModel(BaseEventModel):

    def chunk_to_graph(self, chunk) -> Graph:
        return Graph(directed=True)

    def combine_graphs(self, graphs: Iterable[Graph]) -> Graph:
        return Graph(directed=True)

    def event_completes_chunk(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch and event.endpoint == Endpoint.leave

    def event_updates_chunk(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch and event.endpoint == Endpoint.enter

    def event_skips_chunk_update(self, event: Event) -> bool:
        return False

    @classmethod
    def is_task_register_event(cls, event: Event) -> bool:
        return event.event_type == EventType.task_switch and event.endpoint == Endpoint.enter

    def is_update_task_start_ts_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch and event.endpoint == Endpoint.enter

    def is_update_duration_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch

    def get_tasks_switched(self, event: Event) -> Tuple[int, int]:
        return event.parent_task_id, event.unique_id

    def is_task_complete_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch and event.endpoint == Endpoint.leave

    def get_task_completed(self, event: Event) -> int:
        return event.unique_id

    @staticmethod
    def get_task_entered(event: Event) -> id:
        return event.unique_id

    @classmethod
    def get_task_data(cls, event: Event) -> TaskData:
        assert cls.is_task_register_event(event)
        data = {
            Attr.unique_id:       event.unique_id,
            Attr.task_type:       event.region_type, # TODO: is this correct?
            Attr.parent_task_id:  event.parent_task_id,
            Attr.time:            event.time
        }
        if Attr.source_file_name in event and Attr.source_func_name in event and Attr.source_line_number in event:
            data[Attr.source_file_name] = event.source_file_name
            data[Attr.source_func_name] = event.source_func_name
            data[Attr.source_line_number] = event.source_line_number
        return data



@TaskGraphEventModel.update_chunks_on(event_type=EventType.task_switch)
def update_chunks_task_switch(event: Event, location: Location, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict) -> Optional[Chunk]:
    log = get_module_logger()
    log.debug(f"{event} {event.event_type=} {event.region_type=} {event.endpoint=}")
    enclosing_key = event.parent_task_id
    key = event.unique_id
    if event.endpoint == Endpoint.enter:
        if enclosing_key != NullTaskID:
            enclosing_chunk = chunk_dict[enclosing_key]
            enclosing_chunk.append_event(event)
        assert key not in chunk_dict
        chunk = Chunk(event.region_type)
        chunk_dict[key] = chunk
        chunk.append_event(event)
        return None
    elif event.endpoint == Endpoint.leave:
        chunk = chunk_dict[key]
        chunk.append_event(event)
        return chunk
    else:
        raise ValueError(f"unexpected endpoint: {event.endpoint}")
