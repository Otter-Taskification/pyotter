from __future__ import annotations

from typing import Iterable, List, Optional, Set, Tuple
from warnings import warn

from otter.core.chunks import Chunk
from otter.core.chunk_builder import ChunkBuilderProtocol
from otter.core.events import Event, Location
from otter.core.tasks import Task, TaskRegistry, TaskSynchronisationContext
from otter.definitions import (
    Endpoint,
    EventModel,
    EventType,
    NullTaskID,
    RegionType,
    SourceLocation,
)
from otter.log import logger_getter

from .event_model import BaseEventModel, EventModelFactory, TraceEventIterable

get_module_logger = logger_getter("task_graph_event_model")


@EventModelFactory.register(EventModel.TASKGRAPH)
class TaskGraphEventModel(BaseEventModel):
    def __init__(
        self,
        task_registry: TaskRegistry,
        *args,
        gather_return_addresses: Optional[Set[int]] = None,
        **kwargs,
    ):
        super().__init__(task_registry)
        self._return_addresses = gather_return_addresses

    def event_completes_chunk(self, event: Event) -> bool:
        return (
            event.event_type == EventType.task_switch
            and event.endpoint == Endpoint.leave
        )

    def event_updates_chunk(self, event: Event) -> bool:
        return (
            event.event_type == EventType.task_switch
            and event.endpoint == Endpoint.enter
        )

    def event_skips_chunk_update(self, event: Event) -> bool:
        return False

    def is_task_register_event(self, event: Event) -> bool:
        return (
            event.event_type == EventType.task_switch
            and event.endpoint == Endpoint.enter
        )

    def is_task_create_event(self, event: Event) -> bool:
        return (
            event.event_type == EventType.task_switch
            and event.endpoint == Endpoint.enter
        )

    def is_update_task_start_ts_event(self, event: Event) -> bool:
        return (
            event.event_type == EventType.task_switch
            and event.endpoint == Endpoint.enter
        )

    def is_update_duration_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch

    def get_tasks_switched(self, event: Event) -> Tuple[int, int]:
        return event.parent_task_id, event.unique_id

    def is_task_complete_event(self, event: Event) -> bool:
        return (
            event.event_type == EventType.task_switch
            and event.endpoint == Endpoint.leave
        )

    def is_task_sync_event(self, event: Event) -> bool:
        return event.region_type == RegionType.taskwait

    def get_task_completed(self, event: Event) -> int:
        return event.unique_id

    @staticmethod
    def get_task_entered(event: Event) -> int:
        return event.unique_id

    def get_task_data(self, event: Event) -> Task:
        assert self.is_task_register_event(event)
        return Task(
            event.unique_id,
            event.parent_task_id,
            event.task_flavour,
            event.task_label,
            event.time,
            SourceLocation(
                event.task_init_file, event.task_init_func, event.task_init_line
            ),
        )

    def get_task_start_location(self, event: Event) -> SourceLocation:
        return SourceLocation(event.source_file, event.source_func, event.source_line)

    def get_task_end_location(self, event: Event) -> SourceLocation:
        return SourceLocation(event.source_file, event.source_func, event.source_line)

    def pre_yield_event_callback(self, event: Event) -> None:
        """Called once for each event before it is sent to super().yield_chunks"""
        if (
            event.event_type == EventType.task_switch
            and event.unique_id == event.parent_task_id
        ):
            warn(f"Task is own parent {event=}", category=Warning)

    def post_yield_event_callback(self, event: Event) -> None:
        """Called once for each event after it has been sent to super().yield_chunks"""
        if self._return_addresses is not None:
            address = event.caller_return_address
            if address not in self._return_addresses:
                self._return_addresses.add(address)

    def filter_event(self, event: Event) -> bool:
        """Return True if an event should be processed when yielding chunks"""
        if event.is_buffer_flush_event():
            self.log.warning("buffer flush event encountered - skipped (%s)", event)
            return False
        return True

    def yield_events_with_warning(
        self, events_iter: TraceEventIterable
    ) -> TraceEventIterable:
        for location, location_count, event in events_iter:
            if self.filter_event(event):
                self.pre_yield_event_callback(event)
                yield location, location_count, event
                self.post_yield_event_callback(event)

    def yield_chunks(self, events_iter: TraceEventIterable, chunk_manager) -> Iterable[int]:
        yield from super().yield_chunks(self.yield_events_with_warning(events_iter), chunk_manager)

    def contexts_of(self, chunk: Chunk) -> List[TaskSynchronisationContext]:
        return super().contexts_of(chunk)


@TaskGraphEventModel.update_chunks_on(event_type=EventType.task_switch)
def update_chunks_task_switch(
    event: Event,
    location: Location,
    location_count: int,
    chunk_builder: ChunkBuilderProtocol,
) -> Optional[int]:
    log = get_module_logger()
    log.debug(
        "%s event_type=%s region_type=%s endpoint=%s",
        event,
        event.event_type,
        event.region_type,
        event.endpoint,
    )
    enclosing_key = event.parent_task_id
    key = event.unique_id
    if event.endpoint == Endpoint.enter:
        if enclosing_key != NullTaskID:
            chunk_builder.append_to_chunk(
                enclosing_key, event, location.ref, location_count
            )
        assert not chunk_builder.contains(key)
        chunk_builder.new_chunk(key, event, location.ref, location_count)
        result = None
    elif event.endpoint == Endpoint.leave:
        chunk_builder.append_to_chunk(key, event, location.ref, location_count)
        result = key
    else:
        raise ValueError(f"unexpected endpoint: {event.endpoint}")
    return result
