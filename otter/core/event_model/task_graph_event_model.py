from __future__ import annotations

import otter.log

from typing import Optional, Set, Tuple

from otter.core.chunk_builder import (
    ChunkBuilderProtocol,
    ChunkKeyNotFoundError,
    ChunkKeyDuplicateError,
)
from otter.core.events import Event, Location
from otter.core.tasks import Task
from otter.definitions import Attr, EventModel, EventType, NullTaskID, SourceLocation
from otter.log import logger_getter

from .event_model import (
    BaseEventModel,
    EventModelFactory,
    TaskBuilderProtocol,
    TraceEventIterable,
)

get_module_logger = logger_getter("task_graph_event_model")


"""
EVENT MODEL INCLUDING TASK-CREATE

Possible events that can appear in a Task-Graph trace:
    - task-create
    - task-switch (enter/leave) i.e. task-begin/task-end
    - task-sync (enter/leave sync region, or discrete i.e. just a sync without suspending) i.e. suspend/resume at a synchronisation barrier

Where will these events appear in a chunk?

    - task-create: in the encountering (i.e. parent) task's chunk only.
    - task-switch: (enter/leave) in the (entered/left) task's chunk
    - task-sync: (enter/leave/discrete) in the chunk of the task which is suspended/resumed i.e. the encountering task

So a leaf task's chunk may only look like this:

        task-enter
        task-leave

A branch task's chunk could look like this:

        task-enter
        task-create
        task-sync-enter
        task-sync-leave
        task-create
        task-create
        task-create
        task-sync-enter
        task-sync-leave
        task-create
        ...
        task-leave

NOTE: there is no task-create event for the root task.

So here is the logic to apply to each event:

task-enter:
    create a chunk with this event
    *updates task start ts*
task-sync-enter/task-sync-leave/task-sync-discrete/task-create:
    append to encountering task's chunk (i.e. the current default logic)
    *these events (except task-create) update task timestamps*
    *task-sync-enter/task-sync-discrete timestamps are used for synchronising tasks*
    *task-create timestamp is used for matching tasks to barriers*
task-leave:
    append to the chunk for the task which was left (recorded as event.encountering_task_id) i.e. the current default logic
    *this event completes a chunk*
"""


@EventModelFactory.register(EventModel.TASKGRAPH)
class TaskGraphEventModel(BaseEventModel):
    def __init__(
        self,
        *args,
        gather_return_addresses: Optional[Set[int]] = None,
        **kwargs,
    ):
        super().__init__()
        self._return_addresses = gather_return_addresses

    def event_completes_chunk(self, event: Event) -> bool:
        return event.event_type == EventType.task_leave

    def event_updates_chunk(self, event: Event) -> bool:
        return event.event_type == EventType.task_enter

    def event_skips_chunk_update(self, event: Event) -> bool:
        """The task-create event for the root task shouldn't be added to any chunk as it doesn't have a parent"""
        return event.event_type == EventType.task_create and event.unique_id == 0

    def is_task_register_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_create

    def is_task_create_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_create

    def is_update_task_start_ts_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_enter

    def is_update_duration_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch

    def is_chunk_start_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_enter

    def get_tasks_switched(self, event: Event) -> Tuple[int, int]:
        return event.parent_task_id, event.unique_id

    def is_task_complete_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_leave

    def is_task_sync_event(self, event: Event) -> bool:
        """Use task-sync-begin as the sync event as this easily captures task-create events in the parent"""
        return event.event_type == EventType.sync_begin

    def is_task_suspend_event(self, event: Event) -> bool:
        """Matches both taskwait enter & taskwait discrete"""
        return event.event_type == EventType.sync_begin

    def is_task_resume_event(self, event: Event) -> bool:
        return event.event_type == EventType.sync_end

    def get_task_completed(self, event: Event) -> int:
        return event.encountering_task_id

    @staticmethod
    def get_task_entered(event: Event) -> int:
        return event.encountering_task_id

    def get_task_registered_data(self, event: Event) -> Task:
        assert self.is_task_register_event(event)
        return Task(
            event.unique_id,
            event.encountering_task_id,
            42,
            event.task_label,
            event.time,
            SourceLocation(event.source_file, event.source_func, event.source_line),
        )

    def get_source_location(self, event: Event) -> SourceLocation:
        return SourceLocation(event.source_file, event.source_func, event.source_line)

    def _pre_yield_event_callback(self, event: Event) -> None:
        """Called once for each event before it is sent to super().yield_chunks"""
        return

    def _post_yield_event_callback(self, event: Event) -> None:
        """Called once for each event after it has been sent to super().yield_chunks"""
        if self._return_addresses is not None and hasattr(
            event, Attr.caller_return_address.value
        ):
            address = event.caller_return_address
            if address not in self._return_addresses:
                self._return_addresses.add(address)

    def _filter_event(self, event: Event) -> bool:
        """Return True if an event should be processed when yielding chunks"""
        if event.is_buffer_flush_event():
            self.log.warning("buffer flush event encountered - skipped (%s)", event)
            return False
        return True

    def _filter_with_callbacks(
        self, events_iter: TraceEventIterable
    ) -> TraceEventIterable:
        for location, location_count, event in events_iter:
            if self._filter_event(event):
                self._pre_yield_event_callback(event)
                yield location, location_count, event
                self._post_yield_event_callback(event)

    def generate_chunks(
        self,
        events_iter: TraceEventIterable,
        chunk_builder: ChunkBuilderProtocol,
        task_builder: TaskBuilderProtocol,
    ):
        return super().generate_chunks(
            self._filter_with_callbacks(events_iter), chunk_builder, task_builder
        )


@TaskGraphEventModel.update_chunks_on(event_type=EventType.task_enter)
def update_chunks_task_enter(
    event: Event,
    location: Location,
    location_count: int,
    chunk_builder: ChunkBuilderProtocol,
) -> Optional[int]:
    assert hasattr(event, Attr.encountering_task_id.value)
    task: int = event.encountering_task_id
    assert task != NullTaskID
    try:
        chunk_builder.new_chunk(task, event, location.ref, location_count)
    except ChunkKeyDuplicateError:
        otter.log.error(f"prior events for {task=} when processing task-enter event:")
        otter.log.error(f"  {location=}. {location_count=}")
        otter.log.error(f"  {event}")
        raise
    return None


@TaskGraphEventModel.update_chunks_on(event_type=EventType.task_leave)
def update_chunks_task_leave(
    event: Event,
    location: Location,
    location_count: int,
    chunk_builder: ChunkBuilderProtocol,
) -> Optional[int]:
    assert hasattr(event, Attr.encountering_task_id.value)
    task: int = event.encountering_task_id
    assert task != NullTaskID
    try:
        chunk_builder.append_to_chunk(task, event, location.ref, location_count)
    except ChunkKeyNotFoundError:
        otter.log.error(
            f"no prior events for {task=} when processing task-leave event:"
        )
        otter.log.error(f"  {location=}. {location_count=}")
        otter.log.error(f"  {event}")
        raise
    return task
