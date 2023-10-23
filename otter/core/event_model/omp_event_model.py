from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

from otter.core.chunks import Chunk, ChunkDict
from otter.core.events import Event, Location
from otter.core.tasks import Task
from otter.definitions import (
    EventModel,
    EventType,
    RegionType,
    SourceLocation,
    TaskStatus,
)
from otter.log import logger_getter

from .event_model import BaseEventModel, ChunkStackDict, EventModelFactory

get_module_logger = logger_getter("omp_event_model")

# Type hint aliases
EventList = List[Event]
ChunkStackDict = Dict[Any, Deque[Chunk]]
ChunkUpdateHandlerKey = Tuple[Optional[RegionType], EventType]
ChunkUpdateHandlerFn = Callable[
    [Event, Location, ChunkDict, ChunkStackDict, Any], Optional[Chunk]
]


@EventModelFactory.register(EventModel.OMP)
class OMPEventModel(BaseEventModel):
    def __init__(self, *args, **kwargs):
        super().__init__()
        # A dictionary mapping a single-exec or master-begin event to a list of tasks to be synchronised
        self._task_sync_cache: Dict[Event, List[Task]] = defaultdict(list)

    @classmethod
    def event_completes_chunk(cls, event: Event) -> bool:
        return cls.event_updates_and_completes_chunk(event) or (
            cls.is_task_switch_event(event)
            and event.prior_task_status == TaskStatus.complete
        )

    @classmethod
    def event_updates_chunk(cls, event: Event) -> bool:
        return cls.event_updates_and_doesnt_complete_chunk(event) or (
            cls.is_task_switch_event(event)
            and event.prior_task_status != TaskStatus.complete
        )

    @staticmethod
    def event_updates_and_completes_chunk(event: Event) -> bool:
        # parallel-end, single-executor-end, master-end and initial-task-leave always complete a chunk
        return (event.get("region_type"), event.event_type) in [
            (RegionType.parallel, EventType.parallel_end),
            (RegionType.single_executor, EventType.workshare_end),
            (RegionType.master, EventType.master_end),
            (RegionType.initial_task, EventType.task_leave),
        ]

    @staticmethod
    def event_updates_and_doesnt_complete_chunk(event: Event) -> bool:
        # parallel-begin, single-executor-begin, master-begin, initial-task-enter, implicit-task-enter/leave
        # these events have special chunk-updating logic but never complete a chunk
        return (event.get("region_type"), event.event_type) in [
            (RegionType.parallel, EventType.parallel_begin),
            (RegionType.single_executor, EventType.workshare_begin),
            (RegionType.master, EventType.master_begin),
            (RegionType.initial_task, EventType.task_enter),
            (RegionType.implicit_task, EventType.task_enter),
            (RegionType.implicit_task, EventType.task_leave),
        ]

    @staticmethod
    def event_skips_chunk_update(event: Event) -> bool:
        # thread-begin/end events aren't represented in chunks, so won't update them
        return event.event_type in [EventType.thread_begin, EventType.thread_end]

    @classmethod
    def is_event_type(cls, event: Event, event_type: EventType) -> bool:
        return event.event_type == event_type

    @classmethod
    def is_task_register_event(cls, event: Event) -> bool:
        # True for: TaskEnter, TaskCreate
        return event.event_type in (EventType.task_enter, EventType.task_create)

    @classmethod
    def is_task_create_event(cls, event: Event) -> bool:
        return cls.is_event_type(event, EventType.task_create)

    @classmethod
    def is_task_enter_event(cls, event: Event) -> bool:
        return cls.is_event_type(event, EventType.task_enter)

    @classmethod
    def is_task_leave_event(cls, event: Event) -> bool:
        return cls.is_event_type(event, EventType.task_leave)

    @classmethod
    def is_task_switch_event(cls, event: Event) -> bool:
        return cls.is_event_type(event, EventType.task_switch)

    @classmethod
    def is_update_task_start_ts_event(cls, event: Event) -> bool:
        return cls.is_task_enter_event(event) or (
            cls.is_task_switch_event(event) and not cls.is_task_complete_event(event)
        )

    @classmethod
    def get_task_entered(cls, event: Event) -> int:
        if cls.is_task_enter_event(event):
            return event.unique_id
        elif cls.is_task_switch_event(event):
            return event.next_task_id
        else:
            raise NotImplementedError(f"{event}")

    @classmethod
    def is_update_duration_event(cls, event: Event) -> bool:
        return event.event_type in (
            EventType.task_switch,
            EventType.task_enter,
            EventType.task_leave,
        )

    @classmethod
    def get_tasks_switched(cls, event: Event) -> Tuple[int, int]:
        if event.event_type in (EventType.task_enter, EventType.task_leave):
            return event.encountering_task_id, event.unique_id
        elif cls.is_task_switch_event(event):
            return event.encountering_task_id, event.next_task_id
        else:
            raise NotImplementedError(f"{event}")

    @classmethod
    def is_task_complete_event(cls, event: Event) -> bool:
        return cls.is_task_leave_event(event) or (
            cls.is_task_switch_event(event)
            and event.prior_task_status in (TaskStatus.complete, TaskStatus.cancel)
        )

    @classmethod
    def is_task_switch_complete_event(cls, event: Event) -> bool:
        return event.prior_task_status in [TaskStatus.complete, TaskStatus.cancel]

    @classmethod
    def is_task_group_end_event(cls, event: Event) -> bool:
        return (event.region_type, event.event_type) == (
            RegionType.taskgroup,
            EventType.sync_end,
        )

    @classmethod
    def get_task_completed(cls, event: Event) -> int:
        if cls.is_task_leave_event(event):
            return event.unique_id
        elif cls.is_task_switch_event(event):
            assert cls.is_task_complete_event(event)
            return event.encountering_task_id

    @classmethod
    def get_task_data(cls, event: Event) -> Task:
        # Only defined for RegisterTaskDataMixin classes
        # i.e. task-enter, task-create
        assert cls.is_task_register_event(event)
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


@OMPEventModel.update_chunks_on(event_type=EventType.parallel_begin)
def update_chunks_parallel_begin(
    event: Event, location: Location, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict
) -> None:
    log = get_module_logger()
    task_chunk_key = (location.name, event.encountering_task_id, RegionType.task)
    parallel_chunk_key = (location.name, event.unique_id, RegionType.parallel)
    log.debug(
        f"{update_chunks_parallel_begin.__name__}: {task_chunk_key=}, {parallel_chunk_key=}"
    )
    location.enter_parallel_region(event.unique_id)
    if task_chunk_key in chunk_dict:
        # The master thread will already have recorded an event in the task which encountered this parallel
        # region, so update the chunk which was previously created before creating a nested chunk
        task_chunk = chunk_dict[task_chunk_key]
        task_chunk.append_event(event)
        # record enclosing chunk before creating the nested chunk
        chunk_stack[parallel_chunk_key].append(task_chunk)
    if parallel_chunk_key in chunk_dict:
        parallel_chunk = chunk_dict[parallel_chunk_key]
    else:
        parallel_chunk = Chunk(event.region_type)
        chunk_dict[parallel_chunk_key] = parallel_chunk
    parallel_chunk.append_event(event)


@OMPEventModel.update_chunks_on(event_type=EventType.parallel_end)
def update_chunks_parallel_end(
    event: Event, location: Location, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict
) -> Chunk:
    log = get_module_logger()
    task_chunk_key = (location.name, event.encountering_task_id, RegionType.task)
    log.debug(f"{update_chunks_parallel_end.__name__}: {task_chunk_key=}")
    # append to parallel region chunk
    parallel_chunk_key = (location.name, event.unique_id, RegionType.parallel)
    parallel_chunk = chunk_dict[parallel_chunk_key]
    parallel_chunk.append_event(event)
    location.leave_parallel_region()
    if task_chunk_key in chunk_dict.keys():
        # master thread: restore and update the enclosing chunk
        chunk_dict[parallel_chunk_key] = chunk_stack[parallel_chunk_key].pop()
        chunk_dict[parallel_chunk_key].append_event(event)
        chunk_dict[task_chunk_key] = chunk_dict[parallel_chunk_key]
    # return completed parallel region chunk
    return parallel_chunk


@OMPEventModel.update_chunks_on(
    event_type=EventType.task_enter, region_type=RegionType.initial_task
)
def update_chunks_initial_task_enter(
    event: Event, location: Location, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict
) -> None:
    log = get_module_logger()
    chunk_key = location.name, event.unique_id, RegionType.task
    log.debug(f"{update_chunks_initial_task_enter.__name__}: {chunk_key=}")
    if chunk_key in chunk_dict:
        chunk = chunk_dict[chunk_key]
    else:
        chunk = Chunk(event.region_type)
        chunk_dict[chunk_key] = chunk
    chunk.append_event(event)

    # Ensure the initial task's ID gives the correct chunk so that events
    # nested between it and an enclosed parallel region can get the correct
    # chunk for the initial task
    task_chunk_key = event.unique_id
    chunk_dict[task_chunk_key] = chunk


@OMPEventModel.update_chunks_on(
    event_type=EventType.task_leave, region_type=RegionType.initial_task
)
def update_chunks_initial_task_leave(
    event: Event, location: Location, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict
) -> Chunk:
    log = get_module_logger()
    chunk_key = location.name, event.unique_id, RegionType.task
    log.debug(f"{update_chunks_initial_task_leave.__name__}: {chunk_key=}")
    chunk = chunk_dict[chunk_key]
    chunk.append_event(event)
    return chunk


@OMPEventModel.update_chunks_on(
    event_type=EventType.task_enter, region_type=RegionType.implicit_task
)
def update_chunks_implicit_task_enter(
    event: Event, location: Location, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict
) -> None:
    log = get_module_logger()
    # (location name, current parallel ID, defn.RegionType.parallel)
    chunk_key = location.name, location.current_parallel_region, RegionType.parallel
    log.debug(f"{update_chunks_implicit_task_enter.__name__}: {chunk_key=}")
    chunk = chunk_dict[chunk_key]
    chunk.append_event(event)
    # Ensure implicit-task-id points to the same chunk for later events in this task
    chunk_dict[event.unique_id] = chunk

    # Allow nested parallel regions to find the chunk for this implicit task
    implicit_task_chunk_key = location.name, event.unique_id, RegionType.task
    chunk_dict[implicit_task_chunk_key] = chunk


@OMPEventModel.update_chunks_on(
    event_type=EventType.task_leave, region_type=RegionType.implicit_task
)
def update_chunks_implicit_task_leave(
    event: Event, location: Location, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict
) -> None:
    log = get_module_logger()
    # don't yield until parallel-end
    chunk_key = event.unique_id
    log.debug(f"{update_chunks_implicit_task_leave.__name__}: {chunk_key=}")
    chunk = chunk_dict[chunk_key]
    chunk.append_event(event)


@OMPEventModel.update_chunks_on(
    event_type=EventType.master_begin, region_type=RegionType.master
)
@OMPEventModel.update_chunks_on(
    event_type=EventType.workshare_begin, region_type=RegionType.single_executor
)
def update_chunks_single_begin(
    event: Event, location: Location, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict
) -> None:
    log = get_module_logger()
    # Nested region - append to task chunk, push onto stack, create nested chunk
    task_chunk_key = event.encountering_task_id
    log.debug(f"{update_chunks_single_begin.__name__}: {task_chunk_key=}")
    task_chunk = chunk_dict.pop(task_chunk_key)
    task_chunk.append_event(event)
    # store the enclosing chunk
    chunk_stack[task_chunk_key].append(task_chunk)
    # Create a new nested Chunk for the single region
    if task_chunk_key in chunk_dict:
        chunk = chunk_dict[task_chunk_key]
    else:
        chunk = Chunk(event.region_type)
        chunk_dict[task_chunk_key] = chunk
    chunk.append_event(event)


@OMPEventModel.update_chunks_on(
    event_type=EventType.master_end, region_type=RegionType.master
)
@OMPEventModel.update_chunks_on(
    event_type=EventType.workshare_end, region_type=RegionType.single_executor
)
def update_chunks_single_end(
    event: Event, location: Location, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict
) -> Chunk:
    log = get_module_logger()
    # Nested region - append to inner chunk, yield, then pop enclosing chunk & append to that chunk
    task_chunk_key = event.encountering_task_id
    log.debug(f"{update_chunks_single_end.__name__}: {task_chunk_key=}")
    task_chunk = chunk_dict[task_chunk_key]
    task_chunk.append_event(event)
    chunk_dict[task_chunk_key] = chunk_stack[task_chunk_key].pop()
    chunk_dict[task_chunk_key].append_event(event)
    return task_chunk


@OMPEventModel.update_chunks_on(event_type=EventType.task_switch)
def update_chunks_task_switch(
    event: Event, location: Location, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict
) -> Optional[Chunk]:
    log = get_module_logger()
    this_chunk_key = event.encountering_task_id
    next_chunk_key = event.next_task_id
    log.debug(
        f"{update_chunks_task_switch.__name__}: {this_chunk_key=}, {next_chunk_key=}"
    )
    this_chunk = chunk_dict[this_chunk_key]
    completed_chunk = None
    if (
        event.prior_task_status != TaskStatus.switch
    ):  # only update the prior task's chunk if it wasn't a regular switch event
        log.debug(
            f"{update_chunks_task_switch.__name__}: {event} updating chunk key={this_chunk_key} for {event.region_type} with status {event.prior_task_status}"
        )
        this_chunk.append_event(event)
        if event.prior_task_status == TaskStatus.complete:
            completed_chunk = this_chunk
    else:
        log.debug(
            f"{update_chunks_task_switch.__name__}: {event} skipped updating chunk key={this_chunk_key} for {event.region_type} with status {event.prior_task_status}"
        )
    log.debug(
        f"{update_chunks_task_switch.__name__}: {event} updating chunk key={next_chunk_key}"
    )
    if next_chunk_key in chunk_dict:
        next_chunk = chunk_dict[next_chunk_key]
    else:
        next_chunk = Chunk(event.next_task_region_type)
        chunk_dict[next_chunk_key] = next_chunk
    next_chunk.append_event(event)
    # Allow nested parallel regions to append to next_chunk where a task
    # creates a nested parallel region?
    nested_parallel_region_chunk_key = (location.name, event.unique_id, RegionType.task)
    chunk_dict[nested_parallel_region_chunk_key] = next_chunk
    return completed_chunk  # may be None
