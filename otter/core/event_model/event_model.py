from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from itertools import islice
from typing import Any, Deque, Dict, Iterable, List, Optional, Protocol, Tuple, DefaultDict

import otter
from otter.core.chunks import Chunk
from otter.core.chunk_builder import ChunkBuilderProtocol
from otter.core.events import Event, Location
from otter.core.tasks import Task
from otter.definitions import (
    Attr,
    EventModel,
    EventType,
    RegionType,
    SourceLocation,
    TaskSyncType,
    TaskAction,
    NullTaskID
)
from otter.log import logger_getter
from otter.utils.typing import Decorator

# Type hint aliases
EventList = List[Event]
TraceEventIterable = Iterable[Tuple[Location, int, Event]]
ChunkStackDict = Dict[Any, Deque[Chunk]]
ChunkUpdateHandlerKey = Tuple[Optional[RegionType], EventType]

get_module_logger = logger_getter("event_model")


class TaskBuilderProtocol(Protocol):
    """Capable of building a representation of the tasks in a trace"""

    def add_task_metadata(self, task: int, parent: Optional[int], label: str, flavour: int = -1):
        ...

    def add_task_action(self, task: int, action: TaskAction, time: str, location: SourceLocation):
        ...

    def close(self) -> None:
        ...


class ChunkUpdateHandlerFn(Protocol):
    def __call__(
        self,
        event: Event,
        location: Location,
        location_count: int,
        chunk_builder: ChunkBuilderProtocol,
    ) -> Optional[int]:
        ...


# Using ABC for a common __init__ between concrete models
class BaseEventModel(ABC):
    def __init__(self):
        self.log = logger_getter(self.__class__.__name__)()

    def __init_subclass__(cls):
        # Add to the subclass a dict for registering handlers to update chunks & return completed chunks
        cls.chunk_update_handlers: Dict[
            ChunkUpdateHandlerKey, ChunkUpdateHandlerFn
        ] = {}

    @classmethod
    def update_chunks_on(
        cls, event_type: EventType, region_type: RegionType = None
    ) -> Decorator[ChunkUpdateHandlerFn]:
        """
        Register a function which will be called to update the relevant chunks when a matching event is encountered.
        Some events use both region type and event type in the key, others use just the event type. Handlers are first
        looked up by region type and event type, falling back to just the event type if no handler is found in the first
        case.

        Args:
            event_type: the type of event for which this callback should be invoked
            region_type: the region type of the event for which this callback should be invoked

        Returns:
            A decorator which registers the decorated function to be called when a matching event is encountered.

        """

        def decorator(handler: ChunkUpdateHandlerFn) -> ChunkUpdateHandlerFn:
            key = cls.make_chunk_update_handlers_key(event_type, region_type)
            assert key not in cls.chunk_update_handlers
            cls.chunk_update_handlers[key] = handler
            return handler

        return decorator

    @classmethod
    def get_update_chunk_handler(cls, event: Event) -> Optional[ChunkUpdateHandlerFn]:
        # Look up the handler by region & event type, falling back to just event type.
        key = cls.get_chunk_update_handlers_key(event)
        handler = cls.chunk_update_handlers.get(key)
        if not handler:
            key = cls.get_chunk_update_handlers_key(event, fallback=True)
            handler = cls.chunk_update_handlers.get(key)
        return handler

    @classmethod
    def get_chunk_update_handlers_key(
        cls, event: Event, fallback: bool = False
    ) -> ChunkUpdateHandlerKey:
        if fallback or not (event.get(Attr.region_type) is None):
            return cls.make_chunk_update_handlers_key(event.event_type)
        else:
            return cls.make_chunk_update_handlers_key(
                event.event_type, event.region_type
            )

    @staticmethod
    def make_chunk_update_handlers_key(
        event_type: EventType, region_type: Optional[RegionType] = None
    ) -> ChunkUpdateHandlerKey:
        return region_type, event_type

    @abstractmethod
    def event_completes_chunk(self, event: Event) -> bool:
        """Return True if an event marks the end of a chunk. This event causes a completed chunk to be emitted."""
        raise NotImplementedError()

    @abstractmethod
    def event_updates_chunk(self, event: Event) -> bool:
        """Return True if an event has some bespoke chunk-updating logic to apply, but does not mark the end of any chunk."""
        raise NotImplementedError()

    @abstractmethod
    def event_skips_chunk_update(self, event: Event) -> bool:
        """Return True if an event does not update any chunks i.e. it is not represented in a chunk."""
        raise NotImplementedError()

    @abstractmethod
    def is_task_create_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def is_task_register_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def is_update_task_start_ts_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def is_update_duration_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get_tasks_switched(self, event: Event) -> Tuple[int, int]:
        raise NotImplementedError()

    @abstractmethod
    def is_task_complete_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def is_task_sync_event(self, event: Event) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get_task_completed(self, event: Event) -> int:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def get_task_entered(event: Event) -> int:
        raise NotImplementedError()

    def generate_chunks(self, events_iter: TraceEventIterable, chunk_builder: ChunkBuilderProtocol, task_builder: TaskBuilderProtocol) -> int:
        otter.log.debug(f"receiving events from %s", events_iter)

        total_events = 0
        num_chunks = 0
        for k, (location, location_count, event) in enumerate(events_iter, start=1):
            otter.log.debug("got event %d (location=%d, position=%d): %s", k, location, location_count, event)

            # Update the appropriate chunk
            handler = self.get_update_chunk_handler(event)
            if self.event_completes_chunk(event):
                assert handler is not None
                handler(event, location, location_count, chunk_builder)
                num_chunks += 1
            elif self.event_updates_chunk(event):
                assert handler is not None
                handler(event, location, location_count, chunk_builder)
            elif self.event_skips_chunk_update(event):
                pass
            else:  # event applies default chunk update logic
                self.append_to_encountering_task_chunk(event, location, location_count, chunk_builder)

            # Update the task builder
            if self.is_task_register_event(event):
                task = self.get_task_data(event)
                parent_id = task.parent_id if task.parent_id != NullTaskID else None
                task_builder.add_task_metadata(task.id, parent_id, task.task_label)
                task_builder.add_task_action(task.id, TaskAction.INIT, str(event.time), task.init_location)
            if self.is_update_task_start_ts_event(event):
                task_builder.add_task_action(self.get_task_entered(event), TaskAction.START, str(event.time), self.get_task_start_location(event))
            if self.is_task_complete_event(event):
                task_builder.add_task_action(self.get_task_completed(event), TaskAction.END, str(event.time), self.get_task_end_location(event))

            total_events = k

        otter.log.info(f"read %d events", total_events)

        return num_chunks

    def append_to_encountering_task_chunk(
        self, event: Event, location: Location, location_count: int, chunk_builder: ChunkBuilderProtocol
    ) -> None:
        chunk_builder.append_to_chunk(
            event.encountering_task_id, event, location.ref, location_count
        )

    @abstractmethod
    def get_task_data(self, event: Event) -> Task:
        raise NotImplementedError()

    @abstractmethod
    def get_task_start_location(self, event: Event) -> SourceLocation:
        """For an event which represents the start of a task, get the source location of this event"""
        raise NotImplementedError()

    @abstractmethod
    def get_task_end_location(self, event: Event) -> SourceLocation:
        """For an event which represents the end of a task, get the source location of this event"""
        raise NotImplementedError()

    def contexts_of(self, chunk: Chunk) -> List[Tuple[bool, List[int], int]]:
        contexts: List[Tuple[bool, List[int], int]] = []
        task_cache: DefaultDict[int, List[int]] = defaultdict(list)
        for event in islice(chunk.events, 1, None):
            encountering_task_id = event.encountering_task_id
            if event.encountering_task_id == NullTaskID:
                raise RuntimeError("unexpected NullTask")
            if self.is_task_sync_event(event):
                descendants = bool(event.sync_descendant_tasks == TaskSyncType.descendants)
                contexts.append((descendants, task_cache[encountering_task_id], event.time))
                del task_cache[encountering_task_id]
            elif self.is_task_create_event(event):
                task_cache[encountering_task_id].append(event.unique_id)
        return contexts


class EventModelFactory:
    event_models: Dict[EventModel, type[BaseEventModel]] = {}

    @classmethod
    def get_model_class(cls, model_name: EventModel) -> type[BaseEventModel]:
        """Get the class representing a particular event model"""
        return cls.event_models[model_name]

    @classmethod
    def register(cls, model_name: EventModel):
        """Create a decorator which registers that a class represents the given event model"""

        def wrapper(model_class: type[BaseEventModel]):
            cls.event_models[model_name] = model_class
            return model_class

        return wrapper


def get_event_model(model_name: EventModel, *args, **kwargs) -> BaseEventModel:
    cls = EventModelFactory.get_model_class(model_name)
    return cls(*args, **kwargs)
