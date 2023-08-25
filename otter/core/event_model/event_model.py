from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict, deque
from itertools import islice
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Tuple,
)

from otter.core.chunks import Chunk
from otter.core.events import Event, Location
from otter.core.tasks import NullTask, Task, TaskRegistry, TaskSynchronisationContext
from otter.definitions import (
    Attr,
    EventModel,
    EventType,
    RegionType,
    SourceLocation,
    TaskSyncType,
)
from otter.log import logger_getter
from otter.utils.typing import Decorator

# Type hint aliases
EventList = List[Event]
ChunkDict = Dict[Any, Chunk]
ChunkStackDict = Dict[Any, Deque[Chunk]]
ChunkUpdateHandlerKey = Tuple[Optional[RegionType], EventType]
ChunkUpdateHandlerFn = Callable[
    [Event, Location, ChunkDict, ChunkStackDict, TaskRegistry], Optional[Chunk]
]

get_module_logger = logger_getter("event_model")


# Using a Protocol for better static analysis
class EventModelProtocol(Protocol):
    def __init__(self, task_registry: TaskRegistry):
        ...

    def notify_task_registry(self, event: Event) -> None:
        ...

    def yield_chunks(
        self, events_iter: Iterable[Tuple[Location, Event]]
    ) -> Iterable[Chunk]:
        ...

    def contexts_of(self, chunk: Chunk) -> List[TaskSynchronisationContext]:
        ...


class EventModelFactory:
    event_models: Dict[EventModel, EventModelProtocol] = {}

    @classmethod
    def get_model(cls, model_name: EventModel) -> EventModelProtocol:
        return cls.event_models[model_name]

    @classmethod
    def register(cls, model_name: EventModel):
        def wrapper(model_class: EventModelProtocol):
            cls.event_models[model_name] = model_class
            return model_class

        return wrapper


# Using ABC for a common __init__ between concrete models
class BaseEventModel(ABC):
    def __init__(self, task_registry: TaskRegistry):
        self.log = logger_getter(self.__class__.__name__)()
        self.task_registry: TaskRegistry = task_registry
        self.chunk_dict: Dict[Any, Chunk] = dict()
        self.chunk_stack: Dict[Any, Deque[Chunk]] = defaultdict(deque)

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
    def get_task_completed(self, event: Event) -> int:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def get_task_entered(event: Event) -> int:
        raise NotImplementedError()

    def notify_task_registry(self, event: Event) -> None:
        if self.is_task_register_event(event):
            self.task_registry.register_task(self.get_task_data(event))

        if self.is_update_task_start_ts_event(event):
            task_entered = self.get_task_entered(event)
            self.log.debug(
                "notifying task start time: %d started at %d", task_entered, event.time
            )
            self.task_registry.notify_task_start(
                task_entered, event.time, self.get_task_start_location(event)
            )

        if self.is_update_duration_event(event):
            prior_task_id, next_task_id = self.get_tasks_switched(event)
            self.log.debug(
                "update duration: prior_task=%d next_task=%d %d %s %s",
                prior_task_id,
                next_task_id,
                event.time,
                event.endpoint,
                event.event_type,
            )
            # task_registry.update_task_duration(prior_task_id, next_task_id, event.time)

        if self.is_task_complete_event(event):
            completed_task_id = self.get_task_completed(event)
            self.log.debug(
                "event %s notifying task %d of end_ts", event, completed_task_id
            )
            self.task_registry.notify_task_end(
                completed_task_id, event.time, self.get_task_end_location(event)
            )

    def yield_chunks(
        self, events_iter: Iterable[Tuple[Location, Event]]
    ) -> Iterable[Chunk]:
        log = self.log
        task_registry = self.task_registry
        log.debug(f"receiving events from {events_iter}")

        for k, (location, event) in enumerate(events_iter):
            log.debug(
                "got event %d: %s",
                k,
                event,
            )

            handler = self.get_update_chunk_handler(event)
            if self.event_completes_chunk(event):
                completed_chunk = handler(
                    event, location, self.chunk_dict, self.chunk_stack
                )
                assert completed_chunk is not None
                yield completed_chunk
            elif self.event_updates_chunk(event):
                result = handler(event, location, self.chunk_dict, self.chunk_stack)
                assert result is None
            elif self.event_skips_chunk_update(event):
                pass
            else:  # event applies default chunk update logic
                self.append_to_encountering_task_chunk(event)

            # if self.is_task_register_event(event):
            #     task_registry.register_task(self.get_task_data(event))

            self.notify_task_registry(event)

        log.debug(f"exhausted all events")
        # task_registry.calculate_all_inclusive_duration()
        task_registry.calculate_all_num_descendants()
        task_registry.log_all_task_ts()

    def append_to_encountering_task_chunk(self, event: Event) -> None:
        self.chunk_dict[event.encountering_task_id].append_event(event)

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

    def contexts_of(self, chunk: Chunk) -> List[TaskSynchronisationContext]:
        contexts = list()
        for event in islice(chunk.events, 1, None):
            encountering_task = self.task_registry[event.encountering_task_id]
            if encountering_task is NullTask:
                encountering_task = None
            if event.region_type == RegionType.taskwait:
                chunk.log.debug(
                    f"encountered taskwait barrier: endpoint={event.endpoint}, descendants={event.sync_descendant_tasks == TaskSyncType.descendants}"
                )
                descendants = event.sync_descendant_tasks == TaskSyncType.descendants
                barrier_context = TaskSynchronisationContext(
                    tasks=None, descendants=descendants
                )
                barrier_context.synchronise_from(encountering_task.task_barrier_cache)
                for iterable in encountering_task.task_barrier_iterables_cache:
                    barrier_context.synchronise_lazy(iterable)
                encountering_task.clear_task_barrier_cache()
                encountering_task.clear_task_barrier_iterables_cache()
                contexts.append(barrier_context)
            if self.is_task_create_event(event):
                created_task = self.task_registry[event.unique_id]
                if encountering_task.has_active_task_group:
                    chunk.log.debug(
                        f"registering new task in active task group: parent={encountering_task.id}, child={created_task.id}"
                    )
                    encountering_task.synchronise_task_in_current_group(created_task)
                else:
                    chunk.log.debug(
                        f"registering new task in task barrier cache: parent={encountering_task.id}, child={created_task.id}"
                    )
                    encountering_task.append_to_barrier_cache(created_task)
        return contexts


def get_event_model(
    model_name: EventModel, task_registry: TaskRegistry, *args, **kwargs
) -> EventModelProtocol:
    return EventModelFactory.get_model(model_name)(task_registry, *args, **kwargs)
