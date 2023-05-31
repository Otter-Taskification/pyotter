from typing import Protocol, Dict, Iterable, Any, TypeVar, Type, Deque, Tuple, Optional, List, Callable
from collections import defaultdict, deque
from abc import ABC, abstractmethod
from loggingdecorators import on_init
from igraph import Graph
from otter.definitions import EventModel, Attr, TaskStatus, EventType, RegionType, EdgeType, Endpoint, TaskType, TaskSyncType, SourceLocation
from otter.core.chunks import Chunk
from otter.core.events import Event, Location
from otter.core.tasks import TaskRegistry, NullTask, TaskData
from otter.log import logger_getter
from otter.utils.typing import Decorator
from otter.utils import transpose_list_to_dict

# Type hint aliases
EventList = List[Event]
ChunkDict = Dict[Any, Chunk]
ChunkStackDict = Dict[Any, Deque[Chunk]]
ChunkUpdateHandlerKey = Tuple[Optional[RegionType], EventType]
ChunkUpdateHandlerFn = Callable[[Event, Location, ChunkDict, ChunkStackDict, TaskRegistry], Optional[Chunk]]

get_module_logger = logger_getter("event_model")

# Using a Protocol for better static analysis
class EventModelProtocol(Protocol):

    def __init__(self, task_registry: TaskRegistry):
        pass

    def yield_chunks(self, events_iter: Iterable[Tuple[Location, Event]]) -> Iterable[Chunk]:
        pass

    def chunk_to_graph(self, chunk: Chunk) -> Graph:
        pass

    def combine_graphs(self, graphs: Iterable[Graph]) -> Graph:
        pass


class EventModelFactory:
    event_models: Dict[EventModel, EventModelProtocol] = dict()

    @classmethod
    def get_model(cls, model_name: EventModel) -> Type[EventModelProtocol]:
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

    def __init_subclass__(subclass):
        # Add to the subclass a dict for registering handlers to update chunks & return completed chunks
        subclass.chunk_update_handlers: Dict[ChunkUpdateHandlerKey, ChunkUpdateHandlerFn] = dict()

    @classmethod
    def update_chunks_on(cls, event_type: EventType, region_type: RegionType = None) -> Decorator[ChunkUpdateHandlerFn]:
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
            assert(key not in cls.chunk_update_handlers)
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
    def get_chunk_update_handlers_key(cls, event: Event, fallback: bool = False) -> ChunkUpdateHandlerKey:
        if fallback or not (Attr.region_type in event):
            return cls.make_chunk_update_handlers_key(event.event_type)
        else:
            return cls.make_chunk_update_handlers_key(event.event_type, event.region_type)

    @staticmethod
    def make_chunk_update_handlers_key(event_type: EventType, region_type: Optional[RegionType] = None) -> ChunkUpdateHandlerKey:
        return region_type, event_type

    def warn_for_incomplete_chunks(self, chunks: Iterable[Chunk]) -> None:
        chunks_stored = set(self.chunk_dict.values())
        chunks_returned = set(chunks)
        for incomplete in chunks_stored - chunks_returned:
            warn(f"Chunk was never returned:\n{incomplete}", category=UserWarning)

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

    def yield_chunks(self, events_iter: Iterable[Tuple[Location, Event]]) -> Iterable[Chunk]:

        log = self.log
        task_registry = self.task_registry
        log.debug(f"receiving events from {events_iter}")

        for k, (location, event) in enumerate(events_iter):
            log.debug(f"got event {k} with vertex label {event.get('vertex_label')}: {event}")

            handler = self.get_update_chunk_handler(event)
            if self.event_completes_chunk(event):
                completed_chunk = handler(event, location, self.chunk_dict, self.chunk_stack)
                assert completed_chunk is not None
                yield completed_chunk
            elif self.event_updates_chunk(event):
                result = handler(event, location, self.chunk_dict, self.chunk_stack)
                assert result is None
            elif self.event_skips_chunk_update(event):
                pass
            else: # event applies default chunk update logic
                self.append_to_encountering_task_chunk(event)

            if self.is_task_register_event(event):
                task_registry.register_task(self.get_task_data(event))

            if self.is_update_task_start_ts_event(event):
                task_entered = self.get_task_entered(event)
                log.debug(f"notifying task start time: {task_entered} started at {event.time}")
                task_registry.notify_task_start(task_entered, event.time, self.get_task_start_location(event))

            if self.is_update_duration_event(event):
                prior_task_id, next_task_id = self.get_tasks_switched(event)
                log.debug(f"update duration: prior_task={prior_task_id} next_task={next_task_id} {event.time} {event.endpoint} {event.event_type}")
                # task_registry.update_task_duration(prior_task_id, next_task_id, event.time)

            if self.is_task_complete_event(event):
                completed_task_id = self.get_task_completed(event)
                log.debug(f"event <{event}> notifying task {completed_task_id} of end_ts")
                task_registry.notify_task_end(completed_task_id, event.time, self.get_task_end_location(event))

        log.debug(f"exhausted {events_iter}")
        # task_registry.calculate_all_inclusive_duration()
        task_registry.calculate_all_num_descendants()
        task_registry.log_all_task_ts()

    def append_to_encountering_task_chunk(self, event: Event) -> None:
        self.chunk_dict[event.encountering_task_id].append_event(event)

    @staticmethod
    @abstractmethod
    def get_augmented_event_attributes(event: Event) -> Dict:
        raise NotImplementedError()

    @abstractmethod
    def get_task_data(self, event: Event) -> TaskData:
        raise NotImplementedError()

    @abstractmethod
    def get_task_start_location(self, event: Event) -> SourceLocation:
        """For an event which represents the start of a task, get the source location of this event"""
        raise NotImplementedError()

    @abstractmethod
    def get_task_end_location(self, event: Event) -> SourceLocation:
        """For an event which represents the end of a task, get the source location of this event"""
        raise NotImplementedError()

    @classmethod
    def unpack(cls, event_list: List[Event]) -> Dict:
        return transpose_list_to_dict([cls.get_augmented_event_attributes(event) for event in event_list])


def get_event_model(model_name: EventModel, task_registry: TaskRegistry, *args, **kwargs) -> EventModelProtocol:
    return EventModelFactory.get_model(model_name)(task_registry, *args, **kwargs)
