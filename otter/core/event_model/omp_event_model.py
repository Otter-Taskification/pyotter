from .event_model import EventModelFactory, BaseEventModel
from collections import defaultdict
from typing import Iterable, Dict, Any, Deque, Callable, Tuple, Optional, List
from warnings import warn
from itertools import islice
from igraph import Graph, disjoint_union, Vertex
from otter.definitions import Attr, EventModel, TaskStatus, EventType, RegionType, EdgeType, Endpoint, TaskType, TaskSyncType
from otter.core.chunks import Chunk
from otter.core.events import (
    is_event_list,
    Event,
    ChunkSwitchEventMixin,
    MasterBegin,
    MasterEnd
)
from otter.core.tasks import NullTask, Task, TaskData, TaskRegistry, TaskSynchronisationContext
from otter.log import logger_getter, DEBUG
from otter.utils.typing import Decorator
from otter.utils import SequenceLabeller, LoggingValidatingReduction, ReductionDict
from otter.utils.vertex_predicates import key_is_not_none
from otter.utils.vertex_attr_handlers import Reduction
from otter.utils import handlers

get_module_logger = logger_getter("omp_event_model")

# Type hint aliases
ChunkDict = Dict[Any, Chunk]
ChunkStackDict = Dict[Any, Deque[Chunk]]
ChunkUpdateHandlerKey = Tuple[Optional[RegionType], EventType]
ChunkUpdateHandlerFn = Callable[[Event, ChunkDict, ChunkStackDict, TaskRegistry], Optional[Chunk]]


@EventModelFactory.register(EventModel.OMP)
class OMPEventModel(BaseEventModel):

    # Handlers which update chunks, some of which will return a completed chunk
    chunk_update_handlers: Dict[ChunkUpdateHandlerKey, ChunkUpdateHandlerFn] = dict()

    def __init__(self, *args, **kwargs):
        # A dictionary mapping a single-exec or master-begin event to a list of tasks to be synchronised
        self._task_sync_cache: Dict[Event, List[Task]] = defaultdict(list)
        super().__init__(*args, **kwargs)

    def get_task_synchronisation_cache(self, event: Event) -> List[Task]:
        assert (event.region_type, event.event_type) in ((RegionType.single_executor, EventType.workshare_begin), (RegionType.master, EventType.master_begin))
        return self._task_sync_cache[event]

    def clear_task_synchronisation_cache(self, event: Event):
        assert (event.region_type, event.event_type) in ((RegionType.single_executor, EventType.workshare_begin), (RegionType.master, EventType.master_begin))
        self._task_sync_cache[event].clear()

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
            key = cls.chunk_update_handlers_key(event_type, region_type)
            # print(f"Registering handler: {handler} with {key=}")
            assert(key not in cls.chunk_update_handlers)
            cls.chunk_update_handlers[key] = handler
            return handler
        return decorator

    @staticmethod
    def chunk_update_handlers_key(event_type: EventType, region_type: Optional[RegionType] = None) -> ChunkUpdateHandlerKey:
        return region_type, event_type

    @classmethod
    def get_update_chunk_handler(cls, event: Event) -> Optional[ChunkUpdateHandlerFn]:
        # Look up the handler by region & event type, falling back to just event type.
        key = cls.chunk_update_handlers_key(event.event_type, event.region_type)
        handler = cls.chunk_update_handlers.get(key)
        if not handler:
            key = cls.chunk_update_handlers_key(event.event_type)
            handler = cls.chunk_update_handlers.get(key)
        return handler

    def yield_chunks(self, events: Iterable[Event]) -> Iterable[Chunk]:

        log = self.log
        task_registry = self.task_registry
        log.debug(f"receiving events from {events}")

        for k, event in enumerate(events):
            log.debug(f"got event {k} with vertex label {event.get('vertex_label')}: {event}")

            if self.has_special_chunk_update_logic(event):
                handler = self.get_update_chunk_handler(event)
                if self.event_completes_chunk(event):
                    completed_chunk = handler(event, self.chunk_dict, self.chunk_stack, self.task_registry)
                    assert completed_chunk is not None
                    yield completed_chunk
                elif self.event_updates_chunk(event):
                    assert (
                        self.event_updates_chunks_but_cant_yield_chunk(event)
                    or (self.event_updates_and_may_yield_chunk(event) and not self.event_yields_chunk(event)
                    ))
                    result = handler(event, self.chunk_dict, self.chunk_stack, self.task_registry)
                    assert result is None
                else:
                    assert False # developer error
            elif self.event_doesnt_update_and_doesnt_yield_chunk(event):
                pass
            else:
                self.append_to_encountering_task_chunk(event)

            # TODO: should be able to simplify this nested if-else, since EventType.thread_begin, EventType.thread_end
            # TODO: presumably shouldn't be classed as chunk-switch events (they don't update the chunks at all)
            # if self.is_chunk_switch_event(event):
            #     log.debug(f"updating chunks")
            #     if self.event_updates_chunks(event):
            #         handler = self.get_update_chunk_handler(event)
            #         if self.event_yields_chunk(event):
            #             # update and yield a completed chunk
            #             # require that update_chunks yields non-None value in this case
            #             if not handler:
            #                 # expect this branch should be unreachable as we have implemented callbacks for all
            #                 # events which can yield chunks
            #                 # yield from event.update_chunks(self.chunk_dict, self.chunk_stack)
            #                 raise NotImplementedError(f"no chunk-yielding handler for {event}")
            #             self.log.info(f"applying handler {handler=}")
            #             completed_chunk = handler(event, self.chunk_dict, self.chunk_stack, self.task_registry)
            #             assert completed_chunk is not None
            #             yield completed_chunk
            #         else:
            #             # event must update chunks without producing a completed chunk
            #             assert (
            #                 self.event_updates_chunks_but_cant_yield_chunk(event)
            #             or (self.event_updates_and_may_yield_chunk(event) and not self.event_yields_chunk(event)
            #             ))
            #             self.log.info(f"applying handler {handler=}")
            #             result = handler(event, self.chunk_dict, self.chunk_stack, self.task_registry)
            #             assert result is None
            #     elif self.event_applies_default_chunk_update(event):
            #         self.append_to_encountering_task_chunk(event)
            #     else:
            #         # this event doesn't update the chunks at all e.g. ThreadBegin, ThreadEnd
            #         pass
            # else:
            #     self.append_to_encountering_task_chunk(event)

            # TODO: might want to absorb all the task-updating logic below into the task registry, but guided by an
            #   event model which would be responsible for knowing which events should trigger task updates
            if self.is_task_register_event(event):
                task_registry.register_task(self.get_task_data(event))

            if self.is_update_task_start_ts_event(event):
                task = task_registry[self.get_task_entered(event)]
                log.debug(f"notifying task start time: {task.id} @ {event.time}")
                if task.start_ts is None:
                    task.start_ts = event.time

            if self.is_update_duration_event(event):
                prior_task_id, next_task_id = self.get_tasks_switched(event)
                log.debug(
                    f"update duration: prior_task={prior_task_id} next_task={next_task_id} {event.time} {event.endpoint:>8} {event}")

                prior_task = task_registry[prior_task_id]
                if prior_task is not NullTask:
                    log.debug(f"got prior task: {prior_task}")
                    prior_task.update_exclusive_duration(event.time)

                next_task = task_registry[next_task_id]
                if next_task is not NullTask:
                    log.debug(f"got next task: {next_task}")
                    next_task.resumed_at(event.time)

            if self.is_task_complete_event(event):
                completed_task_id = self.get_task_completed(event)
                log.debug(f"event <{event}> notifying task {completed_task_id} of end_ts")
                completed_task = task_registry[completed_task_id]
                if completed_task is not NullTask:
                    completed_task.end_ts = event.time

        log.debug(f"exhausted {events}")
        task_registry.calculate_all_inclusive_duration()
        task_registry.calculate_all_num_descendants()

        for task in task_registry:
            log.debug(f"task start time: {task.id}={task.start_ts}")

    def chunk_to_graph(self, chunk: Chunk) -> Graph:
        return omp_chunk_to_graph(self, chunk)

    def combine_graphs(self, graphs: Iterable[Graph]) -> Graph:
        return combine_graphs(self, self.task_registry, graphs)

    def append_to_encountering_task_chunk(self, event: Event):
        self.chunk_dict[event.encountering_task_id].append_event(event)

    @classmethod
    def has_special_chunk_update_logic(cls, event: Event) -> bool:
        return (cls.event_updates_and_yields_chunk(event)
                or cls.event_updates_and_may_yield_chunk(event)
                or cls.event_updates_chunks_but_cant_yield_chunk(event))

    @classmethod
    def event_completes_chunk(cls, event: Event) -> bool:
        return (cls.event_updates_and_yields_chunk(event)
                or (cls.event_updates_and_may_yield_chunk(event)
                    and event.prior_task_status == TaskStatus.complete)
                )

    @classmethod
    def event_updates_chunk(cls, event: Event) -> bool:
        return (cls.event_updates_chunks_but_cant_yield_chunk(event)
                or (cls.event_updates_and_may_yield_chunk(event)
                    and event.prior_task_status != TaskStatus.complete))

    @classmethod
    def is_chunk_switch_event(cls, event: Event) -> bool:

        # these events inherit the ChunkSwitchEvent mixin and *always* yield a completed chunk
        if cls.event_updates_and_yields_chunk(event):
            return True

        # these events inherit the ChunkSwitchEvent mixin and *may* yield a completed chunk
        if cls.event_updates_and_may_yield_chunk(event):
            return True

        # these events inherit the ChunkSwitchEvent mixin and *do* update the chunks, but *never* yield a completed chunk
        if cls.event_updates_chunks_but_cant_yield_chunk(event):
            return True

        # these events inherit the ChunkSwitchEvent mixin, *don't* update any chunks and *never* yield a completed chunk
        if cls.event_doesnt_update_and_doesnt_yield_chunk(event):
            return True

        # represents an unhandled _Event which includes this mixin and should have returned True in this function
        if isinstance(event, ChunkSwitchEventMixin):
            raise TypeError(type(event))

    @staticmethod
    def event_updates_and_yields_chunk(event: Event) -> bool:
        # parallel-end, single-executor-end, master-end and initial-task-leave always complete a chunk
        return (event.get("region_type"), event.event_type) in [
            (RegionType.parallel, EventType.parallel_end),
            (RegionType.single_executor, EventType.workshare_end),
            (RegionType.master, EventType.master_end),
            (RegionType.initial_task, EventType.task_leave)
        ]

    @classmethod
    def event_updates_and_may_yield_chunk(cls, event: Event) -> bool:
        # task-switch completes a chunk only if the prior task was completed
        return cls.is_task_switch_event(event)

    @staticmethod
    def event_updates_chunks_but_cant_yield_chunk(event: Event) -> bool:
        # parallel-begin, single-executor-begin, master-begin, initial-task-enter, implicit-task-enter/leave
        # these events have special chunk-updating logic but never complete a chunk
        return (event.get("region_type"), event.event_type) in [
            (RegionType.parallel, EventType.parallel_begin),
            (RegionType.single_executor, EventType.workshare_begin),
            (RegionType.master, EventType.master_begin),
            (RegionType.initial_task, EventType.task_enter),
            (RegionType.implicit_task, EventType.task_enter),
            (RegionType.implicit_task, EventType.task_leave)
        ]

    @staticmethod
    def event_doesnt_update_and_doesnt_yield_chunk(event: Event) -> bool:
        # thread-begin/end events aren't represented in chunks, so won't update them
        return event.event_type in [EventType.thread_begin, EventType.thread_end]

    @classmethod
    def event_updates_chunks(cls, event: Event) -> bool:
        # These events require some specific logic to update chunks
        # Some of these *also* yield a completed chunk
        # EVERYTHING WHICH CAN POSSIBLY YIELD A COMPLETED CHUNK ALSO UPDATES THE CHUNKS
        return (
            cls.event_updates_and_yields_chunk(event)
         or cls.event_updates_and_may_yield_chunk(event)
         or cls.event_updates_chunks_but_cant_yield_chunk(event)
        )

    @classmethod
    def event_applies_default_chunk_update(cls, event: Event) -> bool:
        return not (cls.event_updates_chunks(event) or cls.event_doesnt_update_and_doesnt_yield_chunk(event))

    @classmethod
    def event_yields_chunk(cls, event: Event) -> bool:
        if cls.event_updates_and_yields_chunk(event):
            return True
        if cls.event_updates_and_may_yield_chunk(event):
            assert cls.is_task_switch_event(event)
            return event.prior_task_status == TaskStatus.complete

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
    def get_task_created(cls, event: Event) -> int:
        assert cls.is_task_create_event(event)
        return event.unique_id

    @classmethod
    def is_update_task_start_ts_event(cls, event: Event) -> bool:
        return (
            cls.is_task_enter_event(event) or
            (cls.is_task_switch_event(event) and not cls.is_task_complete_event(event))
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
        return event.event_type in (EventType.task_switch, EventType.task_enter, EventType.task_leave)

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
        return (
            cls.is_task_leave_event(event) or
            (cls.is_task_switch_event(event) and event.prior_task_status in (TaskStatus.complete, TaskStatus.cancel))
        )

    @classmethod
    def get_task_completed(cls, event: Event) -> int:
        if cls.is_task_leave_event(event):
            return event.unique_id
        elif cls.is_task_switch_event(event):
            assert cls.is_task_complete_event(event)
            return event.encountering_task_id

    @classmethod
    def get_task_data(cls, event: Event) -> TaskData:
        # Only defined for RegisterTaskDataMixin classes
        # i.e. task-enter, task-create
        assert cls.is_task_register_event(event)
        data = {
            Attr.unique_id:       event.unique_id,
            Attr.task_type:       event.task_type,
            Attr.parent_task_id:  event.parent_task_id,
            Attr.time:            event.time
        }
        if Attr.source_file_name in event and Attr.source_func_name in event and Attr.source_line_number in event:
            data[Attr.source_file_name] = event.source_file_name
            data[Attr.source_func_name] = event.source_func_name
            data[Attr.source_line_number] = event.source_line_number
        return data

    @staticmethod
    def events_bridge_region(previous: Event, current: Event, region_types: List[RegionType]) -> bool:
        # Used to check for certain enter-leave event sequences
        # assert is_event(previous) and is_event(current)
        return (previous.region_type in region_types and is_enter_event(previous) and
                current.region_type in region_types and is_leave_event(current))

    @classmethod
    def events_bridge_single_master_region(cls, previous: Event, current: Event) -> bool:
        return cls.events_bridge_region(previous, current, [RegionType.single_executor, RegionType.single_other, RegionType.master])

    @classmethod
    def events_bridge_parallel_region(cls, previous: Event, current: Event) -> bool:
        return cls.events_bridge_region(previous, current, [RegionType.parallel])

    def warn_for_incomplete_chunks(self, chunks: Iterable[Chunk]) -> None:
        chunks_stored = set(self.chunk_dict.values())
        chunks_returned = set(chunks)
        for incomplete in chunks_stored - chunks_returned:
            warn(f"Chunk was never returned:\n{incomplete}", category=UserWarning)

    @staticmethod
    def check_events(events: Iterable[Event], reduce: Callable[[Iterable[bool]], bool], predicate: Callable[[Event], bool]) -> bool:
        return reduce(map(predicate, events))

    @classmethod
    def all_single_exec_events(cls, events: Iterable[Event]):
        return cls.check_events(events, all, lambda event: event.region_type == RegionType.single_executor)

    @classmethod
    def all_master_events(cls, events: Iterable[Event]):
        return cls.check_events(events, all, lambda event: event.region_type == RegionType.master)

    @classmethod
    def all_taskwait_events(cls, events: Iterable[Event]):
        return cls.check_events(events, all, lambda event: event.region_type == RegionType.taskwait)

    @classmethod
    def is_empty_task_region(cls, vertex: Vertex) -> bool:
        # Return True if vertex is a task-enter (-leave) node with no outgoing (incoming) edges
        if vertex['_task_cluster_id'] is None:
            return False
        if vertex['_is_task_enter_node'] or vertex['_is_task_leave_node']:
            return ((vertex['_is_task_leave_node'] == True and vertex.indegree() == 0) or
                    (vertex['_is_task_enter_node'] == True and vertex.outdegree() == 0))
        # TODO: could this be refactored? Don't we already ensure that vertex['event'] is always a list?
        if type(vertex['event']) is list and set(map(type, vertex['event'])) in [{EventType.task_switch}]:
            return ((all(vertex['_is_task_leave_node']) and vertex.indegree() == 0) or
                    (all(vertex['_is_task_enter_node']) and vertex.outdegree() == 0))

    def reject_task_create(self, events: List[Event]) -> List[Event]:
        events_filtered = [event for event in events if not self.is_task_create_event(event)]
        if len(events_filtered) == 0:
            raise NotImplementedError("No events remain after filtering")
        n_args = len(events)
        n_accept = len(events_filtered)
        self.log.debug(f"return {n_accept}/{n_args}: {events_filtered}")
        return events_filtered


@OMPEventModel.update_chunks_on(event_type=EventType.parallel_begin)
def update_chunks_parallel_begin(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict, task_registry: TaskRegistry) -> None:
    log = get_module_logger()
    task_chunk_key = (event._location.name, event.encountering_task_id, RegionType.task)
    parallel_chunk_key = (event._location.name, event.unique_id, RegionType.parallel)
    log.debug(f"{update_chunks_parallel_begin.__name__}: {task_chunk_key=}, {parallel_chunk_key=}")
    event._location.enter_parallel_region(event.unique_id)
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
def update_chunks_parallel_end(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict, task_registry: TaskRegistry) -> Chunk:
    log = get_module_logger()
    task_chunk_key = (event._location.name, event.encountering_task_id, RegionType.task)
    log.debug(f"{update_chunks_parallel_end.__name__}: {task_chunk_key=}")
    # append to parallel region chunk
    parallel_chunk_key = (event._location.name, event.unique_id, RegionType.parallel)
    parallel_chunk = chunk_dict[parallel_chunk_key]
    parallel_chunk.append_event(event)
    event._location.leave_parallel_region()
    if task_chunk_key in chunk_dict.keys():
        # master thread: restore and update the enclosing chunk
        chunk_dict[parallel_chunk_key] = chunk_stack[parallel_chunk_key].pop()
        chunk_dict[parallel_chunk_key].append_event(event)
        chunk_dict[task_chunk_key] = chunk_dict[parallel_chunk_key]
    # return completed parallel region chunk
    return parallel_chunk


@OMPEventModel.update_chunks_on(event_type=EventType.task_enter, region_type=RegionType.initial_task)
def update_chunks_initial_task_enter(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict, task_registry: TaskRegistry) -> None:
    log = get_module_logger()
    chunk_key = event._location.name, event.unique_id, RegionType.task
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


@OMPEventModel.update_chunks_on(event_type=EventType.task_leave, region_type=RegionType.initial_task)
def update_chunks_initial_task_leave(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict, task_registry: TaskRegistry) -> Chunk:
    log = get_module_logger()
    chunk_key = event._location.name, event.unique_id, RegionType.task
    log.debug(f"{update_chunks_initial_task_leave.__name__}: {chunk_key=}")
    chunk = chunk_dict[chunk_key]
    chunk.append_event(event)
    return chunk


@OMPEventModel.update_chunks_on(event_type=EventType.task_enter, region_type=RegionType.implicit_task)
def update_chunks_implicit_task_enter(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict, task_registry: TaskRegistry) -> None:
    log = get_module_logger()
    # (location name, current parallel ID, defn.RegionType.parallel)
    chunk_key = event._location.name, event._location.current_parallel_region, RegionType.parallel
    log.debug(f"{update_chunks_implicit_task_enter.__name__}: {chunk_key=}")
    chunk = chunk_dict[chunk_key]
    chunk.append_event(event)
    # Ensure implicit-task-id points to the same chunk for later events in this task
    chunk_dict[event.unique_id] = chunk

    # Allow nested parallel regions to find the chunk for this implicit task
    implicit_task_chunk_key = event._location.name, event.unique_id, RegionType.task
    chunk_dict[implicit_task_chunk_key] = chunk


@OMPEventModel.update_chunks_on(event_type=EventType.task_leave, region_type=RegionType.implicit_task)
def update_chunks_implicit_task_leave(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict, task_registry: TaskRegistry) -> None:
    log = get_module_logger()
    # don't yield until parallel-end
    chunk_key = event.unique_id
    log.debug(f"{update_chunks_implicit_task_leave.__name__}: {chunk_key=}")
    chunk = chunk_dict[chunk_key]
    chunk.append_event(event)


@OMPEventModel.update_chunks_on(event_type=EventType.master_begin, region_type=RegionType.master)
@OMPEventModel.update_chunks_on(event_type=EventType.workshare_begin, region_type=RegionType.single_executor)
def update_chunks_single_begin(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict, task_registry: TaskRegistry) -> None:
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


@OMPEventModel.update_chunks_on(event_type=EventType.master_end, region_type=RegionType.master)
@OMPEventModel.update_chunks_on(event_type=EventType.workshare_end, region_type=RegionType.single_executor)
def update_chunks_single_end(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict, task_registry: TaskRegistry) -> Chunk:
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
def update_chunks_task_switch(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict, task_registry: TaskRegistry) -> Optional[Chunk]:
    log = get_module_logger()
    this_chunk_key = event.encountering_task_id
    next_chunk_key = event.next_task_id
    log.debug(f"{update_chunks_task_switch.__name__}: {this_chunk_key=}, {next_chunk_key=}")
    this_chunk = chunk_dict[this_chunk_key]
    completed_chunk = None
    if event.prior_task_status != TaskStatus.switch: # only update the prior task's chunk if it wasn't a regular switch event
        log.debug(f"{update_chunks_task_switch.__name__}: {event} updating chunk key={this_chunk_key} for {event.region_type} with status {event.prior_task_status}")
        this_chunk.append_event(event)
        if event.prior_task_status == TaskStatus.complete:
            completed_chunk = this_chunk
    else:
        log.debug(f"{update_chunks_task_switch.__name__}: {event} skipped updating chunk key={this_chunk_key} for {event.region_type} with status {event.prior_task_status}")
    log.debug(f"{update_chunks_task_switch.__name__}: {event} updating chunk key={next_chunk_key}")
    if next_chunk_key in chunk_dict:
        next_chunk = chunk_dict[next_chunk_key]
    else:
        next_chunk = Chunk(event.next_task_region_type)
        chunk_dict[next_chunk_key] = next_chunk
    next_chunk.append_event(event)
    # Allow nested parallel regions to append to next_chunk where a task
    # creates a nested parallel region?
    nested_parallel_region_chunk_key = (event._location.name, event.unique_id, RegionType.task)
    chunk_dict[nested_parallel_region_chunk_key] = next_chunk
    return completed_chunk  # may be None


def omp_chunk_to_graph(event_model: OMPEventModel, chunk: Chunk) -> Graph:

    chunk.log.debug(f"transforming chunk to graph (type={chunk.type}) {chunk.first=}")

    graph: Graph = Graph(directed=True)
    prior_vertex = graph.add_vertex(event=[chunk.first])
    prior_event = chunk.first

    # Used to save taskgroup-enter event to match to taskgroup-leave event
    taskgroup_enter_event = None

    # Used to save taskwait-enter event to match to taskwait-leave event
    taskwait_enter_event = None

    # Used to attach dummy label to matching taskwait-enter/leave nodes
    taskwait_cluster_id = None
    taskwait_cluster_label = 0

    barrier_cluster_id = None
    barrier_cluster_label = 0

    # Match master-enter event to corresponding master-leave
    master_enter_event = chunk.first if chunk.first.region_type == RegionType.master else None

    # Add attributes to the first node depending on chunk region type
    if chunk.type == RegionType.parallel:
        prior_vertex["_parallel_sequence_id"] = (chunk.first.unique_id, chunk.first.endpoint)
    elif chunk.type == RegionType.explicit_task:
        prior_vertex['_is_task_enter_node'] = True
        prior_vertex['_task_cluster_id'] = (chunk.first.unique_id, Endpoint.enter)
    elif chunk.type == RegionType.single_executor:
        vcount = 1

    # Used for labelling sequences of certain events in a parallel chunk
    sequence_count = 1

    for event in islice(chunk._events, 1, None):

        if event.region_type in [RegionType.implicit_task]:
            continue

        if event_model.is_task_switch_event(event) and event is not chunk.last:
            continue

        # The vertex representing this event
        # vertex['event'] is always a list of 1 or more events
        v = graph.add_vertex(event=[event])

        encountering_task = event_model.task_registry[event.encountering_task_id]
        if encountering_task is NullTask:
            encountering_task = None

        # Match taskgroup-enter/-leave events
        if event.region_type == RegionType.taskgroup:
            if is_enter_event(event):
                taskgroup_enter_event = event
                # Create a context for this taskgroup
                encountering_task.enter_task_sync_group()
            elif is_leave_event(event):
                if taskgroup_enter_event is None:
                    raise RuntimeError("taskgroup-enter event was None")
                v['_taskgroup_enter_event'] = taskgroup_enter_event
                taskgroup_enter_event = None

                # Leave the context for this taskgroup
                group_context = encountering_task.leave_task_sync_group()
                v['_group_context'] = group_context
                v['_task_sync_context'] = (EdgeType.taskgroup, group_context)

        # Label corresponding barrier-enter/leave events so they can be contracted
        if event.region_type in [RegionType.barrier_implicit, RegionType.barrier_explicit]:
            if is_enter_event(event):
                barrier_cluster_id = (event.encountering_task_id, event.region_type, barrier_cluster_label)
                v['_sync_cluster_id'] = barrier_cluster_id
            elif is_leave_event(event):
                if barrier_cluster_id is None:
                    raise RuntimeError("barrier-enter event was None")
                v['_sync_cluster_id'] = barrier_cluster_id
                barrier_cluster_label += 1
                barrier_cluster_id = None

        # Label corresponding taskwait-enter/-leave events so they can be contracted later
        if event.region_type == RegionType.taskwait:
            chunk.log.debug(f"encountered taskwait barrier: endpoint={event.endpoint}, descendants={event.sync_descendant_tasks==TaskSyncType.descendants}")
            if is_enter_event(event):
                taskwait_cluster_id = (event.encountering_task_id, event.region_type, taskwait_cluster_label)
                v['_sync_cluster_id'] = taskwait_cluster_id

                # Create a context for the tasks synchronised at this barrier
                descendants = event.sync_descendant_tasks==TaskSyncType.descendants
                barrier_context = TaskSynchronisationContext(tasks=None, descendants=descendants)

                # In a single-exec region, created tasks are recorded in the
                # first event's cache rather than in the parent task's cache.
                if chunk.type == RegionType.single_executor:
                    chunk.log.debug(f"registering tasks at taskwait barrier inside a single-executor chunk")
                    barrier_context.synchronise_from(event_model.get_task_synchronisation_cache(chunk.first))
                    event_model.clear_task_synchronisation_cache(chunk.first)

                # Register tasks synchronised at a barrier
                else:
                    chunk.log.debug(f"registering tasks at taskwait barrier")
                    barrier_context.synchronise_from(encountering_task.task_barrier_cache)

                    # If the parent task encountered any single-exec regions
                    # ensure that tasks created and not synchronised in those
                    # regions are now synchronised at this barrier. Must be
                    # done lazily as the enclosed chunk may not have been
                    # parsed fully yet.
                    for iterable in encountering_task.task_barrier_iterables_cache:
                        barrier_context.synchronise_lazy(iterable)

                    # Forget about tasks and task iterables synchronised here
                    encountering_task.clear_task_barrier_cache()
                    encountering_task.clear_task_barrier_iterables_cache()

                v['_barrier_context'] = barrier_context
                v['_task_sync_context'] = (EdgeType.taskwait, barrier_context)

            elif is_leave_event(event):
                if taskwait_cluster_id is None:
                    raise RuntimeError("taskwait-enter event was None")
                v['_sync_cluster_id'] = taskwait_cluster_id
                taskwait_cluster_label += 1
                taskwait_cluster_id = None

        # Store a reference to the single-exec event's task-sync cache so
        # that the parent task can synchronise any remaining tasks not
        # synchronised inside the single-exec region
        if event.region_type == RegionType.single_executor:
            if is_enter_event(event):
                if encountering_task.has_active_task_group:
                    # Lazily add the single-exec event's cache to the active context
                    group_context = encountering_task.get_current_task_sync_group()
                    group_context.synchronise_lazy(event_model.get_task_synchronisation_cache(event))
                else:
                    # Record a reference to the cache to later add lazily to the next
                    # task-sync barrier this task encounters.
                    encountering_task.append_to_barrier_iterables_cache(event_model.get_task_synchronisation_cache(event))
            elif is_leave_event(event) and chunk.type == RegionType.single_executor:
                if graph.vcount() == vcount+1:
                    # No new vertices were added since the single-executor-begin event, so label the vertices with _sync_cluster_id to contract later
                    assert(prior_event.region_type == RegionType.single_executor)
                    assert(is_enter_event(prior_event))
                    v['_sync_cluster_id'] = (event.encountering_task_id, event.region_type, sequence_count)
                    prior_vertex['_sync_cluster_id'] = v['_sync_cluster_id']

        # Match master-enter/-leave events
        elif event.region_type == RegionType.master:
            if is_enter_event(event):
                master_enter_event = event
            elif is_leave_event(event):
                if master_enter_event is None:
                    raise RuntimeError("master-enter event was None")
                v['_master_enter_event'] = master_enter_event
                master_enter_event = None

        # Label nodes in a parallel chunk by their position for easier merging
        if chunk.type == RegionType.parallel and (is_enter_event(event) or is_leave_event(event)) and event.region_type != RegionType.master:
            v["_parallel_sequence_id"] = (chunk.first.unique_id, sequence_count)
            sequence_count += 1

        # Label nested parallel regions for easier merging, except a parallel chunk's closing parallel-end event
        if event.region_type == RegionType.parallel:
            v["_parallel_sequence_id"] = (chunk.first.unique_id if event is chunk.last else event.unique_id, event.endpoint)
            # Should be equivalent to this more explicit form:
            # if event is self.last:
            #     v["_parallel_sequence_id"] = (self.first.unique_id, event.endpoint)
            # else:
            #     v["_parallel_sequence_id"] = (event.unique_id, event.endpoint)

        # Add edge except for (single/master begin -> end) and (parallel N begin -> parallel N end)
        # This avoids creating spurious edges between vertices representing nested chunks
        events_bridge_single_master = event_model.events_bridge_single_master_region(prior_event, event)
        events_bridge_parallel = event_model.events_bridge_parallel_region(prior_event, event)
        events_have_same_id = event.unique_id == prior_event.unique_id if events_bridge_parallel else False
        if not (events_bridge_single_master or (events_bridge_parallel and events_have_same_id)):
        # Should be equivalent and may be more explicit
        # if not events_bridge_single_master and not (events_bridge_parallel and events_have_same_id):
            chunk.log.debug(f"add edge from: {prior_event} to: {event}")
            graph.add_edge(prior_vertex, v)
        else:
            msg = f"edge skipped:\n  src: {prior_event}\n  dst: {event}"
            for line in msg.split("\n"):
                chunk.log.debug(line)

        # For task-create add dummy nodes for easier merging
        if event_model.is_task_create_event(event):
            v['_task_cluster_id'] = (event.unique_id, Endpoint.enter)
            dummy_vertex = graph.add_vertex(event=[event])
            dummy_vertex['_task_cluster_id'] = (event.unique_id, Endpoint.leave)
            dummy_vertex['_is_dummy_task_vertex'] = True

            created_task = event_model.task_registry[event.unique_id]

            # If there is a task group context currently active, add the created task to it
            # Otherwise add to the relevant cache
            if encountering_task.has_active_task_group:
                chunk.log.debug(f"registering new task in active task group: parent={encountering_task.id}, child={created_task.id}")
                encountering_task.synchronise_task_in_current_group(created_task)
            else:

                # In a single-executor chunk, record tasks in the single-exec-enter
                # event's task-sync cache so that any tasks not synchronised
                # at the end of this chunk are made available to the enclosing
                # chunk to synchronise after the single region.
                if chunk.type == RegionType.single_executor:
                    chunk.log.debug(f"registering new task in single-executor cache: parent={encountering_task.id}, child={created_task.id}")
                    event_model.get_task_synchronisation_cache(chunk.first).append(created_task)

                # For all other chunk types, record the task created in the
                # parent task's task-sync cache, to be added to the next task
                # synchronisation barrier.
                else:
                    chunk.log.debug(f"registering new task in task barrier cache: parent={encountering_task.id}, child={created_task.id}")
                    encountering_task.append_to_barrier_cache(created_task)

            continue  # to skip updating prior_vertex

        if event is chunk.last and chunk.type == RegionType.explicit_task:
            v['_is_task_leave_node'] = True
            v['_task_cluster_id'] = (event.encountering_task_id, Endpoint.leave)

        prior_vertex = v
        prior_event = event

    final_vertex = prior_vertex
    first_vertex = graph.vs[0]

    if chunk.type == RegionType.explicit_task and len(chunk) <= 2:
        graph.delete_edges([0])

    # If no internal vertices, require at least 1 edge (except for empty explicit task chunks)
    # Require at least 1 edge between start & end vertices in EMPTY parallel & single-executor chunk if disconnected
    if graph.ecount() == 0:
        chunk.log.debug(f"graph contains no edges (type={chunk.type}, events={len(chunk)})")
        if chunk.type == RegionType.explicit_task:
            chunk.log.debug(f"don't add edges for empty explicit task chunks")
            pass
        elif len(chunk) <= 2 or (chunk.type == RegionType.parallel and len(chunk) <= 4):
            # Parallel chunks contain implicit-task-begin/end events which are skipped, but count towards len(self)
            chunk.log.debug(f"no internal vertices - add edge from: {graph.vs[0]['event']} to: {graph.vs[1]['event']}")
            graph.add_edge(graph.vs[0], graph.vs[1])

    # For parallel & single-executor chunks which are disconnected and have internal vertices (and thus some edges), connect start & end vertices
    if (len(final_vertex.in_edges()) == 0) and (
        (chunk.type == RegionType.single_executor and len(chunk) > 2) or
        (chunk.type == RegionType.parallel and len(chunk) > 4)
    ):
        chunk.log.debug(f"detected disconnected chunk of type {chunk.type}")
        edge = graph.add_edge(first_vertex, final_vertex)

    return graph


def is_enter_event(event: Event) -> bool:
    return event.event_type in (
        EventType.thread_begin, EventType.parallel_begin, EventType.sync_begin, EventType.workshare_begin, EventType.phase_begin
    )


def is_leave_event(event: Event) -> bool:
    return event.event_type in (
        EventType.thread_end, EventType.parallel_end, EventType.sync_end, EventType.workshare_end, EventType.phase_end
    )


"""
Steps to implement here:
log.info(f"combining vertices by parallel sequence ID")
log.info(f"combining vertices by single-begin/end event")
log.info(f"combining vertices by master-begin/end event")
log.info(f"deleting redundant edges due to master regions: {len(redundant_edges)}")
log.info("combining vertices by task ID & endpoint")
log.info("combining vertices by task ID where there are no nested nodes")
log.info("combining redundant sync and loop enter/leave node pairs")
graph.simplify(combine_edges='first')

# The graph is then combined. Remaining steps:
# Unpack vertex event attributes
# Dump graph details to file
# Clean up temporary vertex attributes
# Write report
"""

def reduce_by_parallel_sequence(event_model: OMPEventModel, reductions: ReductionDict, graph: Graph) -> Graph:
    """
    Contract vertices according to _parallel_sequence_id to combine the chunks generated by the threads of a parallel block.
    When combining the 'event' vertex attribute, keep single-executor events over single-other events. All other events
    should be combined in a list.
    """
    event_model.log.info(f"combining vertices by parallel sequence ID")

    # Label vertices with the same _parallel_sequence_id
    labeller = SequenceLabeller(key_is_not_none('_parallel_sequence_id'), group_label='_parallel_sequence_id')

    # When combining the event vertex attribute, prioritise single-executor over single-other
    reductions['event'] = LoggingValidatingReduction(handlers.return_unique_single_executor_event)

    vcount = graph.vcount()
    graph.contract_vertices(labeller.label(graph.vs), combine_attrs=reductions)
    vcount_prev, vcount = vcount, graph.vcount()
    event_model.log.info(f"vertex count updated: {vcount_prev} -> {vcount}")
    return graph

def reduce_by_single_exec_event(event_model: OMPEventModel, reductions: ReductionDict, graph: Graph) -> Graph:
    """
    Contract those vertices which refer to the same single-executor event. This connects single-executor chunks to the
    chunks containing them, as both chunks contain references to the single-exec-begin/end events.
    """
    event_model.log.info(f"combining vertices by single-begin/end event")

    # Label single-executor vertices which refer to the same event.
    labeller = SequenceLabeller(lambda vtx: event_model.all_single_exec_events(vtx['event']), group_label=lambda vtx: vtx['event'][0])

    vcount = graph.vcount()
    graph.contract_vertices(labeller.label(graph.vs), combine_attrs=reductions)
    vcount_prev, vcount = vcount, graph.vcount()
    event_model.log.info(f"vertex count updated: {vcount_prev} -> {vcount}")
    return graph

def reduce_by_master_event(event_model: OMPEventModel, reductions: ReductionDict, graph: Graph) -> Graph:
    """
    master-leave vertices (which refer to their master-leave event) refer to their corresponding master-enter event.
    """
    event_model.log.info(f"combining vertices by master-begin/end event")

    # Label vertices which refer to the same master-begin/end event
    # SUSPECT THIS SHOULD BE "vertex['event'][0]"
    # NOT TESTED!
    labeller = SequenceLabeller(lambda vtx: event_model.all_master_events(vtx['event']), group_label=lambda vtx: vtx['event'][0])

    # When combining events, there should be exactly 1 unique master-begin/end event
    reductions['event'] = LoggingValidatingReduction(handlers.return_unique_master_event)

    vcount = graph.vcount()
    graph.contract_vertices(labeller.label(graph.vs), combine_attrs=reductions)
    vcount_prev, vcount = vcount, graph.vcount()
    event_model.log.info(f"vertex count updated: {vcount_prev} -> {vcount}")
    return graph

def remove_redundant_master_edges(event_model: OMPEventModel, reductions: ReductionDict, graph: Graph) -> Graph:
    """
    Intermediate clean-up: for each master region, remove edges that connect
    the same nodes as the master region

    *** WARNING ********************************************************************
    ********************************************************************************

    *** This step assumes vertex['event'] is a bare event instead of an event list ***
    """

    master_enter_vertices = filter(lambda vertex: isinstance(vertex['event'], MasterBegin), graph.vs)
    master_leave_vertices = filter(lambda vertex: isinstance(vertex['event'], MasterEnd), graph.vs)
    master_enter_vertex_map = {enter_vertex['event']: enter_vertex for enter_vertex in master_enter_vertices}
    master_vertex_pairs = ((master_enter_vertex_map[leave_vertex['_master_enter_event']], leave_vertex) for leave_vertex
                           in master_leave_vertices)
    neighbour_pairs = {(enter.predecessors()[0], leave.successors()[0]) for enter, leave in master_vertex_pairs}
    redundant_edges = list(filter(lambda edge: (edge.source_vertex, edge.target_vertex) in neighbour_pairs, graph.es))
    event_model.log.info(f"deleting redundant edges due to master regions: {len(redundant_edges)}")
    graph.delete_edges(redundant_edges)
    return graph

    """
    ********************************************************************************
    ********************************************************************************
    """

def reduce_by_task_cluster_id(event_model: OMPEventModel, reductions: ReductionDict, graph: Graph) -> Graph:
    """
    Contract by _task_cluster_id, rejecting task-create vertices to replace them with the corresponding task's chunk.
    """
    event_model.log.info("combining vertices by task ID & endpoint")

    # Label vertices which have the same _task_cluster_id
    labeller = SequenceLabeller(key_is_not_none('_task_cluster_id'), group_label='_task_cluster_id')

    # When combining events by _task_cluster_id, reject task-create events (in favour of task-switch events)
    reductions['event'] = LoggingValidatingReduction(event_model.reject_task_create)

    vcount = graph.vcount()
    graph.contract_vertices(labeller.label(graph.vs), combine_attrs=reductions)
    vcount_prev, vcount = vcount, graph.vcount()
    event_model.log.info(f"vertex count updated: {vcount_prev} -> {vcount}")
    return graph

def reduce_by_task_id_for_empty_tasks(event_model: OMPEventModel, reductions: ReductionDict, graph: Graph) -> Graph:
    """
    Contract vertices with the same task ID where the task chunk contains no internal vertices to get 1 vertex per empty
    task region.
    """
    event_model.log.info("combining vertices by task ID where there are no nested nodes")

    # Label vertices which represent empty tasks and have the same task ID
    labeller = SequenceLabeller(event_model.is_empty_task_region, group_label=lambda v: v['_task_cluster_id'][0])

    # Combine _task_cluster_id tuples in a set (to remove duplicates)
    reductions['_task_cluster_id'] = LoggingValidatingReduction(handlers.pass_the_set_of_values,
                                                                accept=tuple,
                                                                msg="combining attribute: _task_cluster_id")

    vcount = graph.vcount()
    graph.contract_vertices(labeller.label(graph.vs), combine_attrs=reductions)
    vcount_prev, vcount = vcount, graph.vcount()
    event_model.log.info(f"vertex count updated: {vcount_prev} -> {vcount}")
    return graph

def reduce_by_sync_cluster_id(event_model: OMPEventModel, reductions: ReductionDict, graph: Graph) -> Graph:
    """
    Contract pairs of directly connected vertices which represent empty barriers, taskwait & loop regions.
    """
    event_model.log.info("combining redundant sync and loop enter/leave node pairs")

    # Label vertices with the same _sync_cluster_id
    labeller = SequenceLabeller(key_is_not_none('_sync_cluster_id'), group_label='_sync_cluster_id')

    # Silently return the list of combined arguments
    reductions['event'] = LoggingValidatingReduction(handlers.pass_args)

    vcount = graph.vcount()
    graph.contract_vertices(labeller.label(graph.vs), combine_attrs=reductions)
    vcount_prev, vcount = vcount, graph.vcount()
    event_model.log.info(f"vertex count updated: {vcount_prev} -> {vcount}")
    return graph


def combine_graphs(event_model: OMPEventModel, task_registry: TaskRegistry, graphs: Iterable[Graph]) -> Graph:
    log = get_module_logger()
    log.info("combining graphs")
    graph = disjoint_union(graphs)
    vcount = graph.vcount()
    log.info(f"graph disjoint union has {vcount} vertices")

    vertex_attribute_names = ['_parallel_sequence_id',
                              '_task_cluster_id',
                              '_is_task_enter_node',
                              '_is_task_leave_node',
                              '_is_dummy_task_vertex',
                              '_region_type',
                              '_master_enter_event',
                              '_taskgroup_enter_event',
                              '_sync_cluster_id',
                              '_barrier_context',
                              '_group_context',
                              '_task_sync_context'
                              ]

    # Ensure some vertex attributes are defined
    for name in vertex_attribute_names:
        if name not in graph.vs.attribute_names():
            graph.vs[name] = None

    # Define some edge attributes
    for name in [Attr.edge_type]:
        if name not in graph.es.attribute_names():
            graph.es[name] = None

    # Make a table for mapping vertex attributes to handlers - used by ig.Graph.contract_vertices
    strategies = ReductionDict(graph.vs.attribute_names(), level=DEBUG)

    # Supply the logic to use when combining each of these vertex attributes
    attribute_handlers: List[Tuple[str, Reduction, Iterable[type]]] = [
        ("_master_enter_event", handlers.return_unique_master_event, (type(None), Event)),
        ("_task_cluster_id", handlers.pass_the_unique_value, (type(None), tuple)),
        ("_is_task_enter_node", handlers.pass_bool_value, (type(None), bool)),
        ("_is_task_leave_node", handlers.pass_bool_value, (type(None), bool))
    ]
    for attribute, handler, accept in attribute_handlers:
        strategies[attribute] = LoggingValidatingReduction(handler, accept=accept,
                                                           msg=f"combining attribute: {attribute}")

    # Give each task a reference to the dummy task-create vertex that was inserted
    # into the chunk where the task-create event happened
    log.debug(f"notify each task of its dummy task-create vertex")
    for dummy_vertex in filter(lambda v: v['_is_dummy_task_vertex'], graph.vs):
        assert is_event_list(dummy_vertex['event'])
        assert len(dummy_vertex['event']) == 1
        event = dummy_vertex['event'][0]
        assert event_model.is_task_create_event(event)
        task_id = event_model.get_task_created(event)
        task_created = task_registry[task_id]
        setattr(task_created, '_dummy_vertex', dummy_vertex)
        log.debug(f" - notify task {task_id} of vertex {task_created._dummy_vertex}")

    # Get all the task sync contexts from the taskwait & taskgroup vertices and create edges for them
    log.debug(f"getting task synchronisation contexts")
    for task_sync_vertex in filter(lambda v: v['_task_sync_context'] is not None, graph.vs):
        log.debug(f"task sync vertex: {task_sync_vertex}")
        edge_type, context = task_sync_vertex['_task_sync_context']
        assert context is not None
        log.debug(f" ++ got context: {context}")
        for synchronised_task in context:
            log.debug(f"    got synchronised task {synchronised_task.id}")
            edge = graph.add_edge(synchronised_task._dummy_vertex, task_sync_vertex)
            edge[Attr.edge_type] = edge_type
            if context.synchronise_descendants:
                # Add edges for descendants of synchronised_task
                for descendant_task_id in task_registry.descendants_while(synchronised_task.id, lambda task: not task.is_implicit()):
                    descendant_task = task_registry[descendant_task_id]
                    # This task is synchronised by the context
                    edge = graph.add_edge(descendant_task._dummy_vertex, task_sync_vertex)
                    edge[Attr.edge_type] = edge_type
                    log.debug(f"    ++ got synchronised descendant task {descendant_task_id}")

    log.info(f"combining vertices...")
    graph = reduce_by_parallel_sequence(event_model, strategies, graph)
    graph = reduce_by_single_exec_event(event_model, strategies, graph)
    graph = reduce_by_master_event(event_model, strategies, graph)
    graph = remove_redundant_master_edges(event_model, strategies, graph)
    graph = reduce_by_task_cluster_id(event_model, strategies, graph)
    graph = reduce_by_task_id_for_empty_tasks(event_model, strategies, graph)
    graph = reduce_by_sync_cluster_id(event_model, strategies, graph)
    graph.simplify(combine_edges='first')
    return graph
