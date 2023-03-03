from .event_model import EventModelFactory, BaseEventModel
from typing import Iterable, Dict, Any, Deque, Callable, Tuple, Optional
from warnings import warn
from itertools import islice
from igraph import Graph
from otter.definitions import Attr, EventModel, TaskStatus, EventType, RegionType, EdgeType, Endpoint, TaskSyncType
from otter.core.chunks import Chunk
from otter.core.chunks import yield_chunks as otter_core_yield_chunks
from otter.core.events import (
    Event,
    ChunkSwitchEventMixin
)
from otter.core.tasks import NullTask, TaskData, TaskRegistry, TaskSynchronisationContext
from otter.log import logger_getter
from otter.utils.typing import Decorator

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

    def yield_chunks(self, events: Iterable[Event], use_core: bool=True, use_event_api=True, update_chunks_via_event: bool=True) -> Iterable[Chunk]:

        # Use otter.core.chunks.yield_chunk by default until logic lifted out of that module and into event_model
        if use_core:
            yield from otter_core_yield_chunks(events, self.task_registry)
            return

        log = self.log
        task_registry = self.task_registry
        log.debug(f"receiving events from {events}")

        for k, event in enumerate(events):
            # [DONE]: remove event api call
            log.debug(f"got event {k} with vertex label {event.get('vertex_label')}: {event}")

            if self.is_chunk_switch_event(event, use_event_api=use_event_api):
                log.debug(f"updating chunks")
                # event.update_chunks will emit the completed chunk if this event represents
                # the end of a chunk
                # NOTE: the event.update_chunks logic should probably be factored out of the event class
                # NOTE: and into a separate high-level module to reduce coupling. Should events "know"
                # NOTE: about chunks?
                # NOTE: maybe want separate update_chunk() and update_and_yield_chunk() methods?
                if update_chunks_via_event:
                    # NOTE: leave this event api call, it represents the case of using the _Event class hierarchy
                    yield from filter(None, event.update_chunks(self.chunk_dict, self.chunk_stack))
                else:
                    if self.event_updates_chunks(event):
                        handler = self.get_update_chunk_handler(event)
                        if self.event_yields_chunk(event):
                            # update and yield a completed chunk
                            # require that update_chunks yields non-None value in this case
                            if not handler:
                                # expect this branch should be unreachable as we have implemented callbacks for all
                                # events which can yield chunks
                                # yield from event.update_chunks(self.chunk_dict, self.chunk_stack)
                                raise NotImplementedError(f"no chunk-yielding handler for {event}")
                            self.log.info(f"applying handler {handler=}")
                            completed_chunk = handler(event, self.chunk_dict, self.chunk_stack, self.task_registry)
                            assert completed_chunk is not None
                            yield completed_chunk
                        else:
                            # event must update chunks without producing a completed chunk
                            assert (
                                self.event_updates_chunks_but_cant_yield_chunk(event)
                            or (self.event_updates_and_may_yield_chunk(event) and not self.event_yields_chunk(event)
                            ))
                            # If no handler found, we haven't re-implemented the update_chunks logic for this event
                            # so fall back to event.update chunks and warn that we don't have a handler
                            if not handler:
                                # will eventually remove this branch and assert that all events return None
                                # from their corresponding handler
                                raise NotImplementedError(f"no chunk-updating handler for {event}")
                            self.log.info(f"applying handler {handler=}")
                            result = handler(event, self.chunk_dict, self.chunk_stack, self.task_registry)
                            assert result is None
                    elif self.event_applies_default_chunk_update(event):
                        # apply the default logic for updating the chunk which owns this event
                        self.append_to_encountering_task_chunk(event)
                    else:
                        # this event doesn't update the chunks at all e.g. ThreadBegin, ThreadEnd
                        pass
            else:
                # NOTE: This does EXACTLY the same thing as DefaultUpdateChunksMixin.update_chunks
                # self.chunk_dict[event.encountering_task_id].append_event(event)
                self.append_to_encountering_task_chunk(event)

            # NOTE: might want to absorb all the task-updating logic below into the task registry, but guided by an
            # NOTE: event model which would be responsible for knowing which events should trigger task updates
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

    @staticmethod
    def chunk_to_graph(chunk: Chunk) -> Graph:
        # TODO: re-implement Chunk.graph here, make all calls to _Event api the responsibility of the event model
        return omp_chunk_to_graph(chunk)

    def combine_graphs(self, graphs):
        raise NotImplementedError()

    def append_to_encountering_task_chunk(self, event: Event):
        self.chunk_dict[event.encountering_task_id].append_event(event)

    @classmethod
    def is_chunk_switch_event(cls, event: Event, use_event_api: bool=True) -> bool:
        if use_event_api:
            # events which inherit the ChunkSwitchEvent mixin return True here
            return event.is_chunk_switch_event

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

    @staticmethod
    def event_updates_and_may_yield_chunk(event: Event) -> bool:
        # task-switch completes a chunk only if the prior task was completed
        return event.event_type == EventType.task_switch

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
            assert event.event_type == EventType.task_switch
            return event.prior_task_status == TaskStatus.complete

    @classmethod
    def is_task_register_event(cls, event: Event) -> bool:
        # True for: TaskEnter, TaskCreate
        return event.event_type in (EventType.task_enter, EventType.task_create)

    @classmethod
    def is_update_task_start_ts_event(cls, event: Event) -> bool:
        return (
            event.event_type == EventType.task_enter or
            (event.event_type == EventType.task_switch and not cls.is_task_complete_event(event))
        )

    @classmethod
    def get_task_entered(cls, event: Event) -> int:
        if event.event_type == EventType.task_enter:
            return event.unique_id
        elif event.event_type == EventType.task_switch:
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
        elif event.event_type == EventType.task_switch:
            return event.encountering_task_id, event.next_task_id
        else:
            raise NotImplementedError(f"{event}")

    @classmethod
    def is_task_complete_event(cls, event: Event) -> bool:
        return (
            event.event_type == EventType.task_leave or (
                event.event_type == EventType.task_switch and
                event.prior_task_status in (TaskStatus.complete, TaskStatus.cancel))
        )

    @classmethod
    def get_task_completed(cls, event: Event) -> None:
        if event.event_type == EventType.task_leave:
            return event.unique_id
        elif event.event_type == EventType.task_switch:
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

    def warn_for_incomplete_chunks(self, chunks: Iterable[Chunk]) -> None:
        chunks_stored = set(self.chunk_dict.values())
        chunks_returned = set(chunks)
        for incomplete in chunks_stored - chunks_returned:
            warn(f"Chunk was never returned:\n{incomplete}", category=UserWarning)


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
        parallel_chunk = Chunk(task_registry, event.region_type)
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
        chunk = Chunk(task_registry, event.region_type)
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
        chunk = Chunk(task_registry, event.region_type)
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
        next_chunk = Chunk(task_registry, event.next_task_region_type)
        chunk_dict[next_chunk_key] = next_chunk
    next_chunk.append_event(event)
    # Allow nested parallel regions to append to next_chunk where a task
    # creates a nested parallel region?
    nested_parallel_region_chunk_key = (event._location.name, event.unique_id, RegionType.task)
    chunk_dict[nested_parallel_region_chunk_key] = next_chunk
    return completed_chunk  # may be None


def omp_chunk_to_graph(chunk: Chunk) -> Graph:

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

        # TODO: lift call to _Event api into Chunk
        if event.is_task_switch_event and event is not chunk.last:
            continue

        # The vertex representing this event
        # vertex['event'] is always a list of 1 or more events
        v = graph.add_vertex(event=[event])

        encountering_task = chunk.tasks[event.encountering_task_id]
        if encountering_task is NullTask:
            encountering_task = None

        # Match taskgroup-enter/-leave events
        if event.region_type == RegionType.taskgroup:
            # TODO: lift call to _Event api into Chunk
            if event.is_enter_event:
                taskgroup_enter_event = event

                # Create a context for this taskgroup
                encountering_task.enter_task_sync_group()

            # TODO: lift call to _Event api into Chunk
            elif event.is_leave_event:
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
            # TODO: lift call to _Event api into Chunk
            if event.is_enter_event:
                barrier_cluster_id = (event.encountering_task_id, event.region_type, barrier_cluster_label)
                v['_sync_cluster_id'] = barrier_cluster_id
            # TODO: lift call to _Event api into Chunk
            elif event.is_leave_event:
                if barrier_cluster_id is None:
                    raise RuntimeError("barrier-enter event was None")
                v['_sync_cluster_id'] = barrier_cluster_id
                barrier_cluster_label += 1
                barrier_cluster_id = None

        # Label corresponding taskwait-enter/-leave events so they can be contracted later
        if event.region_type == RegionType.taskwait:
            chunk.log.debug(f"encountered taskwait barrier: endpoint={event.endpoint}, descendants={event.sync_descendant_tasks==TaskSyncType.descendants}")
            # TODO: lift call to _Event api into Chunk
            if event.is_enter_event:
                taskwait_cluster_id = (event.encountering_task_id, event.region_type, taskwait_cluster_label)
                v['_sync_cluster_id'] = taskwait_cluster_id

                # Create a context for the tasks synchronised at this barrier
                descendants = event.sync_descendant_tasks==TaskSyncType.descendants
                barrier_context = TaskSynchronisationContext(tasks=None, descendants=descendants)

                # In a single-exec region, created tasks are recorded in the
                # first event's cache rather than in the parent task's cache.
                if chunk.type == RegionType.single_executor:
                    chunk.log.debug(f"registering tasks at taskwait barrier inside a single-executor chunk")
                    # TODO: lift call to _Event api into Chunk
                    barrier_context.synchronise_from(chunk.first.task_synchronisation_cache)

                    # Forget about events which have been synchronised
                    # TODO: lift call to _Event api into Chunk
                    chunk.first.clear_task_synchronisation_cache()

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

            # TODO: lift call to _Event api into Chunk
            elif event.is_leave_event:
                if taskwait_cluster_id is None:
                    raise RuntimeError("taskwait-enter event was None")
                v['_sync_cluster_id'] = taskwait_cluster_id
                taskwait_cluster_label += 1
                taskwait_cluster_id = None

        # Store a reference to the single-exec event's task-sync cache so
        # that the parent task can synchronise any remaining tasks not
        # synchronised inside the single-exec region
        if event.region_type == RegionType.single_executor:
            # TODO: lift call to _Event api into Chunk
            if event.is_enter_event:
                if encountering_task.has_active_task_group:
                    # Lazily add the single-exec event's cache to the active context
                    group_context = encountering_task.get_current_task_sync_group()
                    # TODO: lift call to _Event api into Chunk
                    group_context.synchronise_lazy(event.task_synchronisation_cache)
                else:
                    # Record a reference to the cache to later add lazily to the next
                    # task-sync barrier this task encounters.
                    # TODO: lift call to _Event api into Chunk
                    encountering_task.append_to_barrier_iterables_cache(event.task_synchronisation_cache)
            # TODO: lift call to _Event api into Chunk
            elif event.is_leave_event and chunk.type == RegionType.single_executor:
                if graph.vcount() == vcount+1:
                    # No new vertices were added since the single-executor-begin event, so label the vertices with _sync_cluster_id to contract later
                    assert(prior_event.region_type == RegionType.single_executor)
                    # TODO: lift call to _Event api into Chunk
                    assert(prior_event.is_enter_event)
                    v['_sync_cluster_id'] = (event.encountering_task_id, event.region_type, sequence_count)
                    prior_vertex['_sync_cluster_id'] = v['_sync_cluster_id']

        # Match master-enter/-leave events
        elif event.region_type == RegionType.master:
            # TODO: lift call to _Event api into Chunk
            if event.is_enter_event:
                master_enter_event = event
            # TODO: lift call to _Event api into Chunk
            elif event.is_leave_event:
                if master_enter_event is None:
                    raise RuntimeError("master-enter event was None")
                v['_master_enter_event'] = master_enter_event
                master_enter_event = None

        # Label nodes in a parallel chunk by their position for easier merging
        # TODO: lift call to _Event api into Chunk
        if chunk.type == RegionType.parallel and (event.is_enter_event or event.is_leave_event) and event.region_type != RegionType.master:
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
        events_bridge_single_master = chunk.events_bridge_single_master_region(prior_event, event)
        events_bridge_parallel = chunk.events_bridge_parallel_region(prior_event, event)
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
        # TODO: lift call to _Event api into Chunk
        if event.is_task_create_event:
            v['_task_cluster_id'] = (event.unique_id, Endpoint.enter)
            dummy_vertex = graph.add_vertex(event=[event])
            dummy_vertex['_task_cluster_id'] = (event.unique_id, Endpoint.leave)
            dummy_vertex['_is_dummy_task_vertex'] = True

            created_task = chunk.tasks[event.unique_id]

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
                    # TODO: lift call to _Event api into Chunk
                    chunk.first.task_synchronisation_cache.append(created_task)

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