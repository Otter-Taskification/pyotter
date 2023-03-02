from .event_model import EventModelFactory, BaseEventModel
from typing import Iterable, Dict, Any, Deque, Callable, Tuple, Optional
from otter.definitions import EventModel, TaskStatus, EventType, RegionType
from otter.core.chunks import Chunk
from otter.core.chunks import yield_chunks as otter_core_yield_chunks
from otter.core.events import (
    Event,
    ParallelBegin,
    ParallelEnd,
    SingleBegin,
    SingleEnd,
    InitialTaskEnter,
    InitialTaskLeave,
    TaskSwitch,
    ThreadBegin,
    ThreadEnd,
    ImplicitTaskEnter,
    ImplicitTaskLeave,
    ChunkSwitchEventMixin
)
from otter.core.tasks import NullTask
from otter.log import logger_getter

get_module_logger = logger_getter("omp_event_model")

# Type hint aliases
ChunkDict = Dict[Any, Chunk]
ChunkStackDict = Dict[Any, Deque[Chunk]]
ChunkUpdateHandlerKey = Tuple[Optional[RegionType], EventType]
ChunkUpdateHandlerFn = Callable[[Event, ChunkDict, ChunkStackDict], Optional[Chunk]]


@EventModelFactory.register(EventModel.OMP)
class OMPEventModel(BaseEventModel):

    # Handlers which update chunks, some of which will return a completed chunk
    chunk_update_handlers: Dict[ChunkUpdateHandlerKey, ChunkUpdateHandlerFn] = dict()

    @classmethod
    def update_chunks_on(cls, event_type: EventType, region_type: RegionType = None) -> Callable[[ChunkUpdateHandlerFn], ChunkUpdateHandlerFn]:
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
            print(f"Registering handler: {handler} with {key=}")
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
                    # TODO: leave this event api call, it represents the case of using the _Event class hierarchy
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
                            completed_chunk = handler(event, self.chunk_dict, self.chunk_stack)
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
                            result = handler(event, self.chunk_dict, self.chunk_stack)
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
                task_registry.register_task(event)

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


    def chunk_to_graph(self, chunk):
        raise NotImplementedError()

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


@OMPEventModel.update_chunks_on(event_type=EventType.parallel_begin)
def update_chunks_parallel_begin(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict) -> None:
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
    chunk_dict[parallel_chunk_key].append_event(event)


@OMPEventModel.update_chunks_on(event_type=EventType.parallel_end)
def update_chunks_parallel_end(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict) -> Chunk:
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
def update_chunks_initial_task_enter(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict) -> None:
    log = get_module_logger()
    chunk_key = event._location.name, event.unique_id, RegionType.task
    log.debug(f"{update_chunks_initial_task_enter.__name__}: {chunk_key=}")
    chunk = chunk_dict[chunk_key]
    chunk.append_event(event)

    # Ensure the initial task's ID gives the correct chunk so that events
    # nested between it and an enclosed parallel region can get the correct
    # chunk for the initial task
    task_chunk_key = event.unique_id
    chunk_dict[task_chunk_key] = chunk


@OMPEventModel.update_chunks_on(event_type=EventType.task_leave, region_type=RegionType.initial_task)
def update_chunks_initial_task_leave(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict) -> Chunk:
    log = get_module_logger()
    chunk_key = event._location.name, event.unique_id, RegionType.task
    log.debug(f"{update_chunks_initial_task_leave.__name__}: {chunk_key=}")
    chunk = chunk_dict[chunk_key]
    chunk.append_event(event)
    return chunk


@OMPEventModel.update_chunks_on(event_type=EventType.task_enter, region_type=RegionType.implicit_task)
def update_chunks_implicit_task_enter(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict) -> None:
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
def update_chunks_implicit_task_leave(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict) -> None:
    log = get_module_logger()
    # don't yield until parallel-end
    chunk_key = event.unique_id
    log.debug(f"{update_chunks_implicit_task_leave.__name__}: {chunk_key=}")
    chunk = chunk_dict[chunk_key]
    chunk.append_event(event)


@OMPEventModel.update_chunks_on(event_type=EventType.master_begin, region_type=RegionType.master)
@OMPEventModel.update_chunks_on(event_type=EventType.workshare_begin, region_type=RegionType.single_executor)
def update_chunks_single_begin(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict) -> None:
    log = get_module_logger()
    # Nested region - append to task chunk, push onto stack, create nested chunk
    task_chunk_key = event.encountering_task_id
    log.debug(f"{update_chunks_single_begin.__name__}: {task_chunk_key=}")
    task_chunk = chunk_dict.pop(task_chunk_key)
    task_chunk.append_event(event)
    # store the enclosing chunk
    chunk_stack[task_chunk_key].append(task_chunk)
    # Create a new nested Chunk for the single region
    chunk_dict[task_chunk_key].append_event(event)


@OMPEventModel.update_chunks_on(event_type=EventType.master_end, region_type=RegionType.master)
@OMPEventModel.update_chunks_on(event_type=EventType.workshare_end, region_type=RegionType.single_executor)
def update_chunks_single_end(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict) -> Chunk:
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
def update_chunks_task_switch(event: Event, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict) -> Optional[Chunk]:
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
    next_chunk = chunk_dict[next_chunk_key]
    next_chunk.append_event(event)
    # Allow nested parallel regions to append to next_chunk where a task
    # creates a nested parallel region?
    nested_parallel_region_chunk_key = (event._location.name, event.unique_id, RegionType.task)
    chunk_dict[nested_parallel_region_chunk_key] = next_chunk
    return completed_chunk  # may be None
