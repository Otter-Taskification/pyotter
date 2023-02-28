from .event_model import EventModelFactory, BaseEventModel
from typing import Iterable, Dict, Any, Deque
from otter.definitions import EventModel, TaskStatus
from otter.core.chunks import Chunk
from otter.core.chunks import yield_chunks as otter_core_yield_chunks
from otter.core.events import (
    EventType,
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


@EventModelFactory.register(EventModel.OMP)
class OMPEventModel(BaseEventModel):

    def yield_chunks(self, events: Iterable[EventType], use_core: bool=True, use_event_api=True, update_chunks_via_event: bool=True) -> Iterable[Chunk]:

        # Use otter.core.chunks.yield_chunk by default until logic lifted out of that module and into event_model
        if use_core:
            yield from otter_core_yield_chunks(events, self.task_registry)
            return

        log = self.log
        task_registry = self.task_registry
        log.debug(f"receiving events from {events}")

        for k, event in enumerate(events):
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
                    yield from filter(None, event.update_chunks(self.chunk_dict, self.chunk_stack))
                else:
                    if self.event_updates_chunks(event):
                        if self.event_yields_chunk(event):
                            # update and yield
                            # require that update_chunks yields non-None value in this case
                            yield from event.update_chunks(self.chunk_dict, self.chunk_stack)
                        else:
                            # event must update chunks without yielding
                            assert (
                                self.event_updates_chunks_but_cant_yield_chunk(event)
                            or (self.event_updates_and_may_yield_chunk(event) and not self.event_yields_chunk(event)
                            ))
                            # these events currently implement update_chunks as a generator function but this isn't
                            # necessary - can just turn into plan function
                            # TODO: reimplement this case inside this module to remove unnecessary generator
                            yield from filter(None, event.update_chunks(self.chunk_dict, self.chunk_stack))
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
            if event.is_task_register_event:
                task_registry.register_task(event)

            if event.is_update_task_start_ts_event:
                task = task_registry[event.get_task_entered()]
                log.debug(f"notifying task start time: {task.id} @ {event.time}")
                if task.start_ts is None:
                    task.start_ts = event.time

            if event.is_update_duration_event:
                prior_task_id, next_task_id = event.get_tasks_switched()
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

            if event.is_task_complete_event:
                completed_task_id = event.get_task_completed()
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

    def append_to_encountering_task_chunk(self, event: EventType):
        self.chunk_dict[event.encountering_task_id].append_event(event)

    @classmethod
    def is_chunk_switch_event(cls, event: EventType, use_event_api: bool=True) -> bool:
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

        if isinstance(event, ChunkSwitchEventMixin):
            raise TypeError(type(event))

    @staticmethod
    def event_updates_and_yields_chunk(event: EventType) -> bool:
        # these events inherit the ChunkSwitchEvent mixin
        return isinstance(event, (ParallelEnd, SingleEnd, InitialTaskLeave))

    @staticmethod
    def event_updates_and_may_yield_chunk(event: EventType) -> bool:
        # these events inherit the ChunkSwitchEvent mixin
        return isinstance(event, TaskSwitch)

    @staticmethod
    def event_updates_chunks_but_cant_yield_chunk(event: EventType) -> bool:
        # these events inherit the ChunkSwitchEvent mixin
        # all these classes yield None at the end of update_chunks, so they don't need to be generators
        return isinstance(event, (ParallelBegin, SingleBegin, InitialTaskEnter, ImplicitTaskEnter, ImplicitTaskLeave))

    @staticmethod
    def event_doesnt_update_and_doesnt_yield_chunk(event: EventType) -> bool:
        # these events inherit the ChunkSwitchEvent mixin
        return isinstance(event, (ThreadBegin, ThreadEnd))

    @classmethod
    def event_updates_chunks(cls, event: EventType) -> bool:
        # These events require some specific logic to update chunks
        # Some of these *also* yield a completed chunk
        # EVERYTHING WHICH CAN POSSIBLY YIELD A COMPLETED CHUNK ALSO UPDATES THE CHUNKS
        return (
            cls.event_updates_and_yields_chunk(event)
         or cls.event_updates_and_may_yield_chunk(event)
         or cls.event_updates_chunks_but_cant_yield_chunk(event)
        )

    @classmethod
    def event_applies_default_chunk_update(cls, event: EventType) -> bool:
        return not (cls.event_updates_chunks(event) or cls.event_doesnt_update_and_doesnt_yield_chunk(event))

    @classmethod
    def event_yields_chunk(cls, event: EventType) -> bool:
        if cls.event_updates_and_yields_chunk(event):
            return True
        if cls.event_updates_and_may_yield_chunk(event):
            assert isinstance(event, TaskSwitch)
            return event.prior_task_status == TaskStatus.complete
