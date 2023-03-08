from collections import defaultdict, deque
from functools import cached_property
from typing import List, Iterable, Deque, Optional
from itertools import islice
import igraph as ig
from loggingdecorators import on_init

import otter
# from .. import log
from ..log import logger_getter, DEBUG
from .. import definitions as defn
from .tasks import TaskRegistry, TaskSynchronisationContext, NullTask
from .events import is_event, Event
from ..utils.decorators import warn_deprecated

get_module_logger = logger_getter("chunks")


class Chunk:

    @on_init(logger=logger_getter("init_logger"), level=DEBUG)
    def __init__(self, tasks, chunk_type: defn.RegionType):
        self.log = get_module_logger()
        self._events: Deque[Event] = deque()
        self._type = chunk_type
        # TODO: this is ONLY used during Chunk.graph() - don't need to store it if that method is factored out!
        self.tasks = tasks

    def __len__(self):
        return len(self._events)

    @property
    def _base_repr(self):
        return f"{self.__class__.__name__}({len(self._events)} events, self.type={self.type})"

    @property
    def _data_repr(self):
        return "\n".join(f" - {e.__repr__()}" for e in self._events)

    def __repr__(self):
        return f"{self._base_repr}\n{self._data_repr}"

    def to_text(self):
        content = [self._base_repr]
        content.extend([f" - {e}" for e in self._events])
        return content

    @property
    def first(self) -> Optional[Event]:
        return None if len(self._events) == 0 else self._events[0]

    @property
    def last(self) -> Optional[Event]:
        return None if len(self._events) == 0 else self._events[-1]

    @property
    def type(self) -> defn.RegionType:
        return self._type

    def append_event(self, event):
        self.log.debug(f"{self.__class__.__name__}.append_event {event._base_repr} to chunk: {self._base_repr}")
        self._events.append(event)

    @staticmethod
    @warn_deprecated
    def events_bridge_region(previous, current, types: List[defn.RegionType]) -> bool:
        # Used to check for certain enter-leave event sequences
        assert is_event(previous) and is_event(current)
        return previous.region_type in types and previous.is_enter_event \
               and current.region_type in types and current.is_leave_event

    @classmethod
    @warn_deprecated
    def events_bridge_single_master_region(cls, previous, current) -> bool:
        return cls.events_bridge_region(previous, current, [defn.RegionType.single_executor, defn.RegionType.single_other, defn.RegionType.master])

    @classmethod
    @warn_deprecated
    def events_bridge_parallel_region(cls, previous, current) -> bool:
        return cls.events_bridge_region(previous, current, [defn.RegionType.parallel])

    # NOTE: this is an EXTREMELY long function - work on factoring it into multiple 
    # NOTE: shorter functions.
    # NOTE: one idea is to have the chunk just be a dumb container for the sequence of events in the
    # NOTE: chunk and have some GraphFactory class know how to transform the events into a graph
    # NOTE: idea - we have a set of state variables and the graph which are updated over the course
    # NOTE: of iterating over the events in the chunk. The logic applied to each event depends on the event
    # NOTE: which suggests that we can encapsulate the state (and graph) and pass this around to handler functions.
    @cached_property
    @warn_deprecated(stacklevel=3)
    def graph(self) -> ig.Graph:

        self.log.debug(f"transforming chunk to graph (type={self.type}) {self.first=}")

        g: ig.Graph = ig.Graph(directed=True)
        prior_vertex = g.add_vertex(event=[self.first])
        prior_event = self.first

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
        master_enter_event = self.first if self.first.region_type == defn.RegionType.master else None

        # Add attributes to the first node depending on chunk region type
        if self.type == defn.RegionType.parallel:
            prior_vertex["_parallel_sequence_id"] = (self.first.unique_id, self.first.endpoint)
        elif self.type == defn.RegionType.explicit_task:
            prior_vertex['_is_task_enter_node'] = True
            prior_vertex['_task_cluster_id'] = (self.first.unique_id, defn.Endpoint.enter)
        elif self.type == defn.RegionType.single_executor:
            vcount = 1

        # Used for labelling sequences of certain events in a parallel chunk
        sequence_count = 1

        for event in islice(self._events, 1, None):

            if event.region_type in [defn.RegionType.implicit_task]:
                continue

            if event.is_task_switch_event and event is not self.last:
                continue

            # The vertex representing this event
            # vertex['event'] is always a list of 1 or more events
            v = g.add_vertex(event=[event])

            encountering_task = self.tasks[event.encountering_task_id]
            if encountering_task is NullTask:
                encountering_task = None

            # Match taskgroup-enter/-leave events
            if event.region_type == defn.RegionType.taskgroup:

                if event.is_enter_event:
                    taskgroup_enter_event = event

                    # Create a context for this taskgroup
                    encountering_task.enter_task_sync_group()

                elif event.is_leave_event:
                    if taskgroup_enter_event is None:
                        raise RuntimeError("taskgroup-enter event was None")
                    v['_taskgroup_enter_event'] = taskgroup_enter_event
                    taskgroup_enter_event = None

                    # Leave the context for this taskgroup
                    group_context = encountering_task.leave_task_sync_group()
                    v['_group_context'] = group_context
                    v['_task_sync_context'] = (defn.EdgeType.taskgroup, group_context)

            # Label corresponding barrier-enter/leave events so they can be contracted
            if event.region_type in [defn.RegionType.barrier_implicit, defn.RegionType.barrier_explicit]:

                if event.is_enter_event:
                    barrier_cluster_id = (event.encountering_task_id, event.region_type, barrier_cluster_label)
                    v['_sync_cluster_id'] = barrier_cluster_id

                elif event.is_leave_event:
                    if barrier_cluster_id is None:
                        raise RuntimeError("barrier-enter event was None")
                    v['_sync_cluster_id'] = barrier_cluster_id
                    barrier_cluster_label += 1
                    barrier_cluster_id = None

            # Label corresponding taskwait-enter/-leave events so they can be contracted later
            if event.region_type == defn.RegionType.taskwait:
                self.log.debug(f"encountered taskwait barrier: endpoint={event.endpoint}, descendants={event.sync_descendant_tasks==defn.TaskSyncType.descendants}")

                if event.is_enter_event:
                    taskwait_cluster_id = (event.encountering_task_id, event.region_type, taskwait_cluster_label)
                    v['_sync_cluster_id'] = taskwait_cluster_id

                    # Create a context for the tasks synchronised at this barrier
                    descendants = event.sync_descendant_tasks==defn.TaskSyncType.descendants
                    barrier_context = TaskSynchronisationContext(tasks=None, descendants=descendants)

                    # In a single-exec region, created tasks are recorded in the
                    # first event's cache rather than in the parent task's cache.
                    if self.type == defn.RegionType.single_executor:
                        self.log.debug(f"registering tasks at taskwait barrier inside a single-executor chunk")

                        barrier_context.synchronise_from(self.first.task_synchronisation_cache)

                        # Forget about events which have been synchronised

                        self.first.clear_task_synchronisation_cache()

                    # Register tasks synchronised at a barrier
                    else:
                        self.log.debug(f"registering tasks at taskwait barrier")
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
                    v['_task_sync_context'] = (defn.EdgeType.taskwait, barrier_context)

                elif event.is_leave_event:
                    if taskwait_cluster_id is None:
                        raise RuntimeError("taskwait-enter event was None")
                    v['_sync_cluster_id'] = taskwait_cluster_id
                    taskwait_cluster_label += 1
                    taskwait_cluster_id = None

            # Store a reference to the single-exec event's task-sync cache so
            # that the parent task can synchronise any remaining tasks not
            # synchronised inside the single-exec region
            if event.region_type == defn.RegionType.single_executor:

                if event.is_enter_event:
                    if encountering_task.has_active_task_group:
                        # Lazily add the single-exec event's cache to the active context
                        group_context = encountering_task.get_current_task_sync_group()

                        group_context.synchronise_lazy(event.task_synchronisation_cache)
                    else:
                        # Record a reference to the cache to later add lazily to the next
                        # task-sync barrier this task encounters.

                        encountering_task.append_to_barrier_iterables_cache(event.task_synchronisation_cache)

                elif event.is_leave_event and self.type == defn.RegionType.single_executor:
                    if g.vcount() == vcount+1:
                        # No new vertices were added since the single-executor-begin event, so label the vertices with _sync_cluster_id to contract later
                        assert(prior_event.region_type == defn.RegionType.single_executor)

                        assert(prior_event.is_enter_event)
                        v['_sync_cluster_id'] = (event.encountering_task_id, event.region_type, sequence_count)
                        prior_vertex['_sync_cluster_id'] = v['_sync_cluster_id']

            # Match master-enter/-leave events
            elif event.region_type == defn.RegionType.master:

                if event.is_enter_event:
                    master_enter_event = event

                elif event.is_leave_event:
                    if master_enter_event is None:
                        raise RuntimeError("master-enter event was None")
                    v['_master_enter_event'] = master_enter_event
                    master_enter_event = None

            # Label nodes in a parallel chunk by their position for easier merging

            if self.type == defn.RegionType.parallel and (event.is_enter_event or event.is_leave_event) and event.region_type != defn.RegionType.master:
                v["_parallel_sequence_id"] = (self.first.unique_id, sequence_count)
                sequence_count += 1

            # Label nested parallel regions for easier merging, except a parallel chunk's closing parallel-end event
            if event.region_type == defn.RegionType.parallel:
                v["_parallel_sequence_id"] = (self.first.unique_id if event is self.last else event.unique_id, event.endpoint)
                # Should be equivalent to this more explicit form:
                # if event is self.last:
                #     v["_parallel_sequence_id"] = (self.first.unique_id, event.endpoint)
                # else:
                #     v["_parallel_sequence_id"] = (event.unique_id, event.endpoint)

            # Add edge except for (single/master begin -> end) and (parallel N begin -> parallel N end)
            # This avoids creating spurious edges between vertices representing nested chunks
            events_bridge_single_master = self.events_bridge_single_master_region(prior_event, event)
            events_bridge_parallel = self.events_bridge_parallel_region(prior_event, event)
            events_have_same_id = event.unique_id == prior_event.unique_id if events_bridge_parallel else False
            if not (events_bridge_single_master or (events_bridge_parallel and events_have_same_id)):
            # Should be equivalent and may be more explicit
            # if not events_bridge_single_master and not (events_bridge_parallel and events_have_same_id):
                self.log.debug(f"add edge from: {prior_event} to: {event}")
                g.add_edge(prior_vertex, v)
            else:
                msg = f"edge skipped:\n  src: {prior_event}\n  dst: {event}"
                for line in msg.split("\n"):
                    self.log.debug(line)

            # For task-create add dummy nodes for easier merging

            if event.is_task_create_event:
                v['_task_cluster_id'] = (event.unique_id, defn.Endpoint.enter)
                dummy_vertex = g.add_vertex(event=[event])
                dummy_vertex['_task_cluster_id'] = (event.unique_id, defn.Endpoint.leave)
                dummy_vertex['_is_dummy_task_vertex'] = True

                created_task = self.tasks[event.unique_id]

                # If there is a task group context currently active, add the created task to it
                # Otherwise add to the relevant cache
                if encountering_task.has_active_task_group:
                    self.log.debug(f"registering new task in active task group: parent={encountering_task.id}, child={created_task.id}")
                    encountering_task.synchronise_task_in_current_group(created_task)
                else:

                    # In a single-executor chunk, record tasks in the single-exec-enter
                    # event's task-sync cache so that any tasks not synchronised
                    # at the end of this chunk are made available to the enclosing
                    # chunk to synchronise after the single region.
                    if self.type == defn.RegionType.single_executor:
                        self.log.debug(f"registering new task in single-executor cache: parent={encountering_task.id}, child={created_task.id}")
                        self.first.task_synchronisation_cache.append(created_task)

                    # For all other chunk types, record the task created in the
                    # parent task's task-sync cache, to be added to the next task
                    # synchronisation barrier.
                    else:
                        self.log.debug(f"registering new task in task barrier cache: parent={encountering_task.id}, child={created_task.id}")
                        encountering_task.append_to_barrier_cache(created_task)

                continue  # to skip updating prior_vertex

            if event is self.last and self.type == defn.RegionType.explicit_task:
                v['_is_task_leave_node'] = True
                v['_task_cluster_id'] = (event.encountering_task_id, defn.Endpoint.leave)

            prior_vertex = v
            prior_event = event

        final_vertex = prior_vertex
        first_vertex = g.vs[0]

        if self.type == defn.RegionType.explicit_task and len(self) <= 2:
            g.delete_edges([0])

        # If no internal vertices, require at least 1 edge (except for empty explicit task chunks)
        # Require at least 1 edge between start & end vertices in EMPTY parallel & single-executor chunk if disconnected
        if g.ecount() == 0:
            self.log.debug(f"graph contains no edges (type={self.type}, events={len(self)})")
            if self.type == defn.RegionType.explicit_task:
                self.log.debug(f"don't add edges for empty explicit task chunks")
                pass
            elif len(self) <= 2 or (self.type == defn.RegionType.parallel and len(self) <= 4):
                # Parallel chunks contain implicit-task-begin/end events which are skipped, but count towards len(self)
                self.log.debug(f"no internal vertices - add edge from: {g.vs[0]['event']} to: {g.vs[1]['event']}")
                g.add_edge(g.vs[0], g.vs[1])

        # For parallel & single-executor chunks which are disconnected and have internal vertices (and thus some edges), connect start & end vertices
        if (len(final_vertex.in_edges()) == 0) and (
            (self.type == defn.RegionType.single_executor and len(self) > 2) or 
            (self.type == defn.RegionType.parallel and len(self) > 4)
        ):
            self.log.debug(f"detected disconnected chunk of type {self.type}")
            edge = g.add_edge(first_vertex, final_vertex)

        return g


# TODO: remove this function, it uses old Chunk init method and creates implicitly during _Event.update_chunks
def yield_chunks(events: Iterable[Event], task_registry: TaskRegistry) -> Iterable[Chunk]:

    log = get_module_logger()

    # Used to track all chunks currently under construction
    chunk_dict = defaultdict(lambda : Chunk(task_registry))

    # Used to record a nested chunk's enclosing chunk
    chunk_stack = defaultdict(deque)

    log.debug(f"receiving events from {events}")

    for k, event in enumerate(events):
        log.debug(f"got event {k} with vertex label {event.get('vertex_label')}: {event}")

        if event.is_chunk_switch_event:
            log.debug(f"updating chunks")
            # event.update_chunks will emit the completed chunk if this event represents
            # the end of a chunk
            # NOTE: the event.update_chunks logic should probably be factored out of the event class
            # NOTE: and into a separate high-level module to reduce coupling. Should events "know"
            # NOTE: about chunks?
            # NOTE: maybe want separate update_chunk() and update_and_yield_chunk() methods?
            yield from filter(None, event.update_chunks(chunk_dict, chunk_stack))
        else:
            # NOTE: This does EXACTLY the same thing as DefaultUpdateChunksMixin.update_chunks
            chunk_dict[event.encountering_task_id].append_event(event)

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

            # try:
            #     prior_task = task_registry[prior_task_id]
            # except NullTaskError:
            #     pass
            # else:
            #     log.debug(f"got prior task: {prior_task}")
            #     prior_task.update_exclusive_duration(event.time)
            #
            # try:
            #     next_task = task_registry[next_task_id]
            # except task_registry.NullTaskError:
            #     pass
            # else:
            #     log.debug(f"got next task: {next_task}")
            #     next_task.resumed_at(event.time)

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
            # try:
            #     completed_task = task_registry[completed_task_id]
            # except task_registry.NullTaskError:
            #     pass
            # else:
            #     completed_task.end_ts = event.time
            completed_task = task_registry[completed_task_id]
            if completed_task is not NullTask:
                completed_task.end_ts = event.time

    log.debug(f"exhausted {events}")
    task_registry.calculate_all_inclusive_duration()
    task_registry.calculate_all_num_descendants()

    for task in task_registry:
        log.debug(f"task start time: {task.id}={task.start_ts}")
