from collections import defaultdict, deque
from functools import cached_property
from typing import List
from itertools import islice, count
import otf2
import igraph as ig
import loggingdecorators as logdec
from .. import log
from .. import definitions as defn
from . import tasks
from . import events

get_module_logger = log.logger_getter("chunks")

class ChunkFactory:
    """Aggregates a sequence of events into a sequence of Chunks."""

    @logdec.on_init(logger=log.logger_getter("init_logger"))
    def __init__(self, e, t):
        self.log = get_module_logger()
        self.events = e
        self.tasks = t
        # Track all chunks currently under construction according to key
        self.chunk_dict = defaultdict(lambda : Chunk())
        # Record the enclosing chunk when an event indicates a nested chunk
        self.chunk_stack = defaultdict(deque)

    def __repr__(self):
        return f"{self.__class__.__name__}({len(self.chunk_dict)} chunks)"

    def __iter__(self):
        self.log.debug(f"{self.__class__.__name__}.__iter__ receiving events from {self.events}")
        for k, event in enumerate(self.events):
            self.log.debug(f"got event {k} with vertex label {event.get('vertex_label')}: {event}")

            if event.is_chunk_switch_event:
                self.log.debug(f"updating chunks")
                yield from event.update_chunks(self.chunk_dict, self.chunk_stack)
            else:
                self.chunk_dict[event.encountering_task_id].append_event(event)

            if event.is_task_register_event:
                self.tasks.register_task(event)

            if event.is_update_task_start_ts_event:
                task = self.tasks[event.get_task_entered()]
                self.log.debug(f"notifying task start time: {task.id} @ {event.time}")
                if task.start_ts is None:
                    task.start_ts = event.time

            if event.is_update_duration_event:
                prior_task_id, next_task_id = event.get_tasks_switched()
                self.log.debug(f"update duration: prior_task={prior_task_id} next_task={next_task_id} {event.time} {event.endpoint:>8} {event}")

                try:
                    prior_task = self.tasks[prior_task_id]
                except tasks.NullTaskError:
                    pass
                else:
                    self.log.debug(f"got prior task: {prior_task}")
                    prior_task.update_exclusive_duration(event.time)

                try:
                    next_task = self.tasks[next_task_id]
                except tasks.NullTaskError:
                    pass
                else:
                    self.log.debug(f"got next task: {next_task}")
                    next_task.resumed_at(event.time)

            if event.is_task_complete_event:
                completed_task_id = event.get_task_completed()
                self.log.debug(f"event <{event}> notifying task {completed_task_id} of end_ts")
                try:
                    completed_task = self.tasks[completed_task_id]
                except tasks.NullTaskError:
                    pass
                else:
                    completed_task.end_ts = event.time

        self.log.debug(f"exhausted {self.events}")
        self.tasks.calculate_all_inclusive_duration()
        self.tasks.calculate_all_num_descendants()

        for task in self.tasks:
            self.log.debug(f"task start time: {task.id}={task.start_ts}")

    def read(self):
        yield from filter(None, self)

    @cached_property
    def chunks(self):
        return list(self.read())


class Chunk:

    @logdec.on_init(logger=log.logger_getter("init_logger"), level=log.DEBUG)
    def __init__(self):
        self.log = get_module_logger()
        self._events = deque()
        self._type = None

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
    def first(self):
        return None if len(self._events) == 0 else self._events[0]

    @property
    def last(self):
        return None if len(self._events) == 0 else self._events[-1]

    @property
    def type(self):
        if len(self) == 0:
            self.log.debug(f"chunk contains no events!")
            return None
        if self.first.is_task_switch_event:
            return self.first.next_task_region_type
        else:
            return self.first.region_type

    def append_event(self, event):
        self.log.debug(f"{self.__class__.__name__}.append_event {event._base_repr} to chunk: {self._base_repr}")
        self._events.append(event)

    @staticmethod
    def events_bridge_region(previous, current, types: List[defn.RegionType]) -> bool:
        # Used to check for certain enter-leave event sequences
        assert events.is_event(previous) and events.is_event(current)
        return previous.region_type in types and previous.is_enter_event \
               and current.region_type in types and current.is_leave_event

    @classmethod
    def events_bridge_single_master_region(cls, previous, current) -> bool:
        return cls.events_bridge_region(previous, current, [defn.RegionType.single_executor, defn.RegionType.single_other, defn.RegionType.master])

    @classmethod
    def events_bridge_parallel_region(cls, previous, current) -> bool:
        return cls.events_bridge_region(previous, current, [defn.RegionType.parallel])

    @cached_property
    def graph(self):

        self.log.debug(f"transforming chunk to graph (type={self.type}) {self.first=}")

        g = ig.Graph(directed=True)
        prior_vertex = g.add_vertex(event=[self.first])
        prior_event = self.first

        # Used to save taskgroup-enter event to match to taskgroup-leave event
        taskgroup_enter_event = None

        # Used to save taskwait-enter event to match to taskwait-leave event
        taskwait_enter_event = None

        # Used to attach dummy label to matching taskwait-enter/leave nodes
        taskwait_cluster_id = None
        taskwait_cluster_label = 0

        # task-create vertices synchronised within a taskgroup region
        taskgroup_task_create_vertices = list()

        # task-create vertices synchronised when they reach a taskwait barrier
        taskwait_task_create_vertices = list()

        # Match master-enter event to corresponding master-leave
        master_enter_event = self.first if self.first.region_type == defn.RegionType.master else None

        # Add attributes to the first node depending on chunk region type
        if self.type == defn.RegionType.parallel:
            prior_vertex["_parallel_sequence_id"] = (self.first.unique_id, self.first.endpoint)
        elif self.type == defn.RegionType.explicit_task:
            prior_vertex['_is_task_enter_node'] = True
            prior_vertex['_task_cluster_id'] = (self.first.unique_id, defn.Endpoint.enter)

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

            # Match taskgroup-enter/-leave events
            if event.region_type == defn.RegionType.taskgroup:
                if event.is_enter_event:
                    taskgroup_enter_event = event
                elif event.is_leave_event:
                    if taskgroup_enter_event is None:
                        raise RuntimeError("taskgroup-enter event was None")
                    v['_taskgroup_enter_event'] = taskgroup_enter_event
                    taskgroup_enter_event = None

                    # Connect all task-create vertices created within this taskgroup to the current vertex
                    # and reset the list of task-create vertices to be synchronised
                    for task_create_vertex in taskgroup_task_create_vertices:
                        edge = g.add_edge(task_create_vertex, v)
                        edge[defn.Attr.edge_type] = event.region_type
                    taskgroup_task_create_vertices = list()

            # Label corresponding taskwait-enter/-leave events so they can be contracted later
            if event.region_type == defn.RegionType.taskwait:
                if event.is_enter_event:
                    taskwait_cluster_id = (event.encountering_task_id, event.region_type, taskwait_cluster_label)
                    v['_sync_cluster_id'] = taskwait_cluster_id

                    # Add edges for the tasks created prior to this taskwait barrier
                    for task_create_vertex in taskwait_task_create_vertices:
                        edge = g.add_edge(task_create_vertex, v)
                        edge[defn.Attr.edge_type] = event.region_type
                    taskwait_task_create_vertices = list()

                elif event.is_leave_event:
                    if taskwait_cluster_id is None:
                        raise RuntimeError("taskwait-enter event was None")
                    v['_sync_cluster_id'] = taskwait_cluster_id
                    taskwait_cluster_label += 1
                    taskwait_cluster_id = None

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

            # Add edge except for (single/master begin -> end) and (parallel N begin -> parallel N end)
            # This avoids creating spurious edges between vertices representing nested chunks
            events_bridge_single_master = self.events_bridge_single_master_region(prior_event, event)
            events_bridge_parallel = self.events_bridge_parallel_region(prior_event, event)
            events_have_same_id = event.unique_id == prior_event.unique_id if events_bridge_parallel else False
            if not (events_bridge_single_master or (events_bridge_parallel and events_have_same_id)):
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

                # Append for the next taskwait barrier encountered
                taskwait_task_create_vertices.append(dummy_vertex)

                # If inside a taskgroup, record this task to be synchronised
                if taskgroup_enter_event is not None:
                    taskgroup_task_create_vertices.append(dummy_vertex)
                
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
