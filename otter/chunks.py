import otf2
import igraph as ig
from typing import Iterable
from collections import deque, defaultdict
from itertools import islice
from otter.definitions import EventType, RegionType, TaskStatus, TaskType, Endpoint, EdgeType
from otter.trace import AttributeLookup, event_defines_new_chunk
from otf2.events import Enter, Leave, ThreadTaskCreate, ThreadTaskSwitch, ThreadTaskComplete, ThreadBegin, ThreadEnd


class Chunk:
    """A sequence of events delineated by events at which the execution flow may diverge. Contains a reference to an
    AttributeLookup for looking up event attributes by name."""

    def __init__(self, events: Iterable, attr: AttributeLookup):
        self.attr = attr
        self.events = deque(events)
        self._graph = None

    # TODO: also print self._graph stats e.g. #nodes, #edges...
    def __repr__(self):
        s = "Chunk with {} events:\n".format(len(self.events))
        s += "kind: {}\n".format(self.kind)
        s += "     {:18s} {:10s} {:20s} {:20s} {:18s} {}\n".format("Time", "Endpoint", "Region Type", "Event Type", "Region ID/Name", "Encountering Task")
        for j, e in enumerate(self.events):
            try:
                s += "{:>3d}  {:<18d} {:10s} {:20s} {:20s} {:18s} {}\n".format(
                    j,
                    e.time,
                    e.attributes[self.attr['endpoint']],
                    e.attributes.get(self.attr['region_type'], ""),
                    e.attributes[self.attr['event_type']],
                    str(e.attributes[self.attr['unique_id']]) if self.attr['unique_id'] in e.attributes else e.region.name,
                    e.attributes[self.attr['encountering_task_id']])
            except KeyError as err:
                print(err)
                raise
        return s

    def __len__(self):
        return len(self.events)

    def append(self, item):
        if type(item) is Chunk:
            raise TypeError()
        self.events.append(item)

    @property
    def first(self):
        return None if len(self.events) == 0 else self.events[0]

    @property
    def last(self):
        return None if len(self.events) == 0 else self.events[-1]

    @property
    def len(self):
        return len(self.events)

    @property
    def kind(self):
        if len(self.events) == 0:
            return None
        if type(self.first) == Enter:
            return self.first.attributes[self.attr['region_type']]
        elif type(self.first) == ThreadTaskSwitch:
            return self.last.attributes[self.attr['region_type']]
        else:
            raise TypeError(f"Unhandled first event type: {type(self.first)}")

    def get_attr(self, event, attr_name, default=None):
        """Lookup the attribute of an event or list of events by attribute name"""
        if type(event) is list:
            result, = set([e.attributes[self.attr[attr_name]] for e in event])
            return result
        elif type(event) in [Enter, Leave, ThreadTaskCreate, ThreadTaskSwitch]:
            return event.attributes[self.attr[attr_name]]
        else:
            raise TypeError(f"unexpected type: {type(event)}")

    def events_bridge_region(self, previous, current, region_types):
        """Used to check for certain enter-leave event sequences"""
        return (self.get_attr(previous, 'region_type') in region_types
                and self.get_attr(previous, 'endpoint') == Endpoint.enter
                and self.get_attr(current, 'region_type') in region_types
                and self.get_attr(current, 'endpoint') == Endpoint.leave)

    def as_graph(self, verbose: bool=False):
        """Convert a chunk to a DAG representation"""

        if self._graph is not None:
            return self._graph

        # TODO: update this function to work with new chunk event logic e.g. ThreadTaskSwitch instead of Enter-Leave for explicit tasks, etc...

        if verbose and len(self.events) > 2:
            print(self)

        g = ig.Graph(directed=True)
        self._graph = g
        prior_node = g.add_vertex(event=self.first)

        # Used to save taskgroup-enter event to match to taskgroup-leave event
        taskgroup_enter_event = None

        # Match master-enter event to corresponding master-leave
        master_enter_event = self.first if self.get_attr(self.first, 'region_type') == RegionType.master else None

        if self.kind == RegionType.parallel:
            parallel_id = self.get_attr(self.first, 'unique_id')
            parallel_endpoint = self.get_attr(self.first, 'endpoint')
            prior_node["parallel_sequence_id"] = (parallel_id, parallel_endpoint)
        elif self.kind == RegionType.explicit_task:
            prior_node['is_task_enter_node'] = True

        k = 1
        for event in islice(self.events, 1, None):

            if self.get_attr(event, 'region_type') in [RegionType.implicit_task]:
                continue

            if type(event) is ThreadTaskSwitch and event is not self.last:
                continue

            # The node representing this event
            node = g.add_vertex(event=event)

            try:
                unique_id_attr = self.get_attr(event, 'unique_id')
            except KeyError:
                unique_id_attr = None

            # Match taskgroup-enter/-leave events
            if self.get_attr(event, 'region_type') in [RegionType.taskgroup]:
                if type(event) is Enter:
                    taskgroup_enter_event = event
                elif type(event) is Leave:
                    if taskgroup_enter_event is None:
                        raise ValueError("taskgroup-enter event was None")
                    node['taskgroup_enter_event'] = taskgroup_enter_event
                    taskgroup_enter_event = None

            # Match master-enter/-leave events
            elif self.get_attr(event, 'region_type') in [RegionType.master]:
                if type(event) is Enter:
                    master_enter_event = event
                elif type(event) is Leave:
                    if master_enter_event is None:
                        raise ValueError("master-enter event was None")
                    node['master_enter_event'] = master_enter_event
                    master_enter_event = None

            # Label nodes in a parallel chunk by their position for easier merging
            # TODO: update to work with new event types e.g. ThreadTaskSwitch
            if (self.kind == RegionType.parallel
                    and type(event) in [Enter, Leave]
                    and self.get_attr(event, 'region_type') != RegionType.master):
                node["parallel_sequence_id"] = (parallel_id, k)
                k += 1

            if self.get_attr(event, 'region_type') == RegionType.parallel:
                # Label nested parallel regions for easier merging...
                event_endpoint = self.get_attr(event, 'endpoint')
                if event is not self.last:
                    node["parallel_sequence_id"] = (unique_id_attr, event_endpoint)
                # ... but distinguish from a parallel chunk's terminating parallel-end event
                else:
                    node["parallel_sequence_id"] = (parallel_id, event_endpoint)

            # Add edge except for (single/master begin -> end) and (parallel N begin -> parallel N end)
            events_bridge_single_master = self.events_bridge_region(prior_node['event'], node['event'], [RegionType.single_executor, RegionType.single_other, RegionType.master])
            events_bridge_parallel = self.events_bridge_region(prior_node['event'], node['event'], [RegionType.parallel])
            events_have_same_id = self.get_attr(node['event'], 'unique_id') == self.get_attr(prior_node['event'], 'unique_id') if events_bridge_parallel else False
            if (events_bridge_single_master or (events_bridge_parallel and events_have_same_id)):
                pass
            else:
                g.add_edge(prior_node, node)

            # For task_create add dummy nodes for easier merging
            if type(event) is ThreadTaskCreate:
                node['task_cluster_id'] = (unique_id_attr, Endpoint.enter)
                dummy_node = g.add_vertex(event=event, task_cluster_id=(unique_id_attr, Endpoint.leave))
                continue

            if event is self.last and self.kind == RegionType.explicit_task:
                node['is_task_leave_node'] = True

            prior_node = node

        if self.kind == RegionType.explicit_task and len(self.events) <= 2:
            g.delete_edges([0])

        # Require at least 1 edge between start and end nodes if there are no internal nodes, except for empty explicit
        # task chunks
        if self.kind != RegionType.explicit_task and len(self.events) <= 2 and g.ecount() == 0:
            g.add_edge(g.vs[0], g.vs[1])

        return self._graph


class ChunkGenerator:
    """Yields a sequence of Chunks by consuming the sequence of events in a trace."""

    def __init__(self, trace, verbose: bool = False):
        self.events = trace.events
        self.attr = AttributeLookup(trace.definitions.attributes)
        self.strings = trace.definitions.strings
        self._chunks = deque()
        self._generated = False
        self._chunk_dict = defaultdict(lambda : Chunk(list(), self.attr))
        self._task_chunk_map = defaultdict(lambda : Chunk(list(), self.attr))
        self.verbose = verbose
        self.nChunks = 0
        self.events_consumed = 0
        self.task_links = deque()
        self.task_type = dict()
        self.task_crt_ts = dict()
        self.task_end_ts = dict()
        self.task_switch_events = defaultdict(deque)

    def __getitem__(self, key):
        return self._task_chunk_map[key]

    def __setitem__(self, key, value):
        if type(value) is not Chunk:
            raise TypeError()
        self._task_chunk_map[key] = value

    def keys(self):
        return self._task_chunk_map.keys()

    def make_task_tree(self):
        start_time = self.task_crt_ts[0]
        last_time = max(self.task_end_ts.values())
        duration = last_time - start_time
        task_ids = list(self.task_type.keys())
        task_tree = ig.Graph(n=len(task_ids), directed=True)
        task_tree.vs['name'] = task_ids
        task_tree.vs['task_type'] = [self.task_type[k] for k in task_ids]
        task_tree.vs['crt_ts'] = [self.task_crt_ts[k] for k in task_ids]
        task_tree.vs['end_ts'] = [self.task_end_ts[k] for k in task_ids]
        for parent, child in self.task_links:
            if child == 0:
                continue
            task_tree.add_edge(parent, child)
            task_tree.vs.find(child)['parent_index'] = parent
        return task_tree

    def yield_chunk(self, chunk_key):
        if self.verbose:
            self.nChunks += 1
            if self.nChunks % 100 == 0:
                if self.nChunks % 1000 == 0:
                    print("yielding chunks:", end=" ", flush=True)
                print(f"{self.nChunks:4d}", end="", flush=True)
            elif self.nChunks % 20 == 0:
                print(".", end="", flush=True)
            if (self.nChunks+1) % 1000 == 0:
                print("", flush=True)
        yield self[chunk_key]

    def __iter__(self):

        if self._generated:
            yield from self._chunks
            return

        # Map thread ID -> ID of current parallel region at top of deque
        current_parallel_region = defaultdict(deque)

        # Record the enclosing chunk when an event indicates a nested chunk
        chunk_stack = defaultdict(deque)

        # Encountering task interpretation:
        # enter/leave:        the task that encountered the region associated with the event
        # task-switch:        the task that encountered the task-switch event i.e. the task being suspended
        # task-create:        the parent task of the created task

        for location, event in self.events:
            self.events_consumed += 1

            # Ignore thread-begin/end entirely
            if type(event) in [ThreadBegin, ThreadEnd]:
                continue

            # Used to lookup the chunk currently being generated by the 
            # encountering task
            try:
                encountering_task = event.attributes[self.attr['encountering_task_id']]
            except KeyError:
                print(event)
                print(event.attributes)
                raise
            region_type = event.attributes.get(self.attr['region_type'], None)
            unique_id = event.attributes.get(self.attr['unique_id'], None)

            # default:
            chunk_key = encountering_task

            # Record task links, creation time and task-switch events for later extraction of timing data
            if (type(event) == ThreadTaskCreate) or (type(event) == Enter and event.attributes[self.attr['region_type']] in [RegionType.initial_task, RegionType.implicit_task]):
                self.task_links.append((encountering_task, unique_id))
                self.task_type[unique_id] = event.attributes[self.attr['region_type']]
                self.task_crt_ts[unique_id] = event.time
            elif (type(event) == ThreadTaskSwitch):
                self.task_switch_events[encountering_task].append(event)
                self.task_switch_events[unique_id].append(event)
                prior_task_status = event.attributes[self.attr['prior_task_status']]
                if prior_task_status == TaskStatus.complete:
                    self.task_end_ts[encountering_task] = event.time
            elif type(event) == Leave and event.attributes[self.attr['region_type']] in [RegionType.initial_task, RegionType.implicit_task]:
                self.task_end_ts[unique_id] = event.time

            if event_defines_new_task_fragment(event, self.attr):

                if isinstance(event, Enter):

                    # For initial-task-begin, chunk key is (thread ID, initial task unique_id)
                    if region_type == RegionType.initial_task:
                        chunk_key = (location.name, unique_id, 't')
                        self[chunk_key].append(event)

                    # For parallel-begin, chunk_key is (thread ID, parallel ID)
                    # parallel-begin is reported by master AND worker threads
                    # master thread treats it as a chunk boundary (i.e. it nests within the encountering chunk)
                    # worker thread treats it as a new chunk (i.e. it is not a nested chunk)
                    # Record parallel ID as the current parallel region for this thread
                    elif region_type == RegionType.parallel:
                        encountering_task_key = (location.name, encountering_task, 't')
                        parallel_region_key = (location.name, unique_id, 'p')
                        current_parallel_region[location.name].append(unique_id)
                        # if master thread: (does the key (thread ID, encountering_task) exist in self.keys())
                        if encountering_task_key in self.keys():
                            # append event to current chunk for key (thread ID, encountering_task)
                            self[encountering_task_key].append(event)
                            # assign a reference to this chunk to key (thread ID, parallel ID)
                            self[parallel_region_key] = self[encountering_task_key]
                            # push reference to this chunk onto chunk_stack at key (thread ID, parallel ID)
                            chunk_stack[parallel_region_key].append(self[parallel_region_key])
                            # append event to NEW chunk for key (thread ID, parallel ID)
                            self[parallel_region_key] = Chunk((event,), self.attr)

                        else:
                            # append event to NEW chunk with key (thread ID, parallel ID)
                            self[parallel_region_key].append(event)

                    # For implicit-task-enter, chunk_key is encountering_task_id but this must be made to refer to the same chunk as (thread ID, parallel ID)
                    # so that later events in this task are recorded against the same chunk
                    elif region_type == RegionType.implicit_task:
                        parallel_id = current_parallel_region[location.name][-1]
                        chunk_key = (location.name, parallel_id, 'p')
                        self[chunk_key].append(event)
                        # Ensure implicit-task-id points to this chunk for later events in this task
                        self[unique_id] = self[chunk_key]

                    elif region_type in [RegionType.single_executor, RegionType.master]:
                        # do the stack-push thing
                        self[chunk_key].append(event)
                        chunk_stack[chunk_key].append(self[chunk_key])
                        self[chunk_key] = Chunk((event,), self.attr)

                    else:
                        # Nothing should get here
                        print(event)
                        raise ValueError("shouldn't be here")

                elif isinstance(event, Leave):

                    if region_type == RegionType.initial_task:
                        chunk_key = (location.name, unique_id, 't')
                        # append event
                        self[chunk_key].append(event)
                        # yield chunk
                        self._chunks.append(self[chunk_key])
                        yield from self.yield_chunk(chunk_key)

                    elif region_type == RegionType.parallel:
                        encountering_task_key = (location.name, encountering_task, 't')
                        parallel_region_key = (location.name, unique_id, 'p')
                        current_parallel_region[location.name].pop()
                        # if master thread: (does the key (thread ID, encountering_task) exist in self.keys())
                        if encountering_task_key in self.keys():
                            # append event to chunk for key (thread ID, parallel ID)
                            self[parallel_region_key].append(event)
                            # yield this chunk
                            self._chunks.append(self[parallel_region_key])
                            yield from self.yield_chunk(parallel_region_key)
                            # pop from chunk_stack at key (thread ID, parallel ID) and overwrite at this key in self[(thread ID, parallel ID)]
                            self[parallel_region_key] = chunk_stack[parallel_region_key].pop()
                            # append event to now-popped chunk for key (thread ID, parallel ID) which is the one containing the enclosing initial task events
                            self[parallel_region_key].append(event)
                            # update self[(thread ID, encountering task ID)] to refer to the same chunk as self[(thread ID, parallel ID)]
                            self[encountering_task_key] = self[parallel_region_key]
                        else:
                            # append event
                            self[parallel_region_key].append(event)
                            # yield chunk
                            self._chunks.append(self[parallel_region_key])
                            yield from self.yield_chunk(parallel_region_key)

                    elif region_type == RegionType.implicit_task:
                        self[unique_id].append(event)
                        # continue - don't yield until parallel-end

                    elif region_type in [RegionType.single_executor, RegionType.master]:
                        # do the stack-pop thing
                        self[chunk_key].append(event)
                        # yield chunk
                        self._chunks.append(self[chunk_key])
                        yield from self.yield_chunk(chunk_key)
                        self[chunk_key] = chunk_stack[chunk_key].pop()
                        self[chunk_key].append(event)

                    else:
                        # Nothing should get here
                        print(event)
                        raise ValueError("shouldn't be here")

                elif isinstance(event, ThreadTaskSwitch):
                    # encountering task id == prior task
                    prior_task_status = event.attributes[self.attr['prior_task_status']]
                    prior_task_id = event.attributes[self.attr['prior_task_id']]
                    next_task_id = event.attributes[self.attr['next_task_id']]
                    self[chunk_key].append(event)
                    if prior_task_status in [TaskStatus.complete]:
                        self._chunks.append(self[chunk_key])
                        yield from self.yield_chunk(chunk_key)
                    self[next_task_id].append(event)

                else:
                    self[encountering_task].append(event)

            else:
                self[encountering_task].append(event)
        print("")
        self._generated = True


def fmt_event(e, attr):
    s = ""
    try:
        s += "  {:<18d} {:10s} {:20s} {:20s} {:18s} {}".format(
            e.time,
            e.attributes[attr['endpoint']],
            e.attributes.get(attr['region_type'], ""),
            e.attributes[attr['event_type']],
            str(e.attributes[attr['unique_id']]) if attr['unique_id'] in e.attributes else e.region.name,
            e.attributes[attr['encountering_task_id']])
    except KeyError as err:
        print(err)
        print(type(e))
    return s


def event_defines_new_task_fragment(e: otf2.events._EventMeta, a: AttributeLookup) -> bool:
    return (
        (type(e) in [ThreadTaskSwitch]) or
        (type(e) in [Enter, Leave] and e.attributes.get(a['region_type'], None) in
            [RegionType.parallel, RegionType.initial_task, RegionType.implicit_task, RegionType.single_executor, RegionType.master])
    )