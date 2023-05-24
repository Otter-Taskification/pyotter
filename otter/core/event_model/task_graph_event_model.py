from warnings import warn
from .event_model import EventModelFactory, BaseEventModel, EventList, ChunkDict, ChunkStackDict, ChunkUpdateHandlerKey, ChunkUpdateHandlerFn
from itertools import islice
from typing import Iterable, Tuple, Optional, List, Dict, Set
from igraph import Graph, disjoint_union, Vertex
from otter.definitions import EventModel, EventType, TaskStatus, Endpoint, NullTaskID, Attr, RegionType, TaskSyncType, EdgeType, SourceLocation
from otter.core.chunks import Chunk
from otter.core.events import Event, Location, is_event_list
from otter.core.tasks import TaskData, NullTask, TaskSynchronisationContext, TaskRegistry
from otter.utils import SequenceLabeller, LoggingValidatingReduction, ReductionDict, handlers, transpose_list_to_dict
from otter.utils.vertex_attr_handlers import Reduction
from otter.utils.vertex_predicates import key_is_not_none
from otter.log import logger_getter, DEBUG

get_module_logger = logger_getter("task_graph_event_model")


@EventModelFactory.register(EventModel.TASKGRAPH)
class TaskGraphEventModel(BaseEventModel):

    def __init__(self, task_registry: TaskRegistry, *args, gather_return_addresses: Set[int] = None, **kwargs):
        super().__init__(task_registry)
        self._return_addresses = gather_return_addresses if gather_return_addresses is not None else None

    def chunk_to_graph(self, chunk) -> Graph:
        return task_graph_chunk_to_graph(self, chunk)

    def combine_graphs(self, graphs: Iterable[Graph]) -> Graph:
        return combine_graphs(self, self.task_registry, graphs)

    def event_completes_chunk(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch and event.endpoint == Endpoint.leave

    def event_updates_chunk(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch and event.endpoint == Endpoint.enter

    def event_skips_chunk_update(self, event: Event) -> bool:
        return False

    @classmethod
    def is_task_register_event(cls, event: Event) -> bool:
        return event.event_type == EventType.task_switch and event.endpoint == Endpoint.enter

    @classmethod
    def is_task_create_event(cls, event: Event) -> bool:
        return event.event_type == EventType.task_switch and event.endpoint == Endpoint.enter

    def is_update_task_start_ts_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch and event.endpoint == Endpoint.enter

    def is_update_duration_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch

    @classmethod
    def get_task_created(cls, event: Event) -> int:
        assert cls.is_task_create_event(event)
        return event.unique_id

    def get_tasks_switched(self, event: Event) -> Tuple[int, int]:
        return event.parent_task_id, event.unique_id

    def is_task_complete_event(self, event: Event) -> bool:
        return event.event_type == EventType.task_switch and event.endpoint == Endpoint.leave

    def get_task_completed(self, event: Event) -> int:
        return event.unique_id

    @staticmethod
    def get_task_entered(event: Event) -> id:
        return event.unique_id

    def get_task_data(self, event: Event) -> TaskData:
        assert self.is_task_register_event(event)
        data = {
            Attr.unique_id:       event.unique_id,
            Attr.task_type:       event.region_type, # TODO: is this correct?
            Attr.parent_task_id:  event.parent_task_id,
            Attr.task_flavour:    event.task_flavour,
            Attr.time:            event.time,
            Attr.task_init_file:  event.task_init_file,
            Attr.task_init_func:  event.task_init_func,
            Attr.task_init_line:  event.task_init_line,
        }
        if Attr.source_file_name in event and Attr.source_func_name in event and Attr.source_line_number in event:
            data[Attr.source_file_name] = event.source_file_name
            data[Attr.source_func_name] = event.source_func_name
            data[Attr.source_line_number] = event.source_line_number
        return data

    def get_task_start_location(self, event: Event) -> SourceLocation:
        return SourceLocation(event.source_file, event.source_func, event.source_line)

    def get_task_end_location(self, event: Event) -> SourceLocation:
        return SourceLocation(event.source_file, event.source_func, event.source_line)

    @classmethod
    def is_empty_task_region(cls, vertex: Vertex) -> bool:
        # Return True if vertex is a task-enter (-leave) node with no outgoing (incoming) edges
        if vertex['_task_cluster_id'] is None:
            return False
        if vertex['_is_task_enter_node'] or vertex['_is_task_leave_node']:
            return ((vertex['_is_task_leave_node'] is True and vertex.indegree() == 0) or
                    (vertex['_is_task_enter_node'] is True and vertex.outdegree() == 0))
        # TODO: could this be refactored? Don't we already ensure that vertex['event_list'] is always a list?
        if type(vertex['event_list']) is list and set(map(type, vertex['event_list'])) in [{EventType.task_switch}]:
            return ((all(vertex['_is_task_leave_node']) and vertex.indegree() == 0) or
                    (all(vertex['_is_task_enter_node']) and vertex.outdegree() == 0))

    @classmethod
    def unpack(cls, event_list: List[Event]) -> Dict:
        return transpose_list_to_dict([cls.get_augmented_event_attributes(event) for event in event_list], allow_missing=False)

    @staticmethod
    def get_augmented_event_attributes(event: Event) -> Dict:
        attr = event.to_dict()
        unique_id, region_type = event.get(Attr.unique_id), event.get(Attr.region_type)
        attr['vertex_label'] = unique_id
        if Attr.task_flavour in event:
            attr['vertex_color_key'] = event.task_flavour
        else:
            attr['vertex_color_key'] = region_type
        attr['vertex_shape_key'] = region_type
        return attr

    def pre_yield_event_callback(self, event: Event) -> None:
        """Called once for each event before it is sent to super().yield_chunks"""
        if event.event_type == EventType.task_switch and event.unique_id == event.parent_task_id:
            warn(f"Task is own parent {event=}", category=Warning)

    def post_yield_event_callback(self, event: Event) -> None:
        """Called once for each event after it has been sent to super().yield_chunks"""
        if self._return_addresses is not None:
            address = event.caller_return_address
            if address not in self._return_addresses:
                self._return_addresses.add(address)

    def yield_events_with_warning(self, events_iter: Iterable[Tuple[Location, Event]]) -> Iterable[Tuple[Location, Event]]:
        for location, event in events_iter:
            self.pre_yield_event_callback(event)
            yield location, event
            self.post_yield_event_callback(event)

    def yield_chunks(self, events_iter: Iterable[Tuple[Location, Event]]) -> Iterable[Chunk]:
        yield from super().yield_chunks(self.yield_events_with_warning(events_iter))


@TaskGraphEventModel.update_chunks_on(event_type=EventType.task_switch)
def update_chunks_task_switch(event: Event, location: Location, chunk_dict: ChunkDict, chunk_stack: ChunkStackDict) -> Optional[Chunk]:
    log = get_module_logger()
    log.debug(f"{event} {event.event_type=} {event.region_type=} {event.endpoint=}")
    enclosing_key = event.parent_task_id
    key = event.unique_id
    if event.endpoint == Endpoint.enter:
        if enclosing_key != NullTaskID:
            enclosing_chunk = chunk_dict[enclosing_key]
            enclosing_chunk.append_event(event)
        assert key not in chunk_dict
        chunk = Chunk(event.region_type)
        chunk_dict[key] = chunk
        chunk.append_event(event)
        return None
    elif event.endpoint == Endpoint.leave:
        chunk = chunk_dict[key]
        chunk.append_event(event)
        return chunk
    else:
        raise ValueError(f"unexpected endpoint: {event.endpoint}")


def task_graph_chunk_to_graph(event_model: TaskGraphEventModel, chunk: Chunk) -> Graph:

    chunk.log.debug(f"transforming chunk to graph (type={chunk.type}) {chunk.first=}")

    graph: Graph = Graph(directed=True)
    prior_vertex = graph.add_vertex(event_list=[chunk.first])
    prior_event = chunk.first

    if chunk.type == RegionType.explicit_task:
        prior_vertex['_is_task_enter_node'] = True
        prior_vertex['_task_cluster_id'] = (chunk.first.unique_id, Endpoint.enter)

    for event in islice(chunk.events, 1, None):

        # The vertex representing this event
        # vertex['event_list'] is always a list of 1 or more events
        v = graph.add_vertex(event_list=[event])

        encountering_task = event_model.task_registry[event.encountering_task_id]
        if encountering_task is NullTask:
            encountering_task = None

        if event.region_type == RegionType.taskwait:
            chunk.log.debug(f"encountered taskwait barrier: endpoint={event.endpoint}, descendants={event.sync_descendant_tasks == TaskSyncType.descendants}")

            # Create a context for the tasks synchronised at this barrier
            descendants = event.sync_descendant_tasks == TaskSyncType.descendants
            barrier_context = TaskSynchronisationContext(tasks=None, descendants=descendants)

            # Register tasks synchronised at a barrier
            barrier_context.synchronise_from(encountering_task.task_barrier_cache)

            for iterable in encountering_task.task_barrier_iterables_cache:
                barrier_context.synchronise_lazy(iterable)

            # Forget about tasks and task iterables synchronised here
            encountering_task.clear_task_barrier_cache()
            encountering_task.clear_task_barrier_iterables_cache()

            v['_barrier_context'] = barrier_context
            v['_task_sync_context'] = (EdgeType.taskwait, barrier_context)

        chunk.log.debug(f"add edge from: {prior_event} to: {event}")
        graph.add_edge(prior_vertex, v)

        # For task-create add dummy nodes for easier merging
        if event_model.is_task_create_event(event):
            v['_task_cluster_id'] = (event.unique_id, Endpoint.enter)
            # store event in the dummy vertex temporarily only until we notify the corresponding task of this dummy
            # vertex, then set to None so that it doesn't collide with the task-leave event which has the same _task_cluster_id
            dummy_vertex = graph.add_vertex(event_list=[event])
            dummy_vertex['_task_cluster_id'] = (event.unique_id, Endpoint.leave)
            dummy_vertex['_is_dummy_task_vertex'] = True

            created_task = event_model.task_registry[event.unique_id]

            # If there is a task group context currently active, add the created task to it
            # Otherwise add to the relevant cache
            if encountering_task.has_active_task_group:
                chunk.log.debug(f"registering new task in active task group: parent={encountering_task.id}, child={created_task.id}")
                encountering_task.synchronise_task_in_current_group(created_task)
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

    return graph


def reduce_by_task_cluster_id(event_model: TaskGraphEventModel, reductions: ReductionDict, graph: Graph) -> Graph:
    """
    Contract by _task_cluster_id, rejecting task-create vertices to replace them with the corresponding task's chunk.
    """
    event_model.log.info("combining vertices by task ID & endpoint")

    # Label vertices which have the same _task_cluster_id
    labeller = SequenceLabeller(key_is_not_none('_task_cluster_id'), group_label='_task_cluster_id')

    # When combining events by _task_cluster_id, ...
    reductions['event_list'] = LoggingValidatingReduction(handlers.pass_the_unique_value)

    new_labels = labeller.label(graph.vs)

    for vertex, new_label in zip(graph.vs, new_labels):
        event_model.log.debug(f"LABEL={new_label:<3d} {vertex}")

    vcount = graph.vcount()
    graph.contract_vertices(new_labels, combine_attrs=reductions)
    vcount_prev, vcount = vcount, graph.vcount()
    event_model.log.info(f"vertex count updated: {vcount_prev} -> {vcount}")
    return graph


def reduce_by_task_id_for_empty_tasks(event_model: TaskGraphEventModel, reductions: ReductionDict, graph: Graph) -> Graph:
    """
    Contract vertices with the same task ID where the task chunk contains no internal vertices to get 1 vertex per empty
    task region.
    """
    event_model.log.info("combining vertices by task ID where there are no nested nodes")

    # Label vertices which represent empty tasks and have the same task ID
    labeller = SequenceLabeller(event_model.is_empty_task_region, group_label=lambda v: v['_task_cluster_id'][0])

    reductions['event_list'] = LoggingValidatingReduction(handlers.pass_args)

    # Combine _task_cluster_id tuples in a set (to remove duplicates)
    reductions['_task_cluster_id'] = LoggingValidatingReduction(handlers.pass_the_set_of_values,
                                                                accept=tuple,
                                                                msg="combining attribute: _task_cluster_id")

    vcount = graph.vcount()
    graph.contract_vertices(labeller.label(graph.vs), combine_attrs=reductions)
    vcount_prev, vcount = vcount, graph.vcount()
    event_model.log.info(f"vertex count updated: {vcount_prev} -> {vcount}")
    return graph


def combine_graphs(event_model: TaskGraphEventModel, task_registry: TaskRegistry, graphs: Iterable[Graph]) -> Graph:

    log = get_module_logger()
    log.info("combining graphs")
    graph = disjoint_union(graphs)
    vcount = graph.vcount()
    log.info(f"graph disjoint union has {vcount} vertices")

    vertex_attribute_names = [
        '_task_cluster_id',
        '_is_task_enter_node',
        '_is_task_leave_node',
        '_is_dummy_task_vertex',
        '_barrier_context',
        '_task_sync_context',
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
        ("_task_cluster_id", handlers.pass_the_unique_value, (type(None), tuple)),
        ("_is_task_enter_node", handlers.pass_bool_value, (type(None), bool)),
        ("_is_task_leave_node", handlers.pass_bool_value, (type(None), bool))
    ]
    for attribute, handler, accept in attribute_handlers:
        strategies[attribute] = LoggingValidatingReduction(handler, accept=accept, msg=f"combining attribute: {attribute}")

    # Give each task a reference to the dummy task-create vertex that was inserted
    # into the chunk where the task-create event happened
    log.debug(f"notify each task of its dummy task-create vertex")
    for dummy_vertex in (v for v in graph.vs if v['_is_dummy_task_vertex']):
        assert all(isinstance(event, Event) for event in dummy_vertex['event_list'])
        assert len(dummy_vertex['event_list']) == 1
        event = dummy_vertex['event_list'][0]
        assert event_model.is_task_create_event(event)
        task_id = event_model.get_task_created(event)
        task_created = task_registry[task_id]
        setattr(task_created, '_dummy_vertex', dummy_vertex)
        log.debug(f" - notify task {task_id} of vertex {task_created._dummy_vertex}")
        # replace event with None so it doesn't collide with the task-leave event which has the same _task_cluster_id
        dummy_vertex['event_list'][0] = None

    # Get all the task sync contexts from the taskwait vertices and create edges for them
    log.debug(f"getting task synchronisation contexts")
    for task_sync_vertex in (v for v in graph.vs if v['_task_sync_context'] is not None):
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
    graph = reduce_by_task_cluster_id(event_model, strategies, graph)
    graph = reduce_by_task_id_for_empty_tasks(event_model, strategies, graph)
    graph.simplify(combine_edges='first')
    return graph
