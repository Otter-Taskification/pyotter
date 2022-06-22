from itertools import count
from collections import defaultdict, Counter
import otf2
import igraph as ig
import otter
from otter.utils import label_groups_if, combine_attribute_strategy, strategy_lookup
from otter.definitions import RegionType, Endpoint, EdgeType, TaskType

args = otter.get_args()
otter.log.initialise(args)
log = otter.log.get_logger("main")

log.info(f"reading OTF2 anchorfile: {args.anchorfile}")
with otf2.reader.open(args.anchorfile) as r:
    events = otter.EventFactory(r)
    tasks = otter.TaskRegistry()
    log.info(f"generating chunks")
    chunks = otter.ChunkFactory(events, tasks).chunks
    graphs = list(chunk.graph for chunk in chunks)

# Dump chunks and graphs to log file
if args.loglevel == "DEBUG":
    chunk_log = otter.log.get_logger("chunks_debug")
    graph_log = otter.log.get_logger("graphs_debug")
    task_log = otter.log.get_logger("tasks_debug")

    graph_log.debug(">>> BEGIN GRAPHS <<<")
    chunk_log.debug(f">>> BEGIN CHUNKS <<<")

    for chunk in chunks:

        # write chunk
        for line in chunk.to_text():
            chunk_log.debug(f"{line}")

        # write graph
        graph_log.debug(f"Chunk type: {chunk.type}")
        g = chunk.graph
        lines = [" ".join(f"{g}".split("\n"))]
        for line in lines:
            graph_log.debug(f"{line}")
        for v in g.vs:
            graph_log.debug(f"{v}")
        graph_log.debug("")

    chunk_log.debug(f">>> END CHUNKS <<<")
    graph_log.debug(">>> END GRAPHS <<<")

    task_log.debug(">>> BEGIN TASKS <<<")
    attributes = ",".join(tasks.attributes)
    task_log.debug(f"{attributes=}")
    for record in tasks.data:
        task_log.debug(f"{record}")
    task_log.debug(">>> END TASKS <<<")

# Collect all chunks
log.info("combining chunks")
g = ig.disjoint_union([c.graph for c in chunks])
vcount = g.vcount()
log.info(f"graph disjoint union has {vcount} vertices")

vertex_attribute_names = ['_task_cluster_id',
    '_is_task_enter_node',
    '_is_task_leave_node',
    '_is_dummy_task_vertex',
    '_region_type',
    '_master_enter_event',
    '_taskgroup_enter_event',
    '_sync_cluster_id'
]

# Ensure some vertex attributes are defined
for name in vertex_attribute_names:
    if name not in g.vs.attribute_names():
        g.vs[name] = None

# Define some edge attributes
for name in [otter.Attr.edge_type]:
    if name not in g.es.attribute_names():
        g.es[name] = None

# Make a table for mapping vertex attributes to handlers - used by ig.Graph.contract_vertices
strategies = strategy_lookup(g.vs.attribute_names(), level=otter.log.DEBUG)

# Supply the logic to use when combining each of these vertex attributes
attribute_handlers = [
    ("_master_enter_event", otter.utils.handlers.return_unique_master_event, (type(None), otter.core.events._Event)),
    ("_task_cluster_id",    otter.utils.handlers.pass_the_unique_value,      (type(None), tuple)),
    ("_is_task_enter_node", otter.utils.handlers.pass_bool_value,            (type(None), bool)),
    ("_is_task_leave_node", otter.utils.handlers.pass_bool_value,            (type(None), bool))
]
for attribute, handler, accept in attribute_handlers:
    strategies[attribute] = combine_attribute_strategy(handler, accept=accept, msg=f"combining attribute: {attribute}")


"""
Apply taskgroup synchronisation
===============================

Assumptions:
    - child tasks created in a chunk which encountered a taskgroup construct are already synchronised by that taskgroup

Algorithm:
    For each taskgroup-end vertex V:
        For each task t currently synchronised by V:
            For each task d which is a descendant task of t, stopping at descendants which are implicit tasks:
                d is synchronised by V
"""

import pdb

log.debug(f"Filtering for taskgroup-end vertices")

# For each taskgroup-end vertex V:
for taskgroup_end_vertex in filter(otter.utils.is_task_group_end_vertex, g.vs):
    log.debug(f"taskgroup-end vertex: {taskgroup_end_vertex}")

    # For each task t currently synchronised by V:
    for in_edge in taskgroup_end_vertex.in_edges():
        if in_edge[otter.Attr.edge_type] != EdgeType.taskgroup:
            continue
        synchronised_task_vertex = in_edge.source_vertex

        assert synchronised_task_vertex['_is_dummy_task_vertex'] == True

        assert otter.events.is_event_list(synchronised_task_vertex['event'])

        assert len(synchronised_task_vertex['event']) == 1
        assert otter.events.is_event(synchronised_task_vertex['event'][0])
        
        event = synchronised_task_vertex['event'][0]

        assert event.is_task_create_event

        task_synchronised = event.get_task_created()

        log.debug(f" synchronise child task {task_synchronised}")

        # For each task d which is a descendant task of t, stopping at descendants which are implicit tasks:
        stop_at_implicit_task = lambda t : t.task_type != TaskType.implicit
        for descendant_task in tasks.descendants_while(task_synchronised, stop_at_implicit_task):
            log.debug(f"   + {descendant_task}")

# pdb.set_trace()


log.info(f"combining vertices...")

"""
Contract vertices according to _parallel_sequence_id to combine the chunks generated by the threads of a parallel block.
When combining the 'event' vertex attribute, keep single-executor events over single-other events. All other events
should be combined in a list. 
"""
log.info(f"combining vertices by parallel sequence ID")

# Label vertices with the same _parallel_sequence_id
labeller = label_groups_if(otter.utils.key_is_not_none('_parallel_sequence_id'), group_by='_parallel_sequence_id')

# When combining the event vertex attribute, prioritise single-executor over single-other
strategies['event'] = combine_attribute_strategy(otter.utils.handlers.return_unique_single_executor_event)

g.contract_vertices(labeller.apply_to(g.vs), combine_attrs=strategies)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

"""
Contract those vertices which refer to the same single-executor event. This connects single-executor chunks to the
chunks containing them, as both chunks contain references to the single-exec-begin/end events.
"""
log.info(f"combining vertices by single-begin/end event")

# Label single-executor vertices which refer to the same event.
labeller = label_groups_if(otter.utils.is_single_executor, group_by=lambda vertex: vertex['event'][0])

g.contract_vertices(labeller.apply_to(g.vs), combine_attrs=strategies)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

"""
master-leave vertices (which refer to their master-leave event) refer to their corresponding master-enter event.
"""
log.info(f"combining vertices by master-begin/end event")

# Label vertices which refer to the same master-begin/end event
# SUSPECT THIS SHOULD BE "vertex['event'][0]"
# NOT TESTED!
labeller = label_groups_if(otter.utils.is_master, group_by=lambda vertex: vertex['event'][0])

# When combining events, there should be exactly 1 unique master-begin/end event
strategies['event'] = combine_attribute_strategy(otter.utils.handlers.return_unique_master_event)

g.contract_vertices(labeller.apply_to(g.vs), combine_attrs=strategies)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")


"""
Intermediate clean-up: for each master region, remove edges that connect
the same nodes as the master region

*** WARNING ********************************************************************
********************************************************************************

*** This step assumes vertex['event'] is a bare event instead of an event list ***
"""

master_enter_vertices = filter(lambda vertex: isinstance(vertex['event'], otter.core.events.MasterBegin), g.vs)
master_leave_vertices = filter(lambda vertex: isinstance(vertex['event'], otter.core.events.MasterEnd), g.vs)
master_enter_vertex_map = {enter_vertex['event']: enter_vertex for enter_vertex in master_enter_vertices}
master_vertex_pairs = ((master_enter_vertex_map[leave_vertex['_master_enter_event']], leave_vertex) for leave_vertex in master_leave_vertices)
neighbour_pairs = {(enter.predecessors()[0], leave.successors()[0]) for enter, leave in master_vertex_pairs}
redundant_edges = list(filter(lambda edge: (edge.source_vertex, edge.target_vertex) in neighbour_pairs, g.es))
log.info(f"deleting redundant edges due to master regions: {len(redundant_edges)}")
g.delete_edges(redundant_edges)

"""
********************************************************************************
********************************************************************************
"""

"""
Contract by _task_cluster_id, rejecting task-create vertices to replace them with the corresponding task's chunk.
"""
log.info("combining vertices by task ID & endpoint")

# Label vertices which have the same _task_cluster_id
labeller = label_groups_if(otter.utils.key_is_not_none('_task_cluster_id'), group_by='_task_cluster_id')

# When combining events by _task_cluster_id, reject task-create events (in favour of task-switch events)
strategies['event'] = combine_attribute_strategy(otter.utils.handlers.reject_task_create)

g.contract_vertices(labeller.apply_to(g.vs), combine_attrs=strategies)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

"""
Contract vertices with the same task ID where the task chunk contains no internal vertices to get 1 vertex per empty
task region.
"""
log.info("combining vertices by task ID where there are no nested nodes")

# Label vertices which represent empty tasks and have the same task ID
labeller = label_groups_if(otter.utils.is_empty_task_region, group_by=lambda v: v['_task_cluster_id'][0])

# Combine _task_cluster_id tuples in a set (to remove duplicates)
strategies['_task_cluster_id'] = combine_attribute_strategy(otter.utils.handlers.pass_the_set_of_values, accept=tuple, msg="combining attribute: _task_cluster_id")

g.contract_vertices(labeller.apply_to(g.vs), combine_attrs=strategies)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")


"""
Contract pairs of directly connected vertices which represent empty barriers, taskwait & loop regions.  
"""
log.info("combining redundant sync and loop enter/leave node pairs")

# Label vertices with the same _sync_cluster_id
labeller= label_groups_if(otter.utils.key_is_not_none('_sync_cluster_id'), group_by='_sync_cluster_id')

# Silently return the list of combined arguments
strategies['event'] = combine_attribute_strategy(otter.utils.handlers.pass_args)

g.contract_vertices(labeller.apply_to(g.vs), combine_attrs=strategies)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

# Unpack the region_type attribute


# """
#         Apply taskwait synchronisation
#         ==============================

# 1. Give each explicit task a reference to the last vertex which represents its chunk. This is the vertex which will be
# connected to the corresponding taskwait vertex, if applicable. The correct vertex will contain exactly 1 TaskSwitch(complete)
# event for the corresponding task.
# """
# log.debug(f"filtering for task-complete vertices")
# task_complete_vertices = dict()
# for vertex in filter(otter.utils.is_terminal_task_vertex, g.vs):
#     event = vertex['event']
#     if isinstance(event, list):
#         event = otter.utils.handlers.return_unique_taskswitch_complete_event(event)
#     assert otter.core.is_event(event)
#     log.debug(f" - task {event.prior_task_id}: {event}")
#     task_complete_vertices[event.prior_task_id] = vertex

# log.info("applying taskwait synchronisation")

# """
# 2. For each task that encountered a taskwait barrier, make a list of the taskwait-begin/end event pairs & the vertex
# which refers to these events
# """
# log.debug(f"gathering taskwait vertices by encountering task ID")
# taskwait_vertices = defaultdict(list)
# for vertex in filter(otter.utils.is_taskwait, g.vs):
#     log.debug(f"got taskwait vertex {vertex} with events:")
#     for event in vertex['event']: # now guaranteed to have exactly 2 event instances per vertex (tw-begin+end)
#         log.debug(f" - {event}")
#         taskwait_vertices[event.encountering_task_id].append(dict(event=event, vertex=vertex))

# """
# 3. For each task that encountered a taskwait barrier, connect child tasks to the correct taskwait barrier (if any).
# """
# for task_id, event_vertex_dicts in taskwait_vertices.items():

#     log.debug(f"applying taskwait synchronisation for children of task {task_id}: {list(tasks[task_id].children)}")

#     """
#     Keep the event-vertex pairs for which the event is the taskwait-enter event
#     """
#     event_vertex_dicts = sorted(filter(lambda d: d['event'].endpoint == Endpoint.enter, event_vertex_dicts), key=lambda d: d['event'].time)
#     log.debug(f"task {task_id} encountered {(len(event_vertex_dicts))} taskwait barriers:")
#     for record in event_vertex_dicts:
#         log.debug(f" - task {task_id} encountered {record['event']} {record['vertex']}")

#     """Iterate over the taskwait-enter events for this taskID, in chronological order"""
#     event_vertex_pairs_iter = iter(event_vertex_dicts)

#     """Iterate over the children of this task in the order they were created"""
#     children_iter = iter(sorted(tasks[task_id].children, key=lambda id: tasks[id].crt_ts))

#     """Get the first pair of taskwait-enter events"""
#     previous_event, previous_vertex = None, None
#     next_record = next(event_vertex_pairs_iter, None)
#     next_event = next_record['event'] if next_record else None
#     next_vertex = next_record['vertex'] if next_record else None


#     while True:

#         """Get the next child task if any are left to be synchronised"""
#         try:
#             child = next(children_iter)
#         except StopIteration:
#             """ran out of child tasks"""
#             break

#         """Assert: expect that we haven't already missed the taskwait barrier for this child task"""
#         if previous_event and tasks[child].crt_ts < previous_event.time:
#             print(tasks[child])
#             print(previous_event)
#             raise ValueError("child created before previous")

#         """This child is synchronised by either "next_event" or a subsequent taskwait barrier"""
#         while next_event and next_event.time <= tasks[child].crt_ts:
#             previous_event, previous_vertex = next_event, next_vertex
#             next_record = next(event_vertex_pairs_iter, None)
#             next_event = next_record['event'] if next_record else None
#             next_vertex = next_record['vertex'] if next_record else None

#         if next_event is None:
#             """Ran out of taskwait barriers in the parent task which could synchronise the child task"""
#             break

#         task_complete_vertex = task_complete_vertices[child]
#         assert next_event.time > tasks[child].crt_ts and (previous_event is None or previous_event.time < tasks[child].crt_ts)
#         edge = g.add_edge(task_complete_vertex, next_vertex)
#         edge['edge_type'] = EdgeType.taskwait
#         log.debug(f"synchronised task {child}:")
#         log.debug(f"  from: {task_complete_vertex}")
#         log.debug(f"    to: {next_vertex}")

#         if previous_event is None and next_event is None:
#             """ran out of taskwait barriers - no further taskwait synchronisation to apply to children of this task"""
#             break

# del taskwait_vertices

# """
#         Apply taskgroup synchronisation
#         ===============================

# For each task that encountered a taskgroup region, gather a list of the taskgroup begin/end events and the vertex which
# represents the taskgroup-end event.
# """

# log.info("applying taskgroup synchronisation")

# """
# Contractions applied at this stage:
#     - (between chunks) same _parallel_sequence_id
#     - (between chunks) same single-executor events
#     - (between chunks) same master events
#     - (between chunks) same _task_cluster_id to connect task chunks
#     - (along edges) empty tasks chunks
#     - (along edges) _sync_cluster_id (collapse taskwait enter/leave vertices)
# """

# log.debug(f"gathering taskgroup regions by encountering task ID")
# taskgroup_vertices = defaultdict(list)
# for vertex in filter(otter.utils.is_task_group_end_vertex, g.vs):
#     end_event = vertex['event']
#     assert otter.core.is_event_list(end_event)
#     log.debug(f"taskgroup-end event ({len(end_event)}): {end_event}")
#     if otter.core.is_event_list(end_event) and len(end_event) > 1:
#         end_event = otter.utils.handlers.return_unique_taskgroup_complete_event(end_event)
#     assert otter.core.is_event_list(end_event)
#     assert len(end_event) == 1
#     begin_event = vertex['_taskgroup_enter_event']
#     assert begin_event is not None
#     assert otter.core.is_event(begin_event)
#     taskgroup_vertices[end_event[0].encountering_task_id].append((begin_event, end_event[0], vertex))
#     log.debug(f" - task {end_event[0].encountering_task_id}: {(begin_event, end_event[0], vertex)}")


# """
# For each task that encountered any taskgroup regions:
# """
# was_created_during = lambda crt_ts: (lambda items: items[0].time < crt_ts < items[1].time)
# for task_id, taskgroup_vertex_pairs in taskgroup_vertices.items():
#     descendants = tasks.descendants_while(task_id, lambda t : t.task_type != TaskType.implicit)
#     log.debug(f"applying taskgroup synchronisation for descendants of task {task_id} {descendants}")
#     log.debug(f"task {task_id} encountered {(len(taskgroup_vertex_pairs))} taskgroup regions")

    # """
    # For each descendant task (stopping at descendants which are implicit tasks...
    # """
    # for desc_id in descendants:
    #     desc_task = tasks[desc_id]
    #     match = list(filter(was_created_during(desc_task.crt_ts), taskgroup_vertex_pairs))
    #     if len(match) == 0:
    #         log.debug(f" - task {desc_id} {desc_task.crt_ts=} not synchronised by taskgroup region")
    #     else:
    #         assert len(match) == 1
    #         assert isinstance(match, list)
    #         assert isinstance(match[0], tuple)
    #         assert any(map(otter.core.is_event, match[0]))
    #         assert all(map(otter.core.is_event, match[0][0:2]))
    #         assert otter.core.is_event(match[0][0])
    #         assert otter.core.is_event(match[0][1])
    #         *_, tg_end_vertex = match[0]
    #         try:
    #             edge = g.add_edge(task_complete_vertices[desc_id], tg_end_vertex)
    #         except TypeError as e:
    #             print(type(task_complete_vertices[desc_id]))
    #             print(type(tg_end_vertex))
    #             raise e
    #         edge['edge_type'] = EdgeType.taskgroup
    #         log.debug(f" - task {desc_id} synchronised by taskgroup-end event")







g.simplify(combine_edges='first')

if args.output:
    log.info(f"writing graph to {args.output}")
    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        try:
            g.write(args.output)
        except OSError as oserr:
            print(f"igraph error: {oserr}")
            print(f"failed to write to file '{args.output}'")

# Unpack vertex event attributes
for vertex in g.vs:
    event = vertex['event']
    log.debug(f"unpacking vertex {event=}")
    attributes = otter.core.events.unpack(event)
    for key, value in attributes.items():
        log.debug(f"  got {key}={value}")
        if isinstance(value, list):
            s = set(value)
            if len(s) == 1:
                value = s.pop()
            else:
                log.debug(f"  concatenate {len(value)} values")
                value = ";".join(str(item) for item in value)
        if isinstance(value, int):
            value = str(value)
        elif value == "":
            value = None
        log.debug(f"    unpacked {value=}")
        vertex[key] = value

# Dump graph details to file
if args.loglevel == "DEBUG":
    log.info(f"writing graph to graph.log")
    with open("graph.log", "w") as f:
        f.write("### VERTEX ATTRIBUTES:\n")
        for name in g.vs.attribute_names():
            levels = set(otter.utils.flatten(g.vs[name]))
            n_levels = len(levels)
            if n_levels <= 6:
                f.write(f"  {name:>35} {n_levels:>6} levels {list(levels)}\n")
            else:
                f.write(f"  {name:>35} {n_levels:>6} levels (...)\n")

        # Counter = otter.utils.counters.PrettyCounter(value for value in g.vs['region_type'])

        f.write("\nCount of vertex['event'] types:\n")
        f.write(str(Counter))
        f.write("\n\n")

        f.write("### EDGE ATTRIBUTES:\n")
        for name in g.es.attribute_names():
            levels = set(otter.utils.flatten(g.es[name]))
            n_levels = len(levels)
            if n_levels <= 6:
                f.write(f"  {name:>35} {n_levels:>6} levels ({list(levels)})\n")
            else:
                f.write(f"  {name:>35} {n_levels:>6} levels (...)\n")

        f.write("\n")

        f.write("### VERTICES:\n")
        for v in g.vs:
            f.write(f"{v}\n")

        f.write("\n")
        f.write("### EDGES:\n")
        for e in g.es:
            f.write(f"{e.tuple}\n")

# Clean up temporary vertex attributes
for name in g.vs.attribute_names():
    if name.startswith("_"):
        del g.vs[name]

del g.vs['event']

if args.report:
    otter.styling.style_graph(g)
    otter.styling.style_tasks(tasks.task_tree())
    otter.reporting.write_report(args, g, tasks)

if args.interact:
    otter.interact(locals(), g)

log.info("Done!")
