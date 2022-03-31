import otf2
import igraph as ig
import otter
from logging import DEBUG, INFO
from itertools import count
from otter.utils import VertexLabeller, AttributeHandlerTable, VertexAttributeCombiner
from otter.definitions import RegionType

args = otter.get_args()
otter.logging.initialise(args)
log = otter.get_logger()

log.info(f"{args.anchorfile=}")
with otf2.reader.open(args.anchorfile) as r:
    events = otter.EventFactory(r)
    tasks = otter.TaskRegistry()
    log.info(f"generating chunks")
    chunk_list = list(otter.ChunkFactory(events, tasks).read())

# Collect all chunks
log.info("combining chunks")
g = ig.disjoint_union([c.as_graph() for c in chunk_list])
vcount = g.vcount()
log.info(f"combined graph has {vcount} vertices")

# Define some vertex attributes
for name in ['_task_cluster_id', '_is_task_enter_node', '_is_task_leave_node', '_region_type', '_master_enter_event']:
    if name not in g.vs.attribute_names():
        g.vs[name] = None
g.vs['_synchronised_by_taskwait'] = False

# Create vertex labellers
parallel_vertex_labeller = VertexLabeller(otter.utils.key_is_not_none('_parallel_sequence_id'), group_key='_parallel_sequence_id')
single_vertex_labeller = VertexLabeller(otter.utils.is_region_type(RegionType.single_executor), group_key='event')
master_vertex_labeller = VertexLabeller(otter.utils.is_region_type(RegionType.master), group_key='event')
task_vertex_labeller = VertexLabeller(otter.utils.key_is_not_none('_task_cluster_id'), group_key='_task_cluster_id')
empty_task_vertex_labeller = VertexLabeller(otter.utils.is_empty_task_region, group_key=lambda v: v['_task_cluster_id'][0])

# Make a table for mapping vertex attributes to handlers - used by ig.Graph.contract_vertices
handlers = AttributeHandlerTable(g.vs.attribute_names(), handler=otter.utils.handlers.pass_args, level=DEBUG)
handlers['_master_enter_event'] = VertexAttributeCombiner(otter.utils.handlers.pass_unique_master_event, msg="combining attribute: _master_enter_event")
handlers['_task_cluster_id'] = VertexAttributeCombiner(otter.utils.handlers.pass_the_unique_value, cls=tuple, msg="combining attribute: _task_cluster_id")
handlers['_is_task_enter_node'] = VertexAttributeCombiner(otter.utils.handlers.pass_bool_value, cls=bool, msg="combining attribute: _is_task_enter_node")
handlers['_is_task_leave_node'] = VertexAttributeCombiner(otter.utils.handlers.pass_bool_value, cls=bool, msg="combining attribute: _is_task_leave_node")

log.info(f"combining vertices...")

log.info(f"combining vertices by parallel sequence ID")
handlers['event'] = VertexAttributeCombiner(otter.utils.handlers.pass_unique_single_executor)
g.contract_vertices(parallel_vertex_labeller.label(g.vs), combine_attrs=handlers)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

log.info(f"combining vertices by single-begin/end event")
g.contract_vertices(single_vertex_labeller.label(g.vs), combine_attrs=handlers)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

log.info(f"combining vertices by master-begin/end event")
handlers['event'] = VertexAttributeCombiner(otter.utils.handlers.pass_unique_master_event)
g.contract_vertices(master_vertex_labeller.label(g.vs), combine_attrs=handlers)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

# Intermediate clean-up: for each master region, remove edges that connect
# the same nodes as the master region
master_enter_vertices = filter(lambda vertex: isinstance(vertex['event'], otter.events.MasterBegin), g.vs)
master_leave_vertices = filter(lambda vertex: isinstance(vertex['event'], otter.events.MasterEnd), g.vs)
master_enter_vertex_map = {enter_vertex['event']: enter_vertex for enter_vertex in master_enter_vertices}
master_vertex_pairs = ((master_enter_vertex_map[leave_vertex['_master_enter_event']], leave_vertex) for leave_vertex in master_leave_vertices)

neighbour_pairs = {(enter.predecessors()[0], leave.successors()[0]) for enter, leave in master_vertex_pairs}
redundant_edges = list(filter(lambda edge: (edge.source_vertex, edge.target_vertex) in neighbour_pairs, g.es))
log.info(f"deleting redundant edges due to master regions: {len(redundant_edges)}")
g.delete_edges(redundant_edges)

# Collapse by (task-ID, endpoint) to get 1 subgraph per task
log.info("combining vertices by task ID & endpoint")
handlers['event'] = VertexAttributeCombiner(otter.utils.handlers.reject_task_create)
g.contract_vertices(task_vertex_labeller.label(g.vs), combine_attrs=handlers)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

# Collapse by task ID where there are no links between to combine task nodes with nothing nested within
log.info("combining vertices by task ID where there are no nested nodes")
handlers['_task_cluster_id'] = VertexAttributeCombiner(otter.utils.handlers.pass_the_set_of_values, cls=tuple, msg="combining attribute: _task_cluster_id")
updated_labels = empty_task_vertex_labeller.label(g.vs)
g.contract_vertices(updated_labels, combine_attrs=handlers)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

# Label (contracted) task nodes for easier identification
# NOTE: don't think this is currently needed - leave here for reference
# for v in g.vs:
#     if v['is_task_enter_node'] and v['is_task_leave_node']:
#         v['is_contracted_task_node'] = True
#         if type(v['event']) is list and len(v['event']) == 0:
#             pdb.set_trace()
#         v['task_id'] = event_attr(v['event'], 'unique_id')
#     elif v['is_task_enter_node'] or v['is_task_leave_node']:
#         v['task_id'] = v['task_cluster_id'][0]

# Define _sync_cluster_id to identify pairs of connected nodes to contract
dummy_counter = count()
g.vs['_sync_cluster_id'] = None
for edge in g.es:
    if otter.utils.edge_connects_same_type(edge, [RegionType.barrier_implicit, RegionType.barrier_explicit, RegionType.taskwait, RegionType.loop]):
        edge.source_vertex['_sync_cluster_id'] = edge.target_vertex['_sync_cluster_id'] = next(dummy_counter)

# Collapse redundant sync-enter/leave node pairs by labelling unique pairs of nodes identified by their shared edge
handlers['event'] = VertexAttributeCombiner(otter.utils.handlers.pass_args)
sync_vertex_labeller = VertexLabeller(otter.utils.key_is_not_none('_sync_cluster_id'), group_key='_sync_cluster_id')
updated_labels = sync_vertex_labeller.label(g.vs)
log.info("combining redundant sync and loop enter/leave node pairs")
g.contract_vertices(updated_labels, combine_attrs=handlers)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

# Unpack the region_type attribute


# Apply taskwait synchronisation


# Apply taskgroup synchronisation


with open("graph.log", "w") as f:
    f.write("### VERTICES:\n")
    for v in g.vs:
        f.write(f"{v}\n")

    f.write("\n")
    f.write("### EDGES:\n")
    for e in g.es:
        f.write(f"{e.tuple}\n")


log.info("Done!")
