import otf2
import igraph as ig
import otter
from collections import defaultdict
from otter.utils import VertexLabeller, make_event_combiner
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
for name in ['_task_cluster_id', '_is_task_enter_node', '_is_task_leave_node', '_sync_cluster_id', '_region_type']:
    if name not in g.vs.attribute_names():
        g.vs[name] = None
g.vs['_synchronised_by_taskwait'] = False

# Create vertex labellers
parallel_vertex_labeller = VertexLabeller(otter.utils.key_is_not_none('_parallel_sequence_id'), group_key='_parallel_sequence_id')
single_vertex_labeller = VertexLabeller(otter.utils.is_region_type(RegionType.single_executor), group_key='event')
master_vertex_labeller = VertexLabeller(otter.utils.is_region_type(RegionType.master), group_key='event')
task_vertex_labeller = VertexLabeller(otter.utils.key_is_not_none('_task_cluster_id'), group_key='_task_cluster_id')
empty_task_vertex_labeller = VertexLabeller(otter.utils.is_empty_task_region, group_key=lambda v: v['_task_cluster_id'][0])
sync_vertex_labeller = VertexLabeller(otter.utils.key_is_not_none('_sync_cluster_id'), group_key='_sync_cluster_id')

# Create a dict to map vertex attributes to handlers
# default: drop args when combining vertices
log.info(f"{g.vs.attribute_names()=}")
handler = otter.utils.default_attribute_handler(g.vs.attribute_names())

log.info(f"combining vertices...")

log.info(f"combining vertices by parallel sequence ID")
event_combiner = make_event_combiner(otter.utils.handlers.pass_single_executor)
handler['event'] = otter.decorate.log_call_with_msg(event_combiner, "combining events", log)
log.info(f"set attribute handler '{event_combiner.__module__}.{event_combiner.__name__}' for vertex attribute 'event'")
g.contract_vertices(parallel_vertex_labeller.label(g.vs), combine_attrs=handler)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

log.info(f"combining vertices by single-begin/end event")
g.contract_vertices(single_vertex_labeller.label(g.vs), combine_attrs=handler)
vcount_prev, vcount = vcount, g.vcount()
log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

# log.info(f"combining vertices by master-begin/end event")
# g.contract_vertices(master_vertex_labeller.label(g.vs), combine_attrs=attr_handler(attr=chunk_gen.attr))
# vcount_prev, vcount = vcount, g.vcount()
# log.info(f"vertex count updated: {vcount_prev} -> {vcount}")

log.info("Done!")
