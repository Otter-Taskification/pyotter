import pdb
import argparse
import warnings
import igraph as ig
import otf2
from itertools import chain, count, groupby
from collections import Counter
from otf2.events import Enter, Leave
from otter.definitions import EventType, Endpoint, RegionType, TaskStatus, TaskType, EdgeType
from otter.trace import AttributeLookup, RegionLookup, yield_chunks, process_chunk, event_defines_new_chunk
from otter.styling import colormap_region_type, colormap_edge_type, shapemap_region_type
from otter.helpers import set_tuples, reject_task_create, attr_handler, label_clusters, descendants_if, attr_getter, pass_master_event

from otter.chunks import Chunk, ChunkGenerator, event_defines_new_task_fragment, fmt_event


def main():
    parser = argparse.ArgumentParser(
        prog="python3 -m otter",
        description='Convert an Otter OTF2 trace archive to its execution graph representation',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('anchorfile', help='OTF2 anchor file')
    parser.add_argument('-o', '--output', dest='output', help='output file')
    parser.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                        help='print chunks as they are generated')
    parser.add_argument('-i', '--interact', action='store_true', dest='interact',
                        help='drop to an interactive shell upon completion')
    parser.add_argument('-ns', '--no-style', action='store_true', default=False, dest='nostyle',
                        help='do not apply any styling to the graph nodes')
    parser.add_argument('-d', '--debug', action='store_true', default=False, dest='debug',
                        help='step through the code with pdb.set_trace()')
    args = parser.parse_args()

    if args.output is None and not args.interact:
        parser.error("must select at least one of -[o|i]")

    if args.interact:
        print("Otter launched interactively")

    anchorfile = args.anchorfile

    # Convert event stream into graph chunks
    print(f"loading OTF2 anchor file: {anchorfile}")
    print("generating chunks from event stream...")
    with otf2.reader.open(anchorfile) as tr:
        regions = RegionLookup(tr.definitions.regions)
        chunk_gen = ChunkGenerator(tr, verbose=args.verbose)
        chunk_graphs = [c.as_graph(verbose=args.verbose) for c in chunk_gen]
        chunk_types = [c.kind for c in chunk_gen]
        task_tree = chunk_gen.make_task_tree()
        event_attr = attr_getter(chunk_gen.attr)
    
    if not args.nostyle:
        task_tree.vs['style'] = 'filled'
        task_tree.vs['color'] = ['red' if v['task_type'] == RegionType.implicit_task else 'gray' for v in task_tree.vs]
    tt_layout = task_tree.layout_reingold_tilford()

    if args.debug:
        pdb.set_trace()

    # Count chunks by type
    print("graph chunks created:")
    for k, v in Counter(chunk_types).items():
        print(f"  {k:18s} {v:8d}")

    if args.debug:
        pdb.set_trace()

    # Collect all chunks
    print("combining chunks")
    g = ig.disjoint_union(chunk_graphs)
    num_nodes = g.vcount()

    print("{:20s} {:6d}".format("nodes created", num_nodes))

    if 'task_cluster_id' not in g.vs.attribute_names():
        g.vs['task_cluster_id'] = None
    g.vs['sync_cluster_id'] = None

    if args.debug:
        pdb.set_trace()

    # Collapse by parallel sequence ID
    print("contracting by parallel sequence ID")
    g.vs['cluster'] = label_clusters(g.vs, lambda v: v['parallel_sequence_id'] is not None, 'parallel_sequence_id')
    nodes_before = num_nodes
    g.contract_vertices(g.vs['cluster'], combine_attrs=attr_handler(attr=chunk_gen.attr))
    num_nodes = g.vcount()
    print("{:20s} {:6d} -> {:6d} ({:6d})".format("nodes updated", nodes_before, num_nodes, num_nodes-nodes_before))

    if args.debug:
        pdb.set_trace()

    # Collapse by single-begin/end event
    def is_single_executor(v):
        return type(v['event']) in [Enter, Leave] and event_attr(v['event'], 'region_type') == RegionType.single_executor
    print("contracting by single-begin/end event")
    g.vs['cluster'] = label_clusters(g.vs, is_single_executor, 'event')
    nodes_before = num_nodes
    g.contract_vertices(g.vs['cluster'], combine_attrs=attr_handler(attr=chunk_gen.attr))
    num_nodes = g.vcount()
    print("{:20s} {:6d} -> {:6d} ({:6d})".format("nodes updated", nodes_before, num_nodes, num_nodes-nodes_before))

    if args.debug:
        pdb.set_trace()

    # Collapse by master-begin/end event
    def is_master(v):
        return type(v['event']) in [Enter, Leave] and event_attr(v['event'], 'region_type') == RegionType.master
    print("contracting by master-begin/end event")
    g.vs['cluster'] = label_clusters(g.vs, is_master, 'event')
    nodes_before = num_nodes
    g.contract_vertices(g.vs['cluster'], combine_attrs=attr_handler(events=pass_master_event, attr=chunk_gen.attr))
    num_nodes = g.vcount()
    print("{:20s} {:6d} -> {:6d} ({:6d})".format("nodes updated", nodes_before, num_nodes, num_nodes-nodes_before))

    if args.debug:
        pdb.set_trace()

    # Itermediate clean-up: for each master region, remove edges that connect 
    # the same nodes as the master region
    master_enter = filter(lambda v: event_attr(v['event'], 'region_type') == RegionType.master and event_attr(v['event'], 'endpoint') == Endpoint.enter, g.vs)
    master_enter_nodes = {v['event']: v for v in master_enter}
    master_leave = filter(lambda v: event_attr(v['event'], 'region_type') == RegionType.master and event_attr(v['event'], 'endpoint') == Endpoint.leave, g.vs)
    master_node_pairs = ((master_enter_nodes[leave_node['master_enter_event']], leave_node) for leave_node in master_leave)
    def yield_neighbours():
        for enter_node, leave_node in master_node_pairs:
            (p,), (s,) = enter_node.predecessors(), leave_node.successors()
            yield p, s
    neighbour_set = {(p,s) for p, s in yield_neighbours()}
    redundant_edges = list(filter(lambda e: (e.source_vertex, e.target_vertex) in neighbour_set, g.es))
    print(f"deleting redundant edges due to master regions: {len(redundant_edges)}")
    g.delete_edges(redundant_edges)

    if args.debug:
        pdb.set_trace()

    # Collapse by (task-ID, endpoint) to get 1 subgraph per task
    for v in g.vs:
        if v['is_task_enter_node']:
            v['task_cluster_id'] = (event_attr(v['event'], 'unique_id'), Endpoint.enter)
        elif v['is_task_leave_node']:
            v['task_cluster_id'] = (event_attr(v['event'], 'unique_id'), Endpoint.leave)
    print("contracting by task ID & endpoint")
    g.vs['cluster'] = label_clusters(g.vs, lambda v: v['task_cluster_id'] is not None, 'task_cluster_id')
    nodes_before = num_nodes
    g.contract_vertices(g.vs['cluster'],
                        combine_attrs=attr_handler(events=reject_task_create, tuples=set_tuples, attr=chunk_gen.attr))
    num_nodes = g.vcount()
    print("{:20s} {:6d} -> {:6d} ({:6d})".format("nodes updated", nodes_before, num_nodes, num_nodes-nodes_before))

    if args.debug:
        pdb.set_trace()

    # Collapse by task ID where there are no links between to combine task nodes with nothing nested within
    def is_empty_task_region(v):
        if v['task_cluster_id'] is None:
            return False
        if v['is_task_enter_node'] or v['is_task_leave_node']:
            return ((v['is_task_leave_node'] and v.indegree() == 0) or 
                    (v['is_task_enter_node'] and v.outdegree() == 0))
        if type(v['event']) is list and set(map(type, v['event'])) in [{ThreadTaskSwitch}]:
            return ((all(v['is_task_leave_node']) and v.indegree() == 0) or 
                    (all(v['is_task_enter_node']) and v.outdegree() == 0))
    print("contracting by task ID where there are no nested nodes")
    g.vs['cluster'] = label_clusters(g.vs, is_empty_task_region, lambda v: v['task_cluster_id'][0])
    nodes_before = num_nodes
    g.contract_vertices(g.vs['cluster'], combine_attrs=attr_handler(events=reject_task_create, tuples=set_tuples, attr=chunk_gen.attr))
    num_nodes = g.vcount()
    print("{:20s} {:6d} -> {:6d} ({:6d})".format("nodes updated", nodes_before, num_nodes, num_nodes-nodes_before))

    if args.debug:
        pdb.set_trace()

    # Collapse redundant sync-enter/leave node pairs by labelling unique pairs of nodes identified by their shared edge
    dummy_counter = count()
    for e in g.es:
        node_types = set()
        for v in (e.source_vertex, e.target_vertex):
            if type(v['event']) is not list:
                node_types.add(event_attr(v['event'], 'region_type'))
            else:
                for event in v['event']:
                    node_types.add(event_attr(event, 'region_type'))
        if (node_types in [{RegionType.barrier_implicit}, {RegionType.barrier_explicit}, {RegionType.taskwait}, {RegionType.loop}] and
                e.source_vertex.attributes().get('sync_cluster_id', None) is None and
                e.target_vertex.attributes().get('sync_cluster_id', None) is None):
            value = next(dummy_counter)
            e.source_vertex['sync_cluster_id'] = e.target_vertex['sync_cluster_id'] = value
    print("contracting redundant sync-enter/leave node pairs")
    g.vs['cluster'] = label_clusters(g.vs, lambda v: v['sync_cluster_id'] is not None, 'sync_cluster_id')
    nodes_before = num_nodes
    g.contract_vertices(g.vs['cluster'], combine_attrs=attr_handler(tuples=set_tuples, attr=chunk_gen.attr))
    num_nodes = g.vcount()
    print("{:20s} {:6d} -> {:6d} ({:6d})".format("nodes updated", nodes_before, num_nodes, num_nodes-nodes_before))

    if args.debug:
        pdb.set_trace()

    # Unpack the region_type attribute
    for v in g.vs:
        if type(v['event']) is list:
            v['region_type'], = set([event_attr(e, 'region_type') for e in v['event']])
            v['endpoint'] = set([event_attr(e, 'endpoint') for e in v['event']])
        else:
            v['region_type'] = event_attr(v['event'], 'region_type')
            v['endpoint'] = event_attr(v['event'], 'endpoint')
        if type(v['endpoint']) is set and len(v['endpoint']) == 1:
            v['endpoint'], = v['endpoint']

    if args.debug:
        pdb.set_trace()

    # Apply taskwait synchronisation
    print("applying taskwait synchronisation")
    for twnode in g.vs.select(lambda v: v['region_type'] == RegionType.taskwait):
        parents = set(task_tree.vs.find(event_attr(e, 'encountering_task_id')) for e in twnode['event'])
        tw_encounter_ts = {event_attr(e, 'encountering_task_id'): e.time for e in twnode['event'] if type(e) is Enter}
        children = [c.index for c in chain(*[p.neighbors(mode='out') for p in parents])
                    if c['crt_ts'] < tw_encounter_ts[c['parent_index']] < c['end_ts']]
        nodes = [v for v in g.vs if v['region_type'] == RegionType.explicit_task
                                 and event_attr(v['event'], 'unique_id') in children
                                 and v['is_task_leave_node']]
        ecount = g.ecount()
        g.add_edges([(v.index, twnode.index) for v in nodes])
        g.es[ecount:]['type'] = EdgeType.taskwait

    if args.debug:
        pdb.set_trace()

    def event_time_per_task(event):
        """Return the map: encountering task id -> event time for all encountering tasks in the event"""
        if type(event) is list:
            return {event_attr(e, 'encountering_task_id'): e.time for e in event}
        return {event_attr(event, 'encountering_task_id'): event.time}

    # Apply taskgroup synchronisation
    print("applying taskgroup synchronisation")
    for tgnode in g.vs.select(lambda v: v['region_type'] == RegionType.taskgroup and v['endpoint'] == Endpoint.leave):
        tg_enter_ts = event_time_per_task(tgnode['taskgroup_enter_event'])
        tg_leave_ts = event_time_per_task(tgnode['event'])
        parents = [task_tree.vs.find(k) for k in tg_enter_ts]
        children = [c for c in chain(*[p.neighbors(mode='out') for p in parents])
                    if tg_enter_ts[c['parent_index']] < c['crt_ts'] < tg_leave_ts[c['parent_index']]]
        descendants = list(chain(*[descendants_if(c, cond=lambda x: x['task_type'] != RegionType.implicit_task) for c in children]))
        nodes = [v for v in g.vs if v['region_type'] == RegionType.explicit_task
                                 and event_attr(v['event'], 'unique_id') in descendants
                                 and v['is_task_leave_node']]
        ecount = g.ecount()
        g.add_edges([(v.index, tgnode.index) for v in nodes])
        g.es[ecount:]['type'] = EdgeType.taskgroup
        
    if args.debug:
        pdb.set_trace()

    # Apply styling if desired
    if not args.nostyle:
        print("applying node and edge styline")
        g.vs['color'] = [colormap_region_type[v['region_type']] for v in g.vs]
        g.vs['style'] = 'filled'
        g.vs['shape'] = [shapemap_region_type[v['region_type']] for v in g.vs]
        g.es['color'] = [colormap_edge_type[e.attributes().get('type', None)] for e in g.es]
    g.vs['label'] = ["{}".format(event_attr(v['event'], 'unique_id'))
                     if any(s in v['region_type'] for s in ['explicit', 'initial', 'parallel']) else " " for v in g.vs]

    g.simplify(combine_edges='first')

    # Clean up redundant attributes
    for item in ['task_cluster_id', 'parallel_sequence_id', 'cluster', 'sync_cluster_id']:
        if item in g.vs.attribute_names():
            print(f"deleting vertex attribute '{item}'")
            del g.vs[item]

    if args.output:
        print(f"writing graph to '{args.output}'")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                g.write(args.output)
            except OSError as oserr:
                print(f"igraph error: {oserr}")
                print(f"failed to write to file '{args.output}'")

    if args.interact:
        import atexit
        import code
        import os
        import readline
        readline.parse_and_bind("tab: complete")

        hfile = os.path.join(os.path.expanduser("~"), ".otter_history")

        try:
            readline.read_history_file(hfile)
            numlines = readline.get_current_history_length()
        except FileNotFoundError:
            open(hfile, 'wb').close()
            numlines = 0

        def append_history(n, f):
            newlines = readline.get_current_history_length()
            readline.set_history_length(1000)
            readline.append_history_file(newlines - n, f)

        atexit.register(append_history, numlines, hfile)

        k = ""
        for k, v in locals().items():
            if g is v:
                break

        banner = \
f"""
Graph '{k}' has {g.vcount()} nodes and {g.ecount()} edges

Entering interactive mode, use:
    ig.plot({k}, [target="..."], ...)   to view or plot to file
    {k}.write_*()                       to save a representation of the graph e.g. {k}.write_dot("graph.dot")     
"""
        Console = code.InteractiveConsole(locals=locals())
        Console.interact(banner=banner, exitmsg=f"history saved to {hfile}")


if __name__ == "__main__":
    main()
