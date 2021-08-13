import argparse
import warnings
import igraph as ig
import otf2
from itertools import chain, count, groupby
from collections import Counter
from otf2.events import Enter, Leave
from otter.trace import AttributeLookup, RegionLookup, yield_chunks, process_chunk
from otter.styling import colormap_region_type, colormap_edge_type, shapemap_region_type
from otter.helpers import set_tuples, reject_task_create, attr_handler, label_clusters, descendants_if, attr_getter, pass_master_event


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
        attr = AttributeLookup(tr.definitions.attributes)
        regions = RegionLookup(tr.definitions.regions)
        results = (process_chunk(chunk, verbose=args.verbose) for chunk in yield_chunks(tr))
        items = zip(*results)
        chunk_types = next(items)
        chain_sort_next = lambda x: sorted(chain(*next(x)), key=lambda t: t[0])
        task_links, task_crt_ts, task_leave_ts = (chain_sort_next(items) for _ in range(3))
        g_list = next(items)

    # Make function for looking up event attributes
    event_attr = attr_getter(attr)

    task_types, *_, task_ids = zip(*[r.name.split() for r in regions.values() if r.region_role == otf2.RegionRole.TASK])
    task_types, task_ids = (zip(*sorted(zip(task_types, map(int, task_ids)), key=lambda t: t[1])))

    # Gather last leave times per explicit task
    task_end_ts = {k: max(u[1] for u in v) for k, v in groupby(task_leave_ts, key=lambda t: t[0])}

    # Task tree showing parent-child links
    task_tree = ig.Graph(edges=task_links, directed=True)
    task_tree.vs['unique_id'] = task_ids
    task_tree.vs['crt_ts'] = [t[1] for t in task_crt_ts]
    task_tree.vs['end_ts'] = [task_end_ts[node.index] if node.index in task_end_ts else None for node in task_tree.vs]
    task_tree.vs['parent_index'] = list(chain((None,), list(zip(*sorted(task_links, key=lambda t: t[1])))[0]))
    task_tree.vs['task_type'] = task_types
    if not args.nostyle:
        task_tree.vs['style'] = 'filled'
        task_tree.vs['color'] = ['red' if v['task_type'] == 'implicit' else 'gray' for v in task_tree.vs]
    tt_layout = task_tree.layout_reingold_tilford()

    # Count chunks by type
    print("graph chunks created:")
    for k, v in Counter(chunk_types).items():
        print(f"  {k:18s} {v:8d}")

    # Collect all chunks
    print("combining chunks")
    g = ig.disjoint_union(g_list)
    num_nodes = g.vcount()

    print("{:20s} {:6d}".format("nodes created", num_nodes))

    if 'task_cluster_id' not in g.vs.attribute_names():
        g.vs['task_cluster_id'] = None

    # Collapse by parallel sequence ID
    print("contracting by parallel sequence ID")
    g.vs['cluster'] = label_clusters(g.vs, lambda v: v['parallel_sequence_id'] is not None, 'parallel_sequence_id')
    nodes_before = num_nodes
    g.contract_vertices(g.vs['cluster'], combine_attrs=attr_handler(attr=attr))
    num_nodes = g.vcount()
    print("{:20s} {:6d} -> {:6d} ({:6d})".format("nodes updated", nodes_before, num_nodes, num_nodes-nodes_before))

    # Collapse by single-begin/end event
    def is_single_executor(v):
        return type(v['event']) in [Enter, Leave] and event_attr(v['event'], 'region_type') == 'single_executor'
    print("contracting by single-begin/end event")
    g.vs['cluster'] = label_clusters(g.vs, is_single_executor, 'event')
    nodes_before = num_nodes
    g.contract_vertices(g.vs['cluster'], combine_attrs=attr_handler(attr=attr))
    num_nodes = g.vcount()
    print("{:20s} {:6d} -> {:6d} ({:6d})".format("nodes updated", nodes_before, num_nodes, num_nodes-nodes_before))

    # Collapse by master-begin/end event
    def is_master(v):
        return type(v['event']) in [Enter, Leave] and event_attr(v['event'], 'region_type') == 'master'
    print("contracting by master-begin/end event")
    g.vs['cluster'] = label_clusters(g.vs, is_master, 'event')
    nodes_before = num_nodes
    g.contract_vertices(g.vs['cluster'], combine_attrs=attr_handler(events=pass_master_event, attr=attr))
    num_nodes = g.vcount()
    print("{:20s} {:6d} -> {:6d} ({:6d})".format("nodes updated", nodes_before, num_nodes, num_nodes-nodes_before))

    # Itermediate clean-up: for each master region, remove edges that connect 
    # the same nodes as the master region
    master_enter = filter(lambda v: event_attr(v['event'], 'region_type') == 'master' and event_attr(v['event'], 'endpoint') == 'enter', g.vs)
    master_enter_nodes = {v['event']: v for v in master_enter}
    master_leave = filter(lambda v: event_attr(v['event'], 'region_type') == 'master' and event_attr(v['event'], 'endpoint') == 'leave', g.vs)
    master_node_pairs = ((master_enter_nodes[leave_node['master_enter_event']], leave_node) for leave_node in master_leave)
    def yield_neighbours():
        for enter_node, leave_node in master_node_pairs:
            (p,), (s,) = enter_node.predecessors(), leave_node.successors()
            yield p, s
    neighbour_set = {(p,s) for p, s in yield_neighbours()}
    redundant_edges = list(filter(lambda e: (e.source_vertex, e.target_vertex) in neighbour_set, g.es))
    print(f"deleting redundant edges due to master regions: {len(redundant_edges)}")
    g.delete_edges(redundant_edges)

    # Collapse by (task-ID, endpoint) to get 1 subgraph per task
    for v in g.vs:
        if type(v['event']) in [Enter, Leave] and event_attr(v['event'], 'region_type') == 'explicit_task':
            v['task_cluster_id'] = (event_attr(v['event'], 'unique_id'), event_attr(v['event'], 'endpoint'))
    print("contracting by task ID & endpoint")
    g.vs['cluster'] = label_clusters(g.vs, lambda v: v['task_cluster_id'] is not None, 'task_cluster_id')
    nodes_before = num_nodes
    g.contract_vertices(g.vs['cluster'],
                        combine_attrs=attr_handler(events=reject_task_create, tuples=set_tuples, attr=attr))
    num_nodes = g.vcount()
    print("{:20s} {:6d} -> {:6d} ({:6d})".format("nodes updated", nodes_before, num_nodes, num_nodes-nodes_before))

    # Collapse by task ID where there are no links between to combine task nodes with nothing nested within
    def is_empty_task_region(v):
        if v['task_cluster_id'] is None:
            return False
        if type(v['event']) in [Enter, Leave]:
            return (type(v['event']) is Leave and v.indegree() == 0) or \
                   (type(v['event']) is Enter and v.outdegree() == 0)
        if type(v['event']) is list and set(map(type, v['event'])) in [{Enter}, {Leave}]:
            return (set(map(type, v['event'])) == {Leave} and v.indegree() == 0) or \
                   (set(map(type, v['event'])) == {Enter} and v.outdegree() == 0)
    print("contracting by task ID where there are no nested nodes")
    g.vs['cluster'] = label_clusters(g.vs, is_empty_task_region, lambda v: v['task_cluster_id'][0])
    nodes_before = num_nodes
    g.contract_vertices(g.vs['cluster'], combine_attrs=attr_handler(events=reject_task_create, tuples=set_tuples, attr=attr))
    num_nodes = g.vcount()
    print("{:20s} {:6d} -> {:6d} ({:6d})".format("nodes updated", nodes_before, num_nodes, num_nodes-nodes_before))

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
        if node_types in [{'barrier_implicit'}, {'barrier_explicit'}, {'taskwait'}, {'loop'}] and \
                e.source_vertex.attributes().get('sync_cluster_id', None) is None and \
                e.target_vertex.attributes().get('sync_cluster_id', None) is None:
            value = next(dummy_counter)
            e.source_vertex['sync_cluster_id'] = e.target_vertex['sync_cluster_id'] = value
    print("contracting redundant sync-enter/leave node pairs")
    g.vs['cluster'] = label_clusters(g.vs, lambda v: v['sync_cluster_id'] is not None, 'sync_cluster_id')
    nodes_before = num_nodes
    g.contract_vertices(g.vs['cluster'], combine_attrs=attr_handler(tuples=set_tuples, attr=attr))
    num_nodes = g.vcount()
    print("{:20s} {:6d} -> {:6d} ({:6d})".format("nodes updated", nodes_before, num_nodes, num_nodes-nodes_before))

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

    # Apply taskwait synchronisation
    print("applying taskwait synchronisation")
    for twnode in g.vs.select(lambda v: v['region_type'] == 'taskwait'):
        parents = set(task_tree.vs[event_attr(e, 'encountering_task_id')] for e in twnode['event'])
        tw_encounter_ts = {event_attr(e, 'encountering_task_id'): e.time for e in twnode['event'] if type(e) is Enter}
        children = [c.index for c in chain(*[p.neighbors(mode='out') for p in parents])
                    if c['crt_ts'] < tw_encounter_ts[c['parent_index']] < c['end_ts']]
        nodes = [v for v in g.vs if v['region_type'] == 'explicit_task'
                                 and event_attr(v['event'], 'unique_id') in children
                                 and v['endpoint'] != 'enter']
        ecount = g.ecount()
        g.add_edges([(v.index, twnode.index) for v in nodes])
        g.es[ecount:]['type'] = 'taskwait'

    def event_time_per_task(event):
        """Return the map: encountering task id -> event time for all encountering tasks in the event"""
        if type(event) is list:
            return {event_attr(e, 'encountering_task_id'): e.time for e in event}
        return {event_attr(event, 'encountering_task_id'): event.time}

    # Apply taskgroup synchronisation
    print("applying taskgroup synchronisation")
    for tgnode in g.vs.select(lambda v: v['region_type'] == 'taskgroup' and v['endpoint'] == 'leave'):
        tg_enter_ts = event_time_per_task(tgnode['taskgroup_enter_event'])
        tg_leave_ts = event_time_per_task(tgnode['event'])
        parents = [task_tree.vs[k] for k in tg_enter_ts]
        children = [c for c in chain(*[p.neighbors(mode='out') for p in parents])
                    if tg_enter_ts[c['parent_index']] < c['crt_ts'] < tg_leave_ts[c['parent_index']]]
        descendants = list(chain(*[descendants_if(c, cond=lambda x: x['task_type'] != 'implicit') for c in children]))
        nodes = [v for v in g.vs if v['region_type'] == 'explicit_task'
                                 and event_attr(v['event'], 'unique_id') in descendants
                                 and v['endpoint'] != 'enter']
        ecount = g.ecount()
        g.add_edges([(v.index, tgnode.index) for v in nodes])
        g.es[ecount:]['type'] = 'taskgroup'

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
