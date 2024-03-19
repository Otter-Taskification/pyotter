from __future__ import annotations

# TODO: sqlite3 should be internal to otter.db
import json
import os
import sqlite3
import sys
from collections import defaultdict
from contextlib import ExitStack, closing
from itertools import count
from typing import Any, AnyStr, Dict, List, Set

import igraph as ig
import otf2_ext

import otter.log as log
from . import db, reporting
from .core import Chunk, DBChunkBuilder, DBChunkReader
from .core.event_model.event_model import (
    EventModel,
    TraceEventIterable,
    get_event_model,
)
from .core.events import Event, Location
from .core.task_builder import DBTaskBuilder
from .definitions import Attr, SourceLocation, TaskAttributes, TraceAttr
from .utils import CountingDict, LabellingDict


class Project:
    """Prepare to use an anchorfile as input"""

    def __init__(self, anchorfile: str, debug: bool = False) -> None:

        self.debug = debug
        if self.debug:
            log.debug("using project: %s", self)

        self.anchorfile = os.path.abspath(anchorfile)
        if not os.path.isfile(self.anchorfile):
            log.error("no such file: %s", self.anchorfile)
            raise SystemExit(1)

        self.project_root: str = os.path.dirname(self.anchorfile)
        self.aux_dir = "aux"
        self.maps_file = self.abspath(os.path.join(self.aux_dir, "maps"))

        if not os.path.isdir(self.abspath(self.aux_dir)):
            log.error("directory not found: %s", self.abspath(self.aux_dir))
            raise SystemExit(1)
        if not os.path.isfile(self.maps_file):
            log.error("no such file: %s", self.maps_file)
            raise SystemExit(1)

        self.debug_dir = "debug_output"
        self.source_location_db = self.abspath(os.path.join(self.aux_dir, "srcloc.db"))
        self.tasks_db = self.abspath(os.path.join(self.aux_dir, "tasks.db"))
        self.return_addresses: Set[int] = set()
        self.event_model = None
        self.chunks: list[Chunk] = []

        log.info("project root:  %s", self.project_root)
        log.info("anchorfile:    %s", self.anchorfile)
        log.info("maps file:     %s", self.maps_file)
        log.info("tasks:         %s", self.tasks_db)

    def abspath(self, relname: str) -> AnyStr:
        """Get the absolute path of an internal folder"""
        return os.path.abspath(os.path.join(self.project_root, relname))

    def connection(self) -> closing[db.Connection]:
        """Return a connection to this project's tasks db for use in a with-block"""

        return closing(db.Connection(self.tasks_db))


class UnpackTraceProject(Project):
    """Unpack a trace"""

    def __init__(self, anchorfile: str, debug: bool = False) -> None:
        super().__init__(anchorfile, debug)
        self.source_location_id: Dict[SourceLocation, int] = LabellingDict()
        self.string_id: Dict[str, int] = LabellingDict()

    def prepare_environment(self) -> None:
        """Prepare the environment - create any folders, databases, etc required by the project"""

        debug_dir = self.abspath(self.debug_dir)
        log.info("preparing environment")
        if self.debug:
            if not os.path.exists(debug_dir):
                os.mkdir(debug_dir)
        if os.path.exists(self.tasks_db):
            log.warning("overwriting tasks database %s", self.tasks_db)
            os.remove(self.tasks_db)
        else:
            log.info("creating tasks database %s", self.tasks_db)
        with closing(sqlite3.connect(self.tasks_db)) as con:
            log.info("creating databases")
            con.executescript(db.scripts.create_tasks)
            log.info("creating views")
            con.executescript(db.scripts.create_views)

    def process_trace(self, con: sqlite3.Connection):
        """Read a trace and create a database of tasks and their synchronisation constraints"""

        chunk_builder = DBChunkBuilder(con, bufsize=5000)
        task_builder = DBTaskBuilder(con, self.source_location_id, self.string_id)

        # First, build the chunks & tasks data
        with ExitStack() as stack:
            reader = stack.enter_context(otf2_ext.open_trace(self.anchorfile))

            log.info("recorded trace version: %s", reader.trace_version)

            if reader.trace_version != otf2_ext.version:
                log.warning(
                    "version mismatch: trace version is %s, python version is %s",
                    reader.trace_version,
                    otf2_ext.version,
                )

            event_model_name = EventModel(
                reader.get_property(TraceAttr.event_model.value)
            )

            self.event_model = get_event_model(
                event_model_name,
                gather_return_addresses=self.return_addresses,
            )

            log.info("found event model name: %s", event_model_name)
            log.info("using event model: %s", self.event_model)

            locations: Dict[int, Location] = {
                ref: Location(location) for ref, location in reader.locations.items()
            }

            # Count the number of events each location yields
            location_counter = CountingDict(start=1)

            # Get the global event reader which streams all events
            global_event_reader = stack.enter_context(reader.events())

            event_iter: TraceEventIterable = (
                (
                    locations[location],
                    location_counter.increment(location),
                    Event(event, reader.attributes),
                )
                for location, event in global_event_reader
            )

            log.info("building chunks")
            log.info("using chunk builder: %s", str(chunk_builder))
            with closing(chunk_builder) as chunk_builder, closing(
                task_builder
            ) as task_builder:
                num_chunks = self.event_model.generate_chunks(
                    event_iter, chunk_builder, task_builder
                )
            log.info("generated %d chunks", num_chunks)

        # Second, iterate over the chunks to extract synchronisation metadata
        # TODO: consider removing this 2nd loop entirely as it should be possible to read this data on-the-fly from the trace
        log.info("generating task synchronisation metadata")
        with ExitStack() as stack:
            reader = stack.enter_context(otf2_ext.open_trace(self.anchorfile))
            seek_events = stack.enter_context(reader.seek_events())
            chunk_reader = DBChunkReader(reader.attributes, seek_events, con)
            context_id = count()

            for chunk in chunk_reader.chunks:
                assert chunk.first is not None
                try:
                    assert self.event_model.is_chunk_start_event(chunk.first)
                except AssertionError as e:
                    log.error("expected a task-register event")
                    log.error("event: %s", chunk.first)
                    log.error("chunk: %s", chunk)
                    raise e
                # Get the ID of the task which this chunk represents
                chunk_task_id = self.event_model.get_task_entered(chunk.first)
                contexts = self.event_model.contexts_of(chunk)
                context_ids = []
                synchronised_tasks = []
                context_meta = []
                #! NOTE: task-graph event model now uses sync start time as the sync time
                for order, (sync_descendants, task_ids, sync_ts) in enumerate(contexts):
                    cid = next(context_id)
                    synchronised_tasks.extend((cid, task) for task in task_ids)
                    context_ids.append((chunk_task_id, cid, order))
                    context_meta.append((cid, int(sync_descendants), str(sync_ts)))
                con.executemany(db.scripts.insert_synchronisation, synchronised_tasks)
                con.executemany(db.scripts.insert_chunk, context_ids)
                con.executemany(db.scripts.insert_context, context_meta)
                con.commit()

        # Finally, write the definitions of the source locations and then the strings
        source_location_definitions = (
            (
                locid,
                self.string_id[location.file],
                self.string_id[location.func],
                location.line,
            )
            for (location, locid) in self.source_location_id.items()
        )
        con.executemany(db.scripts.define_source_locations, source_location_definitions)

        string_definitions = (
            (string_key, string) for (string, string_key) in self.string_id.items()
        )
        con.executemany(db.scripts.define_strings, string_definitions)

        con.commit()


class BuildGraphFromDB(Project):
    """Read an existing tasks database to create a graph"""

    def __init__(self, anchorfile: str, debug: bool = False) -> None:
        super().__init__(anchorfile, debug=debug)
        if not os.path.isfile(self.tasks_db):
            log.error("no such file: %s", self.tasks_db)
            raise SystemExit(1)

    def build_control_flow_graph_simplified(
        self, con: db.Connection, task: int, keys: list[str], debug: bool = False
    ) -> ig.Graph:
        """Build a task's simplified control-flow graph"""

        def debug_msg(msg, *args) -> None:
            log.debug("[build_control_flow_graph_simplified] " + msg, *args)

        if debug:
            debug_msg("keys: %s", keys)

        graph = ig.Graph(directed=True)

        (parent_attr_row,) = con.attributes_of((task,))
        parent_attr = parent_attr_row.as_dict()
        debug_msg("parent_attr=%s", parent_attr)

        # create head & tail vertices
        head = graph.add_vertex(
            shape="plain",
            style="filled",
            type="task",
            attr=task,
        )
        tail = graph.add_vertex(
            shape="plain",
            style="filled",
            type="task",
            attr=task,
        )
        cur = head

        # for each group of tasks synchronised at a barrier
        for sequence, rows in con.sync_groups(task, debug=debug):
            if debug:
                debug_msg("sequence %s has %d tasks", sequence, len(rows))

            # get the attributes for all tasks in this sequence
            task_attribute_rows = con.attributes_of(row["child_id"] for row in rows)
            if debug:
                debug_msg("got task attributes:")
                for row in task_attribute_rows:
                    debug_msg("%s", row)

            # each sequence is terminated by a barrier vertex
            barrier_vertex = None
            if sequence is not None:
                barrier_vertex = graph.add_vertex(
                    shape="octagon", style="filled", color="red", type="barrier"
                )
                graph.add_edge(cur, barrier_vertex)

            # create a vertex for each sub-group of tasks in this sequence
            group_vertices = {}
            for attr, row in zip(task_attribute_rows, rows, strict=True):
                group_attr = tuple(
                    (key, attr[key]) for key in keys if key in attr.keys()
                )
                if group_attr not in group_vertices:
                    group_vertex = graph.add_vertex(
                        shape="plain",
                        style="filled",
                        type="group",
                        attr=dict(group_attr),
                        tasks={attr["id"]},
                    )
                    group_vertices[group_attr] = group_vertex
                    graph.add_edge(cur, group_vertex)
                    if barrier_vertex:
                        graph.add_edge(group_vertex, barrier_vertex)
                    else:
                        graph.add_edge(group_vertex, tail)
                else:
                    group_vertices[group_attr]["tasks"].add(attr["id"])

            for group_vertex in group_vertices.values():
                group_vertex["attr"]["tasks"] = len(group_vertex["tasks"])

            # update for next sequence
            if barrier_vertex:
                cur = barrier_vertex

        # complete the graph
        graph.add_edge(cur, tail)

        if debug:
            debug_msg("created %d vertices:", len(graph.vs))
            for vertex in graph.vs:
                debug_msg("%s", vertex)

        return graph

    def build_control_flow_graph(
        self, con: db.Connection, task: int, debug: bool
    ) -> ig.Graph:
        """Build a task's control-flow-graph"""

        graph = ig.Graph(directed=True)

        # create head & tail vertices
        head = graph.add_vertex(shape="plain", style="filled", attr=task)
        tail = graph.add_vertex(shape="plain", style="filled", attr=task)
        cur = head

        # for each group of tasks synchronised at a barrier
        for sequence, rows in con.sync_groups(task, debug=debug):
            if log.is_enabled(log.DEBUG):
                log.debug("sequence %s has %d tasks", sequence, len(rows))

            # each sequence is terminated by a barrier vertex
            barrier_vertex = None
            if sequence is not None:
                barrier_vertex = graph.add_vertex(
                    shape="octagon", style="filled", color="red", type="barrier"
                )
                graph.add_edge(cur, barrier_vertex)

            # add vertices for the tasks synchronised at this barrier
            for row in rows:
                log.debug("  %s", row)
                task_id = row["child_id"]
                task_vertex = graph.add_vertex(
                    shape="plain", style="filled", attr=task_id, type="task"
                )
                graph.add_edge(cur, task_vertex)
                if barrier_vertex:
                    graph.add_edge(task_vertex, barrier_vertex)
                else:
                    graph.add_edge(task_vertex, tail)

            # update for next sequence
            if barrier_vertex:
                cur = barrier_vertex

        # complete the graph
        graph.add_edge(cur, tail)
        return graph

    @staticmethod
    def style_graph(
        con: db.Connection,
        graph: ig.Graph,
        label_data: List[Any],
        key: str = "attr",
        debug: bool = False,
    ) -> ig.Graph:
        # TODO: could be significantly simpler, all this does is loop over vertices and assign labels (and sometiems colours). Doesn't need to be a member function
        """Apply styling to a graph, using the vertex attribute "key" to get
        label data.

        If vertex[key] is:

        - None: leave the label blank
        - int: interpret as a task ID and use the attributes of that task as the label data.
        - dict: use as the label data.
        - tuple: expect a tuple of key-value pairs to be used as the label data
        - other: raise ValueError
        """

        colour = reporting.colour_picker()

        if debug:
            log.debug("label data:")
            for val in label_data:
                if isinstance(val, int):
                    log.debug("use task id %d as label key", val)
                elif isinstance(val, dict):
                    log.debug("use dict as label data: %s", val)
                elif isinstance(val, tuple):
                    log.debug("interpret as key-value pairs: %s", val)

        # For vertices where the key is int, assume it indicates a task and get
        # all such tasks' attributes
        task_id_labels = [x for x in label_data if isinstance(x, int)]
        task_attributes = dict(
            (task_id, attributes)
            for task_id, *_, attributes in con.task_attributes(task_id_labels)
        )
        for label_item, vertex in zip(label_data, graph.vs):
            if label_item is None:
                vertex["label"] = ""
            elif isinstance(label_item, int):
                data = {"id": label_item}  # have id as the first item in the dict
                data.update(task_attributes[label_item].asdict())
                r, g, b = (int(x * 256) for x in colour[data["label"]])
                vertex["label"] = reporting.as_html_table(data)
                vertex["color"] = f"#{r:02x}{g:02x}{b:02x}"
            elif isinstance(label_item, dict):
                vertex["label"] = reporting.as_html_table(label_item)
            elif isinstance(label_item, tuple):
                vertex["label"] = reporting.as_html_table(dict(label_item))
            else:
                raise ValueError(
                    f"expected int, dict, None or tuple of name-value pairs, got {k}"
                )
        return graph


def unpack_trace(anchorfile: str, debug: bool = False) -> None:
    """unpack a trace into a database for querying"""

    log.info("using OTF2 python version %s", otf2_ext.version)

    project = UnpackTraceProject(anchorfile, debug=debug)
    project.prepare_environment()
    with project.connection() as con:
        # TODO: these two methods could be combined as neither is called anywhere else
        project.process_trace(con)
        con.print_summary()


def show_task_hierarchy(anchorfile: str, dotfile: str, debug: bool = False) -> None:
    """Show the task hierarchy of a trace"""

    project = BuildGraphFromDB(anchorfile, debug=debug)
    log.debug("project=%s", project)

    graph = ig.Graph(directed=True)
    vertices: dict[TaskAttributes, ig.Vertex] = defaultdict(
        lambda: graph.add_vertex(shape="plain", style="filled")
    )

    with project.connection() as con:
        log.debug("fetching data")
        rows = con.parent_child_attributes()

    if len(rows) == 0:
        log.error("no task hierarchy data was returned")
        raise SystemExit(1)

    for parent, child, total in rows:
        if not child.is_null():
            graph.add_edge(vertices[parent], vertices[child], label=str(total))

    colour = reporting.colour_picker(cycle=True)
    for task, vertex in vertices.items():
        vertex["label"] = reporting.as_html_table(task.asdict())
        r, g, b = (int(x * 256) for x in colour[task.label])
        vertex["color"] = f"#{r:02x}{g:02x}{b:02x}"

    log.debug("writing dotfile: %s", dotfile)
    reporting.write_graph_to_file(graph, filename=dotfile)

    log.debug("converting dotfile to svg")
    result, _, stderr, svgfile = reporting.convert_dot_to_svg(
        dotfile=dotfile, rankdir="LR"
    )
    if result != 0:
        for line in stderr.splitlines():
            print(line, file=sys.stderr)
    else:
        print(f"task hierarchy graph written to {svgfile}")


def show_control_flow_graph(
    anchorfile: str,
    dotfile: str,
    task: int,
    style: bool,
    simple: bool,
    debug: bool,
) -> None:
    """Show the cfg of a given task"""

    if "{task}" in dotfile:
        dotfile = dotfile.format(task=task)

    log.info(" --> STEP: create project")
    project = BuildGraphFromDB(anchorfile, debug=debug)
    with project.connection() as con:
        log.info(" --> STEP: build cfg (task=%d)", task)
        if simple:
            cfg = project.build_control_flow_graph_simplified(
                con,
                task,
                keys=["flavour", "task_label", "init_file", "init_func", "init_line"],
                debug=debug,
            )
        else:
            cfg = project.build_control_flow_graph(con, task, debug=debug)
        if debug:
            log.debug("cfg vertex attributes:")
            for name in cfg.vs.attributes():
                log.debug(" -- %s", name)
        if style:
            log.info(" --> STEP: style cfg (task=%d)", task)
            cfg = project.style_graph(con, cfg, cfg.vs["attr"], debug=debug)
        else:
            log.info(" --> [ * SKIPPED * ] STEP: style cfg (task=%d)", task)
    log.info(" --> STEP: write cfg to file (dotfile=%s)", dotfile)
    reporting.write_graph_to_file(cfg, filename=dotfile)
    log.info(" --> STEP: convert to svg")
    result, _, stderr, svgfile = reporting.convert_dot_to_svg(dotfile)
    if result != 0:
        for line in stderr:
            print(line, file=sys.stderr)
    else:
        project.log.info("cfg for task %d written to %s", task, svgfile)


def summarise_tasks_db(anchorfile: str, debug: bool = False) -> None:
    """Print summary information about a tasks database"""

    project = BuildGraphFromDB(anchorfile, debug=debug)

    with project.connection() as con:
        con.print_summary()


def summarise_source_location(anchorfile: str, debug: bool = False) -> None:
    """Print source locations in the trace"""

    project = BuildGraphFromDB(anchorfile, debug=debug)

    with project.connection() as con:
        source_locations = con.source_locations()

    for location in source_locations:
        print(f"{location.file}:{location.func}:{location.line}")


def summarise_task_types(anchorfile: str, debug: bool = False) -> None:
    """Print all task definitions in the trace"""

    project = BuildGraphFromDB(anchorfile, debug=debug)

    with project.connection() as con:
        task_types = con.task_types()

    task_dicts = []
    for task, num_tasks in task_types:
        task_data = {
            "label": task.label,
            "init_location": str(task.init_location),
            "start_location": str(task.start_location),
            "end_location": str(task.end_location),
        }
        task_dicts.append({"count": num_tasks, "data": task_data})

    print(json.dumps(task_dicts, indent=2))


def print_filter_to_stdout(include: bool, rules: List[List[str]]) -> None:
    """Print a filter file to stdout"""

    header = [
        "# Otter filter file",
        "# =================",
        "# ",
        "# This filter file defines one or more rules for filtering tasks. Each rule ",
        "# uses one or more key-value pairs to match tasks to the rule. A task",
        "# satisfies a rule if it matches all key-value pairs. Tasks are filtered",
        "# if they match at least one rule.",
        "# ",
    ]

    filter_file: List[str] = [*header]
    mode = "include" if include else "exclude"
    filter_file.append("# whether to exclude or include filtered tasks:")
    filter_file.append(f"{'mode':<8s} {mode}")
    filter_file.append("")
    for n, rule in enumerate(rules, start=1):
        keys = set()
        filter_file.append(f"# rule {n}:")
        for item in rule:
            split_at = item.find("=")
            key, value = item[0:split_at], item[split_at + 1 :]
            filter_file.append(f"{key:<8s} {value}")
            if key in keys:
                print(f'Error parsing rule {n}: repeated key "{key}"', file=sys.stderr)
                raise SystemExit(1)
            else:
                keys.add(key)
        filter_file.append(f"# end rule {n}")
        filter_file.append("")

    for line in filter_file:
        print(line)
