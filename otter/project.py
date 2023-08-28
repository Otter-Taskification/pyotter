from __future__ import annotations

import os

# TODO: sqlite3 should be internal to otter.db
import json
import sqlite3
import sys
from collections import defaultdict
from contextlib import closing
from itertools import count
from typing import Any, AnyStr, Dict, Iterable, List, Set, Tuple

import igraph as ig
from otf2 import LocationType as OTF2Location
from otf2.definitions import Attribute as OTF2Attribute

from . import db, log, reporting
from .core.chunks import Chunk
from .core.event_model.event_model import EventModel, get_event_model
from .core.events import Event, Location
from .core.tasks import TaskRegistry, TaskSynchronisationContext
from .definitions import Attr, SourceLocation, TaskAttributes
from .reader import get_otf2_reader
from .utils import CountingDict, batched


class Project:
    """Prepare to use an anchorfile as input"""

    def __init__(self, anchorfile: str, debug: bool = False) -> None:
        self.log = log.get_logger("main")

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
        self.task_registry = TaskRegistry()
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
            con.executescript(db.scripts.create_tasks)
            con.executescript(db.scripts.create_views)

    def process_trace(self, con: sqlite3.Connection) -> Project:
        """Read a trace and create a database of tasks and their synchronisation constraints"""

        with get_otf2_reader(self.anchorfile) as reader:
            event_model_name: EventModel = reader.get_event_model_name()
            self.event_model = get_event_model(
                event_model_name,
                self.task_registry,
                gather_return_addresses=self.return_addresses,
            )
            log.info("found event model name: %s", event_model_name)
            log.info("using event model: %s", self.event_model)
            log.info("reading tasks")
            attributes: Dict[str, OTF2Attribute] = {
                attr.name: attr for attr in reader.definitions.attributes
            }
            locations: Dict[OTF2Location, Location] = {
                location: Location(location)
                for location in reader.definitions.locations
            }
            event_iter: Iterable[Tuple[Location, Event]] = (
                (locations[location], Event(event, attributes))
                for location, event in reader.events
            )
            context_id: Dict[TaskSynchronisationContext, int] = CountingDict(count())
            for chunk in self.event_model.yield_chunks(event_iter):
                contexts = self.event_model.contexts_of(chunk)
                context_ids = []
                synchronised_tasks = []
                context_meta = []
                for order, context in enumerate(contexts):
                    cid = context_id[context]
                    synchronised_tasks.extend((cid, task.id) for task in context)
                    context_ids.append((chunk.task_id, cid, order))
                    context_meta.append((cid, int(context.synchronise_descendants)))
                # write synchronised_tasks and context_ids to db
                # write the definition of each context i.e. sync_descendants flag
                con.executemany(db.scripts.insert_synchronisation, synchronised_tasks)
                con.executemany(db.scripts.insert_chunk, context_ids)
                con.executemany(db.scripts.insert_context, context_meta)
                con.commit()
        if self.debug:
            log.debug("task registry encountered the following task attributes:")
            for name in sorted(self.task_registry.attributes):
                log.debug(" -- %s", name)
        return self

    def write_tasks_to_db(self, con: sqlite3.Connection) -> None:
        """Write a trace's tasks to a db"""

        log.info("Writing tasks to %s", self.tasks_db)

        # Create unique labels for distinct source locations
        source_location_id: Dict[SourceLocation, int] = CountingDict(count())
        string_id: Dict[str, int] = CountingDict(count())

        # Write the tasks and their source locations
        for tasks in batched(self.task_registry, 1000):
            task_data = (
                (
                    task.id,
                    str(task.start_ts),
                    str(task.end_ts),
                    task.naive_duration,
                    source_location_id[task.init_location],
                    source_location_id[task.start_location],
                    source_location_id[task.end_location],
                    task.task_flavour,
                    string_id[task.task_label],
                )
                for task in tasks
            )
            con.executemany(db.scripts.insert_tasks, task_data)

        con.commit()

        # Write the definitions of the source locations, mapping their IDs to string IDs
        source_location_definitions = (
            (
                locid,
                string_id[location.file],
                string_id[location.func],
                location.line,
            )
            for (location, locid) in source_location_id.items()
        )
        con.executemany(db.scripts.define_source_locations, source_location_definitions)

        # Write the string definitions mapping ID to the string value
        string_definitions = (
            (string_key, string) for (string, string_key) in string_id.items()
        )
        con.executemany(db.scripts.define_strings, string_definitions)

        # Write the task parent-child relationships
        all_relations = (
            (task.id, child_id)
            for task in iter(self.task_registry)
            for child_id in task.iter_children()
        )
        for relations in batched(all_relations, 1000):
            con.executemany(db.scripts.insert_task_relations, relations)
        con.commit()

        (tasks_inserted,) = con.execute(db.scripts.count_tasks).fetchone()
        log.info("wrote %s tasks", tasks_inserted)


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
        self, con: db.Connection, task: int, debug: bool = False
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
                    shape="octagon", style="filled", color="red"
                )
                barrier_vertex["type"] = "barrier"
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
        task_attr = {
            row["id"]: row
            for row in con.attributes_of(n for n in label_data if isinstance(n, int))
        }

        for k, vertex in zip(label_data, graph.vs):
            if k is None:
                vertex["label"] = ""
            elif isinstance(k, int):
                attr = task_attr[k]

                # TODO: consider refactoring to use TaskAttributes instead of dict
                data = {
                    "id": k,
                    "label": attr["task_label"],
                    "flavour": attr["flavour"],
                    "init_location": SourceLocation(
                        attr["init_file"], attr["init_func"], attr["init_line"]
                    ),
                    "start_location": SourceLocation(
                        attr["start_file"], attr["start_func"], attr["start_line"]
                    ),
                    "end_location": SourceLocation(
                        attr["end_file"], attr["end_func"], attr["end_line"]
                    ),
                }

                r, g, b = (int(x * 256) for x in colour[attr[Attr.task_label]])
                vertex["label"] = reporting.as_html_table(data)
                vertex["color"] = f"#{r:02x}{g:02x}{b:02x}"
            elif isinstance(k, dict):
                vertex["label"] = reporting.as_html_table(k)
            elif isinstance(k, tuple):
                vertex["label"] = reporting.as_html_table(dict(k))
            else:
                raise ValueError(
                    f"expected int, dict, None or tuple of name-value pairs, got {k}"
                )

        return graph


def unpack_trace(anchorfile: str, debug: bool = False) -> None:
    """unpack a trace into a database for querying"""

    project = UnpackTraceProject(anchorfile, debug=debug)
    project.prepare_environment()
    with project.connection() as con:
        project.process_trace(con)
        project.write_tasks_to_db(con)
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
            graph.add_edge(vertices[parent], vertices[child], label=total)

    colour = reporting.colour_picker()
    for task, vertex in vertices.items():
        vertex["label"] = reporting.as_html_table(task.asdict())
        r, g, b = (int(x * 256) for x in colour[task.label])
        vertex["color"] = f"#{r:02x}{g:02x}{b:02x}"

    reporting.write_graph_to_file(graph, filename=dotfile)

    result, _, stderr, svgfile = reporting.convert_dot_to_svg(
        dotfile=dotfile, rankdir="LR"
    )
    if result != 0:
        for line in stderr:
            print(line, file=sys.stderr)
    else:
        os.unlink(dotfile)
        print(f"task hierarchy graph written to {svgfile}")


def show_control_flow_graph(
    anchorfile: str,
    dotfile: str,
    task: int,
    style: bool = False,
    simple: bool = False,
    debug: bool = False,
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
