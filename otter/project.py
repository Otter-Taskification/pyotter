from __future__ import annotations

import abc
import os
import sqlite3
import subprocess as sp
from collections import defaultdict
from contextlib import closing
from itertools import count
from typing import Any, AnyStr, Dict, Iterable, List, Literal, Optional, Set, Tuple

import address_to_line as a2l
import igraph as ig
from otf2 import LocationType as OTF2Location
from otf2.definitions import Attribute as OTF2Attribute

from . import db, log, utils
from .core.event_model.event_model import EventModel, get_event_model
from .core.events import Event, Location
from .core.tasks import Task, TaskRegistry, TaskSynchronisationContext
from .definitions import Attr, SourceLocation, TaskAttributes
from .reader import get_otf2_reader


def closing_connection(name: str) -> db.Connection:
    return closing(db.Connection(name))


class Project(abc.ABC):
    def __init__(
        self, anchorfile: AnyStr, debug: bool = False, prepare_env: bool = True
    ) -> None:
        self.debug = debug
        self.log = log.get_logger("project")
        self.project_root = os.path.dirname(anchorfile)
        self.anchorfile = anchorfile
        self.aux_dir = "aux"
        self.debug_dir = "debug_output"
        self.maps_file = self.abspath(os.path.join(self.aux_dir, "maps"))
        self.source_location_db = self.abspath(os.path.join(self.aux_dir, "srcloc.db"))
        self.tasks_db = self.abspath(os.path.join(self.aux_dir, "tasks.db"))
        self.return_addresses: Set[int] = set()
        self.event_model = None
        self.task_registry = TaskRegistry()
        self.chunks = list()
        if self.debug:
            self.log.debug(f"using project: {self}")
        try:
            self._check_input_files()
        except NotADirectoryError as err:
            self.log.error("directory not found: %s", err)
            raise SystemExit(1) from err
        except FileNotFoundError as err:
            self.log.error("no such file: %s", err)
            raise SystemExit(1) from err
        if prepare_env:
            self._prepare_environment()

    def _check_input_files(self) -> None:
        if not os.path.isdir(self.abspath(self.aux_dir)):
            raise NotADirectoryError(self.abspath(self.aux_dir))
        if not os.path.isfile(self.maps_file):
            raise FileNotFoundError(self.maps_file)
        self.log.info(f"Found maps file: {self.maps_file}")

    def _prepare_environment(self) -> None:
        """Prepare the environment - create any folders, databases, etc required by the project"""
        abs_debug_dir = self.abspath(self.debug_dir)
        self.log.info(f"preparing environment")
        if self.debug:
            if not os.path.exists(abs_debug_dir):
                os.mkdir(abs_debug_dir)
        if os.path.exists(self.tasks_db):
            self.log.warn(f"overwriting tasks database {self.tasks_db}")
            os.remove(self.tasks_db)
        else:
            self.log.info(f"creating tasks database {self.tasks_db}")
        with closing(sqlite3.connect(self.tasks_db)) as con:
            con.executescript(db.scripts.create_tasks)
            con.executescript(db.scripts.create_views)

    def abspath(self, relname: AnyStr) -> AnyStr:
        return os.path.abspath(os.path.join(self.project_root, relname))

    def process_trace(self, con: Optional[sqlite3.Connection] = None) -> Project:
        with get_otf2_reader(self.anchorfile) as reader:
            event_model_name: EventModel = reader.get_event_model_name()
            self.event_model = get_event_model(
                event_model_name,
                self.task_registry,
                gather_return_addresses=self.return_addresses,
            )
            self.log.info(f"Found event model name: {str(event_model_name)}")
            self.log.info(f"Using event model: {self.event_model}")
            self.log.info(f"generating chunks")
            attributes: Dict[str:OTF2Attribute] = {
                attr.name: attr for attr in reader.definitions.attributes
            }
            locations: Dict[OTF2Location:Location] = {
                location: Location(location)
                for location in reader.definitions.locations
            }
            event_iter: Iterable[Tuple[Location, Event]] = (
                (locations[location], Event(event, attributes))
                for location, event in reader.events
            )
            self.chunks = list(self.event_model.yield_chunks(event_iter))
            # TODO: temporary check, factor out once new event models are passing
            # self.event_model.warn_for_incomplete_chunks(self.chunks)

        if self.debug:
            self.log.debug(f"task registry encountered the following task attributes:")
            for name in sorted(self.task_registry.attributes):
                self.log.debug(f" -- {name}")

        return self

    def convert_chunks_to_graphs(self) -> Project:
        self.log.info("generating graph list")
        self.graphs = list(
            self.event_model.chunk_to_graph(chunk) for chunk in self.chunks
        )
        self.log.info("generating combined graph")
        self.graph = self.event_model.combine_graphs(self.graphs)
        return self

    def resolve_return_addresses(self) -> Project:
        self.log.info(
            f"writing source location information to {self.source_location_db}"
        )
        source_locations = a2l.resolve_source_locations(
            self.maps_file, list(self.return_addresses)
        )
        if self.debug:
            for location in source_locations:
                self.log.debug(f"{location}")
        with sqlite3.connect(self.source_location_db) as con:
            data = (
                (f"0x{s.address:0>16x}", s.function, s.file, s.line)
                for s in source_locations
            )
            con.executescript(
                """
                    drop table if exists files;
                    drop table if exists functions;
                    drop table if exists src_loc;

                    create table files(id, path);
                    create table functions(id, name);
                    create table src_loc(address, function, file, line);
                    """
            )
            con.executemany("insert into src_loc values(?, ?, ?, ?)", data)
        return self

    def unpack_vertex_event_attributes(self) -> None:
        self.log.info("unpacking vertex event attributes")
        logger = log.get_logger("unpack_event")
        for vertex in self.graph.vs:
            event_list = vertex["event_list"]
            logger.debug(f"unpacking vertex event_list")
            if self.debug:
                for event in event_list:
                    logger.debug(f"  {event}")
            attributes = self.event_model.unpack(event_list)
            for key, value in attributes.items():
                logger.debug(f"  got {key}={value}")
                if isinstance(value, list):
                    s = set(value)
                    if len(s) == 1:
                        value = s.pop()
                    else:
                        logger.debug(f"  concatenate {len(value)} values")
                        value = ";".join(str(item) for item in value)
                if isinstance(value, int):
                    value = str(value)
                elif value == "":
                    value = None
                logger.debug(f"    unpacked {value=}")
                vertex[key] = value
        self.log.info("finished unpacking vertex event attributes")

    def cleanup_temporary_attributes(self) -> None:
        self.log.info("cleaning up temporary graph attributes")
        for name in self.graph.vs.attribute_names():
            if name.startswith("_"):
                del self.graph.vs[name]

    def write_tasks_to_db(self) -> None:
        self.log.info(f"Writing tasks to {self.tasks_db}")

        with closing(sqlite3.connect(self.tasks_db)) as con:
            # Create unique labels for distinct source locations
            source_location_id: Dict[SourceLocation, int] = utils.CountingDict(count())
            string_id: Dict[str, int] = utils.CountingDict(count())

            # Write the tasks and their source locations
            for tasks in utils.batched(self.task_registry, 1000):
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
            con.executemany(
                db.scripts.define_source_locations, source_location_definitions
            )

            # Write the string definitions mapping ID to the string value
            string_definitions = (
                (string_key, string) for (string, string_key) in string_id.items()
            )
            con.executemany(db.scripts.define_strings, string_definitions)

            # Write the task parent-child relationships
            all_relations = (
                (task.id, child_id)
                for task in self.task_registry
                for child_id in task.iter_children()
            )
            for relations in utils.batched(all_relations, 1000):
                con.executemany(db.scripts.insert_task_relations, relations)
            con.commit()

            (tasks_inserted,) = con.execute(db.scripts.count_tasks).fetchone()
            self.log.info(f"wrote {tasks_inserted} tasks")

    def connection(self) -> db.Connection:
        """Return a connection to this project's tasks db for use in a with-block"""
        return closing_connection(self.tasks_db)

    def convert_dot_to_svg(
        self, dotfile: str, svgfile: str = ""
    ) -> Tuple[int, str, str, str]:
        dirname, dotname = os.path.split(dotfile)
        name, _ = os.path.splitext(dotname)
        if not svgfile:
            svgfile = os.path.join(dirname, name + ".svg")
        command = f"dot -Gpad=1 -Nfontsize=12 -Tsvg -o {svgfile} {dotfile}"
        proc = sp.Popen(command, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        stdout, stderr = proc.communicate()
        return proc.returncode, stdout.decode(), stderr.decode(), svgfile

    @abc.abstractmethod
    def run(self) -> Project:
        self.process_trace()
        self.convert_chunks_to_graphs()
        self.write_tasks_to_db()
        self.resolve_return_addresses()
        if self.debug:
            utils.assert_vertex_event_list(self.graph)
            self.log.info(f"dumping chunks, tasks and graphs to log files")
            utils.dump_to_log_file(
                self.chunks,
                self.graphs,
                self.task_registry,
                where=self.abspath(self.debug_dir),
            )
            graph_log = self.abspath(os.path.join(self.debug_dir, "graph.log"))
            utils.dump_graph_to_file(
                self.graph,
                filename=graph_log,
                no_flatten=[
                    Task,
                ],
            )
        self.unpack_vertex_event_attributes()
        self.cleanup_temporary_attributes()
        del self.graph.vs["event_list"]
        return self


class SimpleProject(Project):
    """Transforms an OTF2 trace into a basic HTML report"""

    def run(self) -> SimpleProject:
        super().run()
        return self


class DBProject(Project):
    """Extracts tasks from a trace into a DB"""

    def run(self) -> DBProject:
        self.process_trace()
        self.write_tasks_to_db()
        self.quit()
        return self

    def quit(self) -> None:
        self.log.info("done - quitting")
        raise SystemExit(0)


class ReadTasksProject(Project):
    """Simply read the tasks from a trace into a DB"""

    def process_trace(self, con: Optional[sqlite3.Connection] = None) -> Project:
        with get_otf2_reader(self.anchorfile) as reader:
            event_model_name: EventModel = reader.get_event_model_name()
            self.event_model = get_event_model(
                event_model_name,
                self.task_registry,
                gather_return_addresses=self.return_addresses,
            )
            self.log.info(f"Found event model name: {str(event_model_name)}")
            self.log.info(f"Using event model: {self.event_model}")
            self.log.info(f"reading tasks")
            attributes: Dict[str:OTF2Attribute] = {
                attr.name: attr for attr in reader.definitions.attributes
            }
            locations: Dict[OTF2Location:Location] = {
                location: Location(location)
                for location in reader.definitions.locations
            }
            event_iter: Iterable[Tuple[Location, Event]] = (
                (locations[location], Event(event, attributes))
                for location, event in reader.events
            )
            context_id: Dict[TaskSynchronisationContext, int] = utils.CountingDict(
                count()
            )
            for chunk in self.event_model.yield_chunks(event_iter):
                contexts = self.event_model.contexts_of(chunk)
                context_ids = list()
                synchronised_tasks = list()
                context_meta = list()
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
            self.log.debug(f"task registry encountered the following task attributes:")
            for name in sorted(self.task_registry.attributes):
                self.log.debug(f" -- {name}")
        return self

    def get_child_count_by_parent_attributes(
        self,
    ) -> List[Tuple[TaskAttributes, TaskAttributes, int]]:
        def make_row(cursor, values) -> Tuple[TaskAttributes, TaskAttributes, int]:
            parent = TaskAttributes(
                values[0],
                values[1],
                SourceLocation(file=values[2], line=values[3], func=values[4]),
                SourceLocation(file=values[5], line=values[6], func=values[7]),
                SourceLocation(file=values[8], line=values[9], func=values[10]),
            )
            child = TaskAttributes(
                values[11],
                values[12],
                SourceLocation(file=values[13], line=values[14], func=values[15]),
                SourceLocation(file=values[16], line=values[17], func=values[18]),
                SourceLocation(file=values[19], line=values[20], func=values[21]),
            )
            total = values[22]
            return parent, child, total

        with closing(sqlite3.connect(self.tasks_db)) as con:
            cur = con.cursor()
            cur.row_factory = make_row
            return cur.execute(
                db.scripts.count_children_by_parent_attributes
            ).fetchall()

    def print_child_count_by_parent_attributes(self) -> None:
        for parent, child, total in self.get_child_count_by_parent_attributes():
            print(
                parent.label,
                parent.init_location,
                child.label,
                child.init_location,
                total,
            )

    def build_parent_child_graph(self) -> ig.Graph:
        graph = ig.Graph(directed=True)
        vertices = defaultdict(lambda: graph.add_vertex(shape="plain", style="filled"))
        rows = self.get_child_count_by_parent_attributes()
        for parent, child, total in rows:
            if not child.is_null():
                graph.add_edge(vertices[parent], vertices[child], label=total)

        from . import reporting

        html_table_attributes = {"td": {"align": "left", "cellpadding": "6px"}}

        def as_html_table(task: TaskAttributes) -> str:
            label_body = reporting.make.graphviz_record_table(
                task.asdict(), table_attr=html_table_attributes
            )
            return f"<{label_body}>"

        for task, vertex in vertices.items():
            vertex["label"] = as_html_table(task)
            r, g, b = (int(x * 256) for x in reporting.colour_picker[task.label])
            hex_colour = f"#{r:02x}{g:02x}{b:02x}"
            vertex["color"] = hex_colour
        return graph

    def write_graph_to_file(self, graph: ig.Graph, filename: str = "graph.dot") -> None:
        graph.write_dot(filename)
        with open(filename, mode="r") as df:
            original = df.readlines()
        with open(filename, mode="w") as df:
            escape_in, escape_out = '"<<', '>>"'
            escaped_quotation = '\\"'
            for line in original:
                # Workaround - remove escaping quotations around HTML-like labels added by igraph.Graph.write_dot()
                if escape_in in line:
                    line = line.replace(escape_in, "<<")
                if escape_out in line:
                    line = line.replace(escape_out, ">>")
                # Workaround - unescape quotation marks for labels
                if "label=" in line and escaped_quotation in line:
                    line = line.replace(escaped_quotation, '"')
                df.write(line)

    def create_db_from_trace(self: ReadTasksProject) -> ReadTasksProject:
        with closing_connection(self.tasks_db) as con:
            self.process_trace(con=con)
        self.write_tasks_to_db()
        return self

    def write_parent_child_graph(self: ReadTasksProject) -> ReadTasksProject:
        graph = self.build_parent_child_graph()
        self.write_graph_to_file(graph)
        return self

    def run(self) -> None:
        self.create_db_from_trace()
        self.write_parent_child_graph()
        self.quit()

    def quit(self) -> None:
        self.log.info("done - quitting")
        raise SystemExit(0)


class BuildGraphFromDB(Project):
    def __init__(self, anchorfile: AnyStr, debug: bool = False) -> None:
        self.debug = debug
        self.log = log.get_logger("project")
        self.project_root = os.path.dirname(anchorfile)
        self.anchorfile = anchorfile
        self.aux_dir = "aux"
        self.debug_dir = "debug_output"
        self.maps_file = self.abspath(os.path.join(self.aux_dir, "maps"))
        self.source_location_db = self.abspath(os.path.join(self.aux_dir, "srcloc.db"))
        self.tasks_db = self.abspath(os.path.join(self.aux_dir, "tasks.db"))
        self.return_addresses: Set[int] = set()
        self.event_model = None
        self.task_registry = TaskRegistry()
        self.chunks = list()
        if self.debug:
            self.log.debug(f"using project: {self}")
        if not os.path.isfile(self.tasks_db):
            log.error("no such file: %s", self.tasks_db)
            raise SystemExit(1)

    def build_cfg(self, con: db.Connection, task: int) -> ig.Graph:
        graph = ig.Graph(directed=True)
        children = list(row["child_id"] for row in con.children_of(task))
        child_vertex = defaultdict(
            lambda: graph.add_vertex(shape="plain", style="filled")
        )
        head, tail = graph.add_vertex(shape="plain", style="filled"), graph.add_vertex(
            shape="plain", style="filled"
        )
        head[Attr.unique_id] = task
        tail[Attr.unique_id] = task
        cur = head
        for child in children:
            vertex = child_vertex[child]
            vertex[Attr.unique_id] = child
            vertex["label"] = f"{child}"
            graph.add_edge(cur, vertex)
            cur = vertex
        graph.add_edge(cur, tail)
        return graph

    def build_styled_cfg(
        self, con: db.Connection, task: int, graph: Optional[ig.Graph] = None
    ) -> ig.Graph:
        from . import reporting

        if graph is None:
            graph = self.build_cfg(con, task)

        html_table_attributes = {"td": {"align": "left", "cellpadding": "6px"}}

        def as_html_table(data: dict) -> str:
            label_body = reporting.make.graphviz_record_table(
                data, table_attr=html_table_attributes
            )
            return f"<{label_body}>"

        tasks = tuple(
            set(filter(lambda x: x is not None, (v[Attr.unique_id] for v in graph.vs)))
        )
        attributes = {row["id"]: row for row in con.attributes_of(tasks)}
        for vertex in graph.vs:
            task_id = vertex[Attr.unique_id]
            attr = attributes[task_id]
            data = {
                "id": task_id,
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
            vertex["label"] = as_html_table(data)
            r, g, b = (
                int(x * 256) for x in reporting.colour_picker[attr[Attr.task_label]]
            )
            hex_colour = f"#{r:02x}{g:02x}{b:02x}"
            vertex["color"] = hex_colour
        return graph

    def build_control_flow_graph(self, con: db.Connection, task: int) -> ig.Graph:
        graph = ig.Graph(directed=True)

        # create head & tail vertices
        head = graph.add_vertex(shape="plain", style="filled")
        tail = graph.add_vertex(shape="plain", style="filled")
        head[Attr.unique_id] = task
        tail[Attr.unique_id] = task
        cur = head

        # for each group of tasks synchronised at a barrier
        for sequence, rows in con.sync_groups(task, debug=True):
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
                task_vertex = graph.add_vertex(shape="plain", style="filled")
                task_vertex[Attr.unique_id] = task_id
                task_vertex["type"] = "task"
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
        graph.vs["label"] = graph.vs[Attr.unique_id]
        return graph

    def style_graph(self, con: db.Connection, graph: ig.Graph, key: str) -> ig.Graph:
        from . import reporting

        html_table_attributes = {"td": {"align": "left", "cellpadding": "6px"}}

        def as_html_table(content: dict) -> str:
            label_body = reporting.make.graphviz_record_table(
                content, table_attr=html_table_attributes
            )
            return f"<{label_body}>"

        if log.is_enabled(log.DEBUG):
            log.debug("styling vertices using key: %s", key)
            for v in graph.vs:
                val = v[key]
                if val is None:
                    log.warning("key was None (vertex=%s)", v)
        keys = graph.vs[key]
        tasks = {k for k in keys if k is not None}
        attributes = {row["id"]: row for row in con.attributes_of(tasks)}
        for k, vertex in zip(keys, graph.vs):
            if k is None:
                continue
            attr = attributes[k]
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
            vertex["label"] = as_html_table(data)
            r, g, b = (
                int(x * 256) for x in reporting.colour_picker[attr[Attr.task_label]]
            )
            hex_colour = f"#{r:02x}{g:02x}{b:02x}"
            vertex["color"] = hex_colour
        return graph

    def _style_simplified_graph(
        self,
        con: db.Connection,
        graph: ig.Graph,
        simplify_by: Optional[list[str]] = None,
    ) -> ig.Graph:
        from . import reporting

        html_table_attributes = {"td": {"align": "left", "cellpadding": "6px"}}

        def as_html_table(content: dict) -> str:
            label_body = reporting.make.graphviz_record_table(
                content, table_attr=html_table_attributes
            )
            return f"<{label_body}>"

        simplify_by = simplify_by or []

        for vertex in graph.vs:
            if vertex["type"] == "group":
                if len(vertex["_tasks"]) == 1:
                    (k,) = vertex["_tasks"]
                    log.debug("vertex group has 1 task: %s", vertex["_tasks"])
                    (attr,) = con.attributes_of(vertex["_tasks"])
                    log.debug("%s", attr)
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
                    r, g, b = (
                        int(x * 256)
                        for x in reporting.colour_picker[attr[Attr.task_label]]
                    )
                    hex_colour = f"#{r:02x}{g:02x}{b:02x}"
                    vertex["label"] = as_html_table(data)
                    vertex["color"] = hex_colour
                else:
                    vertex_key = vertex["_key"]
                    data = {k: v for k, v in zip(vertex["_cols"], vertex["_key"])}
                    log.debug("%s", data)
                    vertex["label"] = as_html_table(data)

        for name in graph.vs.attributes():
            if name.startswith("_"):
                log.debug("vertex attribute %s -> delete", name)
                del graph.vs[name]
            else:
                log.debug("vertex attribute %s -> keep", name)
                log.debug(
                    "types: %s", ", ".join([str(type(item)) for item in graph.vs[name]])
                )

        print(graph.vs["label"])

        return graph

    def write_graph_to_file(self, graph: ig.Graph, filename: str = "graph.dot") -> None:
        graph.write_dot(filename)
        with open(filename, mode="r") as df:
            original = df.readlines()
        with open(filename, mode="w") as df:
            escape_in, escape_out = '"<<', '>>"'
            escaped_quotation = '\\"'
            for line in original:
                # Workaround - remove escaping quotations around HTML-like labels added by igraph.Graph.write_dot()
                if escape_in in line:
                    line = line.replace(escape_in, "<<")
                if escape_out in line:
                    line = line.replace(escape_out, ">>")
                # Workaround - unescape quotation marks for labels
                if "label=" in line and escaped_quotation in line:
                    line = line.replace(escaped_quotation, '"')
                df.write(line)

    def print_child_sync_points(self, con: db.Connection, task: int) -> None:
        records = con.child_sync_points(task)
        for row in records:
            print(dict(row))
        return

    def run(self) -> None:
        with closing_connection(self.tasks_db) as con:
            root_graph = self.build_styled_cfg(con, 0)
        self.write_graph_to_file(root_graph, filename="root_graph.dot")
        with closing_connection(self.tasks_db) as con:
            task = 0
            self.print_child_sync_points(con, task)
            cfg = self._build_control_flow_graph(con, task)
            self.write_graph_to_file(cfg, "cfg.dot")
