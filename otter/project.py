from __future__ import annotations
import os
import abc
import sqlite3
import address_to_line as a2l
import igraph as ig
from contextlib import closing
from collections import defaultdict
from itertools import count
from typing import AnyStr, Set, Dict, Iterable, Tuple, Set, List
from otf2.definitions import Attribute as OTF2Attribute
from otf2 import LocationType as OTF2Location
from . import log
from . import utils
from . import db
from .reader import get_otf2_reader
from .definitions import SourceLocation, TaskAttributes
from .core.event_model.event_model import EventModel, get_event_model
from .core.events import Event, Location
from .core.tasks import TaskRegistry, Task


class Project(abc.ABC):

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
        self._check_input_files()
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

    def process_trace(self) -> Project:
        with get_otf2_reader(self.anchorfile) as reader:
            event_model_name: EventModel = reader.get_event_model_name()
            self.event_model = get_event_model(event_model_name, self.task_registry, gather_return_addresses=self.return_addresses)
            self.log.info(f"Found event model name: {str(event_model_name)}")
            self.log.info(f"Using event model: {self.event_model}")
            self.log.info(f"generating chunks")
            attributes: Dict[str: OTF2Attribute] = {attr.name: attr for attr in reader.definitions.attributes}
            locations: Dict[OTF2Location: Location] = {location: Location(location) for location in
                                                       reader.definitions.locations}
            event_iter: Iterable[Tuple[Location, Event]] = ((locations[location], Event(event, attributes)) for
                                                            location, event in reader.events)
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
        self.graphs = list(self.event_model.chunk_to_graph(chunk) for chunk in self.chunks)
        self.log.info("generating combined graph")
        self.graph = self.event_model.combine_graphs(self.graphs)
        return self

    def resolve_return_addresses(self) -> Project:
        self.log.info(f"writing source location information to {self.source_location_db}")
        source_locations = a2l.resolve_source_locations(self.maps_file, list(self.return_addresses))
        if self.debug:
            for location in source_locations:
                self.log.debug(f"{location}")
        with sqlite3.connect(self.source_location_db) as con:
            data = ((f"0x{s.address:0>16x}", s.function, s.file, s.line) for s in source_locations)
            con.executescript("""
                    drop table if exists files;
                    drop table if exists functions;
                    drop table if exists src_loc;

                    create table files(id, path);
                    create table functions(id, name);
                    create table src_loc(address, function, file, line);
                    """)
            con.executemany("insert into src_loc values(?, ?, ?, ?)", data)
        return self

    def unpack_vertex_event_attributes(self) -> None:
        self.log.info("unpacking vertex event attributes")
        logger = log.get_logger("unpack_event")
        for vertex in self.graph.vs:
            event_list = vertex['event_list']
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

                task_data = ((
                    task.id,
                    str(task.start_ts),
                    str(task.end_ts),
                    task.naive_duration,
                    source_location_id[task.init_location],
                    source_location_id[task.start_location],
                    source_location_id[task.end_location],
                    task.task_flavour,
                    string_id[task.task_label]
                ) for task in tasks)
                con.executemany(db.scripts.insert_tasks, task_data)

            con.commit()

            # Write the definitions of the source locations, mapping their IDs to string IDs
            source_location_definitions = ((locid, string_id[location.file], string_id[location.func], location.line) for (location, locid) in source_location_id.items())
            con.executemany(db.scripts.define_source_locations, source_location_definitions)

            # Write the string definitions mapping ID to the string value
            string_definitions = ((string_key, string) for (string, string_key) in string_id.items())
            con.executemany(db.scripts.define_strings, string_definitions)

            # Write the task parent-child relationships
            all_relations = ((task.id, child_id) for task in self.task_registry for child_id in task.iter_children())
            for relations in utils.batched(all_relations, 1000):
                con.executemany(db.scripts.insert_task_relations, relations)
            con.commit()

            tasks_inserted, = con.execute(db.scripts.count_tasks).fetchone()
            self.log.info(f"wrote {tasks_inserted} tasks")

    @abc.abstractmethod
    def run(self) -> Project:
        self.process_trace()
        self.convert_chunks_to_graphs()
        self.write_tasks_to_db()
        self.resolve_return_addresses()
        if self.debug:
            utils.assert_vertex_event_list(self.graph)
            self.log.info(f"dumping chunks, tasks and graphs to log files")
            utils.dump_to_log_file(self.chunks, self.graphs, self.task_registry, where=self.abspath(self.debug_dir))
            graph_log = self.abspath(os.path.join(self.debug_dir, "graph.log"))
            utils.dump_graph_to_file(self.graph, filename=graph_log, no_flatten=[Task,])
        self.unpack_vertex_event_attributes()
        self.cleanup_temporary_attributes()
        del self.graph.vs['event_list']
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

    def process_trace(self) -> Project:
        with get_otf2_reader(self.anchorfile) as reader:
            event_model_name: EventModel = reader.get_event_model_name()
            self.event_model = get_event_model(event_model_name, self.task_registry, gather_return_addresses=self.return_addresses)
            self.log.info(f"Found event model name: {str(event_model_name)}")
            self.log.info(f"Using event model: {self.event_model}")
            self.log.info(f"reading tasks")
            attributes: Dict[str: OTF2Attribute] = {attr.name: attr for attr in reader.definitions.attributes}
            for _, event in reader.events:
                self.event_model.notify_task_registry(Event(event, attributes))
        if self.debug:
            self.log.debug(f"task registry encountered the following task attributes:")
            for name in sorted(self.task_registry.attributes):
                self.log.debug(f" -- {name}")
        return self

    def get_child_count_by_parent_attributes(self) -> List[Tuple[TaskAttributes, TaskAttributes, int]]:

        def make_row(cursor, values) -> Tuple[TaskAttributes, TaskAttributes, int]:
            parent = TaskAttributes(values[0],
                values[1],
                SourceLocation(file=values[2], line=values[3], func=values[4]),
                SourceLocation(file=values[5], line=values[6], func=values[7]),
                SourceLocation(file=values[8], line=values[9], func=values[10])
            )
            child = TaskAttributes(values[11],
                values[12],
                SourceLocation(file=values[13], line=values[14], func=values[15]),
                SourceLocation(file=values[16], line=values[17], func=values[18]),
                SourceLocation(file=values[19], line=values[20], func=values[21])
            )
            total = values[22]
            return parent, child, total

        with closing(sqlite3.connect(self.tasks_db)) as con:
            cur = con.cursor()
            cur.row_factory = make_row
            return cur.execute(db.scripts.count_children_by_parent_attributes).fetchall()

    def print_child_count_by_parent_attributes(self) -> None:
        for parent, child, total in self.get_child_count_by_parent_attributes():
            print(parent.label, parent.init_location, child.label, child.init_location, total)

    def build_parent_child_graph(self) -> ig.Graph:

        graph = ig.Graph(directed=True)
        vertices = defaultdict(lambda : graph.add_vertex(shape="plain", style="filled"))
        rows = self.get_child_count_by_parent_attributes()
        for parent, child, total in rows:
            if not child.is_null():
                graph.add_edge(vertices[parent], vertices[child], label=total)

        from . import reporting

        html_table_attributes = {
            'td': {'align': 'left',
                   'cellpadding': '6px'}
        }

        def as_html_table(task: TaskAttributes) -> str:
            label_body = reporting.make.graphviz_record_table(task.asdict(), table_attr=html_table_attributes)
            return f"<{label_body}>"

        # distinctipy.get_colors(15, pastel_factor=0.7)
        some_colours = [(0.41789678359925886, 0.4478800973360014, 0.6603976790578713), (0.5791295025850759, 0.9501035530193626, 0.4146731695259466), (0.9873477590634498, 0.41764219864176216, 0.9232038676343455), (0.41237294263760504, 0.964072612230516, 0.9807107771055183), (0.9734820016268507, 0.6770783213352466, 0.42287950368121985), (0.6993998367145301, 0.677687013392824, 0.9288022099506522), (0.9899562965177089, 0.9957366159760942, 0.521178184203679), (0.7975389054510595, 0.41283266748192166, 0.4629890235704576), (0.43702644596770984, 0.759556176934646, 0.6749048125249932), (0.6965871214404423, 0.9463828725945549, 0.7605229568037236), (0.5834861331237369, 0.4219039575347027, 0.9770369349535316), (0.5607295995085896, 0.6237116443413862, 0.42199815992837764), (0.9882948135565736, 0.7435265893431469, 0.9605990173642993), (0.9071707415444149, 0.5894615743307152, 0.7128698728178723), (0.41403720757157997, 0.5896162315031304, 0.9210362508145612), (0.4215567660131879, 0.9938437194754475, 0.6636627502476266), (0.7639598076246374, 0.7713442915492512, 0.5556698714993118), (0.6355678896897076, 0.5898098792616564, 0.685455897908628), (0.9728576888043581, 0.8468985578080623, 0.7159697818623745), (0.47285542519183504, 0.7751569384799412, 0.9320834162513205), (0.800098097601043, 0.4150509814299012, 0.7281924315258136), (0.5496771656026366, 0.41730631452034933, 0.4521858956509995), (0.41678419641558745, 0.7803626090187631, 0.4272766394798354), (0.8234105355146586, 0.8660148388889043, 0.9561085100428577), (0.7855705031865389, 0.4943568361123591, 0.9988939092821855), (0.9847904786571894, 0.4482606006412153, 0.562910494055332), (0.6798235771065145, 0.9971233740851245, 0.9996569595834145), (0.792809765578224, 0.5906601531245763, 0.4957483837416151), (0.7694157231942473, 0.9524013653707905, 0.5176982404679867), (0.9232053978283504, 0.8401250830830093, 0.44696208905160995), (0.9236863054751214, 0.9993733677837177, 0.7888506268699739), (0.8263908834333781, 0.7439675457620962, 0.7763040928845777), (0.6177129866674549, 0.8183079354608641, 0.6825147487887169), (0.9151439425415392, 0.5898222404445026, 0.9285484173213013), (0.43136801207556663, 0.6020577045316525, 0.5727887822333112), (0.5948650486879187, 0.43262190522867067, 0.7727896623510145), (0.5238812485249263, 0.8919073829043799, 0.8070411720742222), (0.9598639773176977, 0.7150237252118297, 0.6385838504280782), (0.6096499184756766, 0.7652215789853251, 0.4453973667162779), (0.41273971100526313, 0.9704394795215736, 0.476492239648635)]
        colour_iter = iter(some_colours)
        colour_picker = defaultdict(lambda : next(colour_iter))

        for task, vertex in vertices.items():
            vertex["label"] = as_html_table(task)
            r, g, b = (int(x*256) for x in colour_picker[task.label])
            hex_colour = f"#{r:02x}{g:02x}{b:02x}"
            vertex["color"] = hex_colour
        return graph

    def write_graph_to_file(self, graph: ig.Graph, filename: str="graph.dot") -> None:
        graph.write_dot(filename)
        with open(filename, mode="r") as df:
            original = df.readlines()
        with open(filename, mode="w") as df:
            escape_in, escape_out = "\"<<", ">>\""
            escaped_quotation = "\\\""
            for line in original:
                # Workaround - remove escaping quotations around HTML-like labels added by igraph.Graph.write_dot()
                if escape_in in line:
                    line = line.replace(escape_in, "<<")
                if escape_out in line:
                    line = line.replace(escape_out, ">>")
                # Workaround - unescape quotation marks for labels
                if "label=" in line and escaped_quotation in line:
                    line = line.replace(escaped_quotation, "\"")
                df.write(line)

    def run(self) -> None:
        self.process_trace()
        self.write_tasks_to_db()
        graph = self.build_parent_child_graph()
        self.write_graph_to_file(graph)
        self.quit()

    def quit(self) -> None:
        self.log.info("done - quitting")
        raise SystemExit(0)
