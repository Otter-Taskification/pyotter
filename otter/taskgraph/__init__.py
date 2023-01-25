import igraph as ig
import otter
from otter import definitions as defn
from collections import namedtuple, defaultdict
from . import graph_styling as styling

TaskVertexTuple = namedtuple("TaskVertexTuple", "enter leave")

class EventReader():
    """Reads events from an OTF2 task-graph trace"""

    def __init__(self, otf2_reader) -> None:
        self._otf2_reader = otf2_reader
        self._attribute_lookup = {attribute.name: attribute for attribute in otf2_reader.definitions.attributes}

    @property
    def events(self):
        for _, e in self._otf2_reader.events:
            yield Event(e, self._attribute_lookup)


class Event():
    """A basic wrapper for OTF2 events"""

    def __init__(self, otf2_event, attribute_lookup) -> None:
        self._event = otf2_event
        self._attribute_lookup = attribute_lookup

    def __repr__(self) -> str:
        return f"{type(self).__name__}(time={self.time}, endpoint={self._event.attributes[self._attribute_lookup['endpoint']]}, type={type(self._event).__name__})"

    def __str__(self) -> str:
        body = "\n".join([f"  {name}: {value}" for name, value in self.attributes.items()])
        return f"[{self.time}] {self.event_type} {self.endpoint} event:\n{body}"

    def __getattr__(self, name):
        if name == defn.Attr.time:
            return self._event.time
        try:
            return self._event.attributes[self._attribute_lookup[name]]
        except KeyError:
            raise AttributeError(f"attribute '{name}' not found in {self}") from None

    @property
    def attributes(self):
        return {attribute.name: value for attribute, value in self._event.attributes.items()}

    @property
    def label(self):
        if self.event_type == defn.EventType.task_switch:
            return self.unique_id
        else:
            return " "

    def get_task_data(self) -> dict:
        """Return the task data of an event, if there is any"""
        if self.event_type == defn.EventType.task_switch:
            return {
                defn.Attr.time:           self.time,
                defn.Attr.task_type:      defn.TaskType.explicit,
                defn.Attr.unique_id:      self.unique_id,
                defn.Attr.parent_task_id: self.parent_task_id
            }
        return dict()


class EventGraph():

    def __init__(self, style: styling.GraphStylingStrategy = styling.DefaultGraphStyle()) -> None:
        self.graph = ig.Graph(directed=True)
        self.style = style
        self.task_vertex_tuple_lookup = dict() # int -> TaskVertexTuple
        self.task_links = defaultdict(list)
        self.task_registry = otter.TaskRegistry()
        return

    def __repr__(self) -> str:
        return f"{type(self).__name__}()"

    def from_events(self, events) -> None:
        for event in events:
            self.add_event(event)

    def add_task_enter_leave_vertices(self, event, task_id) -> TaskVertexTuple:
        vertices = TaskVertexTuple(
            self.graph.add_vertex(event=event, endpoint=defn.Endpoint.enter),
            self.graph.add_vertex(event=None, endpoint=defn.Endpoint.leave)
        )
        self.task_vertex_tuple_lookup[task_id] = vertices
        self.graph.add_edge(vertices.enter, vertices.leave)
        return vertices

    def get_task_enter_leave_vertices(self, task_id) -> TaskVertexTuple:
        if task_id == defn.NullTask:
            return None
        return self.task_vertex_tuple_lookup[task_id]

    def add_event(self, event) -> None:
        if event.event_type == defn.EventType.task_switch:
            self.add_event_task_switch(event)
        elif event.event_type == defn.EventType.sync_begin:
            self.add_event_synchronise(event)
        else:
            raise ValueError(f"unkown event_type: \"{event.event_type}\"")

    def add_event_task_switch(self, event) -> None:
        task_id = event.unique_id
        parent_id = event.parent_task_id
        encountering_task_id = event.encountering_task_id
        try:
            encountering_task = self.task_registry[encountering_task_id]
        except otter.core.tasks.NullTaskError:
            encountering_task = None
        if event.endpoint == defn.Endpoint.enter:
            created_task = self.task_registry.register_task(event)
            if encountering_task is not None:
                encountering_task.append_to_barrier_cache(created_task)
            task_vertices = self.add_task_enter_leave_vertices(event, task_id)
            parent_vertices = self.get_task_enter_leave_vertices(parent_id)
            if parent_vertices is not None:
                self.graph.add_edge(parent_vertices.enter, task_vertices.enter)
                self.graph.add_edge(task_vertices.leave, parent_vertices.leave)
            self.task_links[parent_id].append(task_id)
        elif event.endpoint == defn.Endpoint.leave:
            task_vertices = self.get_task_enter_leave_vertices(task_id)
            task_vertices.leave["event"] = event
        else:
            raise ValueError(f"invalid endpoint \"{event.endpoint}\" for {event} event")

    def add_event_synchronise(self, event) -> None:
        encountering_task_id = event.encountering_task_id
        try:
            encountering_task = self.task_registry[encountering_task_id]
        except otter.core.tasks.NullTaskError:
            encountering_task = None
        should_sync_descendants = event.sync_descendant_tasks==defn.TaskSyncType.descendants
        # Create a context for the tasks synchronised at this barrier
        barrier_context = otter.core.tasks.TaskSynchronisationContext(descendants=should_sync_descendants)
        # Register tasks synchronised at a barrier
        if encountering_task is not None:
            barrier_context.synchronise_from(encountering_task.task_barrier_cache)
            # Forget about tasks and task iterables synchronised here
            encountering_task.clear_task_barrier_cache()
        self.graph.add_vertex(event=event, _barrier_context=barrier_context)
        return

    def finalise_graph(self) -> None:
        # Apply final clean-up to graph

        # Get all synchronisation contexts create edges for them
        for task_sync_vertex in filter(lambda v: v['_barrier_context'] is not None, self.graph.vs):
            barrier_context = task_sync_vertex['_barrier_context']
            for synchronised_task in barrier_context:
                # Get the synchronised task's vertices
                task_vertices = self.task_vertex_tuple_lookup[synchronised_task.id]
                edge = self.graph.add_edge(task_vertices.leave, task_sync_vertex)
                edge[otter.Attr.edge_type] = defn.EdgeType.taskwait
                if barrier_context.synchronise_descendants:
                    # Add edges for descendants of synchronised_task
                    for descendant_task_id in self.task_registry.descendants_while(synchronised_task.id, lambda task: True):
                        descendant_task = self.task_registry[descendant_task_id]
                        # This task is synchronised by the context
                        # Get the synchronised task's vertices
                        task_vertices = self.task_vertex_tuple_lookup[descendant_task.id]
                        edge = self.graph.add_edge(task_vertices.leave, task_sync_vertex)
                        edge[otter.Attr.edge_type] = defn.EdgeType.taskwait

    def update_style(self, styling_strategy) -> None:
        self.style = styling_strategy

    def apply_styling(self, styling_strategy=None) -> None:
        # Apply chosen style to each vertex and edge
        if styling_strategy is not None:
            self.update_style(styling_strategy)
        per_vertex_styles = [self.style.get_vertex_style(vertex) for vertex in self.graph.vs]
        vertex_styles, vertex_shapes, vertex_colors, vertex_labels = list(map(list, zip(*per_vertex_styles)))
        per_edge_styles = [self.style.get_edge_style(edge, self.graph.vs[edge.source], self.graph.vs[edge.target]) for edge in self.graph.es]
        edge_colors, edge_labels, edge_penwidth = list(map(list, zip(*per_edge_styles)))
        self.graph.vs['style'] = vertex_styles
        self.graph.vs['shape'] = vertex_shapes
        self.graph.vs['color'] = vertex_colors
        self.graph.vs['label'] = vertex_labels
        self.graph.es['color'] = edge_colors
        self.graph.es['label'] = edge_labels
        self.graph.es['penwidth'] = edge_penwidth

    def save_as_dot(self, dotfile) -> None:
        import warnings

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            try:
                self.graph.write(dotfile)
            except OSError as e:
                print(f"error while writing dotfile: {e}")
                raise e

        if type(self.style) == styling.VertexAsHTMLTableStyle:
            with open(dotfile, mode="r") as df:
                original = df.readlines()

            with open(dotfile, mode="w") as df:
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


    def save_as_svg(self, svgfile) -> None:
        dotfile = "temp.dot"
        self.save_as_dot(dotfile)
        from subprocess import run, CalledProcessError, PIPE
        command = f"dot -Tsvg -o {svgfile} " \
            f"-Gpad=1 -Gfontname=\"{self.style.font.name}\" -Gfontsize={self.style.font.size} -Gfontcolor=\"{self.style.font.color}\" " \
            f"-Nfontname=\"{self.style.vertex_font.name}\" -Nfontsize={self.style.vertex_font.size} -Nfontcolor=\"{self.style.vertex_font.color}\" " \
            f"-Efontname=\"{self.style.edge_font.name}\" -Efontsize={self.style.edge_font.size} -Efontcolor=\"{self.style.edge_font.color}\" " \
            f"{dotfile}"
        try:
            run(command, shell=True, check=True, stderr=PIPE, stdout=PIPE)
        except CalledProcessError as Error:
            print(f"{Error}")
            for line in filter(None, Error.stderr.decode('utf-8').split("\n")):
                print(f"{line}")
