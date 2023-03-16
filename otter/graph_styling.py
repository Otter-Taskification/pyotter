from typing import Protocol, NamedTuple, Tuple
from otter.definitions import Attr, EdgeType, Endpoint
from otter.reporting import make


class VertexStyle(NamedTuple):
    style: str
    shape: str
    color: str
    label: str


class EdgeStyle(NamedTuple):
    color: str
    label: str
    width: float


class FontStyle(NamedTuple):
    name: str
    size: str
    color: str


class GraphStylingProtocol(Protocol):
    """An interface for getting the style to apply to a graph's vertices and edges"""

    def graph_font(self) -> FontStyle:
        pass

    def vertex_font(self) -> FontStyle:
        pass

    def edge_font(self) -> FontStyle:
        pass

    def get_vertex_style(self, vertex) -> VertexStyle:
        pass

    def get_edge_style(self, edge, source_vertex, target_vertex) -> EdgeStyle:
        pass


class BaseGraphStyle:

    def __init__(self,
            graph_font: FontStyle = FontStyle("Helvetica", "12", "black"),
            vertex_font: FontStyle = FontStyle("Helvetica", "18", "black"),
            edge_font: FontStyle = FontStyle("Helvetica", "18", "black")):
        self._graph_font = graph_font
        self._vertex_font = vertex_font
        self._edge_font = edge_font

    @property
    def graph_font(self) -> FontStyle:
        return self._graph_font

    @property
    def vertex_font(self) -> FontStyle:
        return self._vertex_font

    @property
    def edge_font(self) -> FontStyle:
        return self._edge_font

    @staticmethod
    def get_vertex_style(vertex) -> VertexStyle:
        return VertexStyle("filled", "circle", "fuchsia", vertex["label"])

    @staticmethod
    def get_edge_style(edge, source_vertex, target_vertex) -> EdgeStyle:
        return EdgeStyle("black", " ", 1.0)


class DebugGraphStyle(BaseGraphStyle):

    def get_vertex_style(self, vertex) -> VertexStyle:
        endpoint = vertex["endpoint"]
        label = str(vertex["event"])
        if endpoint == Endpoint.enter:
            return VertexStyle("filled", "box", "blue", label)
        elif endpoint == Endpoint.leave:
            return VertexStyle("filled", "box", "red", label)
        elif endpoint == Endpoint.discrete:
            return VertexStyle("filled", "box", "green", label)

    def get_edge_style(self, edge, source_vertex, target_vertex) -> EdgeStyle:
        return EdgeStyle("black", f"{source_vertex['unique_id']} -> {target_vertex['unique_id']}", 1.0)


class VertexAsHTMLTableStyle(BaseGraphStyle):

    def __init__(self) -> None:
        super().__init__()

    def get_vertex_style(self, vertex) -> VertexStyle:
        """Make a HTML-like table from the event attributes"""
        endpoint = vertex["endpoint"]
        attributes = vertex["_event.attributes"] # should be the dict of event attributes (task_graph: event.attributes)
        if endpoint == Endpoint.enter:
            color = "lightblue"
        elif endpoint == Endpoint.leave:
            color = "red"
        elif endpoint == Endpoint.discrete:
            color = "green"
        else:
            color = "fuchsia"

        label_body = str(
            make.graphviz_record_table(attributes, table_attr={'td': {'align': 'left'}})
        )
        return VertexStyle("filled", "plaintext", color, f"<{label_body}>")

    def get_edge_style(self, edge, source_vertex, target_vertex) -> EdgeStyle:
        if edge[Attr.edge_type] == EdgeType.taskwait:
            color = "red"
        else:
            color = "black"
        return EdgeStyle(color, " ", 3.0)
