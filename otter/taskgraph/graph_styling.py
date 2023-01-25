from abc import ABC, abstractmethod
from collections import namedtuple
from otter import definitions as defn
from otter.reporting import make

VertexStyle = namedtuple("VertexStyle", "style shape color label")
EdgeStyle = namedtuple("EdgeStyle", "color label penwidth")
FontStyle = namedtuple("FontStyle", "name size color")

class GraphStylingStrategy(ABC):

    @abstractmethod
    def font() -> FontStyle:
        pass

    @abstractmethod
    def get_vertex_style(self, vertex) -> VertexStyle:
        pass

    @abstractmethod
    def get_edge_style(self, edge, source_vertex, target_vertex) -> EdgeStyle:
        pass


class DefaultGraphStyle(GraphStylingStrategy):

    def __init__(self) -> None:
        super().__init__()
        self.graph_font = FontStyle("Times-Roman", "12", "black")
        self.vertex_font = FontStyle("Times-Roman", "12", "black")
        self.edge_font = FontStyle("Helvetica", "18", "black")

    @property
    def font(self) -> FontStyle:
        return self.graph_font

    def get_vertex_style(self, vertex) -> VertexStyle:
        return VertexStyle("filled", "circle", "fuchsia", vertex["event"].label)

    def get_edge_style(self, edge, source_vertex, target_vertex) -> EdgeStyle:
        return EdgeStyle("black", " ", 1.0)


class DebugGraphStyle(GraphStylingStrategy):

    def __init__(self) -> None:
        super().__init__()
        self.graph_font_style = FontStyle("Helvetica", "12", "black")
        self.vertex_font = FontStyle("Helvetica", "18", "black")
        self.edge_font = FontStyle("Helvetica", "18", "black")

    @property
    def font(self) -> FontStyle:
        return self.graph_font_style

    def get_vertex_style(self, vertex) -> VertexStyle:
        if vertex["event"].endpoint == defn.Endpoint.enter:
            color = "blue"
        elif vertex["event"].endpoint == defn.Endpoint.leave:
            color = "red"
        elif vertex["event"].endpoint == defn.Endpoint.discrete:
            color = "green"
        else:
            color = "fuchsia"
        return VertexStyle("filled", "box", color, str(vertex["event"]))

    def get_edge_style(self, edge, source, target) -> EdgeStyle:
        label = f"{source['event'].unique_id} -> {target['event'].unique_id}"
        return EdgeStyle("black", label, 1.0)

class VertexAsHTMLTableStyle(DefaultGraphStyle):

    def __init__(self) -> None:
        super().__init__()
        self.vertex_font = FontStyle("Helvetica", self.vertex_font.size, self.vertex_font.color)

    def get_vertex_style(self, vertex) -> VertexStyle:
        """Make a HTML-like table from the event attributes"""
        if vertex["event"].endpoint == defn.Endpoint.enter:
            color = "lightblue"
        elif vertex["event"].endpoint == defn.Endpoint.leave:
            color = "red"
        elif vertex["event"].endpoint == defn.Endpoint.discrete:
            color = "green"
        else:
            color = "fuchsia"
        label_body = str(
            make.graphviz_record_table(
                vertex["event"].attributes,
                attr={
                    'td': {'align': 'left'}
                }
            )
        )
        return VertexStyle("filled", "plaintext", color, f"<{label_body}>")

    def get_edge_style(self, edge, source_vertex, target_vertex) -> EdgeStyle:
        if edge[defn.Attr.edge_type] == defn.EdgeType.taskwait:
            color = "red"
        else:
            color = "black"
        return EdgeStyle(color, " ", 3.0)
