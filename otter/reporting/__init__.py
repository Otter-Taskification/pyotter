import os
import subprocess as sp
from collections import defaultdict
from typing import Literal, Optional, Tuple

from igraph import Graph

from ..definitions import TaskAttributes
from ..log import logger_getter
from . import make, report
from .classes import Doc
from .make import table, wrap

get_module_logger = logger_getter("edges")


def write_report(args, g, tasks):
    import csv
    import os

    from .. import styling

    task_tree = tasks.task_tree()
    task_attributes = tasks.attributes

    report.create_report_dirs(args)

    # Save graphs
    for obj, name in [(g, "graph"), (task_tree, "tree")]:
        dot = os.path.join(args.report, "data", f"{name}.dot")
        svg = os.path.join(args.report, "img", f"{name}.svg")
        report.save_graph_to_dot(obj, dot)
        report.convert_to_svg(dot, svg)

    # Write HTML report
    html = report.prepare_html(args, tasks)
    html_file = os.path.join(args.report, "report.html")
    get_module_logger().info(f"writing report: {html_file}")
    with open(html_file, "w") as f:
        f.write(html)

    # Save task data to csv
    with open(os.path.join(args.report, "data", "task_attributes.csv"), "w") as csvfile:
        writer = csv.DictWriter(csvfile, task_attributes)
        writer.writerow(
            {
                key: styling.task_attribute_names.get(
                    key, f"unknown task attribute {key=}"
                )
                for key in task_attributes
            }
        )
        writer.writerows(tasks.data)

    return


# distinctipy.get_colors(15, pastel_factor=0.7)
some_colours = [
    (0.41789678359925886, 0.4478800973360014, 0.6603976790578713),
    (0.5791295025850759, 0.9501035530193626, 0.4146731695259466),
    (0.9873477590634498, 0.41764219864176216, 0.9232038676343455),
    (0.41237294263760504, 0.964072612230516, 0.9807107771055183),
    (0.9734820016268507, 0.6770783213352466, 0.42287950368121985),
    (0.6993998367145301, 0.677687013392824, 0.9288022099506522),
    (0.9899562965177089, 0.9957366159760942, 0.521178184203679),
    (0.7975389054510595, 0.41283266748192166, 0.4629890235704576),
    (0.43702644596770984, 0.759556176934646, 0.6749048125249932),
    (0.6965871214404423, 0.9463828725945549, 0.7605229568037236),
    (0.5834861331237369, 0.4219039575347027, 0.9770369349535316),
    (0.5607295995085896, 0.6237116443413862, 0.42199815992837764),
    (0.9882948135565736, 0.7435265893431469, 0.9605990173642993),
    (0.9071707415444149, 0.5894615743307152, 0.7128698728178723),
    (0.41403720757157997, 0.5896162315031304, 0.9210362508145612),
    (0.4215567660131879, 0.9938437194754475, 0.6636627502476266),
    (0.7639598076246374, 0.7713442915492512, 0.5556698714993118),
    (0.6355678896897076, 0.5898098792616564, 0.685455897908628),
    (0.9728576888043581, 0.8468985578080623, 0.7159697818623745),
    (0.47285542519183504, 0.7751569384799412, 0.9320834162513205),
    (0.800098097601043, 0.4150509814299012, 0.7281924315258136),
    (0.5496771656026366, 0.41730631452034933, 0.4521858956509995),
    (0.41678419641558745, 0.7803626090187631, 0.4272766394798354),
    (0.8234105355146586, 0.8660148388889043, 0.9561085100428577),
    (0.7855705031865389, 0.4943568361123591, 0.9988939092821855),
    (0.9847904786571894, 0.4482606006412153, 0.562910494055332),
    (0.6798235771065145, 0.9971233740851245, 0.9996569595834145),
    (0.792809765578224, 0.5906601531245763, 0.4957483837416151),
    (0.7694157231942473, 0.9524013653707905, 0.5176982404679867),
    (0.9232053978283504, 0.8401250830830093, 0.44696208905160995),
    (0.9236863054751214, 0.9993733677837177, 0.7888506268699739),
    (0.8263908834333781, 0.7439675457620962, 0.7763040928845777),
    (0.6177129866674549, 0.8183079354608641, 0.6825147487887169),
    (0.9151439425415392, 0.5898222404445026, 0.9285484173213013),
    (0.43136801207556663, 0.6020577045316525, 0.5727887822333112),
    (0.5948650486879187, 0.43262190522867067, 0.7727896623510145),
    (0.5238812485249263, 0.8919073829043799, 0.8070411720742222),
    (0.9598639773176977, 0.7150237252118297, 0.6385838504280782),
    (0.6096499184756766, 0.7652215789853251, 0.4453973667162779),
    (0.41273971100526313, 0.9704394795215736, 0.476492239648635),
]


def colour_picker():
    colour_iter = iter(some_colours)
    return defaultdict(lambda: next(colour_iter))


def as_html_table(content: dict, table_attr: Optional[dict] = None) -> str:
    """Convert a task's attributes into a formatted html-like graphviz table"""
    if table_attr is None:
        table_attr = {"td": {"align": "left", "cellpadding": "6px"}}
    label_body = make.graphviz_record_table(content, table_attr=table_attr)
    return f"<{label_body}>"


def write_graph_to_file(graph: Graph, filename: str = "graph.dot") -> None:
    """Write a graph to a .dot file, fixing escaped characters in html-like labels"""

    graph.write_dot(filename)
    with open(filename, mode="r", encoding="utf-8") as df:
        original = df.readlines()
    with open(filename, mode="w", encoding="utf-8") as df:
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


def convert_dot_to_svg(
    dotfile: str, svgfile: str = "", rankdir: Literal["LR", "TB"] = "TB"
) -> Tuple[int, str, str, str]:
    """Invoke dot to convert .dot to .svg"""

    dirname, dotname = os.path.split(dotfile)
    name, _ = os.path.splitext(dotname)
    if not svgfile:
        svgfile = os.path.join(dirname, name + ".svg")
    command = (
        f"dot -Grankdir={rankdir} -Gpad=1 -Nfontsize=12 -Tsvg -o {svgfile} {dotfile}"
    )
    with sp.Popen(command, shell=True, stdout=sp.PIPE, stderr=sp.PIPE) as proc:
        stdout, stderr = proc.communicate()
    return proc.returncode, stdout.decode(), stderr.decode(), svgfile
