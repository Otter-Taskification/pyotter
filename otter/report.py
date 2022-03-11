import os
import warnings
import subprocess
from string import Template
try:
    import importlib.resources as pkg_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources as pkg_resources

import csv
from . import templates

def write_report(args, graph, task_tree):

    # Create abs path for new report
    if not os.path.isabs(args.report):
        report = os.path.join(os.getcwd(), args.report)
    else:
        report = args.report

    report = os.path.normpath(report)

    # Create report directory
    try:
        os.mkdir(report)
    except FileExistsError as err:
        print(f"Error: {err}")
        return

    # Create subdirectory
    subdirs = ["html", "img", "data"]
    for s in subdirs:
        os.mkdir(os.path.join(report, s))

    file_names = [("graph", graph), ("tree", task_tree)]

    # Create dotfiles and convert each to svg
    for prefix, obj in file_names:
        dotfile = os.path.join(report, "data", prefix + ".dot")
        svgfile = os.path.join(report, "img", prefix + ".svg")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                obj.write(dotfile)
            except OSError as oserr:
                print(f"igraph error: {oserr}")
                print(f"failed to write to dotfile")
        conversion = f"dot -Tsvg -o {svgfile} -Gpad=1 -Nfontsize=10 {dotfile}"
        print(conversion)
        proc = subprocess.run(conversion, shell=True)
        if proc.returncode != 0:
            raise RuntimeError("error converting .dot to .svg")

    # Create HTML table of task attributes
    # task_attributes = pd.DataFrame([v.attributes() for v in task_tree.vs])

    header = ['name', 'task_type', 'crt_ts', 'end_ts', 'parent_index', 'style', 'color']
    # Map attribute names to column headers
    tidy_names = {
        'name': "Task ID",
        'task_type': "Task Type",
        'crt_ts': "Creation time",
        'end_ts': "End time",
        'parent_index': "Parent ID",
        'style': "Style",
        'color': "Colour"
    }

    html_table = \
"""
<table border="1" class="data-table">
    <thead>
    {}
    </thead>
    <tbody>
    {}
    </tbody>
</table>
"""

    html_table_header = "<tr style=\"text-align: right;\">\n" + "\n".join(["<th>{}</th>".format(tidy_names[x]) for x in header]) + "\n</tr>\n"

    html_table_rows = "\n".join(["<tr>\n" + "\n".join(["<td>{}</td>".format(v[key]) for key in header]) + "\n</tr\n>" for v in task_tree.vs])

    # Substitute variables in HTML template
    html = pkg_resources.read_text(templates, 'report.html')
    src = Template(html).safe_substitute(
        GRAPH_SVG="img/graph.svg",
        TREE_SVG="img/tree.svg",
        TASK_ATTRIBUTES_TABLE=html_table.format(html_table_header, html_table_rows)
    )

    # Save task data to csv
    with open(os.path.join(report, "data", "task_attributes.csv"), "w") as csvfile:
        writer = csv.DictWriter(csvfile, header)
        writer.writerow(tidy_names)
        writer.writerows([v.attributes() for v in task_tree.vs])

    # Write HTML report
    reportfile = os.path.join(report, "report.html")
    print(f"writing report: {reportfile}")
    with open(reportfile, "w") as f:
        f.write(src)

    return
