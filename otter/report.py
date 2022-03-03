import os
import warnings
import subprocess
from string import Template
try:
    import importlib.resources as pkg_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources as pkg_resources

from . import templates

def write_report(args, graph, task_tree):

    # Create report folder
    #   - if report is abs path, create as abs path
    #   - if report not abs path, create relative to cwd
    # Write graph as .dot in report subfolder
    # Convert .dot -> svg
    # Convert task_tree to .dot -> svg
    # Write HTML to display svg(s)
    # Produce summary table of tasks
    # Convert summary table to HTML and paste into report

    # Create abs path for new report
    if not os.path.isabs(args.report):
        report = os.path.join(os.getcwd(), args.report)
    else:
        report = args.report

    report = os.path.normpath(report)
    print(f"Report path: {report}")

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

    # Write graph as dotfile to data/
    dotfile = os.path.join(report, "data/graph.dot")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        try:
            graph.write(dotfile)
        except OSError as oserr:
            print(f"igraph error: {oserr}")
            print(f"failed to write to file '{args.output}'")

    # Create command to convert .dot to .svg
    svgfile = os.path.join(report, "img/graph.svg")
    convert = f"dot -Tsvg -o {svgfile} -Gpad=1 -Nfontsize=10 {dotfile}"
    print(convert)
    proc = subprocess.run(convert, shell=True)
    if proc.returncode != 0:
        raise RuntimeError("error converting .dot to .svg")

    # Substitute variables in HTML template
    html = pkg_resources.read_text(templates, 'report.html')
    src = Template(html).safe_substitute(GRAPH_SVG="img/graph.svg")

    # Write HTML report
    reportfile = os.path.join(report, "report.html")
    with open(reportfile, "w") as f:
        f.write(src)

    return
