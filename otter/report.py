from .log import get_logger
from . import styling
module_logger = get_logger("report")

def write_report(args, g, tasks):
    import os
    import csv

    task_tree = tasks.task_tree()
    task_attributes = tasks.attributes

    # Normalise report path
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

    # Save graphs
    for obj, name in [(g, "graph"), (task_tree, "tree")]:
        dot = os.path.join(report, "data", f"{name}.dot")
        svg = os.path.join(report, "img", f"{name}.svg")
        save_graph_to_dot(obj, dot)
        convert_to_svg(dot, svg)

    # Write HTML report
    html = prepare_html(tasks)
    html_file = os.path.join(report, "report.html")
    module_logger.info(f"writing report: {html_file}")
    with open(html_file, "w") as f:
        f.write(html)

    # Save task data to csv
    with open(os.path.join(report, "data", "task_attributes.csv"), "w") as csvfile:
        writer = csv.DictWriter(csvfile, task_attributes)
        writer.writerow({
            key: styling.task_attribute_names[key]
            for key in task_attributes
        })
        writer.writerows(tasks.data)

    return


def save_graph_to_dot(graph, dotfile):
    import warnings

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")

        module_logger.info(f"writing dotfile: {dotfile}")
        try:
            graph.write(dotfile)
        except OSError as E:
            module_logger.error(f"error while writing dotfile: {E}")


def convert_to_svg(dot, svg):
    from subprocess import run, CalledProcessError, PIPE

    command = f"dot -Tsvg -o {svg} -Gpad=1 -Nfontsize=10 {dot}"
    module_logger.info(f"converting {dot} to svg")
    module_logger.info(command)

    try:
        run(command, shell=True, check=True, stderr=PIPE, stdout=PIPE)
    except CalledProcessError as Error:
        module_logger.error(f"{Error}")
        for line in filter(None, Error.stderr.decode('utf-8').split("\n")):
            module_logger.error(f"{line}")


def prepare_html(tasks):
    from string import Template
    from . import templates
    from . import reporting

    try:
        import importlib.resources as resources
    except ImportError:
        import importlib_resources as resources

    # Make the table of task attributes
    task_table = reporting.table(
        tasks.attributes,
        styling.task_attribute_names,
        tasks.data,
        attr={
            'table': {'border': '1', 'class': 'data-table'},
            'tr': {'style': 'text-align: right;'}
        }
    )

    # Load template
    html = resources.read_text(templates, 'report.html')

    # Insert data into template
    content = Template(html).safe_substitute(
        GRAPH_SVG="img/graph.svg",
        TREE_SVG="img/tree.svg",
        TASK_ATTRIBUTES_TABLE=task_table
    )

    return content
