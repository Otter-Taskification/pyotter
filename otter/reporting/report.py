from .. import log

get_module_logger = log.logger_getter("report")

def create_report_dirs(args):
    import os

    # Create report directory
    try:
        os.mkdir(args.report)
    except FileExistsError as err:
        if not args.force:
            raise

    # Create subdirectory
    subdirs = ["html", "img", "data"]
    for s in subdirs:
        try:
            os.mkdir(os.path.join(args.report, s))
        except FileExistsError as err:
            if not args.force:
                raise

    return


def save_graph_to_dot(graph, dotfile):
    import warnings

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")

        get_module_logger().info(f"writing dotfile: {dotfile}")
        try:
            graph.write(dotfile)
        except OSError as E:
            get_module_logger().error(f"error while writing dotfile: {E}")


def convert_to_svg(dot, svg):
    from subprocess import run, CalledProcessError, PIPE

    command = f"dot -Tsvg -o {svg} -Gpad=1 -Nfontsize=10 {dot}"
    get_module_logger().info(f"converting {dot} to svg")
    get_module_logger().info(command)

    try:
        run(command, shell=True, check=True, stderr=PIPE, stdout=PIPE)
    except CalledProcessError as Error:
        get_module_logger().error(f"{Error}")
        for line in filter(None, Error.stderr.decode('utf-8').split("\n")):
            get_module_logger().error(f"{line}")


def prepare_html(args, tasks):
    from string import Template
    from . import templates
    from . import make
    from .. import styling

    try:
        import importlib.resources as resources
    except ImportError:
        import importlib_resources as resources

    # Make the table of task attributes
    task_table = make.table(
        tasks.attributes,
        styling.task_attribute_names,
        tasks.data,
        attr={
            'table': {'border': '1', 'class': 'data-table'},
            'tr': {'style': 'text-align: center;'}
        }
    )

    # Load template
    html = resources.read_text(templates, 'report.html')

    # Insert data into template
    content = Template(html).safe_substitute(
        ANCHORFILE=args.anchorfile,
        GRAPH_SVG="img/graph.svg",
        TREE_SVG="img/tree.svg",
        TASK_ATTRIBUTES_TABLE=task_table
    )

    return content
