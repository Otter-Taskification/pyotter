from . import report
from .classes import Doc
from .make import wrap, table
from .. import log

get_module_logger = log.logger_getter("edges")

def write_report(args, g, tasks):
    import os
    import csv
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
        writer.writerow({
            key: styling.task_attribute_names[key]
            for key in task_attributes
        })
        writer.writerows(tasks.data)

    return
