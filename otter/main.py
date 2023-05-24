# TODO: include new graph styling methods from taskgraph to otter top level package
# TODO: add TEMPORARY vertex["_event.attributes"] as the dict of event attributes

import warnings
import otter


def main() -> None:

    args = otter.utils.get_args()
    otter.log.initialise(args)
    log = otter.log.get_logger("main")

    if args.loglevel == "DEBUG":
        for arg, value in vars(args).items():
            log.info(f"ARG: {arg}={value}")

    for warning in args.warnings:
        log.info(f"allow warning: {warning}")
        warnings.simplefilter('always', warning)

    project = otter.SimpleProject(
        args.anchorfile,
        debug=(args.loglevel == "DEBUG")
    ).run()

    if args.report:
        log.info("preparing report")
        otter.styling.style_graph(project.graph, style=otter.styling.StyleVertexShapeAsRegionTypeAndColourAsTaskFlavour())
        otter.styling.style_tasks(project.task_registry.task_tree())
        otter.reporting.write_report(args, project.graph, project.task_registry)
        log.info(f"report written to {args.report}")

    if args.interact:
        otter.utils.interact(locals(), project.graph)
