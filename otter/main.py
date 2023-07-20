import cProfile
import warnings
import otter


def run() -> None:
    """Run the main Otter entrypoint"""

    args = otter.args.get_args()
    if args.profile:
        print("Profiling...")
        cProfile.run("_run(args)", filename=args.profile)
        print("Done profiling.")
    else:
        _run(args)


def _run(args) -> None:
    otter.log.initialise(args)
    log = otter.log.get_logger("main")

    if args.loglevel == "DEBUG":
        for arg, value in vars(args).items():
            log.info("ARG: %s=%s", arg, value)

    for warning in args.warnings:
        log.info("allow warning: %s", warning)
        warnings.simplefilter("always", warning)

    cls = {
        "SimpleProject": otter.project.SimpleProject,
        "DBProject": otter.project.DBProject,
        "ReadTasksProject": otter.project.ReadTasksProject,
        "BuildGraphFromDB": otter.project.BuildGraphFromDB,
    }

    project = cls[args.project](args.anchorfile, debug=args.loglevel == "DEBUG").run()

    if args.report:
        log.info("preparing report")
        otter.styling.style_graph(
            project.graph,
            style=otter.styling.StyleVertexShapeAsRegionTypeAndColourAsTaskFlavour(),
        )
        otter.styling.style_tasks(project.task_registry.task_tree())
        otter.reporting.write_report(args, project.graph, project.task_registry)
        log.info("report written to %s", args.report)

    if args.interact:
        otter.utils.interact(locals(), project.graph)
