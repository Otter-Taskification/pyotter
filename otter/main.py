"""Access Otter via the command line"""

from json import dumps

import otter.log
import otter
from otter.args import Action, GraphType


def select_action() -> None:
    """Select an action and forward the arguments to that action"""

    args = otter.args.parse()

    with otter.utils.post_mortem(args.pdb):
        _select_action(args)


def _select_action(args) -> None:
    if args.version:
        print(otter.__version__)
        raise SystemExit(0)

    if args.print_args:
        print(dumps(vars(args), indent=2))

    if args.action is None:
        otter.args.print_help()
        raise SystemExit(0)

    otter.log.initialise(args.loglevel)

    if args.anchorfile is None:
        otter.log.error(
            "No anchorfile specified. Either provide anchorfile argument or set OTTER_ANCHORFILE environment variable")
        raise SystemExit(1)

    debug = args.loglevel == otter.log.Level.DEBUG.name.lower()

    with otter.profile.output(args.profile):
        if args.action == Action.UNPACK:
            otter.project.unpack_trace(args.anchorfile, overwrite=args.force)
        elif args.action == Action.SHOW:
            if args.show == GraphType.CFG:
                otter.project.show_control_flow_graph(
                    args.anchorfile,
                    args.dotfile,
                    args.task,
                    True,  # always apply styling
                    args.simple,
                    debug=debug,
                )
            elif args.show == GraphType.HIER:
                otter.project.show_task_hierarchy(args.anchorfile, args.dotfile, debug=debug)
            elif args.show == GraphType.TREE:
                otter.project.show_task_tree(
                    args.anchorfile, args.dotfile, debug=debug, rankdir=args.rankdir
                )
            else:
                otter.log.error(f"unknown graph type: {args.show}")
                raise SystemExit(1)
        elif args.action == Action.SUMMARY:
            otter.project.summarise_tasks_db(
                args.anchorfile, args.summarise, demangle=args.demangle, debug=debug
            )
        elif args.action == Action.FILTER:
            otter.project.print_filter_to_stdout(bool(args.include), args.include or args.exclude)
        elif args.action == Action.SIMULATE:
            otter.project.simulate_schedule(
                args.anchorfile,
                finite=args.finite,
                dummy=args.dummy,
                num_threads=args.threads
            )
        elif args.action == Action.PLOT:
            result = otter.project.plot_scheduling_data(
                args.anchorfile, title=args.title, task=args.task, do_format=args.format, sim_id=args.sim_id
            )
            if result:
                show, *_ = result
                show()
        else:
            print(f"unknown action: {args.action}")
            otter.args.print_help()

    raise SystemExit(0)
