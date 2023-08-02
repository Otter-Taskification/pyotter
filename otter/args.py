"""Handle argument parsing"""

import argparse
from enum import Enum


class Action(str, Enum):
    """Defines the available actions"""

    UNPACK = "unpack"
    SHOW = "show"
    SUMMARY = "summary"


class GraphType(str, Enum):
    """Defines the graph types available to do_show()"""

    CFG = "cfg"  # control-flow graph
    HIER = "hier"  # task hierarchy


class LoggingLevel(str, Enum):
    """Logging levels"""

    DEBUG = "debug"
    INFO = "info"
    WARN = "warn"
    ERROR = "error"

    @classmethod
    @property
    def levels(cls) -> list[str]:
        return [level.value for level in cls]


description_action = {
    Action.UNPACK: "unpack an Otter OTF2 trace and prepare it for querying by other Otter actions",
    Action.SHOW: "visualise a chosen task's graph or the task hierarchy",
    Action.SUMMARY: "print some summary information about the tasks database",
}


description_show = {
    GraphType.CFG: "show the control-flow graph of a chosen task",
    GraphType.HIER: "show the task hierarchy",
}


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add common arguments to a parser"""

    parser.add_argument(
        "--loglevel",
        dest="loglevel",
        default=LoggingLevel.WARN.value,
        choices=LoggingLevel.levels,
        help=f"logging level ({', '.join(LoggingLevel.levels)})",
        metavar="level",
    )

    parser.add_argument(
        "--logdir",
        dest="logdir",
        default="otter-logs",
        help="logging directory",
        metavar="dir",
    )

    parser.add_argument(
        "--profile",
        dest="profile",
        metavar="file",
        help="profiling output file",
    )

    parser.add_argument(
        "-v",
        "--version",
        dest="version",
        action="store_true",
        help="print version information and exit",
    )

    parser.add_argument(
        "--print-args",
        dest="print_args",
        action="store_true",
        help="print arguments passed to Otter",
    )


def prepare_parser():
    """Prepare argument parser for otter.main.select_action()"""

    formatter_class = argparse.ArgumentDefaultsHelpFormatter
    parser = argparse.ArgumentParser(formatter_class=formatter_class)

    # subparsers for each action (unpack, show, ...)
    subparse_action = parser.add_subparsers(
        dest="action", metavar="action", required=False
    )
    add_common_arguments(parser)

    # parse the unpack action
    parse_action_unpack = subparse_action.add_parser(
        Action.UNPACK.value,
        help=description_action[Action.UNPACK],
        description=description_action[Action.UNPACK],
        formatter_class=formatter_class,
    )
    parse_action_unpack.add_argument(
        "anchorfile", help="the Otter OTF2 anchorfile to use"
    )
    add_common_arguments(parse_action_unpack)

    parse_action_summary = subparse_action.add_parser(
        Action.SUMMARY.value,
        help=description_action[Action.SUMMARY],
        description=description_action[Action.SUMMARY],
        formatter_class=formatter_class,
    )
    parse_action_summary.add_argument(
        "anchorfile", help="the Otter OTF2 anchorfile to use"
    )
    add_common_arguments(parse_action_summary)

    # parse the show action
    parse_action_show = subparse_action.add_parser(
        Action.SHOW.value,
        help=description_action[Action.SHOW],
        description=description_action[Action.SHOW],
        formatter_class=formatter_class,
    )

    # subparsers for each graph type to show (cfg, hier, ...)
    subparse_action_show = parse_action_show.add_subparsers(dest="show")

    # parse the action "show cfg"
    parser_show_cfg = subparse_action_show.add_parser(
        GraphType.CFG.value,
        help=description_show[GraphType.CFG],
        description=description_show[GraphType.CFG],
        formatter_class=formatter_class,
    )
    parser_show_cfg.add_argument("task", help="task ID", type=int)
    parser_show_cfg.add_argument(
        "-o",
        "--out",
        dest="dotfile",
        metavar="dotfile",
        help="where to save the graph",
        default="cfg_{task}.dot",
    )
    parser_show_cfg.add_argument(
        "-s",
        "--style",
        dest="style",
        help="apply styling to the graph",
        action="store_true",
        default=False,
    )
    parser_show_cfg.add_argument(
        "--simple",
        dest="simple",
        help="create a simplified graph",
        action="store_true",
        default=False,
    )
    parser_show_cfg.add_argument("anchorfile", help="the Otter OTF2 anchorfile to use")
    add_common_arguments(parser_show_cfg)

    # parse the action "show hier"
    parser_show_hier = subparse_action_show.add_parser(
        GraphType.HIER.value,
        help=description_show[GraphType.HIER],
        description=description_show[GraphType.HIER],
        formatter_class=formatter_class,
    )
    parser_show_hier.add_argument(
        "-o",
        "--out",
        dest="dotfile",
        metavar="dotfile",
        help="where to save the graph",
        default="hier.dot",
    )
    parser_show_hier.add_argument("anchorfile", help="the Otter OTF2 anchorfile to use")
    add_common_arguments(parser_show_hier)

    return parser


def parse():
    """Parse args for otter.main.select_action()"""

    parser = prepare_parser()
    args = parser.parse_args()
    return args


def print_help() -> None:
    """Print help for otter.main.select_action()"""

    parser = prepare_parser()
    parser.print_help()
