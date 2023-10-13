"""Handle argument parsing"""

from __future__ import annotations

import argparse
from enum import Enum


class Action(str, Enum):
    """Defines the available actions"""

    UNPACK = "unpack"
    SHOW = "show"
    SUMMARY = "summary"
    FILTER = "filter"


class GraphType(str, Enum):
    """Defines the graph types available to do_show()"""

    CFG = "cfg"  # control-flow graph
    HIER = "hier"  # task hierarchy


description_action = {
    Action.UNPACK: "Unpack an Otter OTF2 trace and prepare it for querying by other Otter actions.",
    Action.SHOW: "Visualise a chosen task's graph or the task hierarchy.",
    Action.SUMMARY: "Print some summary information about the tasks database.",
    Action.FILTER: "Define a filter file based on the tasks recorded in a trace.",
}

extra_description_action = {
    Action.FILTER: """

Filter files contain one or more rules used by Otter to filter tasks at runtime.

A filter file is used to either include or exclude matching tasks at rumtime i.e.
the --include and --exclude options are mutually exclusive. Given a filter file
created with --include (or --exclude), Otter will record (or ignore) only those
tasks which match at least one of the rules in the filter file.

A rule is defined by the logical intersection of one or more key-value pairs. A
task satisfies a rule if it matches all the rule's key-value pairs.

Each occurrence of --include/--exclude defines a single rule in the filter file.

NOTE: tasks which satisfy a rule in a filter file do not propogate this to their
descendants i.e. to ignore a set of related tasks, define rules which will cover
all tasks to be ignored.

Accepted key-value pairs are:

    label=<label>
    init=<file>[:<line> | :<func>]
    start=<file>[:<line> | :<func>]
    end=<file>[:<line> | :<func>]

Where:

    <label> is the label of a task to match
    <file> is a source file recorded in the trace
    <line> is a line nuber
    <func> is the name of a function recorded in the trace

Example:

a rule excluding all tasks initialised at line 25 in src/main.cpp with the given
label:

    -e label="init step" init=src/main.cpp:25
"""
}


description_show = {
    GraphType.CFG: "show the control-flow graph of a chosen task",
    GraphType.HIER: "show the task hierarchy",
}

filter_keys = ["label", "init", "start", "end"]


def validate_filter_rule_pair(pair: str) -> str:
    """Validate a key-value pair for a filter rule"""

    if "=" not in pair:
        raise argparse.ArgumentTypeError(f'expected "=" in "{pair}"')

    # split at the first occurrence of "="
    split_at = pair.find("=")

    key = pair[0:split_at]

    if key not in filter_keys:
        raise argparse.ArgumentTypeError(
            f'invalid key "{key}" (must be one of {", ".join(filter_keys)})'
        )

    return pair


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add common arguments to a parser"""

    logging_levels = ["debug", "info", "warn", "error"]

    parser.add_argument(
        "--loglevel",
        dest="loglevel",
        default="warn",
        choices=logging_levels,
        help=f"logging level ({', '.join(logging_levels)})",
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

    parser.add_argument(
        "--pdb",
        action="store_true",
        help="enter interactive post-mortem with pdb if an exception is raised",
    )


def prepare_parser():
    """Prepare argument parser for otter.main.select_action()"""

    formatter_class = argparse.ArgumentDefaultsHelpFormatter
    parser = argparse.ArgumentParser(formatter_class=formatter_class, prog="otter")

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
        "--db-chunk-manager",
        help="select the DB chunk manager",
        dest="db_chunk_manager",
        action="store_true",
    )
    parse_action_unpack.add_argument(
        "anchorfile", help="the Otter OTF2 anchorfile to use"
    )
    add_common_arguments(parse_action_unpack)

    # parse the unpack action
    parse_action_summary = subparse_action.add_parser(
        Action.SUMMARY.value,
        help=description_action[Action.SUMMARY],
        description=description_action[Action.SUMMARY],
        formatter_class=formatter_class,
    )
    parse_action_summary.add_argument(
        "--source",
        action="store_true",
        help="list the source locations recorded in the trace",
    )
    parse_action_summary.add_argument(
        "--tasks",
        action="store_true",
        help="list the tasks recorded in the trace",
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

    # parse the filter action
    parse_action_filter = subparse_action.add_parser(
        Action.FILTER.value,
        help=description_action[Action.FILTER],
        description=description_action[Action.FILTER]
        + extra_description_action[Action.FILTER],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    filter_group = parse_action_filter.add_mutually_exclusive_group(required=True)
    filter_group.add_argument(
        "-e",
        "--exclude",
        help="exclude tasks defined by a set of key-value pairs",
        type=validate_filter_rule_pair,
        action="append",
        metavar="key=value",
        nargs="+",
    )
    filter_group.add_argument(
        "-i",
        "--include",
        help="include tasks defined by a set of key-value pairs",
        type=validate_filter_rule_pair,
        action="append",
        metavar="key=value",
        nargs="+",
    )
    add_common_arguments(parse_action_filter)

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
