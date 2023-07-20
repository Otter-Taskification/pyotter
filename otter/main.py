from typing import Literal
import sys
from enum import StrEnum, auto
import argparse
import otter


class Action(StrEnum):
    UNPACK = auto()


actions = [action.value for action in Action]


def select_action() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", help="The action to perform.", choices=actions)
    argv = sys.argv[2:]
    args = parser.parse_args(sys.argv[1:2])
    if args.action == Action.UNPACK:
        do_unpack(argv)


def do_unpack(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(
        description="Unpack an Otter OTF2 trace and prepare it for querying by other Otter actions."
    )
    parser.add_argument("anchorfile", help="The Otter OTF2 anchorfile to unpack")
    parser.add_argument(
        "--loglevel",
        dest="loglevel",
        default="WARN",
        choices=["DEBUG", "INFO", "WARN", "ERROR"],
        help="logging level",
    )
    parser.add_argument(
        "--logdir", dest="logdir", default="otter-logs", help="logging directory"
    )
    args = parser.parse_args(argv)
    otter.log.initialise(args)
    project = otter.project.ReadTasksProject(
        args.anchorfile, debug=args.loglevel == "DEBUG"
    )
    project.create_db_from_trace()
    project.quit()
