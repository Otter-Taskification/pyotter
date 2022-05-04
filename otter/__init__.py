from .core import EventFactory, events, TaskRegistry, ChunkFactory
from . import styling, reporting
from .definitions import Attr

# Check that the "dot" commandline utility is available
from shutil import which
if which("dot") is None:
    print(f"Error: {__name__} couldn't find the graphviz command line utility \"dot\" (see https://graphviz.org/download/).")
    print("Please install graphviz before continuing.")
    quit()

def get_args():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python3 -m otter",
        description='Convert an Otter OTF2 trace archive to its execution graph representation',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('anchorfile', help='OTF2 anchor file')
    parser.add_argument('-o', '--output', dest='output', help='output file')
    parser.add_argument('-r', '--report', dest='report', help='report path')
    parser.add_argument('-f', '--force', dest='force', action='store_true', default=False, help='force overwrite if files already exist')
    parser.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                        help='print chunks as they are generated')
    parser.add_argument('-i', '--interact', action='store_true', dest='interact',
                        help='drop to an interactive shell upon completion')
    parser.add_argument('-ns', '--no-style', action='store_true', default=False, dest='nostyle',
                        help='do not apply any styling to the graph nodes')
    parser.add_argument('-d', '--debug', action='store_true', default=False, dest='debug',
                        help='step through the code with pdb.set_trace()')
    parser.add_argument('--loglevel', dest='loglevel', default="WARN", choices=["DEBUG", "INFO", "WARN", "ERROR"], help='log level')
    parser.add_argument('--logdir', dest='logdir', default=".", help='log directory')
    args = parser.parse_args()

    if args.output is None and args.report is None and not args.interact:
        parser.error("must select at least one of -[o|i|r]")

    try:
        check_args(args)
    except FileNotFoundError as E:
        print(f"File not found: {E}")
        quit()
    except FileExistsError as E:
        print(f"File already exists: {E}")
        quit()

    return args

def check_args(args):
    import os

    # Anchorfile must exist
    if not os.path.isfile(args.anchorfile):
        raise FileNotFoundError(args.anchorfile)
    args.anchorfile = os.path.abspath(args.anchorfile)


    if args.report is not None:

        # Ensure report path is normalised
        if not os.path.isabs(args.report):
            args.report = os.path.join(os.getcwd(), args.report)
        args.report = os.path.normpath(args.report)

        # Report parent directory must exist
        parent = os.path.dirname(args.report)
        if not os.path.isdir(parent):
            raise FileNotFoundError(os.path.dirname(args.report))

        # Report path must not exist, unless overridden with --force
        if os.path.isdir(args.report) and not args.force:
            raise FileExistsError(f"{args.report}")

    # log dir must be normalised
    if not os.path.isabs(args.logdir):
        args.logdir = os.path.join(os.getcwd(), args.logdir)
    args.logdir = os.path.normpath(args.logdir)

    return

def interact(locals, g):
    import os
    import code
    import atexit
    import readline

    readline.parse_and_bind("tab: complete")

    hfile = os.path.join(os.path.expanduser("~"), ".otter_history")

    try:
        readline.read_history_file(hfile)
        numlines = readline.get_current_history_length()
    except FileNotFoundError:
        open(hfile, 'wb').close()
        numlines = 0

    atexit.register(append_history, numlines, hfile)

    k = ""
    for k, v in locals.items():
        if g is v:
            break

    banner = f"""
Graph {k} has {g.vcount()} nodes and {g.ecount()} edges

Entering interactive mode...
    """

    console = code.InteractiveConsole(locals=locals)
    console.interact(banner=banner, exitmsg=f"history saved to {hfile}")


def append_history(lines, file):
    newlines = readline.get_current_history_length()
    readline.set_history_length(1000)
    readline.append_history_file(newlines - lines, file)
