import argparse
import os

def get_args():

    parser = argparse.ArgumentParser(
        prog="python3 -m otter",
        description='Convert an Otter OTF2 trace archive to its execution graph representation',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('anchorfile', help='OTF2 anchor file')
    parser.add_argument('-o', '--output', dest='output', help='output file')
    parser.add_argument('-r', '--report', dest='report', help='report path')
    parser.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                        help='print chunks as they are generated')
    parser.add_argument('-i', '--interact', action='store_true', dest='interact',
                        help='drop to an interactive shell upon completion')
    parser.add_argument('-ns', '--no-style', action='store_true', default=False, dest='nostyle',
                        help='do not apply any styling to the graph nodes')
    parser.add_argument('-d', '--debug', action='store_true', default=False, dest='debug',
                        help='step through the code with pdb.set_trace()')
    args = parser.parse_args()

    if args.output is None and args.report is None and not args.interact:
        parser.error("must select at least one of -[o|i|r]")

    if args.interact:
        print("Otter launched interactively")

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

    # Ensure report path is normalised
    if not os.path.isabs(args.report):
        args.report = os.path.join(os.getcwd(), args.report)
    args.report = os.path.normpath(args.report)

    # Anchorfile must exist
    if not os.path.isfile(args.anchorfile):
        raise FileNotFoundError(args.anchorfile)

    # Report directory must exist
    if args.report is not None and not os.path.isdir(os.path.dirname(args.report)):
        raise FileNotFoundError(os.path.dirname(args.report))

    # Report path must not exist
    if args.report is not None and os.path.isdir(args.report):
        raise FileExistsError(f"{args.report}")

    return