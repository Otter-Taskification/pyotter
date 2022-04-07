def get_args():
    import argparse

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

    # Ensure report path is normalised
    if args.report is not None:
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

    # log dir must be normalised
    if not os.path.isabs(args.logdir):
        args.logdir = os.path.join(os.getcwd(), args.logdir)
    args.logdir = os.path.normpath(args.logdir)

    return
