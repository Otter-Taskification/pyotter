def get_args():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python3 -m otter",
        description='Convert an Otter OTF2 trace archive to its execution graph representation',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('anchorfile', help='OTF2 anchor file')
    parser.add_argument('-r', '--report', dest='report', help='directory where report output should be saved. If the specified folder already exists, will fail unless --force also specified.')
    parser.add_argument('-f', '--force', dest='force', action='store_true', default=False, help='overwrite the report directory if it already exists')
    parser.add_argument('-i', '--interact', action='store_true', dest='interact',
                        help='drop to an interactive shell upon completion')
    parser.add_argument('--loglevel', dest='loglevel', default="WARN", choices=["DEBUG", "INFO", "WARN", "ERROR"], help='logging level')
    parser.add_argument('--logdir', dest='logdir', default="otter-logs", help='logging directory')
    parser.add_argument('--profile', dest='profile', help='profiling output')
    parser.add_argument('--project', dest='project', help='project type', choices=["SimpleProject", "DBProject", "ReadTasksProject"], default="SimpleProject")
    parser.add_argument('--warn-deprecated', dest='warnings', help='Allow warnings about deprecated code', action="append_const", const=DeprecationWarning)
    parser.add_argument('--warn-user', dest='warnings', help='Allow user warnings', action="append_const", const=UserWarning)
    parser.add_argument('--warn-all', dest='warn_all', help='Turn on all warnings', action="store_true")
    args = parser.parse_args()

    # if args.report is None:
        # parser.error("must specify the report output path with --report")
        # args.report = "otter-report"

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

    if args.warn_all:
        args.warnings = [cls for cls in Warning.__subclasses__()]
    elif args.warnings is None:
        args.warnings = list()

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

    # log dir must exist
    if not os.path.isdir(args.logdir):
        os.mkdir(args.logdir)

    return
