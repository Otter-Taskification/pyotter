import argparse

def get_args():

    parser = argparse.ArgumentParser(
        prog="python3 -m otter",
        description='Convert an Otter OTF2 trace archive to its execution graph representation',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('anchorfile', help='OTF2 anchor file')
    parser.add_argument('-o', '--output', dest='output', help='output file')
    parser.add_argument('-r', '--report', dest='report', help='report name')
    parser.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                        help='print chunks as they are generated')
    parser.add_argument('-i', '--interact', action='store_true', dest='interact',
                        help='drop to an interactive shell upon completion')
    parser.add_argument('-ns', '--no-style', action='store_true', default=False, dest='nostyle',
                        help='do not apply any styling to the graph nodes')
    parser.add_argument('-d', '--debug', action='store_true', default=False, dest='debug',
                        help='step through the code with pdb.set_trace()')
    args = parser.parse_args()

    if args.output is None and not args.interact:
        parser.error("must select at least one of -[o|i]")

    if args.interact:
        print("Otter launched interactively")

    return args
