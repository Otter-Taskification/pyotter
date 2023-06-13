import otter
args = otter.args.get_args()
if args.profile:
    from cProfile import run
    print("Profiling...")
    run("otter.main(args)", filename=args.profile)
    print("Done profiling.")
else:
    otter.main(args)
