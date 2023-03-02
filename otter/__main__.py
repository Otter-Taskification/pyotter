import otter
args = otter.utils.get_args()
if args.profile:
    from cProfile import run
    print("Profiling...")
    run("otter.run()", filename=args.profile)
    print("Done profiling.")
else:
    otter.run()
