"""Run the main Otter entrypoint"""

import otter

if __name__ == "__main__":
    # args = otter.args.get_args()
    # if args.profile:
    #     print("Profiling...")
    #     cProfile.run("run(args)", filename=args.profile)
    #     print("Done profiling.")
    # else:
    #     run(args)
    otter.main.select_action()
