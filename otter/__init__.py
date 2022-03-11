"""
Key functionality:

- load OTF2 trace and extract locations, events, regions, attributes so they can be easily looked up
- parse loaded trace to determine what graph nodes to create and where edges are required
- export execution graph to file
"""

from .args import get_args

from shutil import which

# Check that the "dot" commandline utility is available
if which("dot") is None:
    print(f"Error: {__name__} couldn't find the graphviz command line utility \"dot\" (see https://graphviz.org/download/).")
    print("Please install graphviz before continuing.")
    quit()