import atexit
import code
import os
import readline

def interact(locals, g):

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
