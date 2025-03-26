from typing import Optional, Callable, Mapping, Tuple, Any, List, Iterable
from functools import partial, lru_cache
from collections import Counter
from itertools import count, chain

import pandas as pd
from matplotlib import pyplot as plt

import otter

from otter.db.types import Task, TaskSchedulingState
from otter.db import ReadConnection
from otter.definitions import TaskAction, TaskID
from otter.reporting import colour_picker
from otter.utils import batched

ColourMap = Mapping[Any, Tuple[float, float, float]]
ColourGetter = Callable[[Any], Tuple[float, float, float]]

MAX_QUERY_PARAMS = 50000

COLOUR_WHITE = (1, 1, 1)
COLOUR_BLACK = (0, 0, 0)
COLOUR_LGREY = (0.88,) * 3
COLOUR_GREY = (0.55,) * 3
COLOUR_DGREY = (0.20,) * 3
COLOUR_RED = (1, 0, 0)
COLOUR_YELLOW = (1, 1, 0)
COLOUR_BLUE = (0, 0, 1)
COLOUR_MAGENTA = (1, 0, 1)
COLOUR_DARK_GREEN = (0.29, 0.404, 0.255)
COLOUR_ORANGE = (1, 0.643, 0.125)
TIME_SCALE_FACTOR = 1_000_000_000

ALPHA_FULL = 1.00
ALPHA_DEFAULT = 0.85
ALPHA_MEDIUM = 0.55
ALPHA_LIGHT = 0.25
ALPHA_NOSHOW = 0.00


def make_colour_getter(colours: ColourMap):

    @lru_cache(maxsize=256)
    def getter(arg: Any):
        colour = colours[arg]
        otter.log.debug(f"get colour for {arg=} -> {colour}")
        return colours[arg]

    return getter


@lru_cache(maxsize=256)
def get_demangled_label(label: str):
    result = otter.utils.demangle.demangle([label])[0]
    otter.log.debug(f"demangled {label=}, {result=}")
    return result


def is_root_task(task: Task):
    return task.id == 0


def is_phase_task(task: Task):
    return task.attr.label.startswith("OTTER PHASE")


def is_leaf_task(task: Task):
    return task.children == 0


def is_task_create(state: TaskSchedulingState):
    return state.action_start == TaskAction.CREATE


def get_phase_colour(state: TaskSchedulingState, reader: ReadConnection, get_colour: ColourGetter):
    if is_phase_task(reader.get_task(state.task)):
        if state.is_active:
            return ((1, 1, 1), COLOUR_RED, ALPHA_FULL)
        return ((1, 1, 1), COLOUR_RED, ALPHA_FULL)
    return ((0, 0, 0), (0, 0, 0), ALPHA_NOSHOW)


def get_phase_plotting_data(state: TaskSchedulingState, **kwargs):
    face, edge, alpha = get_phase_colour(state, **kwargs)
    base = {
        "xrange": (state.start_ts / TIME_SCALE_FACTOR, state.duration / TIME_SCALE_FACTOR),
        "facecolor": face,
        "edgecolor": edge,
        "alpha": alpha,
    }
    return base


def make_yaxis_key(state: TaskSchedulingState, reader: ReadConnection, **kwargs) -> Any:
    return (state.tid_start, get_demangled_label(reader.get_task_label(state.task)))


def get_state_colour(
    state: TaskSchedulingState,
    pred: Callable[[Task], bool],
    reader: ReadConnection,
    get_colour: ColourGetter,
):
    task = reader.get_task(state.task)
    if is_root_task(task) or is_phase_task(task):
        return (COLOUR_RED, COLOUR_BLACK, ALPHA_FULL)
    if pred(task):
        if state.is_active:
            return (COLOUR_YELLOW, COLOUR_RED, ALPHA_FULL)
        return (COLOUR_RED, COLOUR_RED, ALPHA_FULL)
    if state.is_active:
        return (get_colour(task.attr.label), COLOUR_DGREY, ALPHA_MEDIUM)
    if state.action_start == TaskAction.CREATE:
        return (COLOUR_BLUE, COLOUR_DGREY, ALPHA_NOSHOW)
    if state.action_start == TaskAction.SUSPEND:
        return (COLOUR_RED, COLOUR_DGREY, ALPHA_NOSHOW)
    otter.log.debug(f"no colour for {state=}")
    return (COLOUR_MAGENTA, COLOUR_BLACK, ALPHA_FULL)


def get_state_plotting_data(state, **kwargs):
    face, edge, alpha = get_state_colour(state, **kwargs)
    base = {
        "xrange": (state.start_ts / TIME_SCALE_FACTOR, state.duration / TIME_SCALE_FACTOR),
        "ykey": make_yaxis_key(state, **kwargs),
        "facecolor": face,
        "edgecolor": edge,
        "alpha": alpha,
    }
    return base


def get_task_crt_plotting_data(state, **kwargs):
    base = {
        "x": state.start_ts / TIME_SCALE_FACTOR,
        "ykey": make_yaxis_key(state, **kwargs),
    }
    return base


def partition_tasks(tasks: Iterable[TaskID], pred: Callable[[TaskID], bool]):
    accept: List[TaskID] = []
    reject: List[TaskID] = []
    for task in tasks:
        if pred(task):
            accept.append(task)
        else:
            reject.append(task)
    return accept, reject


def get_scheduling_states(reader: ReadConnection, task: TaskID):
    tasks = reader.get_related_tasks(task, relation="descendants")
    states = reader.get_task_scheduling_states(tasks)
    return states


def plot_scheduling_data(
    anchorfile: str,
    /,
    *,
    task: Optional[TaskID],
    title: Optional[str] = None,
    sim_id: Optional[int] = None,
    do_format: bool = True,
    y_extent=0.85,
    y_bias=0.00,
):
    otter.log.info(f"plotting from anchorfile: {anchorfile}")

    title = title or f"Scheduling Data (trace={anchorfile})"
    reader = otter.project.ReadTraceData(anchorfile).connect()
    get_colour = make_colour_getter(colour_picker())

    #! A temporary hack here
    otter.log.debug("get critical tasks")
    critical_tasks = list(
        item[0]
        for item in reader._con.execute(
            "select distinct critical_child from critical_task where sim_id = 0 order by critical_child"
        )
    )

    def is_critical_task(task: Task):
        return task.id in critical_tasks

    root_task = reader.get_root_task()
    if task is None:
        task = root_task

    otter.log.debug(f"plot task {task} and descendants")
    otter.log.debug(f"{reader.get_task(task)}")

    task_coll = reader.get_related_tasks(task, relation="descendants")
    if task != root_task or True:
        task_coll.append(task)
    phase_tasks, other_tasks = partition_tasks(
        task_coll, pred=lambda task: reader.get_task_label(task).startswith("OTTER PHASE")
    )

    otter.log.debug("get phase tasks' scheduling states")
    phase_sched = reader.get_task_scheduling_states(phase_tasks, sim_id=sim_id)
    phase_sched_df = pd.DataFrame(
        map(partial(get_phase_plotting_data, reader=reader, get_colour=get_colour), phase_sched)
    )
    print_phase_scheduling_data(reader, phase_sched)

    otter.log.debug(f"get non-phase tasks' scheduling states ({len(other_tasks)} tasks)")
    scheduling_states: List[TaskSchedulingState]
    if len(other_tasks) < MAX_QUERY_PARAMS:
        scheduling_states = reader.get_task_scheduling_states(other_tasks, sim_id=sim_id)
    else:
        otter.log.warning(f"get task scheduling states in batches of {MAX_QUERY_PARAMS})")
        scheduling_batches: List[List[TaskSchedulingState]] = []
        num_batches = 0
        for n, batch in enumerate(batched(other_tasks, batch_size=MAX_QUERY_PARAMS), start=1):
            otter.log.warning(f"... prepare batch {n}")
            scheduling_batches.append(reader.get_task_scheduling_states(list(batch), sim_id=sim_id))
            num_batches += 1
        otter.log.warning(f"flatten {len(other_tasks)} tasks into a single list")
        scheduling_states = list(chain(*scheduling_batches))
        otter.log.warning(f"got all {len(other_tasks)} tasks in {num_batches} batches")
    data_getter = partial(
        get_state_plotting_data, reader=reader, pred=lambda _: False, get_colour=get_colour
    )
    state_df = pd.DataFrame(map(data_getter, scheduling_states))
    task_crt_getter = partial(
        get_task_crt_plotting_data, reader=reader, pred=is_critical_task, get_colour=get_colour
    )

    otter.log.debug("build task-create scheduling states dataframe")
    task_crt_df = pd.DataFrame(map(task_crt_getter, filter(is_task_create, scheduling_states)))

    fig, ax = plt.subplots()
    ykeys = list(set(state_df["ykey"]))
    ykeys.sort()
    ymax = len(ykeys)

    otter.log.debug("add data to figure")

    # plot the phase data
    if phase_sched_df.empty:
        otter.log.warning("no phase tasks were found to plot")
    else:
        ax.broken_barh(
            xranges=phase_sched_df["xrange"],
            yrange=(0, ymax),
            facecolors=phase_sched_df["facecolor"],
            edgecolor=phase_sched_df["edgecolor"],
            alpha=phase_sched_df["alpha"],
        )

    # plot the regular task data
    otter.log.debug("plotting task scheduling data")
    for ytick, ykey in enumerate(ykeys):
        otter.log.debug(f" -- {ytick=}, {ykey=}")
        rows = state_df[state_df["ykey"] == ykey]
        ax.broken_barh(
            xranges=rows["xrange"],
            yrange=(ytick + ((1 - y_extent) / 2) + y_bias, y_extent),
            facecolors=rows["facecolor"],
            edgecolor=rows["edgecolor"],
            alpha=rows["alpha"],
        )
        crt_rows = task_crt_df[task_crt_df["ykey"] == ykey]
        ax.plot(crt_rows["x"], [ytick + 0.5 + y_bias] * len(crt_rows.index), "k.")

    if not do_format:
        otter.log.debug("not formatting")
        return (
            plt.show,
            fig,
            ax,
            ykeys,
            (scheduling_states, state_df),
            (phase_sched, phase_sched_df),
        )

    otter.log.debug("formatting figure")

    ax.set_yticks([x + 0.5 for x in range(len(ykeys))])
    ylabel_depth = set(map(len, ykeys))
    if len(ylabel_depth) != 1:
        otter.log.warning(f"multiple y-label group sizes found: {ylabel_depth}")
        ylabels = [str(k[-1]) for k in ykeys]
    else:
        ylabel_grp_sz = ylabel_depth.pop()
        assert not ylabel_depth
        otter.log.debug(f"y-label group size: {ylabel_grp_sz}")
        ylabel_groups = list(zip(*ykeys))
        ylabel_groups.reverse()
        for offset, ygroup in enumerate(ylabel_groups[1:], start=1):

            # Make labels
            ygroup_labels = list(map(str, dict(zip(ygroup, count())).keys()))

            # Axis for lines between groups
            span = Counter(ygroup)
            posts = [0]
            for v in span.values():
                posts.append(posts[-1] + v)
            midpoints = [(a + b) / 2 for (a, b) in zip(posts[0:-1], posts[1:])]

            # Axis for group labels
            y2 = ax.secondary_yaxis(location=1)
            y2.yaxis.set_label_position("right")
            y2.yaxis.tick_right()
            y2.set_yticks(midpoints)
            y2.set_yticklabels([], fontsize=18)
            y2.tick_params("y", length=0)
            y2.spines["left"].set_linewidth(0)

            # Bars between groups
            y3 = ax.secondary_yaxis(location=1)
            y3.yaxis.set_label_position("right")
            y3.yaxis.tick_right()
            y3.set_yticks(posts, labels=[])
            y3.tick_params("y", length=75, width=1)

            otter.log.debug(f"axis labelling data for group {offset}:")
            otter.log.debug(f"  {ygroup=}")
            otter.log.debug(f"  {ygroup_labels=}")
            otter.log.debug(f"  {midpoints=}")
            otter.log.debug(f"  {posts=}")
            otter.log.debug(f"  {span=}")

        otter.log.debug(str(ylabel_groups))

    ylabels = [str(k[-1]) for k in ykeys]
    ax.yaxis.set_label_position("right")
    ax.yaxis.tick_right()
    ax.set_yticklabels(ylabels, fontsize=16)
    plt.gca().invert_yaxis()
    otter.log.debug(f"set {title=}")
    plt.title(title, fontsize=20)
    plt.xlabel("Time", fontsize=18)
    plt.show()


def print_phase_scheduling_data(reader: ReadConnection, data: List[TaskSchedulingState]):
    for s in data:
        if s.action_start == TaskAction.CREATE:
            continue
        if s.action_start == TaskAction.START:
            print(f"  PHASE ID: {s.task:>8d} [label: {reader.get_task_label(s.task)}]")
            print(
                f"  {'TIME':<15s} | {' DURATION':<15s} | {' CHILDREN':<9s} | {' DESC.':<9s} | {' ACTION':<9s} | {' LOCATION':<14s}"
            )
            print("=" * 80)
        children = reader.get_children_created_between(s.task, s.start_ts, s.end_ts)
        num_children = len(children)
        num_descendants = 0
        for child, _ in children:
            desc = reader.get_related_tasks(child, relation="descendants")
            num_descendants += len(desc)
        print(
            f"{s.start_ts:>17,d} | {s.duration:>15,d} | {num_children:>9,d} | {num_descendants:>9,d} | {s.action_start.name:<9s} | {s.start_location}"
        )
        if s.action_end == TaskAction.END:
            print(
                f"{s.end_ts:>17,d} | {'-':>15s} | {'-':>9s} | {'-':>9s} | {s.action_end.name:<9s} | {s.end_location}"
            )
            print()
