import otf2
from typing import Iterable
from collections import deque, defaultdict
from otter.definitions import EventType, RegionType, TaskStatus, TaskType, Endpoint, EdgeType
from otter.trace import AttributeLookup, event_defines_new_chunk
from otf2.events import Enter, Leave, ThreadTaskCreate, ThreadTaskSwitch, ThreadTaskComplete, ThreadBegin, ThreadEnd

class Chunk:
    """A sequence of events delineated by events at which the execution flow may diverge. Contains a reference to an
    AttributeLookup for looking up event attributes."""

    def __init__(self, events: Iterable, attr: AttributeLookup):
        self.attr = attr
        self.events = deque(events)

    def __repr__(self):
        s = "  {:18s} {:10s} {:20s} {:20s} {:18s} {}\n".format("Time", "Endpoint", "Region Type", "Event Type", "Region ID/Name", "Encountering Task")
        for e in self.events:
            try:
                s += "  {:<18d} {:10s} {:20s} {:20s} {:18s} {}\n".format(
                    e.time,
                    e.attributes[self.attr['endpoint']],
                    e.attributes.get(self.attr['region_type'], ""),
                    e.attributes[self.attr['event_type']],
                    str(e.attributes[self.attr['unique_id']]) if self.attr['unique_id'] in e.attributes else e.region.name,
                    e.attributes[self.attr['encountering_task_id']])
            except KeyError as err:
                print(err)
                raise
        return s

    def append(self, item):
        if type(item) is Chunk:
            raise TypeError()
        self.events.append(item)

    @property
    def first(self):
        return None if len(self.events) == 0 else self.events[0]

    @property
    def last(self):
        return None if len(self.events) == 0 else self.events[-1]

    def __len__(self):
        return len(self.events)

    @property
    def len(self):
        return len(self.events)

    @property
    def kind(self):
        return None if len(self.events) == 0 else event.attributes[self.events[0].attr['region_type']]


def event_defines_new_task_fragment(e: otf2.events._EventMeta, a: AttributeLookup) -> bool:
    return (
        (type(e) in [ThreadTaskSwitch]) or
        (type(e) in [Enter, Leave] and e.attributes.get(a['region_type'], None) in
            [RegionType.parallel, RegionType.initial_task, RegionType.implicit_task, RegionType.single_executor, RegionType.master])
    )
    # return type(e) in [Enter, Leave, ThreadTaskSwitch]
    # return (e.attributes.get(a['region_type'], None) in ['parallel',
    #     'explicit_task', 'initial_task', 'single_executor', 'master'])

class ChunkGenerator:
    """Yields a sequence of Chunks by consuming the sequence of events in a trace."""

    def __init__(self, trace, key: str = None):
        self.events = list(trace.events)
        self.attr = AttributeLookup(trace.definitions.attributes)
        self.key = key or "encountering_task_id"
        self._chunk_dict = defaultdict(lambda : Chunk(list(), self.attr))

    def __getitem__(self, item):
        return self._chunk_dict[item]

    def __setitem__(self, key, value):
        if type(value) is not Chunk:
            raise TypeError()
        # del self._chunk_dict[key]
        self._chunk_dict[key] = value

    def keys(self):
        return self._chunk_dict.keys()

    def __iter__(self):
    # def yield_chunks(self):
        chunk_stack = defaultdict(deque)
        enclosing_task = dict()
        print("yielding chunks:", end="\n", flush=True)
        nChunks = 0
        for location, event in self.events:
            if type(event) in [ThreadBegin, ThreadEnd]:
                continue
            try:
                encountering_task = event.attributes[self.attr['encountering_task_id']]
            except KeyError:
                print(event)
                print(event.attributes)
                raise
            chunk_key = encountering_task
            if event_defines_new_task_fragment(event, self.attr):
                region_type = event.attributes.get(self.attr['region_type'], "")
                if isinstance(event, Enter):
                    # self[encountering_task].append(event)
                    if region_type in [RegionType.initial_task]:
                        task_entered = event.attributes[self.attr['unique_id']]
                        self[task_entered].append(event)
                        # chunk_stack[task_entered].append(self[encountering_task])
                    else:
                        self[encountering_task].append(event)
                        # New chunk due to enter event
                        if region_type in [RegionType.implicit_task]:
                            task_entered = event.attributes[self.attr['unique_id']]
                            self[task_entered].append(event)
                        else:
                            chunk_stack[encountering_task].append(self[encountering_task])
                            self[encountering_task] = Chunk((event,), self.attr)
                elif isinstance(event, Leave):
                    # self[encountering_task].append(event)
                    nChunks += 1
                    if nChunks % 100 == 0:
                        if nChunks % 1000 == 0:
                            print("yielding chunks:", end=" ", flush=True)
                        print(f"{nChunks:4d}", end="", flush=True)
                    elif nChunks % 20 == 0:
                        print(".", end="", flush=True)
                    if (nChunks+1) % 1000 == 0:
                        print("", flush=True)
                    print(f">> Yielding chunk:")
                    print(self[encountering_task])
                    yield self[encountering_task]
                    if region_type in [RegionType.initial_task]:
                        task_left = event.attributes[self.attr['unique_id']]
                        self[task_left].append(event)
                        print(f">> Yielding chunk:")
                        print(self[task_left])
                        yield self[task_left]
                        # self[encountering_task] = chunk_stack[task_left].pop()
                    elif region_type in [RegionType.implicit_task]:
                        task_left = event.attributes[self.attr['unique_id']]
                        self[task_left].append(event)
                        self[encountering_task].append(event)
                    else:
                        # Continue with enclosing chunk, if there is one
                        self[encountering_task].append(event)
                        print(f">> Yielding chunk:")
                        print(self[encountering_task])
                        yield self[encountering_task]
                        try:
                            self[encountering_task] = chunk_stack[encountering_task].pop()
                            self[encountering_task].append(event)
                        except IndexError as err:
                            print(f"Error: {str(err)}")
                            print("Error when processing this event:")
                            print(fmt_event(event, self.attr))
                elif isinstance(event, ThreadTaskSwitch):
                    prior_task_status = event.attributes[self.attr['prior_task_status']]
                    prior_task_id = event.attributes[self.attr['prior_task_id']]
                    next_task_id = event.attributes[self.attr['next_task_id']]
                    self[encountering_task].append(event)
                    self[next_task_id].append(event)
                    if prior_task_status in [TaskStatus.complete]:
                        print(f">> Yielding chunk (task {prior_task_id} complete):")
                        print(self[encountering_task])
                        yield self[encountering_task]
                else:
                    self[encountering_task].append(event)
            else:
                # Append event to current chunk for this location
                self[chunk_key].append(event)
        print("")

def fmt_event(e, attr):
    s = ""
    try:
        s += "  {:<18d} {:10s} {:20s} {:20s} {:18s} {}".format(
            e.time,
            e.attributes[attr['endpoint']],
            e.attributes.get(attr['region_type'], ""),
            e.attributes[attr['event_type']],
            str(e.attributes[attr['unique_id']]) if attr['unique_id'] in e.attributes else e.region.name,
            e.attributes[attr['encountering_task_id']])
    except KeyError as err:
        print(err)
        print(type(e))
    return s