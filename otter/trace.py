import re
import typing as T
import igraph as ig
import otf2
from itertools import chain
from collections import defaultdict, deque
from otf2.events import Enter, Leave, ThreadTaskCreate
from otter.helpers import attr_getter, events_bridge_region

class DefinitionLookup:

    def __init__(self, registry: otf2.registry._RefRegistry):
        self._lookup = dict()
        for d in registry:
            if d.name in self._lookup:
                raise KeyError("{} already present".format(d.name))
            self._lookup[d.name] = d

    def __getitem__(self, name):
        if name in self._lookup:
            return self._lookup[name]
        else:
            raise AttributeError(name)

    def __iter__(self):
        return ((k,v) for k, v in self._lookup.items())

    def keys(self):
        return self._lookup.keys()

    def values(self):
        return self._lookup.values()

    def items(self):
        return self.__iter__()


class AttributeLookup(DefinitionLookup):

    def __init__(self, attributes: otf2.registry._RefRegistry):
        if not isinstance(attributes[0], otf2.definitions.Attribute):
            raise TypeError(type(attributes[0]))
        super().__init__(attributes)

    def __repr__(self):
        s = "{:24s} {:12s} {}\n".format("Name", "Type", "Description")
        format = lambda k, v: "{:24s} {:12s} {}".format(k, str(v.type).split(".")[1], v.description)
        return s+"\n".join([format(k, v) for k,v in self._lookup.items()])


class LocationLookup(DefinitionLookup):

    def __init__(self, locations: otf2.registry._RefRegistry):
        if not isinstance(locations[0], otf2.definitions.Location):
            raise TypeError(type(locations[0]))
        super().__init__(locations)

    def __repr__(self):
        s = "{:12s} {:12s} {:12s} {}\n".format("Group", "Name", "Type", "Events")
        format = lambda v: "{:12s} {:12s} {:12s} {}".format(v.group.name, v.name, str(v.type).split(".")[1], v.number_of_events)
        return s+"\n".join([format(v) for k,v in self._lookup.items()])


class RegionLookup(DefinitionLookup):

    def __init__(self, regions: otf2.registry._RefRegistry):
        self._lookup = dict()
        for r in regions:
            ref = int(re.search(r'\d+', repr(r))[0])
            if ref in self._lookup:
                raise KeyError("{} already present".format(ref))
            self._lookup[ref] = r

    def __repr__(self):
        minref, maxref = min(self._lookup.keys()), max(self._lookup.keys())
        s = "{:3s} {:18s} {}\n".format("Ref", "Name", "Role")
        format_item = lambda l, k: "{:3d} {:18s} {}".format(k, l[k].name, str(l[k].region_role).split(".")[1])
        return s + "\n".join([format_item(self._lookup, i) for i in range(minref, maxref+1)])
