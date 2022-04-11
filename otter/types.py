from typing import Union, Dict, Any
from pathlib import Path
import otf2

# Alias some OTF2 types
OTF2Event = Union[otf2.events._Event]
OTF2Reader = Union[otf2.reader.Reader]
OTF2Location = Union[otf2.definitions.Location]
OTF2DefinitionsRegistry = Union[otf2.registry.DefinitionRegistry]
OTF2Attribute = Union[Any]

# Alias some compound types
Path = Union[str, Path]
AttrDict = Dict[str, OTF2Attribute]
