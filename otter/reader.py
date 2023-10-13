from __future__ import annotations

from contextlib import closing

from otter.otf2_ext.reader import OTF2Reader

def get_otf2_reader(*args, **kwargs) -> closing[OTF2Reader]:
    # allows _OTF2Reader to be used in a with-block
    return closing(OTF2Reader(*args, **kwargs))
