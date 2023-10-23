from __future__ import annotations

from dataclasses import dataclass

from ..definitions import SourceLocation


@dataclass
class Task:
    id: int
    parent_id: int
    task_flavour: int
    task_label: str
    crt_ts: int
    init_location: SourceLocation
