from sqlite3 import Cursor
from typing import Tuple

child_sync_points = """
select rel.child_id
    ,sync.context_id
    ,ctx.sync_descendants
    ,chunk.sequence
    ,attr.task_label
from task
inner join task_relation as rel
    on task.id = rel.parent_id
left join task_attributes as attr
    on rel.child_id = attr.id
left join synchronisation as sync
    on rel.child_id = sync.task_id
left join context as ctx
    on sync.context_id = ctx.context_id
left join chunk
    on task.id = chunk.encountering_task_id
    and sync.context_id = chunk.context_id
where task.id in (
    ?
)
order by task.id
    ,sequence
;
"""
