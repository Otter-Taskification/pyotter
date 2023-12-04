select rel.child_id
    ,crt.time as child_crt_ts
    ,sync.context_id
    ,ctx.sync_descendants
    ,ctx.sync_ts
    ,chunk.sequence
from task

-- get the child tasks of task.id
inner join task_relation as rel
    on task.id = rel.parent_id

left join task_history as crt
    on rel.child_id = crt.id
    and crt.action == 1 -- create

-- add the synchronisation of each child
left join synchronisation as sync
    on rel.child_id = sync.task_id

-- add the context for each synchronisation
left join context as ctx
    on sync.context_id = ctx.context_id

left join chunk
    on task.id = chunk.encountering_task_id
    and sync.context_id = chunk.context_id
where task.id in (
    1
)
order by 1
    -- when sequence is null, these are un-synchronised tasks, so sort them last
    -- (since in our event model, a synchronisation captures all tasks so far, 
    -- so any unsynchronised tasks can only appear after all synchronisation
    -- points)
    ,case when sequence is null then 1 else 0 end
    ,sequence
    ,child_id
;
