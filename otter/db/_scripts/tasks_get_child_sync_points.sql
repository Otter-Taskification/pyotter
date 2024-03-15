select rel.child_id
    ,crt.time as child_crt_ts
    ,sync.context_id
    ,ctx.sync_descendants
    ,ctx.sync_ts
	,chunk.sequence
    ,cast(crt.time as int) - cast(events_dt.start_ts as int) as child_crt_dt
	,events_dt.start_ts
	,events_dt.end_ts
    ,cast(events_dt.end_ts as int) - cast(events_dt.start_ts as int) as chunk_duration
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

left join (

	with events as (
		select *
			,ROW_NUMBER() over (order by id, time) as row_number
		from task_history
		where action in (2, 3, 4, 5)
			and id in (?)
		order by id, time
	)

	select events_left.id
		,events_left.action as parent_action_start
		,events_right.action as parent_action_end
		,events_left.time as start_ts
		,events_right.time as end_ts

    from events as events_left

	inner join events as events_right
		on events_left.id = events_right.id
		and events_left.row_number = events_right.row_number-1

) as events_dt
-- V1: match sync time exactly to the time the parent was suspended
-- 	on task.id = events_dt.id
-- 	and ctx.sync_ts = events_dt.end_ts

-- V2: match child crt_ts to between the start & end of a part of the task (to also catch unsynchronised tasks)
	on task.id = events_dt.id
	and crt.time >= events_dt.start_ts
	and crt.time <= events_dt.end_ts
	
where task.id in (
    ?
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
