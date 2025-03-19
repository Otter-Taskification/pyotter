-- Get task creation events
with events as (
	select *
	from task_history
	where task_history.id in ({placeholder})
    and task_history.action = 1 -- create
)
select events.id
	,events.action
	,src.file_name
	,src.func_name
	,src.line
	,events.time - {since}
    ,events.cpu
    ,events.tid
from events
left join source_location as src
    on events.source_location_id = src.src_loc_id
order by events.id
	,events.time
;
