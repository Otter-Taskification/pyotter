select rel.child_id as id
	,crt.time - {start_ts} as crt_ts
from task_relation as rel
inner join task_history as crt
	on rel.child_id = crt.id
	and crt.action = 1
	and crt.time >= {start_ts}
	and crt.time <= {end_ts}
where rel.parent_id in (?)
order by id
;
