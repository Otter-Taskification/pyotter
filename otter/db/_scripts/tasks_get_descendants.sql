-- Get all descendants of a task, including the task itself, upto some given depth
with descendant as (
    select id
        ,0 as depth
    from task
    where id in (0)
    union all
    select child_id as id
        ,depth+1 as depth
    from descendant, task_relation
    where descendant.id = task_relation.parent_id
)
select *
from descendant
where depth <= ?
;
