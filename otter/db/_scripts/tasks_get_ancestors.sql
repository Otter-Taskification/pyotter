-- Get all ancestors of a task, including the task itself
with ancestor as (
    select id
        ,0 as depth
    from task
    where id in (5013)
    union all
    select parent_id as id
        ,depth+1 as depth
    from ancestor, task_relation
    where ancestor.id = task_relation.child_id
)
select *
from ancestor
order by depth
;
