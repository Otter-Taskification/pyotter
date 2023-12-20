-- Get all descendants of a task
with descendant as (
    select child_id as id
    from task_relation
    where parent_id in (?)
    union all
    select rel.child_id as id
    from descendant
    inner join task_relation as rel
    on descendant.id = rel.parent_id
)
select *
from descendant
;
