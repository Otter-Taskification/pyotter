-- Get all ancestors of a task
with ancestors as (
    select parent_id as id
    from task_relation
    where child_id in (?)
    union all
    select rel.parent_id as id
    from ancestors
    inner join task_relation as rel
    on ancestors.id = rel.child_id
)
select *
from ancestors
;
