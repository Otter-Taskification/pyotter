select attr.task_label
    ,attr.flavour
    ,attr.create_file
    ,attr.create_func
    ,attr.create_line
    ,attr.start_file
    ,attr.start_func
    ,attr.start_line
    ,attr.end_file
    ,attr.end_func
    ,attr.end_line
    ,child.task_label as child_task_label
    ,child.flavour as child_flavour
    ,child.create_file as child_create_file
    ,child.create_func as child_create_func
    ,child.create_line as child_create_line
    ,child.start_file as child_start_file
    ,child.start_func as child_start_func
    ,child.start_line as child_start_line
    ,child.end_file as child_end_file
    ,child.end_func as child_end_func
    ,child.end_line as child_end_line
    ,count(distinct child.id) as child_tasks
from task_attributes as attr
left join task_attributes as child
    on child.parent_id = attr.id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
order by attr.task_label
    ,child_tasks desc
;
