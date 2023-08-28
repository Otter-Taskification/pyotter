select attr.task_label
    ,attr.init_file
    ,attr.init_func
    ,attr.init_line
    ,attr.start_file
    ,attr.start_func
    ,attr.start_line
    ,attr.end_file
    ,attr.end_func
    ,attr.end_line
    ,count(*) as num_tasks
from task_attributes as attr
group by 1,2,3,4,5,6,7,8,9,10
order by num_tasks desc
;
