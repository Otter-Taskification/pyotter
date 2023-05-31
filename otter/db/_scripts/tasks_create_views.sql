-- Create views of the tables in a tasks db

-- Get a readable view of the tasks' initialisation locations
create view if not exists task_init_location(
         task
        ,file
        ,func
        ,line
    ) as
    select task.id as task
        ,file.text as file
        ,func.text as func
        ,src.line as line
    from task
    inner join source as src
        on src.src_loc_id = task.init_loc_id
    inner join string as file
        on file.id = src.file_id
    inner join string as func
        on func.id = src.func_id
;

-- Get a readable view of the tasks' start locations
create view if not exists task_start_location(
         task
        ,file
        ,func
        ,line
    ) as
    select task.id as task
        ,file.text as file
        ,func.text as func
        ,src.line as line
    from task
    inner join source as src
        on src.src_loc_id = task.start_loc_id
    inner join string as file
        on file.id = src.file_id
    inner join string as func
        on func.id = src.func_id
;

-- Get a readable view of the tasks' start locations
create view if not exists task_end_location(
         task
        ,file
        ,func
        ,line
    ) as
    select task.id as task
        ,file.text as file
        ,func.text as func
        ,src.line as line
    from task
    inner join source as src
        on src.src_loc_id = task.end_loc_id
    inner join string as file
        on file.id = src.file_id
    inner join string as func
        on func.id = src.func_id
;

-- Union of all source locations
create view if not exists task_location as
    select *,
           'init' as type
    from task_init_location
    union
    select *,
           'start' as type
    from task_start_location
    union
    select *,
           'end' as type
    from task_end_location
    order by task
;

-- A readable view of a task's attributes (flavour, label, etc)
create view if not exists task_attributes as
    select task.id
        ,task.flavour
        ,string.text as task_label
        ,task.start_ts
        ,task.end_ts
        ,task.duration
    from task
    left join string
    on task.user_label = string.id
;
