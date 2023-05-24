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
        ,def.line as line
    from task
    inner join src_loc as src
        on task.id = src.task_id
    inner join src_loc_def as def
        on src.init_id = def.src_loc_id
    inner join src_str_def as file
        on def.file_id = file.id
    inner join src_str_def as func
        on def.func_id = func.id
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
        ,def.line as line
    from task
    inner join src_loc as src
        on task.id = src.task_id
    inner join src_loc_def as def
        on src.start_id = def.src_loc_id
    inner join src_str_def as file
        on def.file_id = file.id
    inner join src_str_def as func
        on def.func_id = func.id
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
        ,def.line as line
    from task
    inner join src_loc as src
        on task.id = src.task_id
    inner join src_loc_def as def
        on src.end_id = def.src_loc_id
    inner join src_str_def as file
        on def.file_id = file.id
    inner join src_str_def as func
        on def.func_id = func.id
;
