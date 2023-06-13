-- Create views of the tables in a tasks db

-- Get a readable view of the tasks' initialisation locations
create view if not exists task_init_location(
         id
        ,file
        ,func
        ,line
    ) as
    select task.id as id
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
         id
        ,file
        ,func
        ,line
    ) as
    select task.id as id
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
         id
        ,file
        ,func
        ,line
    ) as
    select task.id as id
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
    order by id
        ,type
;

-- A readable view of a task's attributes (flavour, label, etc)
create view if not exists task_attributes as
    select task.id
        ,parent.parent_id
        ,task.flavour
        ,string.text as task_label
        ,task.start_ts
        ,task.end_ts
        ,task.duration
        ,init_loc.file as init_file
        ,init_loc.func as init_func
        ,init_loc.line as init_line
        ,start_loc.file as start_file
        ,start_loc.func as start_func
        ,start_loc.line as start_line
        ,end_loc.file as end_file
        ,end_loc.func as end_func
        ,end_loc.line as end_line
    from task
    left join task_relation as parent
        on task.id = parent.child_id
    left join string
        on task.user_label = string.id
    left join task_init_location as init_loc
        on task.id = init_loc.id
    left join task_start_location as start_loc
        on task.id = start_loc.id
    left join task_end_location as end_loc
        on task.id = end_loc.id
;
