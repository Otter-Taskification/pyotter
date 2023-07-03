-- Set up a database of tasks

-- List the tasks and their start/end times
create table task(
    id int unique not null,
    start_ts,
    end_ts,
    duration int,
    init_loc_id int not null,  -- the location where the task was initialised
    start_loc_id int not null, -- the location where the task started
    end_loc_id int not null,   -- the location where the task ended
    flavour int,
    user_label,
    primary key (id),
    foreign key (init_loc_id) references source (src_loc_id),
    foreign key (start_loc_id) references source (src_loc_id),
    foreign key (end_loc_id) references source (src_loc_id)
);

-- List parent-child links
create table task_relation(
    parent_id int not null,
    child_id int not null,
    foreign key (parent_id) references task (id),
    foreign key (child_id) references task (id)
);

-- List distinct source location definitions
create table source(
    src_loc_id int not null,
    file_id int not null,
    func_id int not null,
    line int not null,
    primary key (src_loc_id),
    foreign key (file_id) references string (id),
    foreign key (func_id) references string (id)
);

-- List source string definitions
create table string(
    id int not null,
    text,
    primary key (id)
);

-- List each task synchronisation and the tasks it applies to
create table synchronisation(
    context_id int not null,
    task_id int not null,
    primary key (context_id, task_id),
    foreign key (task_id) references task (id),
    foreign key (context_id) references sync_context (context_id)
);

-- List the sync contexts in order within each task
create table chunk(
    encountering_task_id int not null,
    context_id int not null,
    sequence int not null,
    primary key (encountering_task_id, context_id, sequence),
    foreign key (encountering_task_id) references task (id),
    foreign key (context_id) references sync_context (context_id)
);

-- Metadata about each task synchronisation context
create table sync_context(
    context_id int not null,
    sync_descendants int not null,
    primary key (context_id)
);
