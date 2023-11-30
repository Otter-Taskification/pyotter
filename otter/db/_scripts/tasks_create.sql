-- Set up a database of tasks

-- List the tasks and their start/end times
create table task(
    id int unique not null,

    -- these fields removed as they can be calculated from the task_history table
    -- start_ts,
    -- end_ts,
    -- duration int,

    -- these fields removed as they are now in the task_history table
    -- init_loc_id int not null,  -- the location where the task was initialised
    -- start_loc_id int not null, -- the location where the task started
    -- end_loc_id int not null,   -- the location where the task ended

    flavour int,
    user_label int,
    primary key (id)

    -- these keys removed as their fields removed
    -- foreign key (init_loc_id) references source (src_loc_id),
    -- foreign key (start_loc_id) references source (src_loc_id),
    -- foreign key (end_loc_id) references source (src_loc_id)
);

-- List actions of each task
create table task_history(
    id int not null,       -- task ID
    action int not null,   -- 0=init/1=start/2=end
    time not null,         -- time of action
    location_id,           -- source location
    primary key (id, action)
    foreign key (id) references task (id)
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
    foreign key (context_id) references context (context_id)
);

-- List the sync contexts in order within each task
create table chunk(
    encountering_task_id int not null,
    context_id int not null,
    sequence int not null,
    primary key (encountering_task_id, context_id, sequence),
    foreign key (encountering_task_id) references task (id),
    foreign key (context_id) references context (context_id)
);

-- Metadata about each task synchronisation context
create table context(
    context_id int not null,
    sync_descendants int not null,
    sync_ts,
    primary key (context_id)
);

-- List the location_ref and local event positions contained in each chunk
create table chunk_contents(
    chunk_key int not null,
    location_ref int not null,
    event_pos int not null,
    primary key (chunk_key, location_ref, event_pos)
);
