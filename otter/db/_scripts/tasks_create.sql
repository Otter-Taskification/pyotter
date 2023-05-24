-- Set up a database of tasks

-- List the tasks and their start/end times
create table task(
    id int unique not null,
    start_ts,
    end_ts,
    primary key (id)
);

-- List parent-child links
create table task_relation(
    parent_id int not null,
    child_id int not null,
    foreign key (parent_id) references task (id),
    foreign key (child_id) references task (id)
);

-- List task ID and source locations in a wide format
create table src_loc(
    task_id int not null,
    init_id int not null,  -- the location where the task was initialised
    start_id int not null, -- the location where the task started
    end_id int not null,   -- the location where the task ended
    primary key (task_id),
    foreign key (task_id) references task (id),
    foreign key (init_id) references src_loc_def (src_loc_id),
    foreign key (start_id) references src_loc_def (src_loc_id),
    foreign key (end_id) references src_loc_def (src_loc_id)
);

-- List distinct source location definitions
create table src_loc_def(
    src_loc_id int not null,
    file_id int not null,
    func_id int not null,
    line int not null,
    primary key (src_loc_id),
    foreign key (file_id) references src_str_def (id),
    foreign key (func_id) references src_str_def (id)
);

-- List source string definitions
create table src_str_def(
    id int not null,
    text,
    primary key (id)
);
