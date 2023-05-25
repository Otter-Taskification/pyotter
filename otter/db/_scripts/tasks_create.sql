-- Set up a database of tasks

-- List the tasks and their start/end times
create table task(
    id int unique not null,
    start_ts,
    end_ts,
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
