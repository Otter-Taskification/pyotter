-- Create the tables needed for a standalone simulated schedule database

create table critical_task(
    sim_id int not null,   -- not strictly necessary in a standalone db, but keep for consistency
    id int not null,
    sequence int not null,
    critical_child int not null,
    primary key (sim_id, id, sequence)
);

-- List actions of each task, using partial keys to enforce uniqueness of some actions
create table sim_task_history(
    sim_id int not null,   -- partition the separate simulations
    id int not null,       -- task ID
    action int not null,   --
    time int not null,     -- time of action
    source_location_id,    -- source location
    cpu int not null,      -- cpu of encountering thread
    tid int not null       -- thread ID
);

-- List metadata about each task-suspend action in a simulated schedule
create table sim_task_suspend_meta(
    sim_id int not null,             -- partition the separate simulations
    id int not null,                 -- task ID
    time int not null,               -- time of action
    sync_descendants int not null,   --! deprecated
    sync_mode int not null,          -- children/descendants/yield?
    primary key (sim_id, id, time)
);
