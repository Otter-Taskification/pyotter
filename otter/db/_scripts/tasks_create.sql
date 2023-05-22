-- Set up a database of tasks

drop table if exists task;
drop table if exists task_relation;

-- List the tasks and their stard/end times
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
