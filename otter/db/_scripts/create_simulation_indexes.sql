-- Create the indexes needed for a standalone simulated schedule database

-- Indexes to enforce data consistency
create index idx_sim_task_history_1
on sim_task_history(sim_id, id, action)
;

-- Partial indexes to enforce uniqueness of create/start/end actions
create unique index idx_sim_task_history_crt
on sim_task_history(sim_id, id)
where action = 1 -- create
;

create unique index idx_sim_task_history_start
on sim_task_history(sim_id, id)
where action = 2 -- start
;

create unique index idx_sim_task_history_end
on sim_task_history(sim_id, id)
where action = 3 -- end
;
