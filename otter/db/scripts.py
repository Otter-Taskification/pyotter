from importlib import resources

from . import _scripts

def get_sql(name: str) -> str:
    return resources.read_text(_scripts, name)


create_tasks: str = get_sql("tasks_create.sql")
create_views: str = get_sql("tasks_create_views.sql")
insert_tasks: str = get_sql("tasks_insert.sql")
insert_task_relations: str = get_sql("tasks_insert_relations.sql")
define_source_locations: str = get_sql("tasks_define_source_locations.sql")
define_strings: str = get_sql("tasks_define_strings.sql")
count_tasks: str = get_sql("tasks_count.sql")
count_tasks_by_attributes: str = get_sql("tasks_count_tasks_by_attributes.sql")
count_children_by_parent_attributes = get_sql("tasks_count_children_by_attributes.sql")
count_chunks = get_sql("chunks_get_num_chunks.sql")
insert_chunk: str = get_sql("tasks_insert_chunk.sql")
insert_chunk_events: str = get_sql("chunks_insert_event_pos.sql")
insert_context: str = get_sql("tasks_insert_context.sql")
insert_synchronisation: str = get_sql("tasks_insert_synchronisation.sql")
insert_task_history: str = get_sql("tasks_insert_history.sql")
get_ancestors: str = get_sql("tasks_get_ancestors.sql")
get_descendants: str = get_sql("tasks_get_descendants.sql")
get_child_sync_points: str = get_sql("tasks_get_child_sync_points.sql")
get_chunk_events: str = get_sql("chunks_get_event_pos.sql")
get_chunk_ids: str = get_sql("chunks_get_chunk_refs.sql")
# get_all_source_locations: str = get_sql("tasks_get_all_source_locations.sql")
