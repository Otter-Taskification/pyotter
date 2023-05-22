from . import _scripts

try:
    import importlib.resources as resources
except ImportError:
    import importlib_resources as resources

def get_sql(name: str) -> str:
    return resources.read_text(_scripts, name)

create_tasks: str = get_sql("tasks_create.sql")
normalise_tasks: str = get_sql("tasks_normalise.sql")
insert_tasks: str = get_sql("tasks_insert.sql")
insert_task_relations: str = get_sql("tasks_insert_relations.sql")