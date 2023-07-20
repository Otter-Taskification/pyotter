from .classes import tag
from collections import defaultdict


def wrap(name, content, **kwargs):
    with tag(name, **kwargs) as t:
        if content is not None:
            t.append(content)
    return t


def table(keys: list, tidy_keys: dict, rows: list, na_value=None, **kwargs):
    attr = defaultdict(lambda: dict(), kwargs.get("attr", dict()))

    with tag("table", **attr["table"]) as t:
        with t.add(tag("thead")) as thead:
            with thead.add(tag("tr", **attr["tr"])) as trow:
                for key in keys:
                    trow.append(wrap("th", tidy_keys.get(key, f"UNKNOWN COLUMN {key}")))

        with t.add(tag("tbody")) as tbody:
            for row in rows:
                with tbody.add(tag("tr")) as trow:
                    for key in keys:
                        value = row.get(key, f"NO VALUE FOR KEY {key}")
                        if value is None and na_value is not None:
                            value = na_value
                        trow.append(wrap("td", value))

    return t


def graphviz_record_table(object_attr: dict, **kwargs):
    """A HTML-like table where each row represents one key-value pair of some object's attributes"""
    table_attr = defaultdict(lambda: dict(), kwargs.get("table_attr", dict()))
    with tag("table", **table_attr["table"]) as t:
        for key, value in object_attr.items():
            with t.add(tag("tr", **table_attr["tr"])) as tr:
                tr.append(wrap("td", wrap("b", key), **table_attr["td"]))
                tr.append(wrap("td", value, **table_attr["td"]))
    return t
