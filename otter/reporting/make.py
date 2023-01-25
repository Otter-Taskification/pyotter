from .classes import tag
from collections import defaultdict

def wrap(name, content, **kwargs):
    with tag(name, **kwargs) as t:
        if content is not None:
            t.append(content)
    return t

def table(keys: list, tidy_keys: dict, rows: list, **kwargs):

    attr = defaultdict(lambda: dict(), kwargs.get("attr", dict()))

    with tag("table", **attr["table"]) as t:

        with t.add(tag("thead")) as thead:
            with thead.add(tag("tr", **attr["tr"])) as trow:
                for key in keys:
                    trow.append(wrap("th", tidy_keys.get(key, "NONE")))

        with t.add(tag("tbody")) as tbody:
            for row in rows:
                with tbody.add(tag("tr")) as trow:
                    for key in keys:
                        trow.append(wrap("td", row.get(key, "NONE")))

    return t

def graphviz_record_table(object_attr: dict, **kwargs):
    """A HTML-like table where each row represents one key-value pair of some object's attributes"""
    attr = defaultdict(lambda: dict(), kwargs.get("attr", dict()))
    with tag("table", **attr["table"]) as table:
        for key, value in object_attr.items():
            with table.add(tag("tr", **attr["tr"])) as row:
                row.append(wrap("td", wrap("b", key), **attr["td"]))
                row.append(wrap("td", value, **attr["td"]))
    return table
