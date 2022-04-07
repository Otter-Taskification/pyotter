from otter.reporting import Doc, wrap, table
from collections import defaultdict

header = ["name", "crt_ts"]

tidy_names = defaultdict(
    lambda: "UNDEFINED",
    {
        "name": "Name",
        "crt_ts": "Created"
    })

rows = [
    dict(name="Adam"),
    dict(),
    dict(),
    dict(name="Matthew"),
    dict(),
    dict()
]

body_attr = {
    'class': 'dummy',
    'bgcolor': 'black',
    'text': 'white'
}

doc = Doc("<!DOCTYPE html>")
title = wrap("title", "Simple Table")
doc.add(wrap("head", title))
doc.add(wrap("h1", "Hello, world!"))
with doc.open("body", **body_attr) as body:
    body.extend(
        table(
            header,
            tidy_names,
            rows,
            attr = {
                "table": {'border': '1', 'class': 'data-table'},
                "tr": {'style': 'text-align: right;'}
            }
        )
    )
    body.append(wrap("a", "This should link to Google.", href="https://www.google.com"))

print(doc)

with open("table.html", "w") as html:
    for line in doc:
        html.write(f"{line}\n")
