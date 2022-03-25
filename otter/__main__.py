import otf2
import otter

args = otter.get_args()
event_counter = otter.utils.PrettyCounter()

print(f"loading OTF2 anchor file: {args.anchorfile}")
with otf2.reader.open(args.anchorfile) as r:
    events = otter.EventFactory(r)
    tr = otter.TaskRegistry()
    for event in events:
        event_counter[type(event)] += 1
        if event.is_task_register_event:
            tr.register_task(event)

print(event_counter)
print(tr)
for t in tr:
    print(t)
