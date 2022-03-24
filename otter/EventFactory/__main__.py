import otf2
import otter

p = "trace/fib.78857/fib.78857.otf2"
print(f"Count of event types in {p}:")

with otf2.reader.open(p) as r:
    counter = otter.utils.PrettyCounter()
    factory = otter.EventFactory(r)
    for event in factory:
        counter[type(event)] += 1

print(counter)
