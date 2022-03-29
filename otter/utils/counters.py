from collections import Counter

class PrettyCounter(Counter):

    def __repr__(self):
        return "\n".join([f"{self[k]:>6} {k}" for k in self]) + f"\nTotal count: {sum(self.values())}"
