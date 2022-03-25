from collections import Counter

class PrettyCounter(Counter):

    def __repr__(self):
        return "\n".join([f"{k}: {self[k]}" for k in self]) + f"\nTotal count: {sum(self.values())}"
