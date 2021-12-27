import itertools
from typing import Iterable

# takes in an iterable
# returns it in chunks
# [1,2,3,4,5], 2 -> [1,2], [3,4], [5]
# https://stackoverflow.com/a/8998040/5443120
def chunks(iterable: Iterable, n) -> Iterable[Iterable]:
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)
