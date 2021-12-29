import lambda_multiprocessing
import multiprocessing
from time import sleep, time

def handler(event={}, context=None):
    n = 5
    #try:
    #    with multiprocessing.Pool() as p:
    #        p.map(sleep, (1,))
    #except OSError:
    #    pass
    #else:
    #    raise RuntimeError("Did not get the expected error from standard library")

    with lambda_multiprocessing.Pool(n) as p:
        start_t = time()
        p.map(sleep, range(n))
        end_t = time()
        delta = end_t - start_t
        expected = max(range(n))
        assert delta > expected, f"Sleep not called, {delta=}s"
        assert delta < expected + 1, f"Not in parallel, took {delta=}s, expected ~ {expected}"
    return True # easy for bash to detect success
