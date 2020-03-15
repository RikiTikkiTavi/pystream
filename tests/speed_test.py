import math
from time import time, sleep

from pystream.infrastructure import pipe
from pystream.infrastructure.collectors import to_collection
from pystream.parallel_stream import ParallelStream
from pystream.stream import Stream


def _filter(x):
    return x % 3 == 0


if __name__ == '__main__':
    collection = tuple(range(10_000, 11_000))
    times = []

    print("Parallel:")
    for i in range(5):
        t_s = time()
        ParallelStream(collection) \
            .filter(_filter) \
            .map(math.factorial) \
            .collect(collector=to_collection(tuple))
        t_elapsed = time() - t_s
        times.append(t_elapsed)
        print(f"Time elapsed on {i}'ths experiment:", t_elapsed)
    t_parallel = sum(times) / len(times)

    times = []
    print("Sequential:")
    for i in range(5):
        t_s = time()
        Stream(collection) \
            .filter(_filter) \
            .map(math.factorial) \
            .collect(collector=to_collection(tuple))
        t_elapsed = time() - t_s
        times.append(t_elapsed)
        print(f"Time elapsed on {i}'ths experiment:", t_elapsed)
    t_seq = sum(times) / len(times)

    print("Avg parallel: ", t_parallel)
    print("Avg sequential: ", t_seq)
    print("Time elapsed Sequential/Parallel", t_seq / t_parallel)
