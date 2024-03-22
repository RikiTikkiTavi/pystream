import math
from statistics import stdev
from time import time

from pystream.collectors import to_collection
from pystream import ParallelStream
from pystream import SequentialStream


def _filter(x) -> bool:
    return x % 3 == 0


if __name__ == '__main__':
    collection = tuple(range(10_000, 10_100))
    times = []

    print("Parallel:")
    for i in range(100):
        t_s = time()
        ParallelStream(collection) \
            .filter(_filter) \
            .map(math.factorial) \
            .collect(collector=to_collection(tuple))
        t_elapsed = time() - t_s
        times.append(t_elapsed)
        print(f"Time elapsed on {i}'ths experiment:", t_elapsed)
    t_parallel = sum(times) / len(times)
    t_std_avg_parallel = stdev(times)/math.sqrt(len(times))

    times = []
    print("Sequential:")
    for i in range(100):
        t_s = time()
        SequentialStream(collection) \
            .filter(_filter) \
            .map(math.factorial) \
            .collect(collector=to_collection(tuple))
        t_elapsed = time() - t_s
        times.append(t_elapsed)
        print(f"Time elapsed on {i}'ths experiment:", t_elapsed)
    t_seq = sum(times) / len(times)
    t_std_avg_seq = stdev(times)/math.sqrt(len(times))

    print(f"Parallel: {t_parallel} +- {t_std_avg_parallel}")
    print(f"Parallel: {t_seq} +- {t_std_avg_seq}")
    print("Time elapsed Sequential/Parallel", t_seq / t_parallel)