from pystream.parallel_stream_new import ParallelStream


def _cube(x):
    return x ** 2


if __name__ == "__main__":
    print(ParallelStream(range(20)).map(_cube).to_list())
