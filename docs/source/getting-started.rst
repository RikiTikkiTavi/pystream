.. _getting-started:

Getting Started
======================

Loading modules
****************
After installing PyStream, you can load public modules and classes of pystream you need.

Importing sequential stream:
from pystream.sequential_stream import SequentialStream as Stream

Importing nullable (~ Java Optional):
from pystream.infrastructure.nullable import Nullable

Importing collectors:
import pystream.infrastructure.collectors as collectors

Usage
*****

For example you have some Iterable of ints and you want multiply each element by 2, then remove elements
bigger then 10, print each (Or perform any other side effect) and then collect to tuple:

ints: List[int] = [10, 3, 5, 1, 2]

processed_ints: Tuple[int, ...] = Stream(ints) \
    .map(lambda x: x*2) \
    .filter(lambda x: x <= 10) \
    .peek(print) \
    .collect(collectors.to_collection(tuple)) \

# processed_ints is (6, 10, 2, 4)

Note that streams are lazy, so 