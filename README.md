# Pystream

Java Stream analogy in python. Completely typed.

Check the code for documentation.

## Example usage

```python
from pystream.sequential_stream import SequentialStream
from pystream.collectors import to_collection

xs: list[int] = (
    SequentialStream(range(100)) # Create stream from int iterator
    .filter(lambda x: x % 3 == 0) # Filter out not divisible by 3 (lazy operation)
    .map(lambda x: x**2) # Square (lazy operation)
    .parallel() # Switch to parallel stream (lazy operation)
    .collect(to_collection(list)) # Collect into list (terminal operation, causes other operations to be executed)
)

print(xs)
```

Output:
```
[0, 9, 36, 81, 144, 225, 324, 441, 576, 729, 900, 1089, 1296, 1521, 1764, 2025, 2304, 2601, 2916, 3249, 3600, 3969, 4356, 4761, 5184, 5625, 6084, 6561, 7056, 7569, 8100, 8649, 9216, 9801]
```