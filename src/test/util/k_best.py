"""
This module provides a utility function, `k_best`, designed to find a stable
minimum measurement from a stream of asynchronous inputs. It uses a k-best
selection strategy combined with a relative error criterion to determine
when a sufficiently "good" minimum has been identified.

The primary function `k_best` continuously takes measurements and maintains
a set of the `k` smallest measurements seen so far, while also tracking the
absolute minimum measurement. It converges when the relative error between
the `k`-th smallest measurement and the overall minimum falls below a specified
epsilon threshold.
"""

__all__: list[str] = ["k_best"]

import heapq
from typing import Awaitable, Callable


def __relative_error(
    measurement: float,
    reference: float,
) -> float:
    """
    Calculates the absolute relative error between a measurement and a reference value.

    Args:
        measurement: The measured value.
        reference: The reference value.

    Returns:
        The absolute relative error.
    """

    return abs(measurement - reference) / reference


async def k_best(
    k: int,
    n: int,
    epsilon: float,
    input_function: Callable[[], Awaitable[float]],
) -> float:
    """
    Finds the minimum measurement such that the relative error between the
    k-th largest measurement (among those currently considered 'best') and
    the overall minimum measurement encountered so far is within `epsilon`.

    This function continuously samples `n` measurements from `input_function`.
    It maintains the `k` largest measurements seen so far using a max-heap
    (simulated with a min-heap by negating values). Simultaneously, it tracks
    the absolute minimum measurement observed. The process terminates and
    returns the overall minimum measurement when the relative error condition is met.

    Args:
        k: The number of "best" (largest) measurements to consider for the error check.
           Must be greater than 0.
        n: The maximum number of measurements to take. Must be greater than 0
           and greater than or equal to k.
        epsilon: The acceptable relative error threshold. Must be greater than 0.
        input_function: An asynchronous callable that returns a new float measurement.
                        Each measurement must be greater than 0.

    Returns:
        The overall minimum measurement encountered when the convergence criterion is met.

    Raises:
        ValueError:
            - If `k`, `n`, or `epsilon` are not valid (e.g., non-positive, or n < k).
            - If any `measurement` returned by `input_function` is not greater than 0.
            - If `n` measurements are processed and the k-best measurements (satisfying
              the epsilon condition) are not found.
    """

    if k <= 0:
        raise ValueError(f"k must be greater than 0, got {k}")
    if n <= 0 or n < k:
        raise ValueError(
            f"n must be greater than 0 and greater than or equal to k, got {n}"
        )
    if epsilon <= 0:
        raise ValueError(f"epsilon must be greater than 0, got {epsilon}")

    measurement_count: int = 0
    minimum: float = float("inf")
    # Use a min-heap to store negated measurements. This effectively simulates a max-heap
    # for the actual measurements, allowing us to easily retrieve the k largest values.
    # The heap will always contain at most 'k' elements, representing the k largest
    # measurements encountered that are still relevant.
    heap: list[float] = []

    while measurement_count < n:
        # Fetch a new measurement value
        measurement: float = await input_function()
        measurement_count += 1

        if measurement <= 0:
            raise ValueError(f"measurement must be greater than 0, got {measurement}")

        # Update the overall minimum measurement encountered
        minimum = min(minimum, measurement)

        # Push the negated measurement onto the min-heap.
        # This way, the smallest element in the heap corresponds to the
        # largest actual measurement (because it's the smallest negative).
        heapq.heappush(heap, -measurement)

        # If the heap size exceeds k, remove the smallest element.
        # For our negated values, this means removing the element that corresponds
        # to the largest actual measurement, thus keeping only the k smallest.
        if len(heap) > k:
            heapq.heappop(heap)

        # Once we have collected k measurements in the heap, we can perform the check.
        if len(heap) == k:
            # The top of the min-heap (smallest element) is the negative of the
            # k-th largest measurement. Negate it to get the actual largest value.
            # This is the 'maximum' of our current k-best set.
            maximum: float = -heap[0]

            # Check if the relative error between the current largest measurement
            # and the overall minimum measurement is within the acceptable epsilon.
            if (
                __relative_error(
                    measurement=maximum,
                    reference=minimum,
                )
                <= epsilon
            ):
                # If the condition is met, return the overall minimum measurement.
                return minimum

    # If n measurements are processed and the k-best condition is not met, raise an error.
    raise ValueError(
        f"k-best measurements not found within {n} attempts. Minimum measurement found is {minimum}"
    )
