"""
This module provides an asynchronous command-line interface for performing k-best measurements.

It leverages `argparse` to configure the measurement parameters (k, n, epsilon)
and `aioconsole` for asynchronous user input of float values.
The core k-best logic is delegated to the `k_best` library.
"""

import asyncio
import argparse

import aioconsole

import k_best


async def _get_float_from_console() -> float:
    """
    Asynchronously prompts the user for a float input from the console.

    This function uses `aioconsole.ainput()` to read input and
    continuously prompts the user until a valid float number is entered.

    Returns:
        float: The valid float number entered by the user.
    """

    while True:
        try:
            user_input: str = await aioconsole.ainput()
            value: float = float(user_input)
            return value
        except ValueError:
            print("Invalid input. Please enter a valid float number.")


async def main():
    """
    Main entry point for the k-best measurement application.

    This function parses command-line arguments for k, n, and epsilon,
    then initiates the asynchronous k-best measurement process using
    `_get_float_from_console` for input. It handles potential configuration
    and runtime errors, printing appropriate messages to the console.
    """
    parser = argparse.ArgumentParser(
        description="Perform k-best measurements with asynchronous console input."
    )
    parser.add_argument(
        "-k",
        type=int,
        default=3,
        help="The k value: the number of best measurements to find.",
    )
    parser.add_argument(
        "-n",
        type=int,
        default=10,
        help="The n value: the total number of measurements to perform.",
    )
    parser.add_argument(
        "-e",
        "--epsilon",
        type=float,
        default=0.05,
        help="The epsilon value: a threshold for considering measurements close to the k-th best.",
    )

    args = parser.parse_args()

    # Basic validation for arguments
    if not (isinstance(args.k, int) and args.k > 0):
        parser.error(f"Argument 'k' must be a positive integer, got: {args.k}")
    if not (isinstance(args.n, int) and args.n > 0):
        parser.error(f"Argument 'n' must be a positive integer, got: {args.n}")
    if not (isinstance(args.epsilon, (float, int)) and args.epsilon > 0):
        parser.error(
            f"Argument 'epsilon' must be a positive float, got: {args.epsilon}"
        )
    if args.k > args.n:
        parser.error(f"Argument 'k' ({args.k}) cannot be greater than 'n' ({args.n}).")

    print("Starting k-best measurement (using aioconsole)...")
    print(f"Configuration: k={args.k}, n={args.n}, epsilon={args.epsilon}")

    try:
        result = await k_best.k_best(
            k=args.k,
            n=args.n,
            epsilon=args.epsilon,
            input_function=_get_float_from_console,
        )
        print(f"The minimum value among the k-best measurements found is: {result}")
    except ValueError as e:
        print(f"Configuration error or invalid measurement values: {e}")


if __name__ == "__main__":
    asyncio.run(main())
