"""Utility functions for the library."""

from typing import TypeVar

T = TypeVar("T")


def chunk(array: list[T], size: int) -> list[list[T]]:
    """Split an array into chunks of specified size.

    Args:
        array: The array to split
        size: The size of each chunk

    Returns:
        List of chunks
    """
    chunks: list[list[T]] = []
    index = 0

    while index < len(array):
        chunks.append(array[index : index + size])
        index += size

    return chunks
