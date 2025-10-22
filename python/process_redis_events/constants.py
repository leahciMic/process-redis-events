"""Constants used throughout the library."""

from enum import Enum


class StartFrom(str, Enum):
    """Enum for specifying where to start reading from a Redis stream."""

    OLDEST = "0"
    LATEST = "$"


class RedisStreamCursors(str, Enum):
    """Redis stream cursor positions."""

    OLDEST = "0-0"
    LATEST = ">"
