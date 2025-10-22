"""Parse JSON utility."""

import json
from typing import Any, TypeVar

T = TypeVar("T")


def parse_json(value: Any) -> Any:
    """Parse a JSON string.

    Args:
        value: The value to parse (must be a string)

    Returns:
        Parsed JSON data

    Raises:
        ValueError: If value is not a string
        json.JSONDecodeError: If JSON parsing fails
    """
    if not isinstance(value, (str, bytes)):
        raise ValueError("Value is not a string")
    return json.loads(value)
