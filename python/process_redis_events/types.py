"""Type definitions used throughout the library."""

from typing import Any, TypeAlias

# JSON-serializable types
Json: TypeAlias = str | int | float | bool | None | dict[str, "Json"] | list["Json"]
