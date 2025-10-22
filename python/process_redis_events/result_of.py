"""Result type for safe error handling."""

from typing import Any, Generic, TypeVar

T = TypeVar("T")


async def result_of(coro_or_func: Any) -> tuple[None, T] | tuple[Exception, None]:
    """Safely unwrap the result of a coroutine or function.

    Args:
        coro_or_func: The coroutine or function to execute

    Returns:
        A tuple containing either (None, result) or (error, None)
    """
    try:
        if callable(coro_or_func) and not hasattr(coro_or_func, "__await__"):
            result = coro_or_func()
        else:
            result = await coro_or_func
        return (None, result)
    except Exception as error:
        return (error, None)


from typing import Any
