"""QueueItem type definition."""

from typing import Awaitable, Callable, Generic, Protocol, TypeVar

T = TypeVar("T")


class QueueItem(Generic[T]):
    """Represents an item in the processing queue."""

    def __init__(
        self,
        id: str,
        data: T,
        attempts: int,
        report_progress: Callable[[float, str], Awaitable[None]],
    ):
        self.id = id
        self.data = data
        self.attempts = attempts
        self.report_progress = report_progress
