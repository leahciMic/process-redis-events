"""StreamEvent type definitions."""

from typing import Literal, TypedDict


class StreamProgressEvent(TypedDict):
    """Event indicating progress in processing."""

    type: Literal["progress"]
    completionRatio: float
    status: str


class StreamCompletedEvent(TypedDict):
    """Event indicating successful completion."""

    type: Literal["completed"]


class StreamFailedEvent(TypedDict):
    """Event indicating processing failure."""

    type: Literal["failed"]
    error: str


class StreamStartedEvent(TypedDict):
    """Event indicating processing started."""

    type: Literal["started"]


class _StreamEventBase(TypedDict):
    """Base event with id."""

    id: str


# Union type for all stream events
class StreamProgressEventWithId(_StreamEventBase, StreamProgressEvent):
    """Progress event with id."""

    pass


class StreamCompletedEventWithId(_StreamEventBase, StreamCompletedEvent):
    """Completed event with id."""

    pass


class StreamFailedEventWithId(_StreamEventBase, StreamFailedEvent):
    """Failed event with id."""

    pass


class StreamStartedEventWithId(_StreamEventBase, StreamStartedEvent):
    """Started event with id."""

    pass


StreamEvent = (
    StreamProgressEventWithId
    | StreamCompletedEventWithId
    | StreamFailedEventWithId
    | StreamStartedEventWithId
)
