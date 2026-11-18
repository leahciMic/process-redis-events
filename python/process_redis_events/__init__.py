"""Process Redis Events - A library for processing Redis Stream events with consumer groups."""

from process_redis_events.constants import StartFrom
from process_redis_events.consumer import (
    BatchConsumer,
    Message,
    StreamConsumer,
    install_signal_handlers,
)
from process_redis_events.dlq import dlq_data, dlq_key
from process_redis_events.entries import agroup_backlog, group_backlog, parse_fields
from process_redis_events.errors import (
    Paused,
    TransientError,
    backoff_delay,
    is_transient,
    transient_kind,
)
from process_redis_events.naming import CONSUMER_NAME_ENV, consumer_name
from process_redis_events.queue_item import QueueItem
from process_redis_events.stream import (
    ConsumerGroupInfo,
    MapFunction,
    ProcessCallback,
    ProcessOptions,
    RetryFunction,
    Stream,
    StreamInfo,
    create_process_options,
)
from process_redis_events.stream_event import StreamEvent
from process_redis_events.timeouts import safe_block_ms

__version__ = "5.2.0"

__all__ = [
    "CONSUMER_NAME_ENV",
    "BatchConsumer",
    "ConsumerGroupInfo",
    "MapFunction",
    "Message",
    "Paused",
    "ProcessCallback",
    "ProcessOptions",
    "QueueItem",
    "RetryFunction",
    "StartFrom",
    "Stream",
    "StreamConsumer",
    "StreamEvent",
    "StreamInfo",
    "TransientError",
    "agroup_backlog",
    "backoff_delay",
    "consumer_name",
    "create_process_options",
    "dlq_data",
    "dlq_key",
    "group_backlog",
    "install_signal_handlers",
    "is_transient",
    "parse_fields",
    "safe_block_ms",
    "transient_kind",
]
