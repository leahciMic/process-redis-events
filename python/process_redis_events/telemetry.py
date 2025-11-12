"""OpenTelemetry integration for Redis stream processing."""

import time
from typing import Any, Callable, Literal

from opentelemetry import metrics, trace
from opentelemetry.metrics import Counter, Histogram, Meter
from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer


class TelemetryConfig:
    """Configuration for telemetry."""

    def __init__(
        self,
        enabled: bool = True,
        version: str = "5.0.6",
        meter_name: str = "process-redis-events",
        tracer_name: str = "process-redis-events",
        stream_name: str = "",
        consumer_group: str = "",
        consumer_id: str = "",
    ):
        self.enabled = enabled
        self.version = version
        self.meter_name = meter_name
        self.tracer_name = tracer_name
        self.stream_name = stream_name
        self.consumer_group = consumer_group
        self.consumer_id = consumer_id


class RedisStreamTelemetry:
    """Telemetry handler for Redis stream processing."""

    def __init__(self, config: TelemetryConfig):
        self.config = config

        if not config.enabled:
            # Create no-op implementations
            self.meter = metrics.get_meter("noop")
            self.tracer = trace.get_tracer("noop")
            self._initialize_noop_metrics()
        else:
            self.meter = metrics.get_meter(config.meter_name, config.version)
            self.tracer = trace.get_tracer(config.tracer_name, config.version)
            self._initialize_metrics()

    def child(self, **kwargs: Any) -> "RedisStreamTelemetry":
        """Create a child telemetry instance with updated config."""
        new_config = TelemetryConfig(
            enabled=self.config.enabled,
            version=self.config.version,
            meter_name=self.config.meter_name,
            tracer_name=self.config.tracer_name,
            stream_name=kwargs.get("stream_name", self.config.stream_name),
            consumer_group=kwargs.get("consumer_group", self.config.consumer_group),
            consumer_id=kwargs.get("consumer_id", self.config.consumer_id),
        )
        return RedisStreamTelemetry(new_config)

    def _initialize_metrics(self) -> None:
        """Initialize OpenTelemetry metrics."""
        self.messages_processed_counter = self.meter.create_counter(
            name="redis_stream_messages_processed_total",
            description="Total number of messages processed from Redis streams",
            unit="1",
        )

        self.processing_duration_histogram = self.meter.create_histogram(
            name="redis_stream_processing_duration_seconds",
            description="Time taken to process a batch of messages",
            unit="s",
        )

        self.batch_size_histogram = self.meter.create_histogram(
            name="redis_stream_batch_size",
            description="Distribution of batch sizes processed",
            unit="1",
        )

        # Note: Using histogram for pending messages as OpenTelemetry Python
        # doesn't have synchronous gauge in the same way
        self.pending_messages_gauge = self.meter.create_histogram(
            name="redis_stream_pending_messages",
            description="Number of pending messages in the stream",
            unit="1",
        )

    def _initialize_noop_metrics(self) -> None:
        """Initialize no-op metrics when telemetry is disabled."""

        class NoOpCounter:
            def add(self, *args: Any, **kwargs: Any) -> None:
                pass

        class NoOpHistogram:
            def record(self, *args: Any, **kwargs: Any) -> None:
                pass

        self.messages_processed_counter = NoOpCounter()  # type: ignore
        self.processing_duration_histogram = NoOpHistogram()  # type: ignore
        self.batch_size_histogram = NoOpHistogram()  # type: ignore
        self.pending_messages_gauge = NoOpHistogram()  # type: ignore

    def create_span(self, name: str, attributes: dict[str, Any] | None = None) -> Span:
        """Create a new trace span.

        Args:
            name: Name of the span
            attributes: Optional attributes to attach to the span

        Returns:
            A new span
        """
        return self.tracer.start_span(
            name, kind=SpanKind.INTERNAL, attributes=attributes or {}
        )

    def record_messages_processed(
        self, count: int, labels: dict[str, str] | None = None
    ) -> None:
        """Record the number of messages processed.

        Args:
            count: Number of messages processed
            labels: Additional labels (e.g., {"status": "success"})
        """
        attrs = {
            "stream_name": self.config.stream_name,
            "consumer_group": self.config.consumer_group,
            "consumer_id": self.config.consumer_id,
        }
        if labels:
            attrs.update(labels)

        self.messages_processed_counter.add(count, attributes=attrs)

    def record_processing_duration(self, duration: float) -> None:
        """Record processing duration in seconds.

        Args:
            duration: Duration in seconds
        """
        self.processing_duration_histogram.record(
            duration,
            attributes={
                "stream_name": self.config.stream_name,
                "consumer_group": self.config.consumer_group,
                "consumer_id": self.config.consumer_id,
            },
        )

    def record_batch_size(self, size: int) -> None:
        """Record the size of a processed batch.

        Args:
            size: Number of items in the batch
        """
        self.batch_size_histogram.record(
            size,
            attributes={
                "stream_name": self.config.stream_name,
                "consumer_group": self.config.consumer_group,
            },
        )

    def record_pending_messages(self, count: int) -> None:
        """Record the number of pending messages.

        Args:
            count: Number of pending messages
        """
        self.pending_messages_gauge.record(
            count,
            attributes={
                "stream_name": self.config.stream_name,
                "consumer_group": self.config.consumer_group,
            },
        )

    def start_timer(self) -> Callable[[], float]:
        """Start a timer for measuring duration.

        Returns:
            A function that when called returns the elapsed time in seconds
        """
        start = time.time()
        return lambda: time.time() - start
