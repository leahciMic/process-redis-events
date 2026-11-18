"""The wire format against the shared fixture (../fixtures/stream-format.json), which the TypeScript tests and
producers in other languages (clok's Go relay) check too, so the libraries can't drift apart.
"""

import json
import os
import uuid
from pathlib import Path

import pytest
from redis.asyncio import Redis

from process_redis_events import Stream, dlq_data, parse_fields

FIXTURE = json.loads(
    (Path(__file__).parents[2] / "fixtures" / "stream-format.json").read_text()
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")


@pytest.mark.parametrize("case", FIXTURE["messages"], ids=lambda c: c["stream"])
async def test_add_writes_the_fixture_bytes(case):
    r = Redis.from_url(REDIS_URL)
    try:
        await r.ping()
    except Exception:
        pytest.skip("no Redis")
    name = f"test-pre-{uuid.uuid4().hex[:8]}"
    s = Stream[dict](name=name, create_redis=lambda: Redis.from_url(REDIS_URL))
    try:
        assert (
            s.stream_key == f"stream:{name}"
            and case["key"] == f"stream:{case['stream']}"
        )
        await s.add(case["payload"])
        ((_, fields),) = await r.xrange(s.stream_key)
        assert {k.decode(): v.decode() for k, v in fields.items()} == case["fields"]
    finally:
        await r.delete(s.stream_key)
        await r.aclose()
        await s.redis.aclose()


@pytest.mark.parametrize("case", FIXTURE["messages"] + FIXTURE["legacy"], ids=str)
def test_fields_parse_to_the_payload(case):
    assert parse_fields(case["fields"]) == (case["payload"], None)


@pytest.mark.parametrize(
    "case", FIXTURE["deadLetters"], ids=lambda c: c["original"]["id"]
)
def test_dead_letters_match_the_fixture(case):
    o = case["original"]
    payload, error = (
        parse_fields(o["fields"]) if "fields" in o else (o["payload"], None)
    )
    data = json.loads(
        dlq_data(
            payload,
            original_id=o["id"],
            attempts=o["attempts"],
            consumer_group=o["consumerGroup"],
            consumer_id=o["consumerId"],
            raw_fields=o.get("fields"),
            error=str(error) if error else None,
        )
    )
    expected = case["data"]
    meta, want = data.pop("_dlqMetadata"), dict(expected["_dlqMetadata"])
    assert data == {k: v for k, v in expected.items() if k != "_dlqMetadata"}
    assert isinstance(meta.pop("timestamp"), int)
    want.pop("timestamp")
    if "error" in want:
        assert meta.pop("error") and want.pop("error")
    assert meta == want
    assert Stream(name="product-updated", create_redis=lambda: None).dlq_stream_key == "dlq:product-updated"  # type: ignore[arg-type,return-value]
