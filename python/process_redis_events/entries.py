"""Turning a stream entry's fields into the message payload, as the TypeScript library's parseEntry()."""

from __future__ import annotations

import json
from typing import Any

from process_redis_events.dlq import decode_fields


def parse_fields(fields: dict[Any, Any] | None) -> tuple[Any, Exception | None]:
    """(payload, parse_error). ``add()`` writes a single ``data`` field holding JSON; anything else (a legacy
    ``id <x>`` message) becomes an object of its fields (strings). An entry without fields gives None. A ``data``
    field that isn't valid JSON gives (None, the error)."""
    if not fields:
        return None, None
    decoded = decode_fields(fields)
    if list(decoded) == ["data"]:
        try:
            return json.loads(decoded["data"]), None
        except ValueError as e:
            return None, e
    return decoded, None


# Resets the idle time of the ids still owned by this consumer and returns the ids it no longer owns (acked, or
# claimed by another consumer after a lease expired). A plain XCLAIM ... IDLE 0 would take a message back from the
# consumer that reclaimed it, and both would then process it.
HEARTBEAT_LUA = """
local key, group, consumer = KEYS[1], ARGV[1], ARGV[2]
local lost = {}
for i = 3, #ARGV do
  local p = redis.call('XPENDING', key, group, ARGV[i], ARGV[i], 1)
  if p[1] and p[1][2] == consumer then
    redis.call('XCLAIM', key, group, consumer, 0, ARGV[i], 'IDLE', 0, 'JUSTID')
  else
    lost[#lost + 1] = ARGV[i]
  end
end
return lost
"""


# How far a group is behind, counted from the entry ids (as clok's monitor-streams), not XINFO GROUPS' `lag`:
# that's entries-added minus the group's entries-read counter, which Redis 7.2 doesn't keep across an AOF reload
# (XREADGROUP goes to the AOF as XCLAIM ... LASTID, which moves the last-delivered-id but not entries-read), so
# after a Redis restart it can stay wrong for good. Counts on the server, returning only numbers: the entries after
# the group's last-delivered-id, at most `limit`; past that, XLEN minus the entries up to it, if those are at most
# `limit` (true once the stream is trimmed), else limit + 1 (a lower bound). Returns {undelivered, pending, exact}
# or false without the stream or the group.
BACKLOG_LUA = """
local function numcmp(a, b)
  if #a ~= #b then
    if #a < #b then return -1 else return 1 end
  end
  if a == b then return 0 end
  if a < b then return -1 else return 1 end
end
local function idcmp(a, b)
  local ams, aseq = string.match(a, '^(%d+)-(%d+)$')
  local bms, bseq = string.match(b, '^(%d+)-(%d+)$')
  local c = numcmp(ams, bms)
  if c ~= 0 then return c end
  return numcmp(aseq, bseq)
end
local key, name, limit = KEYS[1], ARGV[1], tonumber(ARGV[2])
if redis.call('EXISTS', key) == 0 then return false end
local group = nil
for _, reply in ipairs(redis.call('XINFO', 'GROUPS', key)) do
  local t = {}
  for i = 1, #reply, 2 do t[reply[i]] = reply[i + 1] end
  if t['name'] == name then group = t end
end
if not group then return false end
local function count(start, stop, max)
  local n = 0
  while n < max do
    local batch = redis.call('XRANGE', key, start, stop, 'COUNT', math.min(1000, max - n))
    if #batch == 0 then return n end
    n = n + #batch
    start = '(' .. batch[#batch][1]
  end
  return n
end
local delivered = group['last-delivered-id']
local last = redis.call('XREVRANGE', key, '+', '-', 'COUNT', 1)[1]
if not last or idcmp(delivered, last[1]) >= 0 then return {0, group['pending'], 1} end
local undelivered = count('(' .. delivered, '+', limit + 1)
if undelivered <= limit then return {undelivered, group['pending'], 1} end
local kept = count('-', delivered, limit + 1)
if kept <= limit then return {redis.call('XLEN', key) - kept, group['pending'], 1} end
return {undelivered, group['pending'], 0}
"""


def parse_backlog(reply: Any) -> tuple[int, int, bool] | None:
    if not reply:
        return None
    undelivered, pending, exact = reply
    return int(undelivered), int(pending), int(exact) == 1


def group_backlog(
    r: Any, key: str, group: str, limit: int = 10_000
) -> tuple[int, int, bool] | None:
    """(undelivered, pending, exact) of a consumer group on the stream at ``key`` (sync client), counted from the
    entry ids in one atomic step; None when the stream or the group doesn't exist. undelivered = entries not yet
    given to any of its consumers; a lower bound (limit + 1) when not exact."""
    return parse_backlog(r.eval(BACKLOG_LUA, 1, key, group, limit))


async def agroup_backlog(
    r: Any, key: str, group: str, limit: int = 10_000
) -> tuple[int, int, bool] | None:
    """group_backlog() for an asyncio client."""
    return parse_backlog(await r.eval(BACKLOG_LUA, 1, key, group, limit))
