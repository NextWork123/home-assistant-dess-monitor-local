"""Per-transport command queues with user-over-poll priority.

Background polling and user writes share one worker **per transport
identity** (``host:port`` or serial path), not one global FIFO. User
commands use priority 0 so they jump ahead of queued poll commands after
the in-flight I/O completes — restoring Elfin/direct-TCP set latency
without allowing unrestricted dual TCP clients by default.
"""
from __future__ import annotations

import asyncio
import itertools
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urlparse

# Lower number = higher priority (asyncio.PriorityQueue).
PRIORITY_USER = 0
PRIORITY_POLL = 1

_REGISTRY_KEY = "queue_registry"


def transport_key(uri: str) -> str:
    """Stable identity for the physical bus / TCP endpoint.

    Two URIs that talk to the same Elfin bridge or serial port must share
    one queue so frames never interleave on a half-duplex link.
    """
    if not uri:
        return ""
    if "://" not in uri:
        # Bare serial path (``/dev/ttyUSB0``, ``COM3``).
        return f"serial:{uri}"
    parsed = urlparse(uri)
    scheme = (parsed.scheme or "").lower()
    if scheme in ("eybond", "eybond-pi18", "eybond-modbus"):
        # EyBond bypasses this registry; key is only used if someone enqueues.
        return f"eybond:{parsed.netloc or uri}"
    if scheme in ("tcp", "pi18", "modbus", "agent"):
        netloc = parsed.netloc or ""
        return f"{scheme}:{netloc.lower()}"
    if scheme in ("pi18-serial", "serial"):
        return f"serial:{parsed.path or uri}"
    return uri


def is_tcp_transport(uri: str) -> bool:
    """True when a second concurrent TCP session is physically possible."""
    return uri.startswith(("tcp://", "pi18://", "modbus://", "agent://"))


class CommandQueue:
    """Async priority queue for one transport endpoint."""

    def __init__(self, min_delay: float = 0.3):
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._lock = asyncio.Lock()
        self._worker_task: asyncio.Task | None = None
        self.min_delay = min_delay
        self._seq = itertools.count()

    async def start(self):
        if not self._worker_task:
            self._worker_task = asyncio.create_task(self._worker())

    async def stop(self):
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

    async def enqueue(
        self,
        fn: Callable[[], Awaitable[Any]],
        desc: str = "",
        *,
        priority: int = PRIORITY_POLL,
    ) -> Any:
        """Add a command; lower ``priority`` runs first."""
        fut = asyncio.get_running_loop().create_future()
        await self._queue.put((priority, next(self._seq), fn, fut, desc))
        return await fut

    async def _worker(self):
        while True:
            _prio, _seq, fn, fut, desc = await self._queue.get()
            try:
                async with self._lock:
                    result = await fn()
                    if not fut.done():
                        fut.set_result(result)
            except Exception as e:
                if not fut.done():
                    fut.set_exception(e)
            finally:
                await asyncio.sleep(self.min_delay)
                self._queue.task_done()


class QueueRegistry:
    """Lazy per-transport queues with per-config-entry ownership."""

    def __init__(self, min_delay: float = 0.3):
        self.min_delay = min_delay
        self._queues: dict[str, CommandQueue] = {}
        self._refcount: dict[str, int] = {}
        self._entry_keys: dict[str, set[str]] = {}
        self._ensure_lock = asyncio.Lock()

    async def enqueue(
        self,
        entry_id: str,
        uri: str,
        fn: Callable[[], Awaitable[Any]],
        *,
        priority: int = PRIORITY_POLL,
        desc: str = "",
    ) -> Any:
        key = transport_key(uri)
        queue = await self._ensure(entry_id, key)
        return await queue.enqueue(fn, desc=desc, priority=priority)

    async def _ensure(self, entry_id: str, key: str) -> CommandQueue:
        async with self._ensure_lock:
            if key not in self._queues:
                queue = CommandQueue(min_delay=self.min_delay)
                await queue.start()
                self._queues[key] = queue
                self._refcount[key] = 0
            owned = self._entry_keys.setdefault(entry_id, set())
            if key not in owned:
                owned.add(key)
                self._refcount[key] = self._refcount.get(key, 0) + 1
            return self._queues[key]

    async def release_entry(self, entry_id: str) -> None:
        """Drop ownership for an unloaded config entry; stop unused queues."""
        async with self._ensure_lock:
            for key in self._entry_keys.pop(entry_id, set()):
                left = self._refcount.get(key, 1) - 1
                if left <= 0:
                    self._refcount.pop(key, None)
                    queue = self._queues.pop(key, None)
                    if queue is not None:
                        await queue.stop()
                else:
                    self._refcount[key] = left


def get_queue_registry(hass) -> QueueRegistry:
    """Return the domain QueueRegistry, creating it if needed."""
    from custom_components.dess_monitor_local.const import DOMAIN

    domain_data = hass.data.setdefault(DOMAIN, {})
    registry = domain_data.get(_REGISTRY_KEY)
    if registry is None:
        registry = QueueRegistry(min_delay=0.3)
        domain_data[_REGISTRY_KEY] = registry
    return registry


async def run_on_bus(
    hass,
    entry_id: str,
    uri: str,
    fn: Callable[[], Awaitable[Any]],
    *,
    priority: int = PRIORITY_POLL,
    bus_mode: str | None = None,
    desc: str = "",
) -> Any:
    """Run ``fn`` via the per-transport queue, or bypass for concurrent writes.

    EyBond always bypasses (per-dongle locks live in the EyBond manager).
    ``concurrent_writes`` bypasses only for user-priority TCP I/O so set
    commands can open an independent session while polls stay serialized.
    """
    from custom_components.dess_monitor_local.const import (
        BUS_MODE_CONCURRENT_WRITES,
        DEFAULT_BUS_MODE,
    )

    if bus_mode is None:
        bus_mode = DEFAULT_BUS_MODE
    if uri.startswith("eybond"):
        return await fn()
    if (
        bus_mode == BUS_MODE_CONCURRENT_WRITES
        and priority == PRIORITY_USER
        and is_tcp_transport(uri)
    ):
        return await fn()
    registry = get_queue_registry(hass)
    return await registry.enqueue(
        entry_id, uri, fn, priority=priority, desc=desc
    )
