"""Async tests for the command queue and Modbus framing.

No pytest-asyncio dependency — each test drives the coroutine with
``asyncio.run`` and fakes the stream pair so no real socket is opened.
"""
import asyncio

import pytest

from custom_components.dess_monitor_local.api.commands.direct_command_queue import (
    PRIORITY_POLL,
    PRIORITY_USER,
    CommandQueue,
    QueueRegistry,
    is_tcp_transport,
    run_on_bus,
    transport_key,
)
from custom_components.dess_monitor_local.api.crc import crc16_modbus
from custom_components.dess_monitor_local.api.protocols import modbus_rtu
from custom_components.dess_monitor_local.const import (
    BUS_MODE_CONCURRENT_WRITES,
    BUS_MODE_SERIALIZED,
    DOMAIN,
)


# ---------------------------------------------------------------------------
# CommandQueue
# ---------------------------------------------------------------------------
class TestCommandQueue:
    def test_preserves_order_and_results(self):
        async def scenario():
            q = CommandQueue(min_delay=0.0)
            await q.start()
            order = []

            def make(n):
                async def fn():
                    order.append(n)
                    return n * 10
                return fn

            results = await asyncio.gather(
                q.enqueue(make(1)), q.enqueue(make(2)), q.enqueue(make(3))
            )
            await q.stop()
            return order, results

        order, results = asyncio.run(scenario())
        # Serialized through the single worker, in submission order.
        assert order == [1, 2, 3]
        assert results == [10, 20, 30]

    def test_exception_propagates_to_caller(self):
        async def scenario():
            q = CommandQueue(min_delay=0.0)
            await q.start()

            async def boom():
                raise RuntimeError("device NAK")

            try:
                with pytest.raises(RuntimeError, match="device NAK"):
                    await q.enqueue(boom)
            finally:
                await q.stop()

        asyncio.run(scenario())

    def test_one_failure_does_not_kill_worker(self):
        async def scenario():
            q = CommandQueue(min_delay=0.0)
            await q.start()

            async def boom():
                raise ValueError("x")

            async def ok():
                return 42

            with pytest.raises(ValueError):
                await q.enqueue(boom)
            # Worker must still process the next command.
            result = await q.enqueue(ok)
            await q.stop()
            return result

        assert asyncio.run(scenario()) == 42

    def test_stop_drains_cleanly(self):
        async def scenario():
            q = CommandQueue(min_delay=0.0)
            await q.start()
            await q.enqueue(lambda: _const(1))
            # stop() awaits the cancelled worker — must not raise.
            await q.stop()
            return True

        assert asyncio.run(scenario()) is True


    def test_user_priority_beats_queued_poll(self):
        async def scenario():
            q = CommandQueue(min_delay=0.0)
            await q.start()
            order = []
            started = asyncio.Event()
            release = asyncio.Event()

            async def slow_poll():
                started.set()
                await release.wait()
                order.append("poll")
                return "poll"

            async def user_cmd():
                order.append("user")
                return "user"

            poll1 = asyncio.create_task(
                q.enqueue(slow_poll, priority=PRIORITY_POLL)
            )
            await started.wait()
            poll2 = asyncio.create_task(
                q.enqueue(lambda: _const("poll2"), priority=PRIORITY_POLL)
            )
            user = asyncio.create_task(
                q.enqueue(user_cmd, priority=PRIORITY_USER)
            )
            await asyncio.sleep(0.01)
            release.set()
            results = await asyncio.gather(poll1, poll2, user)
            await q.stop()
            return order, results

        order, results = asyncio.run(scenario())
        assert order == ["poll", "user"]
        assert results[0] == "poll"
        assert "user" in results
        assert "poll2" in results


class TestTransportKey:
    def test_tcp_host_port(self):
        assert transport_key("tcp://192.168.1.5:8899") == "tcp:192.168.1.5:8899"

    def test_bare_serial(self):
        assert transport_key("/dev/ttyUSB0") == "serial:/dev/ttyUSB0"

    def test_is_tcp_transport(self):
        assert is_tcp_transport("tcp://1.2.3.4:8899") is True
        assert is_tcp_transport("/dev/ttyUSB0") is False


class TestQueueRegistry:
    def test_per_key_isolation(self):
        async def scenario():
            reg = QueueRegistry(min_delay=0.0)
            order = []

            async def a():
                order.append("a")
                await asyncio.sleep(0.05)
                return "a"

            async def b():
                order.append("b")
                return "b"

            ra, rb = await asyncio.gather(
                reg.enqueue("e1", "tcp://1.1.1.1:8899", a),
                reg.enqueue("e1", "tcp://2.2.2.2:8899", b),
            )
            await reg.release_entry("e1")
            return order, ra, rb

        order, ra, rb = asyncio.run(scenario())
        assert ra == "a" and rb == "b"
        assert "a" in order and "b" in order

    def test_release_stops_unused_queue(self):
        async def scenario():
            reg = QueueRegistry(min_delay=0.0)
            await reg.enqueue("e1", "tcp://1.1.1.1:8899", lambda: _const(1))
            key = transport_key("tcp://1.1.1.1:8899")
            assert key in reg._queues
            await reg.release_entry("e1")
            return key not in reg._queues

        assert asyncio.run(scenario()) is True

    def test_shared_key_survives_partial_release(self):
        async def scenario():
            reg = QueueRegistry(min_delay=0.0)
            uri = "tcp://1.1.1.1:8899"
            await reg.enqueue("e1", uri, lambda: _const(1))
            await reg.enqueue("e2", uri, lambda: _const(2))
            await reg.release_entry("e1")
            key = transport_key(uri)
            still = key in reg._queues
            await reg.release_entry("e2")
            gone = key not in reg._queues
            return still, gone

        still, gone = asyncio.run(scenario())
        assert still is True
        assert gone is True


class TestRunOnBus:
    def test_concurrent_writes_bypasses_queue_for_user_tcp(self):
        async def scenario():
            class _Hass:
                def __init__(self):
                    self.data = {}

            hass = _Hass()
            called = {"n": 0}

            async def fn():
                called["n"] += 1
                return "ok"

            out = await run_on_bus(
                hass,
                "entry",
                "tcp://1.2.3.4:8899",
                fn,
                priority=PRIORITY_USER,
                bus_mode=BUS_MODE_CONCURRENT_WRITES,
            )
            registry = hass.data.get(DOMAIN, {}).get("queue_registry")
            return out, called["n"], registry is None or not registry._queues

        out, n, empty = asyncio.run(scenario())
        assert out == "ok" and n == 1 and empty is True

    def test_serialized_user_uses_queue(self):
        async def scenario():
            class _Hass:
                def __init__(self):
                    self.data = {}

            hass = _Hass()

            async def fn():
                return 7

            out = await run_on_bus(
                hass,
                "entry",
                "tcp://1.2.3.4:8899",
                fn,
                priority=PRIORITY_USER,
                bus_mode=BUS_MODE_SERIALIZED,
            )
            registry = hass.data[DOMAIN]["queue_registry"]
            await registry.release_entry("entry")
            return out

        assert asyncio.run(scenario()) == 7


async def _const(v):
    return v


# ---------------------------------------------------------------------------
# Fake asyncio stream pair
# ---------------------------------------------------------------------------
class _FakeReader:
    def __init__(self, buffer: bytes):
        self._buf = buffer
        self._pos = 0

    async def readexactly(self, n: int) -> bytes:
        chunk = self._buf[self._pos:self._pos + n]
        if len(chunk) < n:
            raise asyncio.IncompleteReadError(chunk, n)
        self._pos += n
        return chunk


class _FakeWriter:
    def __init__(self):
        self.written = bytearray()
        self.closed = False

    def write(self, data: bytes):
        self.written.extend(data)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


def _modbus_response(unit: int, func: int, data: bytes) -> bytes:
    """Assemble a valid func-0x03 read response: unit, func, byte_count,
    data, CRC (lo, hi)."""
    hdr = bytes([unit, func])
    bc = bytes([len(data)])
    crc = crc16_modbus(hdr + bc + data)
    return hdr + bc + data + bytes([crc & 0xFF, (crc >> 8) & 0xFF])


def _modbus_echo(unit: int, func: int, addr: int, value: int) -> bytes:
    """Assemble the 8-byte write echo: unit, func, addr(2), value(2), CRC."""
    body = bytes([unit, func, (addr >> 8) & 0xFF, addr & 0xFF,
                  (value >> 8) & 0xFF, value & 0xFF])
    crc = crc16_modbus(body)
    return body + bytes([crc & 0xFF, (crc >> 8) & 0xFF])


def _patch_open_connection(monkeypatch, reader, writer):
    async def fake_open(host, port):
        return reader, writer
    monkeypatch.setattr(modbus_rtu.asyncio, "open_connection", fake_open)


# ---------------------------------------------------------------------------
# read_modbus_block
# ---------------------------------------------------------------------------
class TestReadModbusBlock:
    def test_request_frame_bytes(self, monkeypatch):
        writer = _FakeWriter()
        # Two registers in the response so the read completes.
        reader = _FakeReader(_modbus_response(1, 3, bytes([0x00, 0x01, 0x00, 0x02])))
        _patch_open_connection(monkeypatch, reader, writer)

        asyncio.run(modbus_rtu.read_modbus_block("h", 502, start=201, count=2))

        req = bytes(writer.written)
        # unit=1, func=3, start=201 (0x00C9), count=2, then CRC.
        assert req[0] == 1
        assert req[1] == 3
        assert req[2:4] == bytes([0x00, 0xC9])
        assert req[4:6] == bytes([0x00, 0x02])
        crc = crc16_modbus(req[:6])
        assert req[6:8] == bytes([crc & 0xFF, (crc >> 8) & 0xFF])

    def test_parses_registers(self, monkeypatch):
        writer = _FakeWriter()
        reader = _FakeReader(_modbus_response(1, 3, bytes([0x12, 0x34, 0x56, 0x78])))
        _patch_open_connection(monkeypatch, reader, writer)

        regs = asyncio.run(modbus_rtu.read_modbus_block("h", 502, start=201, count=2))
        assert regs == [0x1234, 0x5678]

    def test_crc_mismatch_raises(self, monkeypatch):
        writer = _FakeWriter()
        good = bytearray(_modbus_response(1, 3, bytes([0x00, 0x01])))
        good[-1] ^= 0xFF  # corrupt CRC high byte
        reader = _FakeReader(bytes(good))
        _patch_open_connection(monkeypatch, reader, writer)

        with pytest.raises(Exception):
            asyncio.run(modbus_rtu.read_modbus_block("h", 502, start=201, count=1))


# ---------------------------------------------------------------------------
# write_modbus_single_register
# ---------------------------------------------------------------------------
class TestWriteModbusSingleRegister:
    def test_func06_happy_path(self, monkeypatch):
        writer = _FakeWriter()
        reader = _FakeReader(_modbus_echo(1, 0x06, 426, 1))
        _patch_open_connection(monkeypatch, reader, writer)

        out = asyncio.run(
            modbus_rtu.write_modbus_single_register("h", 502, 426, 1)
        )
        assert out["status"] == "OK"
        assert out["func"] == 0x06
        # Request: unit, 0x06, addr(2), value(2), CRC.
        req = bytes(writer.written)
        assert req[1] == 0x06
        assert req[2:4] == bytes([0x01, 0xAA])  # 426 = 0x01AA
        assert req[4:6] == bytes([0x00, 0x01])

    def test_falls_back_to_func10(self, monkeypatch):
        # First connection (func 0x06) gets a CRC-bad reply -> raises ->
        # the code retries with func 0x10, which we answer correctly.
        attempts = {"n": 0}
        good10 = _modbus_echo(1, 0x10, 301, 2)

        async def fake_open(host, port):
            attempts["n"] += 1
            if attempts["n"] == 1:
                bad = bytearray(_modbus_echo(1, 0x06, 301, 2))
                bad[-1] ^= 0xFF
                return _FakeReader(bytes(bad)), _FakeWriter()
            return _FakeReader(good10), _FakeWriter()

        monkeypatch.setattr(modbus_rtu.asyncio, "open_connection", fake_open)

        out = asyncio.run(
            modbus_rtu.write_modbus_single_register("h", 502, 301, 2)
        )
        assert out["status"] == "OK"
        assert out["func"] == 0x10
        assert attempts["n"] == 2

    def test_both_fail_returns_error(self, monkeypatch):
        async def fake_open(host, port):
            bad = bytearray(_modbus_echo(1, 0x06, 1, 1))
            bad[-1] ^= 0xFF
            return _FakeReader(bytes(bad)), _FakeWriter()

        monkeypatch.setattr(modbus_rtu.asyncio, "open_connection", fake_open)

        out = asyncio.run(
            modbus_rtu.write_modbus_single_register("h", 502, 1, 1)
        )
        assert "error" in out
