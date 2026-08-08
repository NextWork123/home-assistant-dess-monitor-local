import asyncio
import logging
import time
from datetime import datetime, timedelta

import async_timeout
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
)

from custom_components.dess_monitor_local import diag_hub
from custom_components.dess_monitor_local.api.commands.direct_command_queue import (
    PRIORITY_POLL,
    PRIORITY_USER,
    run_on_bus,
)
from custom_components.dess_monitor_local.api.dispatcher import get_direct_data
from custom_components.dess_monitor_local.const import (
    CONF_BUS_MODE,
    CONF_DEVICE,
    CONF_NAME,
    CONF_PROTOCOL,
    CONF_STRICT_CRC,
    CONF_UPDATE_INTERVAL,
    DEFAULT_BUS_MODE,
    DEFAULT_STRICT_CRC,
    DEFAULT_UPDATE_INTERVAL,
    PROTOCOL_VOLTRONIC,
)
from custom_components.dess_monitor_local.coordinators.device_target import DeviceTarget
from custom_components.dess_monitor_local.coordinators.failure_tracker import (
    FailureOutcome,
    FailureTracker,
)
from custom_components.dess_monitor_local.sanity import is_plausible_qpigs

_LOGGER = logging.getLogger(__name__)

# Consecutive NAKs before a command is treated as unsupported for this
# coordinator lifetime (avoids burning bus time on QPIGS2/QFWS forever,
# without suppressing QPIGS after a single EMI glitch).
_NAK_SUPPRESS_THRESHOLD = 2


def _is_error_result(result) -> bool:
    """True when a decode/transport outcome must not replace last-known data."""
    if not result or not isinstance(result, dict):
        return True
    if "error" in result:
        return True
    if result.get("status") == "NAK":
        return True
    return False


def _is_nak_result(result) -> bool:
    if not isinstance(result, dict):
        return False
    if result.get("status") == "NAK":
        return True
    err = result.get("error")
    return isinstance(err, str) and "NAK" in err


class DirectCoordinator(DataUpdateCoordinator):
    """My custom coordinator."""
    devices = []

    # Resilience to transient transport errors (CRC mismatches, brief
    # buffer corruption, gateway hiccups). One fast retry per command,
    # then up to N-1 consecutive failures fall back to the last known
    # sub-dict before the entity finally goes to "unavailable".
    _RETRY_DELAY_S = 0.25
    # 6 (not 3): EyBond dongles clean-close every ~4s and reconnect in ~1s, so a
    # child's poll occasionally lands in a gap and fails. With the now-short
    # cycles a child reaches 3 consecutive failures in ~30s, flickering entities
    # to "unavailable" during normal cycling. Tolerate more transient misses —
    # stay on frozen last-known — before flipping unavailable.
    _MAX_CONSECUTIVE_FAILURES = 6
    # Per-command poll cadence (poll every Nth cycle; carry forward in between).
    # Live telemetry every cycle keeps values fresh and the per-cycle command
    # count tiny (fits a cycling dongle's brief window → far fewer failures);
    # static/slow data is refreshed periodically. (cmd, section, every_n_cycles)
    _CMD_SCHEDULE = (
        ("QPIGS", "qpigs", 1),    # live telemetry — every cycle
        ("QMOD", "qmod", 2),      # operating mode — changes slowly
        ("QPIGS2", "qpigs2", 2),  # 2nd MPPT (often NAK'd)
        ("QPIWS", "qpiws", 3),    # warnings/faults (PI30)
        ("QFWS", "qfws", 3),      # warnings/faults (PI18)
        ("QPIRI", "qpiri", 6),    # ratings + editable settings — confirm changes faster
    )
    # Per-device poll bound, applied ONLY with multiple devices (an EyBond hub
    # with several children). One stuck/half-attentive dongle answering FC=4
    # slowly used to drag the whole-hub gather past the 120s cycle cap, which
    # failed the entire update and starved EVERY child of data ("one update per
    # ~30 min" in the field). Capping each child means a stuck one freezes on
    # last-known for the cycle while the healthy ones still publish on time.
    # Single-device entries stay uncapped — a legacy cloud-proxied transport can
    # legitimately take minutes and has no sibling to starve.
    # With per-dongle parallelism the cycle is the SLOWEST child (not the sum),
    # and the publish is atomic at that point, so keep the bound tight: a child
    # stuck past this (its dongle gone for the whole poll) freezes on last-known
    # rather than dragging every entity's update behind it.
    _PER_DEVICE_POLL_TIMEOUT = 25.0

    def __init__(self, hass: HomeAssistant, config_entry, targets=None):
        """Initialize my coordinator.

        ``targets`` is an explicit list of :class:`DeviceTarget` to poll
        (used by the EyBond hub, where children are derived from the
        discovery registry). When ``None``, the coordinator falls back to
        the legacy single ``CONF_DEVICE`` from the entry options.
        """
        interval_seconds = int(
            config_entry.options.get(CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL)
        )
        super().__init__(
            hass,
            _LOGGER,
            # Name of the data. For logging purposes.
            name="Direct request sensor",
            config_entry=config_entry,
            update_interval=timedelta(seconds=interval_seconds),
            # Set always_update to `False` if the data returned from the
            # api can be compared via `__eq__` to avoid duplicate updates
            # being dispatched to listeners
            always_update=False

        )
        self._targets = targets
        # Per-(target id, command) consecutive-failure counter + freeze policy.
        self._failures = FailureTracker(self._MAX_CONSECUTIVE_FAILURES)
        # Commands that consistently NAK (e.g. QPIGS2 on single-MPPT units).
        self._unsupported: set[tuple[str, str]] = set()
        self._nak_counts: dict[tuple[str, str], int] = {}
        # Cycle counter driving the split poll cadence (see _CMD_SCHEDULE).
        self._cycle = 0

    def _bus_mode(self) -> str:
        return self.config_entry.options.get(CONF_BUS_MODE, DEFAULT_BUS_MODE)

    def _is_unsupported(self, key: str, cmd: str) -> bool:
        return (key, cmd) in self._unsupported

    def _note_nak(self, key: str, cmd: str) -> bool:
        """Count a NAK; return True once the command is marked unsupported."""
        pair = (key, cmd)
        if pair in self._unsupported:
            return True
        count = self._nak_counts.get(pair, 0) + 1
        self._nak_counts[pair] = count
        if count >= _NAK_SUPPRESS_THRESHOLD:
            self._unsupported.add(pair)
            _LOGGER.info(
                "%s/%s NAK'd %d times; skipping for this session",
                key, cmd, count,
            )
            return True
        return False

    def _clear_nak_streak(self, key: str, cmd: str) -> None:
        self._nak_counts.pop((key, cmd), None)

    def _accept_result(self, key: str, cmd: str, result: dict) -> dict | None:
        """Classify a raw decode: return cleaned data, or None to retry/fail.

        Error/NAK dicts must never call ``on_success`` or overwrite last-known
        good sections (that was the main sensor-unknown regression path).
        """
        if _is_nak_result(result):
            self._note_nak(key, cmd)
            return None
        if _is_error_result(result):
            return None
        if cmd == "QPIGS" and not is_plausible_qpigs(result):
            _LOGGER.warning(
                "%s/%s: implausible QPIGS rejected "
                "(chg=%s dis=%s V=%s)",
                key,
                cmd,
                result.get("battery_charging_current"),
                result.get("battery_discharge_current"),
                result.get("battery_voltage"),
            )
            return None
        self._clear_nak_streak(key, cmd)
        self._failures.on_success(key, cmd)
        return result

    async def _async_setup(self):
        """Set up the coordinator

        This is the place to set up your coordinator,
        or to load data, that only needs to be loaded once.

        This method will be called automatically during
        coordinator.async_config_entry_first_refresh.
        """
        self.devices = await self.get_active_devices()

    def set_targets(self, targets) -> None:
        """Swap the explicit poll-target list at runtime.

        Used by the EyBond hub's in-place child reconcile (no entry reload):
        the next poll cycle reads ``self.devices``, and ``_async_update_data``
        snapshots it per cycle, so replacing the list between cycles is safe.
        """
        self._targets = list(targets)
        self.devices = list(targets)

    def _child_failure_summary(self) -> dict:
        """Per-child ``"ok"`` / ``"fail:N"`` for the debug panel's cycle event.

        ``FailureTracker._counts`` is ``{device: {command: consecutive_fails}}``;
        sum a child's command failures.
        """
        counts = getattr(self._failures, "_counts", {}) or {}
        out: dict = {}
        for target in self.devices:
            key = getattr(target, "id", target)
            n = sum((counts.get(key) or {}).values())
            out[key] = "ok" if n == 0 else f"fail:{n}"
        return out

    async def get_active_devices(self):
        # Explicit targets (EyBond hub children) take precedence.
        if self._targets is not None:
            return list(self._targets)

        device = self.config_entry.options.get(CONF_DEVICE, None)
        if not device:
            # No device URI configured (entry created but never finished setup,
            # or options got wiped). Returning an empty list lets the
            # coordinator complete without crashing in `_async_update_data`
            # where dispatcher code does ``device.startswith(...)``.
            _LOGGER.warning(
                "No device URI configured for entry %s; nothing to poll",
                self.config_entry.entry_id,
            )
            return []
        # Legacy single-device entry: id == uri preserves existing
        # entity unique_ids and HA device identifiers.
        protocol = self.config_entry.options.get(CONF_PROTOCOL, PROTOCOL_VOLTRONIC)
        name = self.config_entry.data.get(CONF_NAME) or "Inverter"
        return [DeviceTarget(id=device, uri=device, protocol=protocol, name=name)]

    async def async_refresh_command(self, key: str, cmd: str, section: str) -> None:
        """Force-read one command for one device NOW and publish it.

        Used right after a write (set) so the affected section (e.g. QPIRI
        for a priority/current change) is confirmed immediately instead of
        waiting for its scheduled cadence — which is what made settings
        appear to "revert" in the UI.
        """
        uri = next((t.uri for t in self.devices if t.id == key), None)
        if uri is None:
            return
        strict_crc = bool(
            self.config_entry.options.get(CONF_STRICT_CRC, DEFAULT_STRICT_CRC)
        )
        entry_id = self.config_entry.entry_id
        bus_mode = self._bus_mode()
        try:
            result = await run_on_bus(
                self.hass,
                entry_id,
                uri,
                lambda: get_direct_data(uri, cmd, 30, strict_crc=strict_crc),
                priority=PRIORITY_USER,
                bus_mode=bus_mode,
                desc=f"refresh {cmd}",
            )
        except Exception:
            result = None
        accepted = self._accept_result(key, cmd, result) if result else None
        if not accepted:
            return
        data = dict(self.data or {})
        dev = dict(data.get(key) or {})
        dev[section] = accepted
        data[key] = dev
        self.async_set_updated_data(data)

    async def _async_update_data(self):
        strict_crc = bool(
            self.config_entry.options.get(CONF_STRICT_CRC, DEFAULT_STRICT_CRC)
        )
        prev_data = self.data or {}
        entry_id = self.config_entry.entry_id
        bus_mode = self._bus_mode()

        async def fetch_with_retry(key: str, uri: str, cmd: str, section: str) -> dict:
            """Read a command with one fast retry, then apply the pure
            freeze/unavailable policy (see FailureTracker).

            ``key`` is the target's stable id (failure tracking + last-known
            lookup); ``uri`` is the transport address the command is sent to.
            """
            prev_section = (prev_data.get(key) or {}).get(section) or {}
            if self._is_unsupported(key, cmd):
                # Carry last non-error section (usually {}) without I/O.
                return prev_section if "error" not in prev_section else {}

            for attempt in range(2):
                try:
                    # EyBond bypasses the registry inside run_on_bus so hub
                    # children still poll in parallel (per-dongle send_lock).
                    result = await run_on_bus(
                        self.hass,
                        entry_id,
                        uri,
                        lambda d=uri, c=cmd: get_direct_data(
                            d, c, 30, strict_crc=strict_crc
                        ),
                        priority=PRIORITY_POLL,
                        bus_mode=bus_mode,
                        desc=f"poll {cmd}",
                    )
                except Exception as err:  # transport raised unexpectedly
                    _LOGGER.debug(
                        "%s/%s attempt %d raised %r", key, cmd, attempt + 1, err
                    )
                    result = None
                if result is not None:
                    if _is_nak_result(result):
                        # Unsupported / not implemented: do not burn the
                        # failure budget; after threshold, skip this cmd.
                        if self._note_nak(key, cmd):
                            return (
                                prev_section
                                if "error" not in prev_section
                                else {}
                            )
                        result = None
                    else:
                        accepted = self._accept_result(key, cmd, result)
                        if accepted is not None:
                            return accepted
                if attempt == 0:
                    await asyncio.sleep(self._RETRY_DELAY_S)

            count = self._failures.on_failure(key, cmd)
            last_known = prev_section if "error" not in prev_section else {}
            data, outcome = self._failures.resolve(count, last_known)
            if outcome is FailureOutcome.FREEZE:
                _LOGGER.debug(
                    "%s/%s read failed (consecutive=%d/%d); freezing on last known data",
                    key, cmd, count, self._MAX_CONSECUTIVE_FAILURES,
                )
            elif outcome is FailureOutcome.UNAVAILABLE:
                _LOGGER.warning(
                    "%s/%s failed %d times in a row; flipping to unavailable",
                    key, cmd, count,
                )
            return data

        try:
            async with async_timeout.timeout(120):
                cycle = self._cycle

                async def fetch_device_data(target):
                    key = target.id
                    uri = target.uri
                    prev = prev_data.get(key) or {}
                    # Split cadence (see _CMD_SCHEDULE): poll live telemetry
                    # (QPIGS) every cycle so values stay fresh AND the per-cycle
                    # command count stays small enough to fit a cycling dongle's
                    # brief connection window; static/rare sections (ratings,
                    # faults, mode, PV2) are polled every Nth cycle and carried
                    # forward in between, so a skipped slow command never empties
                    # a section or trips its failure counter.
                    sections: dict = {"timestamp": datetime.now()}
                    for cmd, section, cadence in self._CMD_SCHEDULE:
                        if cycle % cadence == 0:
                            sections[section] = await fetch_with_retry(
                                key, uri, cmd, section
                            )
                        else:
                            sections[section] = prev.get(section) or {}
                    return key, sections

                # With multiple devices (hub children), bound each one so a
                # single stuck dongle can't blow the 120s cycle and starve the
                # others (see _PER_DEVICE_POLL_TIMEOUT). A device that exceeds
                # the bound freezes on its last-known data for this cycle.
                per_device_timeout = (
                    self._PER_DEVICE_POLL_TIMEOUT if len(self.devices) > 1 else None
                )

                async def fetch_device_guarded(target):
                    if per_device_timeout is None:
                        return await fetch_device_data(target)
                    key = target.id
                    try:
                        return await asyncio.wait_for(
                            fetch_device_data(target), per_device_timeout
                        )
                    except TimeoutError:
                        _LOGGER.warning(
                            "%s: poll exceeded %.0fs — freezing on last-known "
                            "data this cycle so other devices still update",
                            key, per_device_timeout,
                        )
                        return key, dict(prev_data.get(key) or {})

                _t0 = time.monotonic()
                data_map = dict(
                    await asyncio.gather(*map(fetch_device_guarded, self.devices))
                )
                if diag_hub.active():
                    diag_hub.publish({
                        "t": "cycle",
                        "dur_s": round(time.monotonic() - _t0, 2),
                        "n": cycle,
                        "children": self._child_failure_summary(),
                    })
                self._cycle += 1  # advance the split-cadence schedule
                return data_map
        except TimeoutError as err:
            # Raising ConfigEntryAuthFailed will cancel future updates
            # and start a config flow with SOURCE_REAUTH (async_step_reauth)
            raise err
