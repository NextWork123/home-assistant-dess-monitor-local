"""Unit tests for coordinator result classification (NAK / error / QPIGS).

Imports helpers and a lightly constructed DirectCoordinator without
running a full Home Assistant update cycle.
"""
from unittest.mock import MagicMock

import pytest

pytest.importorskip("homeassistant")

from custom_components.dess_monitor_local.coordinators.direct_coordinator import (  # noqa: E402
    DirectCoordinator,
    _is_error_result,
    _is_nak_result,
)
from custom_components.dess_monitor_local.coordinators.failure_tracker import (  # noqa: E402
    FailureTracker,
)


class TestResultHelpers:
    def test_empty_is_error(self):
        assert _is_error_result({}) is True
        assert _is_error_result(None) is True

    def test_error_dict_is_error(self):
        assert _is_error_result({"error": "CRC"}) is True

    def test_nak_status_is_error_and_nak(self):
        r = {"status": "NAK"}
        assert _is_error_result(r) is True
        assert _is_nak_result(r) is True

    def test_nak_message_is_nak(self):
        r = {"error": "NAK response received. Command not accepted."}
        assert _is_nak_result(r) is True

    def test_good_qpigs_not_error(self):
        r = {"battery_voltage": "26.5"}
        assert _is_error_result(r) is False
        assert _is_nak_result(r) is False


def _coord() -> DirectCoordinator:
    hass = MagicMock()
    hass.data = {}
    entry = MagicMock()
    entry.options = {"update_interval": 10}
    entry.entry_id = "test-entry"
    entry.data = {"name": "Test"}
    c = DirectCoordinator(hass, entry, targets=[])
    c._failures = FailureTracker(6)
    return c


class TestAcceptResult:
    def test_error_dict_does_not_count_as_success(self):
        c = _coord()
        out = c._accept_result("dev", "QPIGS", {"error": "CRC fail"})
        assert out is None
        assert c._failures.count("dev", "QPIGS") == 0

    def test_nak_marks_toward_unsupported(self):
        c = _coord()
        assert c._accept_result("dev", "QPIGS2", {"error": "NAK response"}) is None
        assert c._is_unsupported("dev", "QPIGS2") is False
        assert c._accept_result("dev", "QPIGS2", {"status": "NAK"}) is None
        assert c._is_unsupported("dev", "QPIGS2") is True

    def test_implausible_qpigs_rejected(self):
        c = _coord()
        bad = {
            "battery_voltage": "26.5",
            "battery_charging_current": "447",
            "battery_discharge_current": "0",
        }
        assert c._accept_result("dev", "QPIGS", bad) is None
        assert c._failures.count("dev", "QPIGS") == 0

    def test_good_qpigs_resets_failures(self):
        c = _coord()
        c._failures.on_failure("dev", "QPIGS")
        good = {
            "battery_voltage": "26.5",
            "battery_charging_current": "10",
            "battery_discharge_current": "0",
        }
        out = c._accept_result("dev", "QPIGS", good)
        assert out == good
        assert c._failures.count("dev", "QPIGS") == 0
