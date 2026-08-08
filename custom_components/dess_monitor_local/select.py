import asyncio
import logging
import time

from homeassistant.components.select import SelectEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from custom_components.dess_monitor_local import HubConfigEntry
from custom_components.dess_monitor_local.api.commands.direct_command_queue import (
    PRIORITY_USER,
    run_on_bus,
)
from custom_components.dess_monitor_local.api.commands.direct_commands import (
    ChargeSourcePrioritySetting,
    OutputSourcePrioritySetting,
    set_charge_source_priority,
    set_max_combined_charge_current,
    set_max_utility_charge_current,
    set_output_source_priority,
)
from custom_components.dess_monitor_local.const import (
    CONF_BUS_MODE,
    DEFAULT_BUS_MODE,
    DOMAIN,
)
from custom_components.dess_monitor_local.coordinators.direct_coordinator import DirectCoordinator
from custom_components.dess_monitor_local.hub import InverterDevice

_LOGGER = logging.getLogger(__name__)

BATTERY_MODE_LI_VOLTAGE = "Lithium (Voltage)"
BATTERY_MODE_LI_BMS = "Lithium (BMS)"
BATTERY_MODE_LEAD_ACID = "Lead-acid"
BATTERY_MODES = (BATTERY_MODE_LI_VOLTAGE, BATTERY_MODE_LI_BMS, BATTERY_MODE_LEAD_ACID)


#
# SCAN_INTERVAL = timedelta(seconds=30)
# PARALLEL_UPDATES = 1


async def async_setup_entry(
        hass: HomeAssistant,
        config_entry: HubConfigEntry,
        async_add_entities: AddEntitiesCallback,
) -> None:
    """Add sensors for passed config_entry in HA."""
    hub = config_entry.runtime_data
    coordinator = hub.direct_coordinator

    new_devices = []
    for item in hub.items:
        new_devices.append(InverterOutputPrioritySelect(item, coordinator))
        new_devices.append(InverterChargeSourcePrioritySelect(item, coordinator))
        new_devices.append(InverterMaxUtilityChargingCurrentNumber(item, coordinator))
        new_devices.append(InverterMaxChargingCurrentSelect(item, coordinator))
        new_devices.append(BatteryModeSelect(item))

    if new_devices:
        async_add_entities(new_devices)


class BatteryModeSelect(SelectEntity, RestoreEntity):
    """User-selected battery chemistry / connection preset.

    Drives the SoC algorithm strategy:
      - "Lithium (Voltage)" — LFP-style voltage snap + Coulomb counter,
        eff=0.97, tail=0.05C, hysteresis=0.2V
      - "Lithium (BMS)"     — mirror battery_capacity field (BMS source)
      - "Lead-acid"         — wider hysteresis 0.5V, eff=0.85/0.90, tail=0.02C
    """

    _attr_options = list(BATTERY_MODES)
    _attr_icon = "mdi:battery-sync"
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(self, inverter_device: InverterDevice):
        self._inverter_device = inverter_device
        self._attr_unique_id = f"{inverter_device.inverter_id}_battery_mode"
        self._attr_name = f"{inverter_device.name} vSoC Battery Mode"
        # Default preserves the existing (LFP voltage-based) behavior so
        # existing users don't see their SoC sensor change after upgrade.
        self._attr_current_option = BATTERY_MODE_LI_VOLTAGE
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, inverter_device.inverter_id)},
            name=inverter_device.name,
            manufacturer="ESS",
            model=inverter_device.inverter_id,
            sw_version=inverter_device.firmware_version,
        )

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        state = await self.async_get_last_state()
        if state and state.state in self._attr_options:
            self._attr_current_option = state.state

    async def async_select_option(self, option: str) -> None:
        if option in self._attr_options:
            self._attr_current_option = option
            self.async_write_ha_state()


class SelectBase(CoordinatorEntity, SelectEntity):
    # should_poll = True

    # While a write is in flight, the chosen value is held so an
    # interleaving scheduled poll (which may carry forward a stale QPIRI
    # section) can't revert the dropdown before the change is confirmed.
    _pending_option: str | None = None
    _pending_since: float = 0.0
    # Hold window must exceed the QPIRI poll interval (~120s at the default
    # 10s cadence*12) so a natural read can confirm the change before any
    # revert; otherwise the value flips back to stale carried-forward data.
    _PENDING_TTL: float = 150.0
    # After an ACK, re-read fresh QPIRI a few times to catch inverter commit
    # lag / a transient CRC/timeout on the first read.
    _CONFIRM_ATTEMPTS: int = 4
    _CONFIRM_DELAY: float = 1.0

    def __init__(self, inverter_device: InverterDevice, coordinator: DirectCoordinator):
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._inverter_device = inverter_device

    # To link this entity to the cover device, this property must return an
    # identifiers value matching that used in the cover, but no other information such
    # as name. If name is returned, this entity will then also become a device in the
    # HA UI.
    @property
    def device_info(self) -> DeviceInfo:
        """Information about this entity/device."""
        return {
            "identifiers": {(DOMAIN, self._inverter_device.inverter_id)},
            "name": self._inverter_device.name,
            "sw_version": self._inverter_device.firmware_version,
            "model": self._inverter_device.inverter_id,
            "manufacturer": 'ESS'
        }

    @property
    def available(self) -> bool:
        """Return True if inverter_device and hub is available."""
        return True
        # return self._inverter_device.online and self._inverter_device.hub.online

    @property
    def data(self):
        # Safe: a freshly-added hub child isn't in coordinator.data until its
        # first poll. Returning {} avoids a KeyError that would crash select
        # setup / the in-place reconcile.
        return (self.coordinator.data or {}).get(
            self._inverter_device.inverter_id
        ) or {}

    def _read_current(self, data) -> str | None:
        """Return the option reflected by ``data`` (subclasses override)."""
        raise NotImplementedError

    @callback
    def _handle_coordinator_update(self) -> None:
        current = self._read_current(self.data)
        if self._pending_option is not None:
            confirmed = current == self._pending_option
            expired = (time.monotonic() - self._pending_since) > self._PENDING_TTL
            if confirmed or expired:
                self._pending_option = None
                self._attr_current_option = current
            else:
                # Hold the user's choice until the device confirms it.
                self._attr_current_option = self._pending_option
        else:
            self._attr_current_option = current
        self.async_write_ha_state()

    @staticmethod
    def _is_ack(result) -> bool:
        """True if an inverter set-response indicates acceptance.

        Handles both transports: TCP returns ``{"status": "ACK"}`` while the
        serial path decodes to ``{"Raw": "ACK9..."}``.
        """
        if not isinstance(result, dict):
            return False
        if result.get("status") == "ACK":
            return True
        if result.get("status") == "NAK" or "error" in result:
            return False
        return any(
            isinstance(v, str) and "ACK" in v and "NAK" not in v
            for v in result.values()
        )

    async def _set_and_confirm(
        self, send_fn, option: str, section: str = "qpiri", cmd: str = "QPIRI"
    ) -> None:
        """Optimistically apply ``option``, retry the write until ACK, then
        force an immediate targeted re-read so the value is confirmed
        instead of reverting on the next scheduled poll."""
        self._pending_option = option
        self._pending_since = time.monotonic()
        self._attr_current_option = option
        self.async_write_ha_state()
        entry = self.coordinator.config_entry
        bus_mode = entry.options.get(CONF_BUS_MODE, DEFAULT_BUS_MODE)
        uri = self._inverter_device.device_data
        acked = False
        for _ in range(3):
            result = await run_on_bus(
                self.hass,
                entry.entry_id,
                uri,
                send_fn,
                priority=PRIORITY_USER,
                bus_mode=bus_mode,
                desc="set",
            )
            if self._is_ack(result):
                acked = True
                break
            await asyncio.sleep(0.5)
        if not acked:
            _LOGGER.warning("%s: set not ACKed after retries", self._attr_name)
        # Re-read fresh QPIRI until it reflects the new value. Each refresh
        # publishes and drives _handle_coordinator_update, which clears
        # _pending_option once the readback confirms the change.
        for _ in range(self._CONFIRM_ATTEMPTS):
            await asyncio.sleep(self._CONFIRM_DELAY)
            await self.coordinator.async_refresh_command(
                self._inverter_device.inverter_id, cmd, section
            )
            if self._pending_option is None:
                return


def resolve_output_priority(device_data):
    # ``(... or {})`` so an offline child (no qpiri section yet) doesn't crash
    # select setup / reconcile with 'NoneType' has no attribute 'get'.
    return (device_data.get('qpiri') or {}).get('output_source_priority')


def resolve_chrage_source_priority(device_data):
    return (device_data.get('qpiri') or {}).get('charger_source_priority')


def resolve_max_utility_charging_current(device_data):
    return (device_data.get('qpiri') or {}).get('max_utility_charging_current')


def resolve_max_charging_current(device_data):
    return (device_data.get('qpiri') or {}).get('max_charging_current')


class InverterOutputPrioritySelect(SelectBase):
    _attr_current_option = None

    def __init__(self, inverter_device: InverterDevice, coordinator: DirectCoordinator):
        super().__init__(inverter_device, coordinator)
        self._attr_unique_id = f"{self._inverter_device.inverter_id}_output_priority"
        self._attr_name = f"{self._inverter_device.name} Output Priority"
        self._attr_options = ['UtilityFirst', 'SBU', 'Solar']

        if coordinator.data is not None:
            data = coordinator.data.get(self._inverter_device.inverter_id) or {}
            # device_data = self._inverter_device.device_data
            # print('device_data')
            output_source_priority = resolve_output_priority(data)
            self._attr_current_option = output_source_priority
            # self._attr_current_option = resolve_output_priority(data, device_data)

    def _read_current(self, data) -> str | None:
        mapper = {
            'UtilityFirst': 'UtilityFirst',
            'SBU': 'SBU',
            'Solar': 'Solar',
            'SolarFirst': 'Solar',
        }
        priority = resolve_output_priority(data)
        return mapper.get(priority, priority)

    async def async_select_option(self, option: str):
        if option not in self._attr_options:
            return
        map_priority = {
            'UtilityFirst': OutputSourcePrioritySetting.UTILITY_FIRST,
            'SBU': OutputSourcePrioritySetting.SBU_PRIORITY,
            'Solar': OutputSourcePrioritySetting.SOLAR_FIRST,
        }
        await self._set_and_confirm(
            lambda: set_output_source_priority(
                self._inverter_device.device_data, map_priority[option]
            ),
            option,
        )


class InverterChargeSourcePrioritySelect(SelectBase):
    _attr_current_option = None

    def __init__(self, inverter_device: InverterDevice, coordinator: DirectCoordinator):
        super().__init__(inverter_device, coordinator)
        self._attr_unique_id = f"{self._inverter_device.inverter_id}_charge_source_priority"
        self._attr_name = f"{self._inverter_device.name} Charge Source Priority"
        self._attr_options = ['UtilityFirst', 'SolarFirst', 'SolarAndUtility']  ## ChargeSourcePriority

        if coordinator.data is not None:
            data = coordinator.data.get(self._inverter_device.inverter_id) or {}
            self._attr_current_option = resolve_chrage_source_priority(data)

    def _read_current(self, data) -> str | None:
        return resolve_chrage_source_priority(data)

    async def async_select_option(self, option: str):
        if option not in self._attr_options:
            return
        map_priority = {
            'UtilityFirst': ChargeSourcePrioritySetting.UTILITY_FIRST,
            'SolarFirst': ChargeSourcePrioritySetting.SOLAR_FIRST,
            'SolarAndUtility': ChargeSourcePrioritySetting.SOLAR_AND_UTILITY,
        }
        await self._set_and_confirm(
            lambda: set_charge_source_priority(
                self._inverter_device.device_data, map_priority[option]
            ),
            option,
        )


def _normalize_amps(raw) -> str | None:
    """Coerce firmware-reported current ('02.0', '030', '2.0') to canonical str(int)."""
    if raw is None:
        return None
    try:
        return str(int(float(raw)))
    except (TypeError, ValueError):
        return None


class InverterMaxUtilityChargingCurrentNumber(SelectBase):
    def __init__(self, inverter_device: InverterDevice, coordinator: DirectCoordinator):
        super().__init__(inverter_device, coordinator)
        self._attr_unique_id = f"{self._inverter_device.inverter_id}_max_utility_charging_current"
        self._attr_name = f"{self._inverter_device.name} Max Utility Charging Current"
        self._attr_options = ['2', '10', '20', '30', '40', '50', '60', '70', '80', '90', '100', '110', '120']
        self._raw_readback: str | None = None

        if coordinator.data is not None:
            data = coordinator.data.get(self._inverter_device.inverter_id) or {}
            raw = resolve_max_utility_charging_current(data)
            self._raw_readback = raw if raw is None else str(raw)
            self._attr_current_option = _normalize_amps(raw)

    def _read_current(self, data) -> str | None:
        raw = resolve_max_utility_charging_current(data)
        self._raw_readback = raw if raw is None else str(raw)
        return _normalize_amps(raw)

    async def async_select_option(self, option: str):
        if option not in self._attr_options:
            return
        amps = int(option)
        float_format = self._raw_readback is not None and '.' in self._raw_readback
        await self._set_and_confirm(
            lambda: set_max_utility_charge_current(
                self._inverter_device.device_data, amps, float_format=float_format
            ),
            option,
        )


class InverterMaxChargingCurrentSelect(SelectBase):
    def __init__(self, inverter_device: InverterDevice, coordinator: DirectCoordinator):
        super().__init__(inverter_device, coordinator)
        self._attr_unique_id = f"{self._inverter_device.inverter_id}_max_charging_current"
        self._attr_name = f"{self._inverter_device.name} Max Charging Current"
        self._attr_options = ['10', '20', '30', '40', '50', '60', '70', '80']

        if coordinator.data is not None:
            data = coordinator.data.get(self._inverter_device.inverter_id) or {}
            self._attr_current_option = _normalize_amps(resolve_max_charging_current(data))

    def _read_current(self, data) -> str | None:
        return _normalize_amps(resolve_max_charging_current(data))

    async def async_select_option(self, option: str):
        if option not in self._attr_options:
            return
        amps = int(option)
        await self._set_and_confirm(
            lambda: set_max_combined_charge_current(
                self._inverter_device.device_data, amps
            ),
            option,
        )
