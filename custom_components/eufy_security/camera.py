from __future__ import annotations

import asyncio
import contextlib
import logging
import traceback

from haffmpeg.camera import CameraMjpeg
from haffmpeg.tools import ImageFrame
from base64 import b64decode
from homeassistant.components import ffmpeg
from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.components.ffmpeg import DATA_FFMPEG
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_platform
from homeassistant.helpers.aiohttp_client import async_aiohttp_proxy_stream
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import COORDINATOR, DOMAIN, Schema
from .coordinator import EufySecurityDataUpdateCoordinator
from .entity import EufySecurityEntity
from .eufy_security_api.camera import (
    STREAM_SLEEP_SECONDS,
    STREAM_TIMEOUT_SECONDS,
    StreamProvider,
    StreamStatus,
)
from .eufy_security_api.metadata import Metadata
from .eufy_security_api.util import wait_for_value_to_equal
from .util import get_ha_go2rtc_client

_LOGGER: logging.Logger = logging.getLogger(__package__)


async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    """Setup camera entities."""
    coordinator: EufySecurityDataUpdateCoordinator = hass.data[DOMAIN][COORDINATOR]

    # Give every camera product a way to reach HA's own embedded go2rtc instance
    # (Unix socket + correct auth) instead of guessing a legacy fixed TCP port
    # that modern HA's go2rtc no longer listens on. See util.get_ha_go2rtc_client
    # for why this is necessary; falls back to the old behavior if unavailable.
    go2rtc_session, go2rtc_base_url = get_ha_go2rtc_client(hass)
    # The P2P streamer shells out to ffmpeg to push video into go2rtc - give it HA's
    # own ffmpeg binary path rather than assuming "ffmpeg" is on PATH.
    ffmpeg_binary = hass.data[DATA_FFMPEG].binary

    product_properties = []
    for product in coordinator.devices.values():
        if product.is_camera is True:
            product.set_go2rtc_client(go2rtc_session, go2rtc_base_url)
            product.set_ffmpeg_binary(ffmpeg_binary)
            product_properties.append(Metadata.parse(product, {"name": "camera", "label": "Camera"}))

    entities = [EufySecurityCamera(coordinator, metadata) for metadata in product_properties]
    async_add_entities(entities)

    # register entity level services
    platform = entity_platform.async_get_current_platform()
    platform.async_register_entity_service("generate_image", {}, "_generate_image")
    platform.async_register_entity_service("start_p2p_livestream", {}, "_start_livestream")
    platform.async_register_entity_service("stop_p2p_livestream", {}, "_stop_livestream")
    platform.async_register_entity_service("start_rtsp_livestream", {}, "_start_rtsp_livestream")
    platform.async_register_entity_service("stop_rtsp_livestream", {}, "_stop_rtsp_livestream")
    platform.async_register_entity_service("ptz", Schema.PTZ_SERVICE_SCHEMA.value, "_async_ptz")
    platform.async_register_entity_service("ptz_up", {}, "_async_ptz_up")
    platform.async_register_entity_service("ptz_down", {}, "_async_ptz_down")
    platform.async_register_entity_service("ptz_left", {}, "_async_ptz_left")
    platform.async_register_entity_service("ptz_right", {}, "_async_ptz_right")
    platform.async_register_entity_service("ptz_360", {}, "_async_ptz_360")
    platform.async_register_entity_service("preset_position", Schema.PRESET_POSITION_SERVICE_SCHEMA.value, "_async_preset_position")
    platform.async_register_entity_service("save_preset_position", Schema.PRESET_POSITION_SERVICE_SCHEMA.value, "_async_save_preset_position")
    platform.async_register_entity_service("delete_preset_position", Schema.PRESET_POSITION_SERVICE_SCHEMA.value, "_async_delete_preset_position")
    platform.async_register_entity_service("calibrate", {}, "_async_calibrate")

    platform.async_register_entity_service("trigger_camera_alarm_with_duration", Schema.TRIGGER_ALARM_SERVICE_SCHEMA.value, "_async_alarm_trigger")
    platform.async_register_entity_service("reset_alarm", {}, "_async_reset_alarm")
    platform.async_register_entity_service("quick_response", Schema.QUICK_RESPONSE_SERVICE_SCHEMA.value, "_async_quick_response")
    platform.async_register_entity_service("snooze", Schema.SNOOZE.value, "_snooze")


class EufySecurityCamera(Camera, EufySecurityEntity):
    """Base camera entity for integration"""

    def __init__(self, coordinator: EufySecurityDataUpdateCoordinator, metadata: Metadata) -> None:
        Camera.__init__(self)
        EufySecurityEntity.__init__(self, coordinator, metadata)
        self._attr_supported_features = CameraEntityFeature.STREAM
        self._attr_name = f"{self.product.name}"

        # camera image
        self._last_url = None
        self._last_image = None
        if self.product.picture_base64 is not None:
            self._last_image = self.product.picture_bytes

        # ffmpeg entities
        self.ffmpeg = self.coordinator.hass.data[DATA_FFMPEG]

        # Serializes concurrent auto-start attempts (see _ensure_streaming_for_view) -
        # async_camera_image() can be called several times a second while HA's
        # default MJPEG handler is polling for frames, and without this guard each
        # of those calls landing before the first one finishes starting the stream
        # would try to call async_turn_on() again too.
        self._auto_start_lock = asyncio.Lock()

    async def stream_source(self) -> str:
        # Deliberately does NOT auto-start here (see _ensure_streaming_for_view,
        # hooked into async_camera_image instead): handle_async_mjpeg_stream()
        # below calls this first and only falls through to HA's default,
        # async_camera_image()-based MJPEG relay when this returns None. If
        # this auto-started and returned a real URL instead, every live-view
        # request would take the CameraMjpeg()+ffmpeg direct-proxy path below
        # instead - a second, separately-opened RTSP consumer per request that
        # hasn't been exercised or tested today, and which is exactly the kind
        # of short-lived-consumer churn flagged elsewhere as having previously
        # destabilized the push (see Eufy Integration Log, 2026-08-27). Staying
        # None here keeps live-view traffic on the already-tested
        # async_camera_image() path, which does auto-start.
        if self.is_streaming is False:
            return None
        return self.product.stream_url

    async def handle_async_mjpeg_stream(self, request):
        """this is probabaly triggered by user request, turn on"""
        stream_source = await self.stream_source()
        if stream_source is None:
            return await super().handle_async_mjpeg_stream(request)
        stream = CameraMjpeg(self.ffmpeg.binary)
        await stream.open_camera(stream_source)
        try:
            return await async_aiohttp_proxy_stream(
                self.hass,
                request,
                await stream.get_reader(),
                self.ffmpeg.ffmpeg_stream_content_type,
            )
        finally:
            await stream.close()

    async def async_create_stream(self):
        if self.coordinator.config.no_stream_in_hass is True:
            return None
        return await super().async_create_stream()

    async def _start_hass_streaming(self):
        await wait_for_value_to_equal(self.product.__dict__, "stream_status", StreamStatus.STREAMING)
        if self.product.stream_provider == StreamProvider.P2P:
            # Wait for go2rtc to actually confirm our RTSP-push producer registered
            # (announced_ok) before letting HA's Stream (or the snapshot poll below)
            # attach as a consumer. first_push_ok only confirms our own write into
            # ffmpeg's stdin succeeded - it says nothing about whether ffmpeg's
            # ANNOUNCE has reached go2rtc yet, so it doesn't close the actual race
            # window: a consumer's DESCRIBE landing while go2rtc is still appending
            # our pushed producer to its internal list crashes the whole go2rtc
            # process (a real go2rtc v1.9.14 upstream bug - unsynchronized read/
            # append on Stream.producers in AddConsumer/AddProducer). Waiting for
            # announced_ok instead means that append has already happened by the
            # time any consumer attaches. See Eufy Integration Log 2026-08-28
            # (continued 8) for the full diagnosis; 2026-08-27 for the earlier,
            # weaker first_push_ok guard this replaces.
            await wait_for_value_to_equal(self.product.p2p_streamer.__dict__, "announced_ok", True)
        await self._stop_hass_streaming()
        await self.async_create_stream()
        if self.stream is not None:
            await self.stream.start()
        await self.async_camera_image()

    async def _stop_hass_streaming(self):
        if self.stream is not None:
            await self.stream.stop()
            self.stream = None

    @property
    def is_streaming(self) -> bool:
        """Return true if the device is recording."""
        return self.product.stream_status == StreamStatus.STREAMING

    @property
    def available(self) -> bool:
        return True

    @property
    def extra_state_attributes(self):
        return {"stream_debug": self.product.stream_debug}

    # Requests narrower than this are almost certainly dashboard/area-card
    # thumbnail polling (observed ~175px wide) rather than someone actually
    # opening the live view (observed ~1024px wide for a full snapshot, and no
    # width at all - None - for the raw MJPEG live stream, since HA's default
    # handle_async_mjpeg_stream() calls async_camera_image() with no size args).
    _AUTO_START_MIN_WIDTH = 300

    async def _ensure_streaming_for_view(self, width: int | None) -> None:
        """Auto-start the live P2P/RTSP session on demand when something actually
        requests a real view of this camera - the dashboard live-view dialog, or
        camera_proxy_stream's continuous MJPEG feed - instead of requiring a
        separate explicit `camera.turn_on` call first.

        Deliberately skips narrow (thumbnail-sized) requests: without this, an
        always-on wall dashboard showing an area card with this camera's small
        preview thumbnail would keep calling async_camera_image() on its own
        refresh cadence and re-trigger camera.turn_on indefinitely, defeating
        the whole reason P2P sessions are gated behind on/off in the first
        place (Eufy's cloud/P2P livestream is a real, limited resource, not a
        free-running feed). See Eufy Integration Log, 2026-08-28 (continued 12).
        """
        if self.is_streaming:
            return
        if width is not None and width < self._AUTO_START_MIN_WIDTH:
            return
        async with self._auto_start_lock:
            if self.is_streaming:  # another concurrent call may have started it already
                return
            _LOGGER.debug(f"_ensure_streaming_for_view - auto-starting (width={width})")
            await self.async_turn_on()

    async def _get_image_from_stream_url(self, width, height):
        while True:
            result = await ffmpeg.async_get_image(self.hass, await self.stream_source(), width=width, height=height)
            if result is not None:
                _LOGGER.debug(f"_get_image_from_stream_url - received {len(result)}")
                return result
            _LOGGER.debug(f"_get_image_from_stream_url - is_empty {result is None}")
            await asyncio.sleep(STREAM_SLEEP_SECONDS)

    async def async_camera_image(self, width: int | None = None, height: int | None = None) -> bytes | None:
        _LOGGER.debug(f"image 1 - {self.is_streaming} - {self.stream}")
        await self._ensure_streaming_for_view(width)
        if self.is_streaming is True:
            if self.stream is not None:
                # Reuse HA's own already-connected Stream instead of opening a brand new
                # ffmpeg RTSP consumer on every poll (HA polls camera images roughly every
                # 0.5-1s while streaming). That churn of short-lived consumers connecting
                # and disconnecting against the same go2rtc stream our own RTSP push is
                # using appears to be what destabilizes the push connection - see Eufy
                # Integration Log 2026-08-27. HA's Stream already has a persistent
                # connection and can hand back a cached/decoded keyframe locally.
                with contextlib.suppress(Exception):
                    self._last_image = await self.stream.async_get_image(width=width, height=height)
            if self._last_image is None:
                with contextlib.suppress(asyncio.TimeoutError):
                    self._last_image = await asyncio.wait_for(self._get_image_from_stream_url(width, height), STREAM_TIMEOUT_SECONDS)
            _LOGGER.debug(f"image 2 - is_empty {self._last_image is None}")

        _LOGGER.debug(f"async_camera_image 5 - is_empty {self._last_image is None}")
        if self._last_image is not None:
            _LOGGER.debug(f"async_camera_image 6 - {len(self._last_image)}")
        return self._last_image

    async def _start_livestream(self) -> None:
        """start byte based livestream on camera"""
        if await self.product.start_livestream() is False:
            await self._stop_livestream()
        else:
            await self._start_hass_streaming()
        self.async_write_ha_state()

    async def _stop_livestream(self) -> None:
        """stop byte based livestream on camera"""
        await self._stop_hass_streaming()
        await self.product.stop_livestream()
        self.async_write_ha_state()

    async def _start_rtsp_livestream(self) -> None:
        """start rtsp based livestream on camera"""
        if await self.product.start_rtsp_livestream() is False:
            await self._stop_rtsp_livestream()
        else:
            await self._start_hass_streaming()
        self.async_write_ha_state()

    async def _stop_rtsp_livestream(self) -> None:
        """stop rtsp based livestream on camera"""
        await self._stop_hass_streaming()
        await self.product.stop_rtsp_livestream()
        self.async_write_ha_state()

    async def _async_alarm_trigger(self, duration: int = 10):
        """trigger alarm for a duration on camera"""
        await self.product.trigger_alarm(duration)

    async def _async_reset_alarm(self) -> None:
        """reset ongoing alarm"""
        await self.product.reset_alarm()

    async def async_turn_on(self) -> None:
        """Turn off camera."""
        if self.product.stream_provider == StreamProvider.RTSP:
            await self._start_rtsp_livestream()
        else:
            await self._start_livestream()

    async def async_turn_off(self) -> None:
        """Turn off camera."""
        if self.product.stream_provider == StreamProvider.RTSP:
            await self._stop_rtsp_livestream()
        else:
            await self._stop_livestream()

    async def _async_ptz(self, direction: str) -> None:
        await self.product.ptz(direction)

    async def _async_ptz_up(self) -> None:
        await self.product.ptz_up()

    async def _async_ptz_down(self) -> None:
        await self.product.ptz_down()

    async def _async_ptz_left(self) -> None:
        await self.product.ptz_left()

    async def _async_ptz_right(self) -> None:
        await self.product.ptz_right()

    async def _async_ptz_360(self) -> None:
        await self.product.ptz_360()

    async def _async_preset_position(self, position: int) -> None:
        await self.product.preset_position(position)

    async def _async_save_preset_position(self, position: int) -> None:
        await self.product.save_preset_position(position)

    async def _async_delete_preset_position(self, position: int) -> None:
        await self.product.delete_preset_position(position)

    async def _async_calibrate(self) -> None:
        await self.product.calibrate()

    async def _generate_image(self) -> None:
        await self.async_camera_image()

    async def _async_quick_response(self, voice_id: int) -> None:
        await self.product.quick_response(voice_id)

    async def _snooze(self, snooze_time: int, snooze_chime: bool, snooze_motion: bool, snooze_homebase: bool) -> None:
        await self.product.snooze(snooze_time, snooze_chime, snooze_motion, snooze_homebase)
