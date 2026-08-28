""" Module to handle go2rtc interactions.

Pushes P2P H.264 video into go2rtc by piping raw Annex-B bytes into an ffmpeg
subprocess, which remuxes them and pushes into go2rtc over RTSP (`-f rtsp
-rtsp_transport tcp`). go2rtc's actual supported mechanism for a client to push a
stream in is RTSP ANNOUNCE/RECORD (RFC 2326), mirroring the standard PLAY method HA
already uses to pull the stream back out - so a client still has to speak that
handshake and RTP packetization (RFC 6184) correctly. An earlier version of this
module hand-rolled that handshake and RTP packetization directly in Python: the
handshake completed reliably (ANNOUNCE/SETUP/RECORD all 200 every time), but go2rtc
dropped the connection within milliseconds of every real RTP write. Rather than keep
debugging a hand-rolled RTSP/RTP client, this version hands the whole
ANNOUNCE/RECORD/RTP job to ffmpeg's own well-tested RTSP muxer instead.

ROOT CAUSE of the "handshake succeeds, connection dies immediately" failure
(found 2026-08-28, see Eufy Integration Log for the full diagnostic trail,
including a synthetic-RTP-injection test that proved it wasn't a client-timing
race): `_register_stream()` used to PUT to go2rtc's /api/streams with only a
`name` param, no `src`. go2rtc's own handler (internal/streams/api.go) silently
no-ops that request - an early-return guard fires before the PUT case ever runs
when `src` is empty - so no *Stream* ever actually lands in go2rtc's registry,
even though the HTTP response looks like success. When ffmpeg's ANNOUNCE then
arrives, go2rtc's RTSP server (internal/rtsp/rtsp.go) finds no registered stream
for that name and skips ever calling the code path that reads RTP off the
socket, closing the connection right after RECORD's 200 OK instead. Fixed by
registering with a self-referencing `src` (see `_register_stream` for detail).

Historically (before either of the above) this integration pushed video via a raw
HTTP POST to an `/api/stream` (singular) endpoint that doesn't exist on any current
go2rtc version at all (only `/api/streams`, plural, is a management API - it
doesn't accept a raw video body).
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import subprocess
import threading
import time
import traceback
import aiohttp
from .const import GO2RTC_API_PORT, GO2RTC_API_URL, GO2RTC_RTSP_PORT, HA_MANAGED_GO2RTC_RTSP_PORT, STREAM_TIMEOUT_SECONDS

_LOGGER: logging.Logger = logging.getLogger(__package__)

# Annex-B NAL unit type values (H.264) relevant to probing - see ITU-T H.264 Table 7-1.
_NAL_TYPE_SPS = 7
_NAL_TYPE_PPS = 8


def _nal_types_in_chunk(chunk: bytes) -> set[int]:
    """Return the set of Annex-B NAL unit types found in `chunk` (scans for
    00 00 01 / 00 00 00 01 start codes; good enough for a presence check, not a
    full parser)."""
    types = set()
    length = len(chunk)
    i = 0
    while i < length - 2:
        if chunk[i] == 0 and chunk[i + 1] == 0:
            if chunk[i + 2] == 1:
                start = i + 3
            elif i + 3 < length and chunk[i + 2] == 0 and chunk[i + 3] == 1:
                start = i + 4
            else:
                i += 1
                continue
            if start < length:
                types.add(chunk[start] & 0x1F)
            i = start
        else:
            i += 1
    return types


class P2PStreamer:
    """Class to manage external stream provider and byte based ffmpeg streaming"""

    def __init__(self, camera) -> None:
        self.camera = camera
        self.retry = None
        # Set once the first access unit is written to ffmpeg's stdin without error.
        # HA's own Stream/ffmpeg-snapshot consumers wait on this before attaching -
        # kept from the earlier hand-rolled implementation as a cheap, harmless
        # safeguard even though it turned out not to be the cause of that version's
        # failure (see module docstring).
        self.first_push_ok = False
        self._proc: subprocess.Popen | None = None

    # ---------------------------------------------------------------------
    # go2rtc /api/streams registration - reuses HA's own go2rtc session when
    # available (see Camera.set_go2rtc_client). See _register_stream for why
    # a self-referencing `src` is required.
    # ---------------------------------------------------------------------

    def _go2rtc_api_url(self, suffix: str = "") -> tuple[str, aiohttp.ClientSession | None]:
        """Return (url, session) to reach go2rtc's stream-management API (/api/streams)."""
        if self.camera.go2rtc_session is not None:
            base = self.camera.go2rtc_base_url.rstrip("/")
            return f"{base}/api/streams{suffix}", self.camera.go2rtc_session
        url = GO2RTC_API_URL.format(self.camera.config.rtsp_server_address, GO2RTC_API_PORT)
        return f"{url}s{suffix}", None

    def _rtsp_host_port(self) -> tuple[str, int]:
        host = str(self.camera.config.rtsp_server_address)
        port = HA_MANAGED_GO2RTC_RTSP_PORT if self.camera.go2rtc_session is not None else GO2RTC_RTSP_PORT
        return host, port

    async def _register_stream(self) -> None:
        """(Re)create a stream entry in go2rtc, ready to accept our RTSP producer.

        ROOT CAUSE (2026-08-28): this used to PUT with only a `name` param, no
        `src`. go2rtc's own /api/streams handler (internal/streams/api.go) has an
        early-return guard - `if src == "" && r.Method != "POST"` - that fires
        before the PUT case's `New(name, ...)` ever runs, so that call was a no-op
        that happened to return 200 with the current streams list (looking like
        success). Because no *Stream ever landed in go2rtc's registry,
        `streams.Get(name)` returned nil when our ffmpeg's RTSP ANNOUNCE arrived
        (internal/rtsp/rtsp.go's tcpHandler), which left `closer` unset - so the
        RTSP handshake completed in full (OPTIONS/ANNOUNCE/SETUP/RECORD all 200)
        but `conn.Handle()` (the call that actually reads RTP off the socket) was
        skipped entirely, and the connection was torn down immediately after
        RECORD's 200 OK. Confirmed via a synthetic-RTP-injection test (Eufy
        Integration Log, 2026-08-28): even a protocol-valid RTP frame written
        immediately after RECORD's response got dropped with the exact same
        byte-count EOF as real ffmpeg traffic - proving the closure was
        independent of any client-side timing, i.e. structural, not a race.

        Fix: register with a self-referencing `src` (an RTSP pull URL pointing at
        our own push endpoint for this same stream name). This satisfies
        `HasProducer()`/`Validate()` so `New()` actually creates the *Stream* and
        stores it in go2rtc's registry. The URL is never dialed at registration
        time (go2rtc's producers are lazy - see internal/streams/stream.go
        `NewProducer`); it's only used as a fallback if a consumer requests the
        stream before our own RTSP push has ANNOUNCEd (in which case dialing
        it just fails harmlessly, since nothing is listening on that exact path
        as a producer yet). Once our ffmpeg pushes in, `AddProducer` registers
        that connection as `external` alongside the lazy pull source.
        """
        url, shared_session = self._go2rtc_api_url()
        name = str(self.camera.serial_no)
        host, port = self._rtsp_host_port()
        self_src = f"rtsp://{host}:{port}/{name}"

        async with contextlib.AsyncExitStack() as stack:
            session = shared_session or await stack.enter_async_context(aiohttp.ClientSession())
            async with session.delete(url, params={"name": name}) as response:
                result = response.status, await response.text()
                _LOGGER.debug(f"_register_stream - delete response {result}")

        async with contextlib.AsyncExitStack() as stack:
            session = shared_session or await stack.enter_async_context(aiohttp.ClientSession())
            async with session.put(url, params={"name": name, "src": self_src}) as response:
                result = response.status, await response.text()
                _LOGGER.debug(f"_register_stream - put response {result}")

    # ---------------------------------------------------------------------
    # ffmpeg subprocess push - blocking, runs in its own thread (via
    # asyncio.to_thread from start()).
    # ---------------------------------------------------------------------

    def _start_ffmpeg(self, host: str, port: int, name: str) -> subprocess.Popen:
        """Launch ffmpeg reading raw Annex-B H.264 from stdin and pushing it into
        go2rtc's RTSP producer endpoint. No re-encoding - `-c:v copy` just remuxes."""
        binary = self.camera.ffmpeg_binary or "ffmpeg"
        url = f"rtsp://{host}:{port}/{name}"
        cmd = [
            binary,
            "-loglevel", "warning",
            "-f", "h264",
            "-i", "pipe:0",
            "-c:v", "copy",
            "-an",
            "-f", "rtsp",
            "-rtsp_transport", "tcp",
            url,
        ]
        return subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    def _wait_for_keyframe(self, queue, name: str, timeout_seconds: float) -> bool:
        """Peek `queue` (without discarding anything) until an SPS+PPS pair has been
        seen among its queued chunks, so ffmpeg's stdin starts with real codec
        parameters instead of racing its own auto-probe against a P-frame-only
        prefix.

        This reinstates a guard the earlier hand-rolled RTSP implementation had
        (`_wait_for_sps_pps`) - dropped when this module was rewritten around an
        ffmpeg subprocess, and not missed until some fraction of restart cycles
        started failing immediately with ffmpeg's "Output file does not contain
        any stream" / exit code -22, before ever attempting the RTSP handshake
        (see Eufy Integration Log, 2026-08-28).
        """
        deadline = time.monotonic() + timeout_seconds
        seen_types = set()
        while time.monotonic() < deadline:
            for chunk in list(queue):
                seen_types |= _nal_types_in_chunk(bytes(chunk))
            if _NAL_TYPE_SPS in seen_types and _NAL_TYPE_PPS in seen_types:
                return True
            time.sleep(0.05)
        _LOGGER.debug(f"_wait_for_keyframe {name} - timed out waiting for SPS/PPS")
        return False

    def _drain_ffmpeg_stderr(self, proc: subprocess.Popen, name: str) -> None:
        """Log ffmpeg's stderr in the background so RTSP-level failures are visible."""
        with contextlib.suppress(Exception):
            for line in iter(proc.stderr.readline, b""):
                if not line:
                    break
                _LOGGER.debug(f"_run {name} - ffmpeg: {line.decode(errors='replace').rstrip()}")

    def _teardown(self) -> None:
        proc = self._proc
        if proc is None:
            return
        with contextlib.suppress(Exception):
            if proc.stdin is not None:
                proc.stdin.close()
        with contextlib.suppress(Exception):
            proc.terminate()
        self._proc = None

    def _run_push_loop(self, queue, queue_name, proc: subprocess.Popen) -> None:
        """Blocking loop: drain `queue`, write each access unit into ffmpeg's stdin.
        Runs in its own thread.

        Mirrors the old write_bytes()'s idle-timeout behavior (stop after
        STREAM_TIMEOUT_SECONDS with nothing new in the queue) - the queue
        naturally stops filling once the P2P session stops, no separate
        cancellation signal exists elsewhere in this integration.
        """
        idle_since = None

        while True:
            try:
                chunk = queue.popleft()
                idle_since = None
            except IndexError:
                if idle_since is None:
                    idle_since = time.monotonic()
                elif time.monotonic() - idle_since > STREAM_TIMEOUT_SECONDS:
                    _LOGGER.debug(f"_run_push_loop {queue_name} - idle timeout, stopping")
                    self.retry = False
                    return
                if proc.poll() is not None:
                    _LOGGER.debug(f"_run_push_loop {queue_name} - ffmpeg exited with {proc.returncode}")
                    self.retry = True
                    return
                time.sleep(0.05)
                continue

            try:
                proc.stdin.write(bytes(chunk))
                proc.stdin.flush()
                self.first_push_ok = True
            except (BrokenPipeError, ConnectionError, OSError) as ex:
                _LOGGER.debug(f"_run_push_loop {queue_name} - connection error {ex} - traceback: {traceback.format_exc()}")
                self.retry = True
                return
            except Exception as ex:  # pylint: disable=broad-except
                _LOGGER.debug(f"_run_push_loop {queue_name} - general exception {ex} - traceback: {traceback.format_exc()}")
                self.retry = False
                return

    def _run(self, queue, name, connect_host: str, connect_port: int):
        """Thread entry point: launch ffmpeg, then push the queue's bytes into it.
        `connect_host`/`connect_port` is what ffmpeg actually dials - go2rtc's
        real RTSP port (see start())."""
        self.retry = None
        self.first_push_ok = False
        host, port = connect_host, connect_port

        self._wait_for_keyframe(queue, name, STREAM_TIMEOUT_SECONDS)

        try:
            proc = self._start_ffmpeg(host, port, name)
        except Exception as ex:  # pylint: disable=broad-except
            _LOGGER.debug(f"_run {name} - could not start ffmpeg - {ex} - traceback: {traceback.format_exc()}")
            self.retry = True
            return

        self._proc = proc
        threading.Thread(target=self._drain_ffmpeg_stderr, args=(proc, name), daemon=True).start()

        try:
            self._run_push_loop(queue, name, proc)
        finally:
            self._teardown()

    async def start(self):
        """start streaming thread"""
        self.retry = None
        await self._register_stream()
        # The RTSP push target's path MUST match the name _register_stream() just
        # registered with go2rtc (the camera's serial number - see StreamProvider.P2P
        # in eufy_security_api.camera, which every consumer already dials by serial
        # number).
        name = str(self.camera.serial_no)
        host, port = self._rtsp_host_port()
        await asyncio.to_thread(self._run, self.camera.video_queue, name, host, port)
