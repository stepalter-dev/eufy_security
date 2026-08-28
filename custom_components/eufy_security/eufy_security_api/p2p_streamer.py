""" Module to handle go2rtc interactions.

Pushes P2P H.264 video into go2rtc by piping raw Annex-B bytes into an ffmpeg
subprocess, which remuxes them and pushes into go2rtc over RTSP (`-f rtsp
-rtsp_transport tcp`). go2rtc's actual supported mechanism for a client to push a
stream in is RTSP ANNOUNCE/RECORD (RFC 2326), mirroring the standard PLAY method HA
already uses to pull the stream back out - so a client still has to speak that
handshake and RTP packetization (RFC 6184) correctly. An earlier version of this
module hand-rolled that handshake and RTP packetization directly in Python: the
handshake completed reliably (ANNOUNCE/SETUP/RECORD all 200 every time), but go2rtc
dropped the connection within milliseconds of every real RTP write, and three
separate rounds of protocol-level fixes - correcting the SETUP/RECORD channel
negotiation, adding proper SDP codec parameters (profile-level-id,
sprop-parameter-sets), and ruling out a race with consumers attaching before the
producer had sent anything - each tested clean individually and none changed the
outcome (see the Eufy Integration Log entries for 2026-08-27 for the full
diagnostic trail). Rather than keep debugging a hand-rolled RTSP/RTP client against
an opaque go2rtc-side failure, this version hands the whole ANNOUNCE/RECORD/RTP job
to ffmpeg's own well-tested RTSP muxer instead.

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
    # available (see Camera.set_go2rtc_client). Registers an empty
    # placeholder-free stream, ready for ffmpeg's RTSP producer to ANNOUNCE
    # into, instead of the old dead `tcp://127.0.0.1:65535` address.
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
        """(Re)create an empty stream entry in go2rtc, ready to accept our RTSP producer."""
        url, shared_session = self._go2rtc_api_url()
        parameters = {"name": str(self.camera.serial_no)}

        async with contextlib.AsyncExitStack() as stack:
            session = shared_session or await stack.enter_async_context(aiohttp.ClientSession())
            async with session.delete(url, params=parameters) as response:
                result = response.status, await response.text()
                _LOGGER.debug(f"_register_stream - delete response {result}")

        async with contextlib.AsyncExitStack() as stack:
            session = shared_session or await stack.enter_async_context(aiohttp.ClientSession())
            # No `src` - creates an empty stream, ready for a producer to ANNOUNCE into,
            # instead of a `src` go2rtc would try to actively dial itself.
            async with session.put(url, params=parameters) as response:
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
        # RULED OUT (2026-08-28): a `?timeout=` query param on this URL was tried as
        # a way to raise go2rtc's passive-producer read-timeout, on the theory that
        # timeout was the mechanism behind the recurring Broken Pipe. `?timeout=3`
        # and `?timeout=60` both produced the *same* ~1.7-2.1s Broken Pipe interval -
        # if the declared value actually controlled anything, those would differ by
        # 20x. Conclusion: the query param isn't reaching/affecting go2rtc's timeout
        # at all (ffmpeg may not preserve it in the actual ANNOUNCE request line, or
        # go2rtc's query parsing doesn't apply to producer connections the way it
        # does for consumers) - and its mere presence seems to trigger a different,
        # *shorter-lived* failure than the plain URL does. Reverted to no query
        # string, which empirically gives the longest hold time observed so far
        # (~6-8s). See Eufy Integration Log 2026-08-28 for the full timing data.
        url = f"rtsp://{host}:{port}/{name}"
        cmd = [
            binary,
            "-loglevel", "debug",
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

    def _run(self, queue, name):
        """Thread entry point: launch ffmpeg, then push the queue's bytes into it."""
        self.retry = None
        self.first_push_ok = False
        host, port = self._rtsp_host_port()

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
        # number). A literal "video" here used to silently mismatch: go2rtc's RTSP
        # server still answers ANNOUNCE/SETUP/RECORD with 200 for an unregistered
        # stream name (protocol-layer code doesn't check), but internal/rtsp's own
        # bookkeeping (streams.Get(name) returning nil) then skips ever calling
        # conn.Handle() - the loop that actually reads the pushed RTP data - and
        # closes the connection immediately instead. That produced the exact
        # "handshake succeeds, then instant broken pipe" symptom chased all through
        # 2026-08-27/28 across three different push-client implementations.
        await asyncio.gather(
            asyncio.to_thread(self._run, self.camera.video_queue, str(self.camera.serial_no))
        )
