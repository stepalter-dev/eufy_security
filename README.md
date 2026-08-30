# Eufy Security for Home Assistant (personal fork)

This is a personal fork of [fuatakgun/eufy_security](https://github.com/fuatakgun/eufy_security), the community-maintained Eufy Security integration for Home Assistant. All credit for the original design and implementation goes to [@fuatakgun](https://github.com/fuatakgun); this fork exists to carry a set of local fixes to the go2rtc/RTSP live-view pipeline (see [Changes in this fork](#changes-in-this-fork)) that haven't been merged upstream.

If you don't need those fixes, use the upstream repo instead — it's more widely used and better supported.

- [Changes in this fork](#changes-in-this-fork)
- [Credits](#credits)
- [How it works](#how-it-works)
- [Supported devices](#supported-devices)
- [Installation](#installation)
- [Setting up the camera dashboard](#setting-up-the-camera-dashboard)
- [Features](#features)
- [Example automations](#example-automations)
- [Getting help](#getting-help)

## Changes in this fork

A series of fixes to the embedded go2rtc live-view path (HA 2023.4+'s modern embedded go2rtc, replacing the legacy fixed-port assumptions the upstream vendored client still uses), plus an auto-start-on-view behavior for camera streams. See the commit history for full detail on each fix.

## Credits

- [@bropat](https://github.com/bropat) built [`eufy-security-ws`](https://github.com/bropat/eufy-security-ws) on top of [`eufy-security-client`](https://github.com/bropat/eufy-security-client) and packaged it as the [`hassio-eufy-security-ws`](https://github.com/bropat/hassio-eufy-security-ws) Home Assistant add-on that this integration talks to.
- [@fuatakgun](https://github.com/fuatakgun) built and maintains the original integration this fork is based on — see the [upstream repo](https://github.com/fuatakgun/eufy_security) and its [Home Assistant community thread](https://community.home-assistant.io/t/eufy-security-integration/318353).

## How it works

- The `eufy-security-ws` add-on holds the actual Eufy cloud session and bridges events to this integration over a local WebSocket connection.
- The add-on needs a Eufy account (email, password, country code), an event duration, and a trusted device name. Because logging in from the add-on ends other active Eufy sessions, it's worth using a secondary account with the devices shared to it (with admin rights), and confirming that account can see the devices from the Eufy mobile app first.
- Country code must match where your Eufy account is actually registered — devices won't show up if the add-on queries the wrong regional server.
- "Event duration" controls how long motion/person-detected sensors stay `on` after a push notification.
- Enable all push notification types in the Eufy mobile app (motion, person, lock, alarm) — the add-on relies on them to know what's happening. You can quiet the notifications themselves on your phone without affecting the integration.

## Supported devices

See upstream's [known-working devices list](https://github.com/bropat/eufy-security-client#known-working-devices).

## Installation

You'll need one add-on and one integration.

**Important:** set streaming quality and codec to LOW everywhere you can in the Eufy app — Home Assistant's video pipeline struggles otherwise.

1. **Install the `eufy-security-ws` add-on** — follow [bropat's guide](https://github.com/bropat/hassio-eufy-security-ws). If you're on HA Core without Supervisor, you'll need to run the container yourself.
2. **(Optional) Install a go2rtc-capable WebRTC integration** if your cameras don't support RTSP natively — this converts P2P streams into something HA can play. [fuatakgun/WebRTC](https://github.com/fuatakgun/WebRTC) bundles go2rtc; [AlexxIT/go2rtc](https://github.com/AlexxIT/go2rtc) is the standalone add-on.
3. **Install this integration.** Since it's not in the default HACS list, add it as a [custom repository](https://hacs.xyz/docs/faq/custom_repositories/) in HACS (Integration type), then install "Eufy Security" and restart Home Assistant.
4. **Add the integration**: Settings → Devices & Services → Add Integration → "Eufy Security" (not "Eufy"). Enter the add-on's host (`127.0.0.1` if it's a local add-on) and port (default `3000`).
5. If prompted for a captcha or MFA code, reconfigure the integration entry to supply it — the code arrives by email/SMS from Eufy. A restart may be needed afterward.
6. If you installed a WebRTC/go2rtc integration, point this integration's config at its host too.
7. Optional settings: cloud scan interval, video analyze duration, and display-name overrides for your first three custom Guard Modes — these map to the alarm panel's `arm_custom_bypass` / `arm_night` / `arm_vacation` services, letting you trigger custom Eufy modes (e.g. a "bedtime" mode) that the stock alarm panel has no button for. See [upstream issue #145](https://github.com/fuatakgun/eufy_security/issues/145) for background.
8. Some diagnostic entities are disabled by default to limit noise. Enable one from its device page (Settings → toggle Enabled and Visible) if you need it — it'll populate within about 30 seconds.

## Setting up the camera dashboard

Home Assistant's built-in camera streaming is workable but slow; a WebRTC card generally performs better. Swap in your own entity IDs:

```yaml
type: custom:webrtc-camera
entity: camera.entrance
poster: image.entrance_event_image
ui: true
shortcuts:
  - name: Play
    icon: mdi:play
    service: camera.turn_on
    service_data:
      entity_id: camera.entrance
  - name: Stop
    icon: mdi:stop
    service: camera.turn_off
    service_data:
      entity_id: camera.entrance
```

Cameras with pan/tilt support can wire up PTZ controls:

```yaml
type: custom:webrtc-camera
entity: camera.garden
ptz:
  service: eufy_security.ptz
  data_left:
    entity_id: camera.garden
    direction: LEFT
  data_right:
    entity_id: camera.garden
    direction: RIGHT
  data_up:
    entity_id: camera.garden
    direction: UP
  data_down:
    entity_id: camera.garden
    direction: DOWN
```

## Features

- **Integration service:** `force_sync` — pull latest state from the cloud on demand (useful for anything not pushed via notification).
- **Camera services:** `turn_on` / `turn_off` (auto-picks RTSP or P2P), `start_rtsp_livestream` / `stop_rtsp_livestream`, `start_p2p_livestream` / `stop_p2p_livestream`, `generate_image`, `ptz_up` / `ptz_down` / `ptz_left` / `ptz_right` / `ptz_360`, `trigger_camera_alarm_with_duration`, `quick_response` (doorbell only, requires an active P2P stream — get `voice_id` from the device's Debug sensor), `snooze`.
- **Alarm panel services:** the Guard Mode select entity mirrors Eufy's own alarm state. `arm_home`, `arm_away`, `disarm`, `alarm_arm_custom1/2/3` (your custom Guard Modes), `geofence`, `schedule`, `trigger_base_alarm_with_duration`, `reset_alarm`, `snooze`, `chime`.
- **Lock services:** `lock` / `unlock`, plus code-based `unlock` for safes.
- Missing a sensor? Share the `Debug (device)` / `Debug (station)` attributes from the affected device when opening an issue.

## Example automations

### Notify with a snapshot on motion/person detection

```yaml
alias: Capture Image on Trigger, Send Mobile Notification with Actions, Snooze or Alarm via Actions
description: ""
trigger:
  - platform: state
    entity_id:
      - binary_sensor.entrance_motion_detected
      - binary_sensor.entrance_person_detected
    to: "on"
    id: sensor
  - platform: event
    event_type: mobile_app_notification_action
    id: snooze
    event_data:
      action: SNOOZE
  - platform: event
    event_type: mobile_app_notification_action
    id: alarm
    event_data:
      action: ALARM
condition: []
action:
  - choose:
      - conditions:
          - condition: trigger
            id: sensor
        sequence:
          - delay:
              hours: 0
              minutes: 0
              seconds: 3
              milliseconds: 0
          - service: notify.mobile_app_fuatx3pro
            data:
              message: Motion detected
              data:
                image: /api/image_proxy/image.entrance_event_image
                actions:
                  - action: ALARM
                    title: Alarm
                  - action: SNOOZE
                    title: Snooze
      - conditions:
          - condition: trigger
            id: snooze
        sequence:
          - service: eufy_security.snooze
            data:
              snooze_time: 10
              snooze_chime: false
              snooze_motion: true
              snooze_homebase: false
            target:
              entity_id: camera.entrance
      - conditions:
          - condition: trigger
            id: alarm
        sequence:
          - service: eufy_security.trigger_camera_alarm_with_duration
            data:
              duration: 1
            target:
              entity_id: camera.entrance
mode: single
```

If snapshots come back stale, trigger on the event image updating instead:

```yaml
trigger:
  - platform: state
    entity_id:
      - image.entrance_cam
    id: sensor
condition:
  - condition: template
    value_template: >-
      {{ as_timestamp(states.image.entrance_cam.last_changed) == as_timestamp(states.image.entrance_cam.last_updated) }}
```

### Unlock a safe with a code

```yaml
service: lock.unlock
data:
  code: "testtest"
target:
  entity_id: lock.safe
```

## Getting help

For anything unrelated to this fork's own changes, the [upstream repo's issue tracker](https://github.com/fuatakgun/eufy_security/issues) and [community thread](https://community.home-assistant.io/t/eufy-security-integration/318353) are the better place to look — most setup problems trace back to push-notification settings, streaming quality, or network-level isolation blocking the add-on. For issues specific to the go2rtc fixes in this fork, open an issue here.
