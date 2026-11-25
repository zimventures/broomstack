# Broomstack streaming receiver

Streams a curling sheet's camera to YouTube when a game starts in [Broomstack](https://broomstack.club/), and stops when it ends.

Broomstack creates each game's YouTube broadcast itself. When a game on your sheet starts, it sends this receiver a signed webhook with the ingest URL and stream key. The receiver pushes your camera there, with ffmpeg or with OBS, and stops on the finish. Nobody at the rink has to touch the streaming PC on game night.

This is the reference implementation of the receiver contract: **[Streaming webhooks: the receiver contract](https://broomstack.club/guides/streaming-webhooks/)**. Run it as is, or use it as a starting point for your own.

## What it does

- **Verifies every request's signature** and refuses anything unsigned or replayed (`401`).
- **Acts on each delivery once.** A repeated delivery is answered `200` and ignored.
- **On `game.started`:** checks the camera or OBS is there, answers, then starts pushing. If it can't stream, it answers `503`, so Broomstack retries and emails the facility's admins.
- **On `stream.restart`:** restarts the push with the stream it is given.
- **On `game.finished`:** stops.
- **On `scoreboard.snapshot`** (live scoring): answers with a recent frame of the stream, a JPEG, or `503` when it isn't streaming. Never queued or remembered with the game deliveries: each request wants a frame now.
- **A start with no stream** (the club has no YouTube channel connected) pushes nothing.
- **A late start** that arrives after its game's finish is ignored.
- **Never logs the stream key.** Every log line is scrubbed of keys it has been given, ffmpeg's own output included.

## Requirements

- Python 3.11 or later.
- One of:
  - **ffmpeg**, for a camera that ffmpeg can read directly (most USB cameras and capture cards); or
  - **OBS Studio 28+**, if you want OBS's scenes and an overlay. The OBS pusher is new and hasn't been run against a live OBS by the maintainers yet: please report how it goes.
- A way for Broomstack to reach the PC over **HTTPS**: a tunnel such as ngrok, or your own reverse proxy.

## Set up

1. **Install it** on the sheet's PC:

   ```
   python3 -m venv .venv
   .venv/bin/pip install "git+https://github.com/zimventures/broomstack.git"
   # with OBS support:
   .venv/bin/pip install "broomstack-receiver[obs] @ git+https://github.com/zimventures/broomstack.git"
   ```

2. **Give it an HTTPS address.** With [ngrok](https://ngrok.com), reserve a domain first, so the address survives restarts:

   ```
   ngrok http --domain=sheet-a-yourrink.ngrok.app 8800
   ```

3. **Add the endpoint in Broomstack.** On the Ice calendar's **Webhooks** tab, add one:
   - **URL:** your HTTPS address.
   - **Sheet:** this PC's sheet.
   - **Stream key and restarts:** turned on, with **Game started and finished** left on too.

   Copy the **signing secret**: it is shown only once.

4. **Configure it.** Copy `receiver.example.toml` to `receiver.toml`, and name your sheet and pusher there. Put the secret in the environment rather than the file:

   ```
   export BROOMSTACK_WEBHOOK_SECRET='the secret you copied'
   ```

5. **Run it:**

   ```
   .venv/bin/broomstack-receiver --config receiver.toml
   ```

6. **Test it.** Press **Send test** beside the endpoint on the Webhooks page. The receiver logs the ping, and Broomstack shows it delivered. Then start a game on the sheet from the kiosk, and end it a minute later. Without an `input` configured, ffmpeg sends a test pattern and a tone, so you can see a broadcast before a camera is plugged in.

Run it as a service (launchd, systemd, or a scheduled task on Windows) so it starts with the PC.

## Pushers

**ffmpeg** reads the `input` you give it (see the example config for macOS, Linux and Windows cameras) and encodes what YouTube asks for: H.264 and AAC in FLV, with a keyframe every two seconds. Override `output` to change the bitrate. The same ffmpeg also writes a frame every `snapshot_every` seconds (default 5) for scoreboard snapshots, so the camera is never opened twice.

**OBS** sets OBS's stream service to the ingest URL and key Broomstack gives it, then starts streaming. Your scenes, camera and browser sources stay as you set them up. A scoreboard snapshot is a screenshot of the scene on air. Turn on OBS's WebSocket server (Tools → WebSocket Server Settings) and give the receiver its password.

OBS saves stream settings, the key included, in its profile on disk. That's OBS's file, not this receiver's, so protect the PC accordingly.

**Your own:** subclass `broomstack_receiver.pushers.Pusher` and implement `check`, `start`, `stop` and `running`, and `snapshot` for live scoring. `start` is handed the stream key: don't log it, don't put it in an exception message, and don't write it anywhere of your own.

## Not yet

Broomstack is adding these to the contract. The receiver will follow:

- **Failure reports.** A push that fails after Broomstack was answered is only logged for now. Broomstack will add a signed URL to report it, so the facility's admins hear about it (`receiver.report_failure`).
- **Overlays:** the scoreboard as a browser source in OBS.

## Developing

```
python3 -m venv .venv && .venv/bin/pip install -e '.[test]'
.venv/bin/pytest
```

The tests use the contract's own example payload (`tests/fixtures/start.json`) and a pinned signature that Broomstack's tests pin too.

## License

Apache License 2.0. See [LICENSE](LICENSE). If you redistribute it, or something built from it, keep the [NOTICE](NOTICE) file with it.
