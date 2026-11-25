"""Push with ffmpeg: a camera (or any ffmpeg input) straight to the ingest.

The command line holds the key (it's part of the RTMP URL), so it is never
logged, and ffmpeg's own output, which can echo its output URL, goes
through the key scrubber before it reaches the log.
"""

import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time

from .. import secrets
from . import Pusher

logger = logging.getLogger(__name__)

# A test pattern and tone when no input is configured: enough to see a
# receiver work end to end before a camera is plugged in.
TEST_INPUT = [
    '-re', '-f', 'lavfi', '-i', 'testsrc2=size=1280x720:rate=30',
    '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=48000',
]
# What YouTube asks for: H.264 and AAC in FLV, a keyframe every 2 seconds.
DEFAULT_OUTPUT = [
    '-c:v', 'libx264', '-preset', 'veryfast', '-b:v', '4500k', '-maxrate', '4500k',
    '-bufsize', '9000k', '-pix_fmt', 'yuv420p', '-g', '60',
    '-c:a', 'aac', '-b:a', '128k', '-ar', '48000',
    '-f', 'flv',
]


#: How often the push also writes a frame for scoreboard snapshots, seconds.
SNAPSHOT_EVERY = 5


class FfmpegPusher(Pusher):
    """Pushes with ffmpeg, and, from the same process, writes a small JPEG
    of the picture every few seconds. That file answers Broomstack's
    scoreboard snapshot requests: the camera is opened once, by the push,
    never a second time while it's streaming."""

    name = 'ffmpeg'

    def __init__(self, config):
        self.binary = config.get('ffmpeg', 'ffmpeg')
        self.input = list(config.get('input', TEST_INPUT))
        self.output = list(config.get('output', DEFAULT_OUTPUT))
        self.snapshot_every = int(config.get('snapshot_every', SNAPSHOT_EVERY))
        self.snapshot_path = os.path.join(
            tempfile.mkdtemp(prefix='broomstack-frame-'), 'frame.jpg'
        )
        self._process = None
        self._lock = threading.Lock()

    def check(self):
        if shutil.which(self.binary) is None:
            return f'ffmpeg not found ({self.binary})'
        return None

    def command(self, ingest_url, stream_key):
        target = f'{ingest_url.rstrip("/")}/{stream_key}'
        # A second output: one frame every few seconds, overwritten in place.
        # Options before an output apply to it alone, so the push's encoding
        # above is untouched.
        frame = [
            '-an', '-vf', f"fps=1/{self.snapshot_every},scale='min(1280,iw)':-2",
            '-q:v', '5', '-update', '1', '-f', 'image2', '-y', self.snapshot_path,
        ]
        return [self.binary, '-hide_banner', '-loglevel', 'warning',
                *self.input, *self.output, target, *frame]

    def start(self, ingest_url, stream_key):
        secrets.remember(stream_key)
        with self._lock:
            self._stop_locked()
            try:
                self._process = subprocess.Popen(
                    self.command(ingest_url, stream_key),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            except OSError as error:
                # The message names the program, not the command line.
                raise RuntimeError(f'could not run ffmpeg: {error.strerror}') from None
            process = self._process
        threading.Thread(target=self._relay, args=(process,), daemon=True).start()
        logger.info('ffmpeg started (pid %s)', process.pid)

    def _relay(self, process):
        for line in process.stderr:
            logger.warning('ffmpeg: %s', secrets.scrub(line.rstrip()))
        code = process.wait()
        if code not in (0, -15, 255):  # 255/-15: stopped by us
            logger.error('ffmpeg exited with %s', code)

    def stop(self):
        with self._lock:
            self._stop_locked()

    def _stop_locked(self):
        process, self._process = self._process, None
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
        logger.info('ffmpeg stopped')

    def running(self):
        return self._process is not None and self._process.poll() is None

    def snapshot(self):
        """The latest frame the push wrote, if it's recent; else ``None``."""
        if not self.running():
            return None
        try:
            age = time.time() - os.path.getmtime(self.snapshot_path)
            if age > max(3 * self.snapshot_every, 15):
                return None
            with open(self.snapshot_path, 'rb') as handle:
                data = handle.read(2 * 1024 * 1024 + 1)
        except OSError:
            return None
        if not data.startswith(b'\xff\xd8\xff') or len(data) > 2 * 1024 * 1024:
            return None  # mid-write, or not what we asked ffmpeg for
        return 'image/jpeg', data
