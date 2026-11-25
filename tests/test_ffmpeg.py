"""The ffmpeg pusher with a stand-in ffmpeg that echoes its output URL to
stderr, as a failing ffmpeg can: the key must be masked in the log."""

import logging
import os
import stat
import sys
import time

from broomstack_receiver import secrets
from broomstack_receiver.pushers.ffmpeg import FfmpegPusher

KEY = 'abcd-efgh-ijkl-mnop'


def stand_in(tmp_path):
    script = tmp_path / 'ffmpeg'
    script.write_text(
        f'#!{sys.executable}\n'
        'import sys, time\n'
        'url = next(a for a in sys.argv if a.startswith("rtmp"))\n'
        'print("Error opening output", url, file=sys.stderr, flush=True)\n'
        'time.sleep(30)\n'
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return str(script)


def test_start_runs_ffmpeg_and_its_output_is_scrubbed(tmp_path):
    lines = []

    class Capture(logging.Handler):
        def emit(self, record):
            lines.append(self.format(record))

    handler = secrets.install(Capture())
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    pusher = FfmpegPusher({'ffmpeg': stand_in(tmp_path)})
    try:
        assert pusher.check() is None
        pusher.start('rtmp://a.rtmp.youtube.com/live2', KEY)
        deadline = time.time() + 5
        while not any('Error opening output' in line for line in lines):
            assert time.time() < deadline, lines
            time.sleep(0.05)
        assert pusher.running()
    finally:
        pusher.stop()
        root.removeHandler(handler)
    assert not pusher.running()
    assert not any(KEY in line for line in lines)
    assert any('rtmp://a.rtmp.youtube.com/live2/[stream key]' in line for line in lines)


def test_check_says_when_ffmpeg_is_missing():
    assert 'not found' in FfmpegPusher({'ffmpeg': os.path.join('/nope', 'ffmpeg')}).check()


def test_the_push_also_writes_a_frame_and_it_is_offered_when_fresh(tmp_path):
    pusher = FfmpegPusher({'ffmpeg': stand_in(tmp_path)})
    command = pusher.command('rtmp://a.rtmp.youtube.com/live2', KEY)
    target = command.index(f'rtmp://a.rtmp.youtube.com/live2/{KEY}')
    frame = command[target + 1 :]
    assert frame[-1] == pusher.snapshot_path and '-an' in frame and 'image2' in frame
    assert pusher.snapshot() is None  # not streaming
    try:
        pusher.start('rtmp://a.rtmp.youtube.com/live2', KEY)
        with open(pusher.snapshot_path, 'wb') as handle:
            handle.write(b'\xff\xd8\xff\xe0' + b'\x00' * 10)
        assert pusher.snapshot()[0] == 'image/jpeg'
        old = time.time() - 600
        os.utime(pusher.snapshot_path, (old, old))
        assert pusher.snapshot() is None  # stale
    finally:
        pusher.stop()
