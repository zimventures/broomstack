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
        'print("Error opening output", sys.argv[-1], file=sys.stderr, flush=True)\n'
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
