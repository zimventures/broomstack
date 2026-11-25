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
    pusher = FfmpegPusher({'ffmpeg': stand_in(tmp_path)}, state_dir=str(tmp_path))
    command = pusher.command('rtmp://a.rtmp.youtube.com/live2', KEY)
    target = command.index(f'rtmp://a.rtmp.youtube.com/live2/{KEY}')
    frame = command[target + 1 :]
    assert frame[-1] == pusher.snapshot_path and '-an' in frame and 'image2' in frame
    assert pusher.snapshot_path.startswith(str(tmp_path))
    assert pusher.snapshot() is None  # not streaming
    whole = b'\xff\xd8\xff\xe0' + b'\x00' * 10 + b'\xff\xd9'
    try:
        pusher.start('rtmp://a.rtmp.youtube.com/live2', KEY)
        with open(pusher.snapshot_path, 'wb') as handle:
            handle.write(whole)
        assert pusher.snapshot() == ('image/jpeg', whole)
        # Mid-rewrite: cut short. The last whole frame is offered instead.
        with open(pusher.snapshot_path, 'wb') as handle:
            handle.write(whole[:8])
        assert pusher.snapshot() == ('image/jpeg', whole)
        old = time.time() - 600
        os.utime(pusher.snapshot_path, (old, old))
        pusher._last_frame = (old, whole)
        assert pusher.snapshot() is None  # stale
    finally:
        pusher.stop()


def test_snapshot_every_is_checked_when_the_pusher_is_built():
    import pytest

    for bad in (0, 11, 2.5, '5'):
        with pytest.raises(ValueError, match='snapshot_every'):
            FfmpegPusher({'snapshot_every': bad})


def test_with_real_ffmpeg_the_push_survives_a_missing_frame_directory(tmp_path):
    """The reviewer's case: the frame directory gone between games. It is
    made again before each push, so the push runs and frames come."""
    import shutil

    import pytest

    if shutil.which('ffmpeg') is None:
        pytest.skip('ffmpeg is not installed')
    pusher = FfmpegPusher(
        {'snapshot_every': 1, 'output': ['-c:v', 'libx264', '-preset', 'ultrafast', '-c:a', 'aac', '-f', 'flv']},
        state_dir=str(tmp_path / 'state'),
    )
    out = tmp_path / 'pushed'
    out.mkdir()
    shutil.rmtree(tmp_path / 'state', ignore_errors=True)
    try:
        pusher.start(str(out), 'game.flv')  # a local file stands in for RTMP
        deadline = time.time() + 10
        frame = None
        while frame is None and time.time() < deadline:
            time.sleep(0.25)
            frame = pusher.snapshot()
        assert frame is not None and frame[1].startswith(b'\xff\xd8\xff')
        assert pusher.running()  # the push is still going
    finally:
        pusher.stop()
    assert (out / 'game.flv').stat().st_size > 0
