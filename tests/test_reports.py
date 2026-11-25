"""Failure reports to Broomstack's report_url."""

import io
import json
import time
import urllib.error

from broomstack_receiver import reporter, secrets
from broomstack_receiver.receiver import Receiver
from broomstack_receiver.dedupe import Seen
from broomstack_receiver.signature import verify

from .conftest import FakePusher


class Opener:
    def __init__(self, *statuses):
        self.statuses = list(statuses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append(request)
        status = self.statuses.pop(0)
        if isinstance(status, Exception):
            raise status
        if status >= 400:
            raise urllib.error.HTTPError(request.full_url, status, 'x', {}, io.BytesIO())
        return _Response(status)


class _Response:
    def __init__(self, status):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_a_report_is_signed_like_broomstacks_own_calls():
    opener = Opener(200)
    assert reporter.send('https://b.example/api/streams/4/report/', 'whsec', 'no camera', opener=opener) == 200
    (request,) = opener.requests
    body = request.data
    assert json.loads(body) == {'status': 'error', 'message': 'no camera'}
    assert verify('whsec', body, request.headers['X-broomstack-signature'])
    assert request.headers['X-broomstack-report']


def test_retries_keep_the_same_report_id_and_stop_on_a_4xx():
    opener = Opener(OSError('down'), 503, 200)
    waits = []
    assert reporter.send('https://b.example/r/', 's', 'x', opener=opener, wait=waits.append) == 200
    ids = {r.headers['X-broomstack-report'] for r in opener.requests}
    assert len(opener.requests) == 3 and len(ids) == 1 and len(waits) == 2
    refused = Opener(401)
    assert reporter.send('https://b.example/r/', 's', 'x', opener=refused, wait=waits.append) == 401
    assert len(refused.requests) == 1


def test_a_key_in_the_message_is_masked():
    secrets.remember('SECRET-KEY-123')
    opener = Opener(200)
    reporter.send('https://b.example/r/', 's', 'push to rtmp://x/SECRET-KEY-123 failed', opener=opener)
    assert b'SECRET-KEY-123' not in opener.requests[0].data


def test_the_receiver_reports_to_the_current_starts_url(start_payload, monkeypatch):
    sent = []
    monkeypatch.setattr(reporter, 'send_later', lambda url, secret, why: sent.append((url, secret, why)))
    pusher = FakePusher(fail_with='OBS refused the stream')
    receiver = Receiver({'A': pusher}, seen=Seen(), secret='whsec')
    start_payload['stream']['report_url'] = 'https://b.example/api/streams/40/report/'
    receiver.receive('1', start_payload)
    receiver.workers['A'].drain()
    assert sent == [('https://b.example/api/streams/40/report/', 'whsec', 'OBS refused the stream')]


def test_nothing_to_report_to_between_games(start_payload, monkeypatch):
    sent = []
    monkeypatch.setattr(reporter, 'send_later', lambda *args: sent.append(args))
    receiver = Receiver({'A': FakePusher()}, seen=Seen(), secret='whsec')
    start_payload['stream']['report_url'] = 'https://b.example/r/'
    receiver.receive('1', start_payload)
    finish = {k: v for k, v in start_payload.items() if k != 'stream'}
    finish.update(event='game.finished', occurred_at='2099-01-01T00:00:00+00:00')
    receiver.receive('2', finish)
    receiver.workers['A'].drain()
    receiver.report_failure('A', 'late')
    assert sent == []


def test_ffmpeg_ending_on_its_own_is_reported_but_a_stop_is_not(tmp_path):
    import stat
    import sys

    from broomstack_receiver.pushers.ffmpeg import FfmpegPusher

    quits = tmp_path / 'ffmpeg-quits'
    quits.write_text(f'#!{sys.executable}\nimport sys\nsys.exit(1)\n')
    quits.chmod(quits.stat().st_mode | stat.S_IEXEC)
    pusher = FfmpegPusher({'ffmpeg': str(quits)}, state_dir=str(tmp_path))
    exits = []
    pusher.on_exit = exits.append
    pusher.start('rtmp://x/live2', 'KEY')
    deadline = time.time() + 5
    while not exits and time.time() < deadline:
        time.sleep(0.05)
    assert exits == ['ffmpeg stopped unexpectedly (exit 1)']

    sleeps = tmp_path / 'ffmpeg-sleeps'
    sleeps.write_text(f'#!{sys.executable}\nimport time\ntime.sleep(30)\n')
    sleeps.chmod(sleeps.stat().st_mode | stat.S_IEXEC)
    stopped = FfmpegPusher({'ffmpeg': str(sleeps)}, state_dir=str(tmp_path / 's'))
    stopped_exits = []
    stopped.on_exit = stopped_exits.append
    stopped.start('rtmp://x/live2', 'KEY')
    process = stopped._process
    stopped.stop()
    process.wait(timeout=5)  # the relay's own wait has returned too, or is about to
    time.sleep(0.2)
    assert stopped_exits == []


def test_a_failure_goes_to_its_own_starts_url_not_the_next_ones(start_payload, monkeypatch):
    """Part 1 fails while part 2's restart waits in the queue: part 1's URL."""
    import copy
    import threading

    sent = []
    monkeypatch.setattr(reporter, 'send_later', lambda url, secret, why: sent.append(url))
    gate = threading.Event()

    class SlowFailing(FakePusher):
        def start(self, ingest_url, stream_key):
            gate.wait(5)
            raise RuntimeError('OBS refused the stream')

    receiver = Receiver({'A': SlowFailing()}, seen=Seen(), secret='whsec')
    first = copy.deepcopy(start_payload)
    first['stream']['report_url'] = 'https://b.example/api/streams/1/report/'
    receiver.receive('1', first)
    second = copy.deepcopy(start_payload)
    second['event'] = 'stream.restart'
    second['occurred_at'] = '2026-11-24T23:30:00+00:00'
    second['stream']['report_url'] = 'https://b.example/api/streams/2/report/'
    receiver.receive('2', second)
    gate.set()
    receiver.workers['A'].drain()
    assert sent[0] == 'https://b.example/api/streams/1/report/'


def test_ffmpeg_dying_is_reported_through_the_sheet_to_its_url(start_payload, monkeypatch, tmp_path):
    import stat
    import sys

    from broomstack_receiver.pushers.ffmpeg import FfmpegPusher

    sent = []
    monkeypatch.setattr(reporter, 'send_later', lambda url, secret, why: sent.append((url, why)))
    quits = tmp_path / 'ffmpeg'
    quits.write_text(f'#!{sys.executable}\nimport sys, time\ntime.sleep(0.3)\nsys.exit(1)\n')
    quits.chmod(quits.stat().st_mode | stat.S_IEXEC)
    receiver = Receiver(
        {'A': FfmpegPusher({'ffmpeg': str(quits)}, label='A', state_dir=str(tmp_path))},
        seen=Seen(),
        secret='whsec',
    )
    start_payload['stream']['report_url'] = 'https://b.example/api/streams/40/report/'
    receiver.receive('1', start_payload)
    receiver.workers['A'].drain()
    deadline = time.time() + 5
    while not sent and time.time() < deadline:
        time.sleep(0.05)
    assert sent == [('https://b.example/api/streams/40/report/', 'ffmpeg stopped unexpectedly (exit 1)')]


def test_a_garbled_answer_is_retried_not_dropped():
    import http.client

    opener = Opener(http.client.BadStatusLine('x'), 200)
    assert reporter.send('https://b.example/r/', 's', 'x', opener=opener, wait=lambda s: None) == 200
