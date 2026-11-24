import copy
import logging

from broomstack_receiver import secrets
from broomstack_receiver.dedupe import Seen
from broomstack_receiver.receiver import Receiver

from .conftest import FakePusher


def receiver_for(pusher, label='A', **kwargs):
    return Receiver({label: pusher}, seen=Seen(), **kwargs)


def finish(payload, occurred_at=None):
    done = {k: v for k, v in payload.items() if k != 'stream'}
    done['event'] = 'game.finished'
    if occurred_at:
        done['occurred_at'] = occurred_at
    return done


def settle(receiver):
    for worker in receiver.workers.values():
        worker.drain()


def test_a_start_pushes_to_the_ingest_with_the_key(start_payload, fake):
    receiver = receiver_for(fake)
    outcome = receiver.receive('1', start_payload)
    settle(receiver)
    assert outcome.status == 202
    stream = start_payload['stream']
    assert fake.calls == [('start', stream['ingest_url'], stream['stream_key'])]


def test_a_finish_stops(start_payload, fake):
    receiver = receiver_for(fake)
    receiver.receive('1', start_payload)
    later = finish(start_payload, '2026-11-24T23:59:00+00:00')
    assert receiver.receive('2', later).status == 200
    settle(receiver)
    assert [call[0] for call in fake.calls] == ['start', 'stop']


def test_a_repeated_delivery_is_not_acted_on_twice(start_payload, fake):
    receiver = receiver_for(fake)
    receiver.receive('1', start_payload)
    assert receiver.receive('1', start_payload).reason == 'already handled'
    settle(receiver)
    assert len(fake.calls) == 1


def test_a_start_that_cannot_stream_is_503_and_its_retry_is_not_a_duplicate(start_payload):
    pusher = FakePusher(problem='no camera')
    receiver = receiver_for(pusher)
    assert receiver.receive('1', start_payload).status == 503
    pusher.problem = None  # the camera is plugged back in
    assert receiver.receive('1', start_payload).status == 202
    settle(receiver)
    assert [call[0] for call in pusher.calls] == ['start']


def test_no_stream_block_or_a_null_half_means_dont_push(start_payload, fake):
    receiver = receiver_for(fake)
    for index, change in enumerate(
        (
            lambda p: p.pop('stream'),
            lambda p: p['stream'].update(stream_key=None),
            lambda p: p['stream'].update(ingest_url=None),
        )
    ):
        payload = copy.deepcopy(start_payload)
        change(payload)
        assert receiver.receive(str(index), payload).reason == 'no stream to push'
    settle(receiver)
    assert all(call == ('stop',) for call in fake.calls)


def test_a_late_start_after_its_finish_is_ignored(start_payload, fake):
    receiver = receiver_for(fake)
    receiver.receive('2', finish(start_payload, '2026-11-24T23:59:00+00:00'))
    assert 'older' in receiver.receive('1', start_payload).reason
    settle(receiver)
    assert fake.calls == [('stop',)]


def test_another_sheets_game_is_not_this_pcs(start_payload, fake):
    receiver = receiver_for(fake, label='B')
    assert 'not a sheet' in receiver.receive('1', start_payload).reason
    settle(receiver)
    assert fake.calls == []


def test_any_sheet(start_payload, fake):
    receiver = receiver_for(fake, label='*')
    assert receiver.receive('1', start_payload).status == 202


def test_a_ping_answers_and_starts_nothing(fake):
    receiver = receiver_for(fake)
    assert receiver.receive('9', {'event': 'ping'}).reason == 'pong'
    assert fake.calls == []


def test_a_restart_starts_again(start_payload, fake):
    receiver = receiver_for(fake)
    receiver.receive('1', start_payload)
    restart = copy.deepcopy(start_payload)
    restart['event'] = 'stream.restart'
    restart['occurred_at'] = '2026-11-24T23:30:00+00:00'
    assert receiver.receive('2', restart).status == 202
    settle(receiver)
    assert [call[0] for call in fake.calls] == ['start', 'start']


def test_the_key_never_reaches_a_log_or_the_state_file(start_payload, tmp_path):
    """A push that fails with the key in its error, the worst case."""
    key = start_payload['stream']['stream_key']
    lines = []

    class Capture(logging.Handler):
        def emit(self, record):
            lines.append(self.format(record))

    handler = secrets.install(Capture())
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    failures = []
    try:
        pusher = FakePusher(fail_with=f'rtmp://a.rtmp.youtube.com/live2/{key} refused')
        receiver = Receiver(
            {'A': pusher},
            seen=Seen(str(tmp_path / 'seen.json')),
            on_failure=lambda sheet, why: failures.append(why),
        )
        receiver.receive('1', start_payload)
        settle(receiver)
    finally:
        root.removeHandler(handler)

    assert lines, 'expected the failure to be logged'
    assert not any(key in line for line in lines)
    assert any('[stream key]' in line for line in lines)
    assert not any(key in why for why in failures)
    for path in tmp_path.rglob('*'):
        assert key not in path.read_text()
