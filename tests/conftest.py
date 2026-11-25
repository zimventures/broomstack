import json
import pathlib

import pytest

FIXTURES = pathlib.Path(__file__).parent / 'fixtures'


@pytest.fixture
def start_payload():
    """The contract's own example start, copied from the guide."""
    return json.loads((FIXTURES / 'start.json').read_text())


class FakePusher:
    name = 'fake'

    def __init__(self, problem=None, fail_with=None):
        self.problem = problem
        self.fail_with = fail_with
        self.calls = []

    def check(self):
        return self.problem

    def start(self, ingest_url, stream_key):
        self.calls.append(('start', ingest_url, stream_key))
        if self.fail_with:
            raise RuntimeError(self.fail_with)

    def stop(self):
        self.calls.append(('stop',))

    def running(self):
        return bool(self.calls) and self.calls[-1][0] == 'start'

    frame = None

    def snapshot(self):
        return self.frame


@pytest.fixture
def fake():
    return FakePusher()
