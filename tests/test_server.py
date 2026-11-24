import hashlib
import hmac
import http.client
import json
import threading
import time

import pytest

from broomstack_receiver.dedupe import Seen
from broomstack_receiver.receiver import Receiver
from broomstack_receiver.server import make_server

SECRET = 'whsec_test'


@pytest.fixture
def server(fake):
    receiver = Receiver({'A': fake}, seen=Seen())
    httpd = make_server(receiver, SECRET, port=0)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield httpd
    httpd.shutdown()


def post(httpd, payload, *, delivery='1', secret=SECRET):
    body = json.dumps(payload, separators=(',', ':'))
    now = int(time.time())
    signature = hmac.new(secret.encode(), f'{now}.{body}'.encode(), hashlib.sha256)
    connection = http.client.HTTPConnection(*httpd.server_address[:2], timeout=5)
    connection.request(
        'POST',
        '/',
        body=body,
        headers={
            'Content-Type': 'application/json',
            'X-Broomstack-Event': payload.get('event', ''),
            'X-Broomstack-Delivery': delivery,
            'X-Broomstack-Signature': f't={now},v1={signature.hexdigest()}',
        },
    )
    response = connection.getresponse()
    return response.status, response.read().decode().strip()


def test_a_signed_start_is_accepted_and_a_repeat_is_not_acted_on(server, start_payload):
    assert post(server, start_payload) == (202, 'starting')
    assert post(server, start_payload) == (200, 'already handled')


def test_a_bad_signature_is_401_and_does_nothing(server, start_payload, fake):
    assert post(server, start_payload, secret='wrong')[0] == 401
    assert fake.calls == []


def test_a_ping(server):
    assert post(server, {'event': 'ping'}) == (200, 'pong')
