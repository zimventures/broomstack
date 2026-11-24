import hashlib
import hmac
import re
import time

from broomstack_receiver.signature import verify

# Pinned in Broomstack's own tests too (tests_receiver_contract.py), so a
# change to signing on either side fails a test, not a facility.
PINNED = 'aa8efe37b751e71157c508c5ac4acb1e9fe5225db98355dfc00f4b680afbc447'


def sign(secret, body, timestamp):
    message = f'{timestamp}.{body}'.encode()
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def test_the_pinned_vector():
    assert sign('whsec_test', '{"event":"ping"}', 1700000000) == PINNED
    header = f't=1700000000,v1={PINNED}'
    assert verify('whsec_test', b'{"event":"ping"}', header, now=1700000000)


def test_refuses_tampering_a_wrong_secret_a_replay_and_junk():
    now = int(time.time())
    body = '{"event":"ping"}'
    header = f't={now},v1={sign("s", body, now)}'
    assert verify('s', body.encode(), header)
    assert not verify('s', b'{"event":"pong"}', header)
    assert not verify('other', body.encode(), header)
    assert not verify('s', body.encode(), header, now=now + 301)
    assert not verify('s', body.encode(), '')
    assert not verify('s', body.encode(), 't=x,v1=y')


def test_verify_is_the_guides_sample_verbatim():
    """The receiver's verify is the contract's; keep them identical."""
    import inspect

    from broomstack_receiver import signature

    source = inspect.getsource(signature.verify)
    assert re.search(r'message = f\'\{timestamp\}\.\'\.encode\(\) \+ body', source)
