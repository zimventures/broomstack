"""Verifying X-Broomstack-Signature.

``verify`` is the contract's own sample, unchanged, so the two can't drift:
https://broomstack.club/guides/streaming-webhooks/#verifying-the-signature
"""

import hashlib
import hmac
import time

TOLERANCE_SECONDS = 300


def verify(secret, body, header, now=None):
    """Whether ``header`` (X-Broomstack-Signature) signs ``body`` (bytes)."""
    try:
        parts = dict(part.split('=', 1) for part in header.split(','))
        timestamp = int(parts['t'])
        signature = parts['v1']
    except (KeyError, ValueError):
        return False
    if abs((now or time.time()) - timestamp) > TOLERANCE_SECONDS:
        return False  # too old, or a clock far off: treat as a replay
    message = f'{timestamp}.'.encode() + body
    expected = hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)
