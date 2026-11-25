"""Telling Broomstack a push failed after it was answered.

The start's stream block names a ``report_url``. When the push fails later
(ffmpeg dies mid-game, OBS refuses the stream), the receiver POSTs a short
signed report there, so the facility's admins hear about it:

    {"status": "error", "message": "ffmpeg stopped unexpectedly (exit 1)"}

Signed exactly as Broomstack signs its calls to the PC, with the same
secret. Sent from a thread of its own, a few tries, the same
``X-Broomstack-Report`` id each time so Broomstack counts it once.
The message is already scrubbed of stream keys; it is never built from one.
"""

import hashlib
import hmac
import json
import logging
import threading
import time
import urllib.error
import urllib.request
import uuid

from . import secrets

logger = logging.getLogger(__name__)

TRIES = 3
WAIT_SECONDS = 5
TIMEOUT_SECONDS = 10
MAX_MESSAGE = 300


def sign(secret, body, timestamp):
    message = f'{timestamp}.'.encode() + body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def send(report_url, secret, message, *, opener=urllib.request.urlopen, wait=time.sleep):
    """POST one report, a few tries. Returns the final HTTP status, or None."""
    body = json.dumps(
        {'status': 'error', 'message': secrets.scrub(message)[:MAX_MESSAGE]},
        separators=(',', ':'),
    ).encode()
    report_id = uuid.uuid4().hex
    status = None
    for attempt in range(1, TRIES + 1):
        timestamp = int(time.time())
        request = urllib.request.Request(
            report_url,
            data=body,
            method='POST',
            headers={
                'Content-Type': 'application/json',
                'User-Agent': 'Broomstack-Receiver',
                'X-Broomstack-Report': report_id,
                'X-Broomstack-Signature': f't={timestamp},v1={sign(secret, body, timestamp)}',
            },
        )
        try:
            with opener(request, timeout=TIMEOUT_SECONDS) as response:
                status = response.status
        except urllib.error.HTTPError as error:
            status = error.code
        except Exception as error:  # noqa: BLE001 - any failure: try again, quietly
            # Network errors, a garbled answer (http.client's errors aren't
            # OSErrors), a malformed URL: logged by type only, never text.
            logger.warning('Report not sent (try %s): %s', attempt, type(error).__name__)
            status = None
        if status is not None and status < 500:
            break  # done, or refused for good (4xx): retrying won't change it
        if attempt < TRIES:
            wait(WAIT_SECONDS)
    logger.info('Failure report: %s', status if status is not None else 'not delivered')
    return status


def send_later(report_url, secret, message):
    """Send on a thread of its own. A daemon: a report still being tried
    when the receiver shuts down is dropped, which is acceptable for a
    failure the stream's own status will show anyway."""
    threading.Thread(
        target=send, args=(report_url, secret, message), name='report', daemon=True
    ).start()
