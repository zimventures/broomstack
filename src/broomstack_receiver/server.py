"""The HTTP side: read, verify, de-duplicate, hand to the Receiver, answer.

Never logs a request body: on a start it holds the stream key.
"""

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .receiver import snapshot
from .signature import verify

logger = logging.getLogger(__name__)

MAX_BODY = 1024 * 1024


def make_server(receiver, secret, host='127.0.0.1', port=8800):
    class Handler(BaseHTTPRequestHandler):
        server_version = 'BroomstackReceiver'

        def do_GET(self):
            self._answer(200, 'Broomstack receiver: POST webhooks here.')

        def do_POST(self):
            try:
                length = int(self.headers.get('Content-Length') or 0)
            except ValueError:
                return self._answer(400, 'bad Content-Length')
            if length <= 0 or length > MAX_BODY:
                return self._answer(413 if length > MAX_BODY else 400, 'bad body size')
            body = self.rfile.read(length)

            header = self.headers.get('X-Broomstack-Signature') or ''
            if not verify(secret, body, header):
                logger.warning('Refused a request with a bad or missing signature')
                return self._answer(401, 'bad signature')
            try:
                payload = json.loads(body)
            except ValueError:
                return self._answer(400, 'not JSON')
            if not isinstance(payload, dict):
                return self._answer(400, 'not a JSON object')

            delivery = self.headers.get('X-Broomstack-Delivery') or ''
            if payload.get('event') == 'scoreboard.snapshot':
                status, content_type, frame = snapshot(receiver, payload)
                logger.info('Snapshot %s: %s', delivery or '-', status)
                return self._send(status, content_type, frame)
            outcome = receiver.receive(delivery, payload)
            logger.info(
                'Delivery %s (%s): %s %s',
                delivery or '-', payload.get('event'), outcome.status, outcome.reason,
            )
            self._answer(outcome.status, outcome.reason)

        def _answer(self, status, text):
            self._send(status, 'text/plain; charset=utf-8', (text + '\n').encode())

        def _send(self, status, content_type, data):
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass  # our own log lines above, never the default access log

    return ThreadingHTTPServer((host, port), Handler)
