"""What to do with each event: the contract's rules, in one place.

https://broomstack.club/guides/streaming-webhooks/

* Answer within Broomstack's 10 seconds. A start is checked here (is the
  camera / OBS there?) and answered 503 when it can't stream, so Broomstack
  retries and the facility's admins are told. The push itself runs after.
* Remember a delivery as handled only once it's accepted, so a 503's retry
  isn't thrown away as a duplicate.
* Each sheet's actions run in order on its own worker, so a start and the
  finish right after it can't overtake each other.
* Ignore an event older than the last one acted on for that sheet: a start
  retried for ten minutes can arrive after its game's finish.
"""

import logging
import queue
import threading
from dataclasses import dataclass
from datetime import datetime

from . import secrets
from .dedupe import Seen

logger = logging.getLogger(__name__)

ANY_SHEET = '*'


@dataclass
class Outcome:
    status: int
    reason: str


def usable_stream(payload):
    """``(ingest_url, stream_key)``, or ``None`` when there's nothing to push
    to: no stream block, or either half of it null."""
    block = payload.get('stream') or {}
    ingest, key = block.get('ingest_url'), block.get('stream_key')
    if not ingest or not key:
        return None
    return ingest, secrets.remember(key)


def _when(payload):
    try:
        return datetime.fromisoformat(payload['occurred_at'])
    except (KeyError, TypeError, ValueError):
        return None


class SheetWorker:
    """Runs one sheet's starts and stops, one at a time, in order."""

    def __init__(self, label, pusher, on_failure):
        self.label = label
        self.pusher = pusher
        self.on_failure = on_failure
        self.last = None  # occurred_at of the last event acted on
        #: Where to report a failure of the push now running: set on this
        #: worker's own thread just before that push starts, cleared when it
        #: stops. Never the next game's, which may be waiting in the queue
        #: (code review on PR #2).
        self.report_url = None
        # A push that dies on its own, mid-game, is a failure too.
        pusher.on_exit = lambda why: on_failure(label, why)
        self._queue = queue.Queue()
        threading.Thread(target=self._run, name=f'sheet-{label}', daemon=True).start()

    def submit(self, action, *args):
        self._queue.put((action, args))

    def start(self, report_url, ingest_url, stream_key):
        """Queued: start a push, and from then on report its failures to
        the start that asked for it."""
        self.report_url = report_url
        self.pusher.start(ingest_url, stream_key)

    def stop(self):
        """Queued: stop the push; nothing to report to until the next."""
        self.report_url = None
        self.pusher.stop()

    def drain(self):
        """Wait until everything submitted so far has run (tests, shutdown)."""
        self._queue.join()

    def _run(self):
        while True:
            action, args = self._queue.get()
            try:
                action(*args)
            except Exception as error:  # noqa: BLE001 - one bad push mustn't stop the sheet
                why = secrets.scrub(str(error))
                logger.error('Sheet %s: %s', self.label, why)
                self.on_failure(self.label, why)
            finally:
                self._queue.task_done()


class Receiver:
    def __init__(self, pushers, *, seen=None, on_failure=None, secret=''):
        """``pushers``: sheet label (or ``'*'`` for any) to a Pusher.
        ``secret`` signs failure reports to Broomstack."""
        self.seen = seen if seen is not None else Seen()
        self.secret = secret
        self.on_failure = on_failure or self.report_failure
        self.workers = {
            label: SheetWorker(label, pusher, self.on_failure)
            for label, pusher in pushers.items()
        }

    def report_failure(self, sheet, why):
        """A push that failed after Broomstack was answered: logged, and
        reported to the start's ``report_url`` so the facility's admins hear."""
        from .reporter import send_later

        why = secrets.scrub(why)
        logger.error('Sheet %s: the push failed: %s', sheet, why)
        worker = self.workers.get(sheet)
        if worker is None or not worker.report_url or not self.secret:
            return
        send_later(worker.report_url, self.secret, why)

    def worker_for(self, payload):
        label = ((payload.get('sheet') or {}).get('label')) or None
        return self.workers.get(label) or self.workers.get(ANY_SHEET)

    def receive(self, delivery_id, payload):
        """Handle one verified delivery. Returns the :class:`Outcome` to answer."""
        if delivery_id and delivery_id in self.seen:
            return Outcome(200, 'already handled')
        outcome = self._handle(payload)
        if 200 <= outcome.status < 300 and delivery_id:
            self.seen.add(delivery_id)
        return outcome

    def _handle(self, payload):
        event = payload.get('event')
        if event == 'ping':
            return Outcome(200, 'pong')
        if event not in ('game.started', 'game.finished', 'stream.restart'):
            return Outcome(200, f'ignored: {event!r} is not an event this receiver acts on')

        worker = self.worker_for(payload)
        if worker is None:
            return Outcome(200, 'ignored: not a sheet this receiver streams')

        sheet = ((payload.get('sheet') or {}).get('label')) or worker.label
        when = _when(payload)
        if when is not None and worker.last is not None and when < worker.last:
            return Outcome(200, 'ignored: older than the last event for this sheet')

        if event == 'game.finished':
            worker.last = when or worker.last
            worker.submit(worker.stop)
            logger.info('Sheet %s: game finished, stopping', sheet)
            return Outcome(200, 'stopping')

        target = usable_stream(payload)
        if target is None:
            # No broadcast for this game: nothing to push to. Stop a push
            # left over from a game whose finish never arrived.
            worker.last = when or worker.last
            worker.submit(worker.stop)
            logger.info('Sheet %s: %s without a stream, not pushing', sheet, event)
            return Outcome(200, 'no stream to push')

        problem = worker.pusher.check()
        if problem:
            logger.error('Sheet %s: cannot stream: %s', sheet, problem)
            return Outcome(503, f'cannot stream: {problem}')

        worker.last = when or worker.last
        report_url = (payload.get('stream') or {}).get('report_url') or None
        worker.submit(worker.start, report_url, *target)
        part = (payload.get('stream') or {}).get('part')
        logger.info('Sheet %s: %s, pushing part %s', sheet, event, part)
        return Outcome(202, 'starting')


def snapshot(receiver, payload):
    """Answer a ``scoreboard.snapshot`` request: ``(status, content_type,
    body)``. Never remembered with the game deliveries, and never queued:
    each request wants a frame now."""
    worker = receiver.worker_for(payload)
    if worker is None:
        return 404, 'text/plain; charset=utf-8', b'not a sheet this receiver streams\n'
    try:
        frame = worker.pusher.snapshot()
    except Exception as error:  # noqa: BLE001 - no frame, said plainly
        logger.warning('Sheet %s: no snapshot: %s', worker.label, secrets.scrub(str(error)))
        frame = None
    if frame is None:
        return 503, 'text/plain; charset=utf-8', b'no frame right now\n'
    return 200, frame[0], frame[1]
