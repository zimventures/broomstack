"""Delivery ids already handled, so a repeat isn't acted on twice.

Broomstack can send the same delivery again (its worker stopped after you
answered). Only an *accepted* delivery is remembered: one answered 503 has
to be retried, not thrown away as a duplicate. Kept for a day, in a small
JSON file of ids and times, nothing else: never a payload.
"""

import json
import os
import tempfile
import threading
import time

KEEP_SECONDS = 24 * 60 * 60


class Seen:
    def __init__(self, path=None, keep=KEEP_SECONDS, clock=time.time):
        self.path = path
        self.keep = keep
        self.clock = clock
        self._lock = threading.Lock()
        self._ids = self._load()

    def _load(self):
        if not self.path or not os.path.exists(self.path):
            return {}
        try:
            with open(self.path) as handle:
                data = json.load(handle)
            return {str(key): float(value) for key, value in data.items()}
        except (OSError, ValueError, AttributeError):
            return {}  # a damaged file costs at most a repeat, not a crash

    def _save(self):
        if not self.path:
            return
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        fd, temporary = tempfile.mkstemp(dir=directory, prefix='.seen-')
        with os.fdopen(fd, 'w') as handle:
            json.dump(self._ids, handle)
        os.replace(temporary, self.path)

    def __contains__(self, delivery_id):
        with self._lock:
            return str(delivery_id) in self._ids

    def add(self, delivery_id):
        with self._lock:
            now = self.clock()
            self._ids = {
                key: when for key, when in self._ids.items() if now - when < self.keep
            }
            self._ids[str(delivery_id)] = now
            self._save()
