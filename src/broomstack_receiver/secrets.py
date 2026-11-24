"""Keeping stream keys out of logs.

A stream key lets anyone broadcast on the club's channel. The receiver
never logs one on purpose; this filter is the second line, for the
accident: a library that prints a URL, an exception that carries a command
line. Every key the receiver is handed is registered here first, and every
log record is scrubbed of all of them before it is written.
"""

import logging
import threading

MASK = '[stream key]'

_lock = threading.Lock()
_keys = set()


def remember(key):
    """Register ``key`` to be masked from now on. Returns it."""
    if key:
        with _lock:
            _keys.add(key)
    return key


def scrub(text):
    with _lock:
        keys = sorted(_keys, key=len, reverse=True)
    for key in keys:
        text = text.replace(key, MASK)
    return text


class ScrubFilter(logging.Filter):
    """Masks every remembered key in a record's final message."""

    def filter(self, record):
        message = record.getMessage()
        if record.exc_info:
            message += '\n' + logging.Formatter().formatException(record.exc_info)
            record.exc_info = None
            record.exc_text = None
        record.msg = scrub(message)
        record.args = None
        return True


def install(handler):
    handler.addFilter(ScrubFilter())
    return handler
