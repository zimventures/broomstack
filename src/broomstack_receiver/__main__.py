"""``python -m broomstack_receiver --config receiver.toml``"""

import argparse
import logging
import os
import sys
import tomllib

from . import __version__, pushers, secrets
from .dedupe import Seen
from .receiver import Receiver
from .server import make_server

SECRET_ENV = 'BROOMSTACK_WEBHOOK_SECRET'


def load(path):
    with open(path, 'rb') as handle:
        config = tomllib.load(handle)
    secret = os.environ.get(SECRET_ENV) or config.get('secret') or ''
    if not secret:
        raise SystemExit(
            f'No signing secret: set {SECRET_ENV}, or "secret" in {path}. '
            "It's shown once, when the endpoint is added on Broomstack's Webhooks page."
        )
    sheets = config.get('sheets') or {}
    if not sheets:
        raise SystemExit(f'No [sheets.<label>] in {path}: which sheet does this PC stream?')
    return config, secret, sheets


def main(argv=None):
    parser = argparse.ArgumentParser(prog='broomstack-receiver')
    parser.add_argument('--config', default='receiver.toml')
    parser.add_argument('--version', action='version', version=__version__)
    args = parser.parse_args(argv)

    handler = secrets.install(logging.StreamHandler(sys.stderr))
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logging.basicConfig(level=logging.INFO, handlers=[handler])

    config, secret, sheets = load(args.config)
    state = config.get('state_dir', 'state')
    receiver = Receiver(
        {
            str(label): pushers.build(section, label=label, state_dir=state)
            for label, section in sheets.items()
        },
        seen=Seen(os.path.join(state, 'seen.json')),
        secret=secret,
    )
    host, _, port = str(config.get('listen', '127.0.0.1:8800')).rpartition(':')
    server = make_server(receiver, secret, host or '127.0.0.1', int(port))
    logging.info(
        'Listening on http://%s:%s/ for sheet(s) %s', host, port, ', '.join(sheets)
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        # Shutting down: a Ctrl-C reaches ffmpeg too, and its exit isn't a
        # failure to tell the facility's admins about (code review on PR #2).
        for worker in receiver.workers.values():
            worker.pusher.on_exit = lambda why: None
        for worker in receiver.workers.values():
            try:
                worker.pusher.stop()
            except Exception:  # noqa: BLE001
                logging.exception('Sheet %s: could not stop on exit', worker.label)


if __name__ == '__main__':
    main()
