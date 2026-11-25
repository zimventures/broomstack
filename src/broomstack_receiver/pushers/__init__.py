"""What actually sends the camera to YouTube.

A pusher drives one sheet's video. The receiver calls ``check()`` before it
answers Broomstack (so a missing camera or a closed OBS gets a 503 and the
facility's admins hear about it), and ``start`` / ``stop`` after.

``start`` receives the stream key. A pusher must not log it, put it in an
exception message, or write it to a file of its own.
"""


class Pusher:
    #: Shown in logs; never includes a key.
    name = 'pusher'

    def check(self):
        """A short reason this pusher can't stream right now, or ``None``.
        Called while Broomstack waits: keep it well under ten seconds."""
        return None

    def start(self, ingest_url, stream_key):
        """Start pushing. Stops any push already running first."""
        raise NotImplementedError

    def stop(self):
        """Stop pushing, if pushing. Never raises for "wasn't running"."""
        raise NotImplementedError

    def running(self):
        raise NotImplementedError

    def snapshot(self):
        """A recent frame of what's being streamed, for live scoring:
        ``(content_type, bytes)``, a JPEG or PNG of at most 2 MB, or ``None``
        when there's no frame to give (not streaming). Called while
        Broomstack waits: keep it well under ten seconds."""
        return None


def build(config):
    """The pusher a sheet's config section asks for."""
    kind = config.get('pusher', 'ffmpeg')
    if kind == 'ffmpeg':
        from .ffmpeg import FfmpegPusher

        return FfmpegPusher(config)
    if kind == 'obs':
        from .obs import ObsPusher

        return ObsPusher(config)
    raise ValueError(f'unknown pusher {kind!r}: use "ffmpeg" or "obs"')
