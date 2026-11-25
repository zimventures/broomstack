"""Push with OBS Studio, through its WebSocket server (OBS 28 and later).

Sets OBS's stream service to the ingest URL and key it was given, then
starts streaming; stops streaming on a finish. Your scenes, camera and any
overlay browser source stay as you set them up in OBS.

OBS keeps its stream settings, key included, in its profile on disk. That
is OBS's file, not this receiver's: protect the PC accordingly.

Needs ``pip install 'broomstack-receiver[obs]'`` (obsws-python). In OBS:
Tools > WebSocket Server Settings > Enable, and set a password.

**Not yet tried against a live OBS by the maintainers.** Reports welcome.
"""

import logging

from .. import secrets
from . import Pusher

logger = logging.getLogger(__name__)


class ObsPusher(Pusher):
    name = 'obs'

    def __init__(self, config):
        self.host = config.get('obs_host', 'localhost')
        self.port = int(config.get('obs_port', 4455))
        self.password = config.get('obs_password', '')
        self.timeout = float(config.get('obs_timeout', 3))

    def _client(self):
        try:
            import obsws_python
        except ImportError:
            raise RuntimeError(
                "OBS support isn't installed: pip install 'broomstack-receiver[obs]'"
            ) from None
        return obsws_python.ReqClient(
            host=self.host, port=self.port, password=self.password, timeout=self.timeout
        )

    def check(self):
        try:
            client = self._client()
            client.get_version()
            client.disconnect()
        except Exception as error:  # noqa: BLE001 - any failure is "not ready"
            return f'OBS not reachable at {self.host}:{self.port} ({type(error).__name__})'
        return None

    def start(self, ingest_url, stream_key):
        secrets.remember(stream_key)
        client = self._client()
        try:
            if client.get_stream_status().output_active:
                client.stop_stream()
            client.set_stream_service_settings(
                'rtmp_custom', {'server': ingest_url, 'key': stream_key}
            )
            client.start_stream()
        except Exception as error:  # noqa: BLE001
            # Never the error's text: a library may have put the key in it.
            raise RuntimeError(f'OBS refused the stream ({type(error).__name__})') from None
        finally:
            client.disconnect()
        logger.info('OBS streaming started')

    def stop(self):
        client = self._client()
        try:
            if client.get_stream_status().output_active:
                client.stop_stream()
                logger.info('OBS streaming stopped')
        finally:
            client.disconnect()

    def snapshot(self):
        """A screenshot of the scene on air: what the stream shows."""
        import base64

        try:
            client = self._client()
        except RuntimeError:
            return None
        try:
            if not client.get_stream_status().output_active:
                return None
            scene = client.get_current_program_scene()
            name = getattr(scene, 'current_program_scene_name', None) or getattr(
                scene, 'scene_name', None
            )
            shot = client.get_source_screenshot(name, 'jpg', 1280, 720, 80)
            data = getattr(shot, 'image_data', '') or ''
            data = base64.b64decode(data.split(',', 1)[-1])
        except Exception:  # noqa: BLE001 - no frame is an answer, not a crash
            return None
        finally:
            client.disconnect()
        if not data.startswith(b'\xff\xd8\xff') or len(data) > 2 * 1024 * 1024:
            return None
        return 'image/jpeg', data

    def running(self):
        client = self._client()
        try:
            return bool(client.get_stream_status().output_active)
        finally:
            client.disconnect()
