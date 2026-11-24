"""The reference receiver for Broomstack's streaming webhooks.

Runs on a sheet's streaming PC. Broomstack POSTs a signed webhook when a
curling game starts and ends; on a start this pushes the PC's camera to the
YouTube ingest URL and stream key it was given, and on a finish it stops.

The contract it keeps: https://broomstack.club/guides/streaming-webhooks/
"""

__version__ = '0.1.0'
