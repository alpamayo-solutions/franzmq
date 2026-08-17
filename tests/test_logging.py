"""Unit tests for the MQTT log handler's topic construction.

(The previous contents of this file were a manual script that connected to a
live broker at import time — it could not run without one, so it only ever
errored during collection. The runnable demo lives in ``examples/log_handler.py``.)
"""
import logging

import pytest

from franzmq.client import Client
from franzmq.log_handlers import MQTTHandler


class _RecordingClient(Client):
    """Client that captures publishes instead of sending them."""

    def __init__(self, node_id=None):
        super().__init__()
        self.node_id = node_id
        self.published = []

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((str(topic), payload))


def _record(name="svc.module", level="INFO"):
    return logging.LogRecord(
        name=name, level=logging.INFO, pathname=__file__, lineno=7,
        msg="hello", args=(), exc_info=None, func="emit_test",
    )


def test_log_topic_carries_the_client_identity():
    client = _RecordingClient(node_id="edge-1")
    handler = MQTTHandler(client)
    handler.setFormatter(logging.Formatter("%(message)s"))

    handler.emit(_record())

    topic, payload = client.published[0]
    assert topic == "example/v1/_Log/edge-1/svc.module/INFO"
    assert payload.message == "hello"


def test_log_handler_reports_a_missing_identity():
    client = _RecordingClient(node_id=None)
    handler = MQTTHandler(client)
    handler.setFormatter(logging.Formatter("%(message)s"))

    with pytest.raises(RuntimeError, match="node_id"):
        client.require_node_id()

    # The handler itself must not raise into the logging call site; it routes
    # the failure through logging's own error handling and publishes nothing.
    handler.handleError = lambda record: None
    handler.emit(_record())
    assert client.published == []
