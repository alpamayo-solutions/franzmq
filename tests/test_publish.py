"""Unit tests for franzmq.Client.publish and publish_tombstone."""
from dataclasses import dataclass
from unittest.mock import patch

from paho.mqtt.client import Client as PahoClient

from franzmq.client import Client
from franzmq.data_contracts.base import Payload
from franzmq.topic import Topic


@dataclass
class _DummyPayload(Payload):
    value: int = 0


def _topic() -> Topic:
    return Topic(payload_type=_DummyPayload, context=("a", "b"))


def test_publish_encodes_payload():
    client = Client()
    with patch.object(PahoClient, "publish") as mock_publish:
        client.publish(_topic(), _DummyPayload(value=7))
    mock_publish.assert_called_once()
    args, _ = mock_publish.call_args
    assert args[1] == _DummyPayload(value=7).encode()
    assert args[2] == 0
    assert args[3] is False


def test_publish_tombstone_sends_empty_retained_payload():
    client = Client()
    with patch.object(PahoClient, "publish") as mock_publish:
        client.publish_tombstone(_topic())
    mock_publish.assert_called_once()
    args, kwargs = mock_publish.call_args
    assert args[1] == b""
    assert args[2] == 0
    assert kwargs.get("retain", args[3] if len(args) > 3 else None) is True


def test_publish_tombstone_passes_custom_qos():
    client = Client()
    with patch.object(PahoClient, "publish") as mock_publish:
        client.publish_tombstone(_topic(), qos=1)
    args, kwargs = mock_publish.call_args
    assert args[2] == 1
    assert kwargs.get("retain", args[3] if len(args) > 3 else None) is True
