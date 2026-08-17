"""Unit tests for publishing: encoding, tombstones, reason codes, ordering."""
import threading
import time
from dataclasses import dataclass
from unittest.mock import patch

import pytest
from paho.mqtt.client import Client as PahoClient, MQTTMessage, MQTTMessageInfo
from paho.mqtt.reasoncodes import ReasonCode

from franzmq.client import Client
from franzmq.data_contracts.base import Payload
from franzmq.errors import PublishRejected, PublishTimeout
from franzmq.topic import Topic


@dataclass
class DummyPayload(Payload):
    value: int = 0


def _topic(leaf: str = "b") -> Topic:
    return Topic(payload_type=DummyPayload, node_id="m1", context=("a", leaf))


class _Info:
    """Stand-in for paho's MQTTMessageInfo."""

    def __init__(self, mid: int):
        self.mid = mid
        self.rc = 0


def _reason(value: int) -> ReasonCode:
    return ReasonCode(4, identifier=value)  # 4 = PUBACK


def _ack(client: Client, mid: int, value: int = 0) -> None:
    """Deliver a PUBACK the way paho's packet handler would.

    paho pops the out-message after the callback, so the entry has to exist —
    with `PahoClient.publish` patched out, nothing registered it.
    """
    msg = MQTTMessage(mid)
    msg.qos = 0  # keeps paho's inflight accounting out of a synthetic ack
    msg.info = MQTTMessageInfo(mid)
    client._out_messages[mid] = msg
    client._do_on_publish(mid, _reason(value), None)


def test_publish_encodes_payload():
    client = Client()
    with patch.object(PahoClient, "publish") as mock_publish:
        client.publish(_topic(), DummyPayload(value=7), qos=0)
    args, _ = mock_publish.call_args
    assert args[1] == DummyPayload(value=7).encode()
    assert args[2] == 0
    assert args[3] is False


def test_publish_tombstone_sends_empty_retained_payload():
    client = Client()
    with patch.object(PahoClient, "publish") as mock_publish:
        client.publish_tombstone(_topic())
    args, _ = mock_publish.call_args
    assert args[1] == b""
    assert args[2] == 0
    assert args[3] is True


def test_qos1_publish_waits_for_the_puback():
    client = Client()
    with patch.object(PahoClient, "publish", return_value=_Info(mid=1)):
        threading.Timer(0.05, lambda: _ack(client, 1)).start()
        start = time.monotonic()
        client.publish(_topic(), DummyPayload(value=1), qos=1)
    assert time.monotonic() - start >= 0.05


def test_a_rejecting_puback_raises_with_the_topic_and_the_reason():
    client = Client()
    with patch.object(PahoClient, "publish", return_value=_Info(mid=1)):
        threading.Timer(0.01, lambda: _ack(client, 1, 0x99)).start()
        with pytest.raises(PublishRejected) as exc:
            client.publish(_topic(), DummyPayload(value=1), qos=1)
    assert exc.value.reason_code == 0x99
    assert "payload" in str(exc.value)
    assert str(_topic()) in str(exc.value)


def test_a_silent_broker_times_out_rather_than_blocking_forever():
    client = Client()
    client.publish_timeout = 0.05
    with patch.object(PahoClient, "publish", return_value=_Info(mid=1)):
        with pytest.raises(PublishTimeout):
            client.publish(_topic(), DummyPayload(value=1), qos=1)


def test_fire_and_forget_skips_the_wait():
    client = Client()
    with patch.object(PahoClient, "publish", return_value=_Info(mid=1)):
        info = client.publish(_topic(), DummyPayload(value=1), qos=1, wait=False)
    assert info.mid == 1


def test_qos1_publishes_are_serialized():
    """A second publish must not reach the wire before the first is acked.

    Unbounded in-flight QoS-1 is what lets a reconnect replay an unacked
    message after a newer one already went out.
    """
    client = Client()
    sent = []

    def fake_publish(topic, *_args, **_kwargs):
        sent.append(topic)
        return _Info(mid=len(sent))

    with patch.object(PahoClient, "publish", side_effect=fake_publish):
        first = threading.Thread(target=lambda: client.publish(_topic("one"), DummyPayload(), qos=1))
        first.start()
        time.sleep(0.05)
        second = threading.Thread(target=lambda: client.publish(_topic("two"), DummyPayload(), qos=1))
        second.start()
        time.sleep(0.05)

        assert sent == [str(_topic("one"))], "second publish went out before the first was acked"
        _ack(client, 1)
        first.join(timeout=1)
        time.sleep(0.05)
        assert sent == [str(_topic("one")), str(_topic("two"))]
        _ack(client, 2)
        second.join(timeout=1)

    assert not first.is_alive() and not second.is_alive()


def test_a_tombstone_decodes_to_none():
    """An empty payload retires the record; a consumer must see that, not a
    decode error that leaves it acting on state which no longer exists."""
    from franzmq.data_contracts import PAYLOAD_CLASSES

    PAYLOAD_CLASSES[DummyPayload.get_identifier()] = DummyPayload
    client = Client()
    raw = MQTTMessage(1)
    raw.topic = b"example/v1/_DummyPayload/m1/a/b"
    raw.payload = b""

    message = client._decode_message(raw)

    assert message.payload is None
    assert str(message.topic) == "example/v1/_DummyPayload/m1/a/b"


def test_publishing_from_the_network_thread_does_not_wait():
    """A blocking publish inside a callback would wait for a PUBACK that only
    the blocked thread can deliver — it deadlocks until the timeout, always."""
    client = Client()
    client.publish_timeout = 30.0  # would hang this long if the guard were gone
    result = {}

    def publish_as_the_network_loop():
        with patch.object(PahoClient, "publish", return_value=_Info(mid=1)):
            start = time.monotonic()
            client.publish(_topic(), DummyPayload(value=1), qos=1)
            result["elapsed"] = time.monotonic() - start

    thread = threading.Thread(target=publish_as_the_network_loop)
    client._thread = thread  # paho's own handle on its network loop
    thread.start()
    thread.join(timeout=5)

    assert not thread.is_alive(), "publish from the network thread blocked"
    assert result["elapsed"] < 1
