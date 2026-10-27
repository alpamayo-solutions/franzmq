"""Unit tests for publishing: encoding, tombstones, reason codes, ordering."""
import socket
import threading
import time
from dataclasses import dataclass
from unittest.mock import patch

import pytest
import paho.mqtt.client as paho_client
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


def test_qos1_publishes_beyond_the_window_queue_in_order_and_publishers_do_not_wait():
    """No more than the in-flight window reaches the wire before PUBACKs come
    back; the rest sits queued inside paho, in publish order, and no
    publisher was blocked to achieve that."""
    client = Client()
    window = client._max_inflight_messages
    ours, theirs = socket.socketpair()
    client._sock = ours
    try:
        infos = [client.publish(_topic(f"t{i}"), DummyPayload(), qos=1, wait=False) for i in range(window + 2)]
        states = [client._out_messages[info.mid].state for info in infos]
    finally:
        client._sock = None
        ours.close()
        theirs.close()

    assert states[:window] == [paho_client.mqtt_ms_wait_for_puback] * window, states
    assert states[window:] == [paho_client.mqtt_ms_queued] * 2


def test_a_publish_from_a_message_callback_does_not_wait_behind_a_waiting_publisher():
    """The deadlock this replaces: thread A waits for its PUBACK; a message
    arrives; the network thread joins the callback thread; the callback
    publishes. With a lock held across A's wait the callback blocked on it,
    the network thread on the callback, and A's PUBACK on the network
    thread -- until the timeout. Here the main thread plays the callback
    thread: its publish must return at once, and A's PUBACK, delivered
    afterwards (as the freed network thread would), must resolve A."""
    client = Client()
    client.publish_timeout = 1.0
    outcome: dict = {}

    def waiter():
        try:
            client.publish(_topic("a"), DummyPayload(), qos=1)
            outcome["ok"] = True
        except PublishTimeout as exc:
            outcome["error"] = exc

    with patch.object(PahoClient, "publish", side_effect=[_Info(mid=1), _Info(mid=2)]):
        a = threading.Thread(target=waiter)
        a.start()
        time.sleep(0.05)

        start = time.monotonic()
        client.publish(_topic("cb"), DummyPayload(), qos=1, wait=False)
        assert time.monotonic() - start < 0.2, "the callback's publish queued behind the waiter"

        _ack(client, 1)
        a.join(timeout=1)

    assert outcome == {"ok": True}, outcome


def test_a_waiting_publish_on_a_callback_thread_is_sent_without_waiting():
    """A callback thread is joined by the network thread, so a PUBACK cannot
    be read while it waits -- franzmq sends and returns instead of timing out."""
    client = Client()
    client.publish_timeout = 1.0
    elapsed: dict = {}

    def callback(_message):
        start = time.monotonic()
        client.publish(_topic("cb"), DummyPayload(), qos=1)  # wait=True, and no ack ever comes
        elapsed["s"] = time.monotonic() - start

    with patch.object(PahoClient, "publish", return_value=_Info(mid=7)):
        client._execute_callbacks_concurrently(object(), [callback])

    assert elapsed["s"] < 0.5, f"a callback-thread publish waited {elapsed['s']:.2f}s for a PUBACK it cannot read"


def test_a_puback_that_beats_the_registration_still_resolves_the_waiter():
    """paho may send and the broker answer before `publish()` has returned the
    mid to the caller; that ack must not be lost to the waiter."""
    client = Client()
    client.publish_timeout = 0.5

    def publish_and_ack_first(*_args, **_kwargs):
        _ack(client, 3)
        return _Info(mid=3)

    with patch.object(PahoClient, "publish", side_effect=publish_and_ack_first):
        start = time.monotonic()
        client.publish(_topic(), DummyPayload(value=1), qos=1)
    assert time.monotonic() - start < 0.3


def test_a_rejection_is_attributed_to_the_message_that_earned_it():
    client = Client()
    client.publish_timeout = 1.0
    verdicts: dict = {}

    def publisher(name):
        try:
            client.publish(_topic(name), DummyPayload(), qos=1)
            verdicts[name] = "ok"
        except PublishRejected as exc:
            verdicts[name] = exc.reason_code

    with patch.object(PahoClient, "publish", side_effect=[_Info(mid=1), _Info(mid=2)]):
        one = threading.Thread(target=publisher, args=("one",))
        one.start()
        time.sleep(0.05)
        two = threading.Thread(target=publisher, args=("two",))
        two.start()
        time.sleep(0.05)
        _ack(client, 2, value=0x87)  # NotAuthorized, for the SECOND message
        _ack(client, 1)
        one.join(timeout=1)
        two.join(timeout=1)

    assert verdicts == {"one": "ok", "two": 0x87}


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


def test_a_tombstone_reaches_a_subscription_callback_as_none():
    """The path paho actually delivers through: a retained tombstone a node
    hands a new subscriber must reach its callback, not kill the network
    thread in a second decoder."""
    import threading

    from franzmq.data_contracts import PAYLOAD_CLASSES

    PAYLOAD_CLASSES[DummyPayload.get_identifier()] = DummyPayload
    client = Client()
    received = []
    done = threading.Event()
    client.subscribe(
        "example/v1/_DummyPayload/m1/#",
        callback=lambda message: (received.append(message), done.set()),
    )
    raw = MQTTMessage(1)
    raw.topic = b"example/v1/_DummyPayload/m1/a/b"
    raw.payload = b""
    raw.retain = True

    client._handle_on_message(raw)

    assert done.wait(2), "the callback never ran"
    assert [message.payload for message in received] == [None]
    assert str(received[0].topic) == "example/v1/_DummyPayload/m1/a/b"


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


# -- order across a reconnect -------------------------------------------------


def _connected(client: Client):
    """Give paho a socket so publishes take the in-flight window; nothing is
    written until a loop runs. Returns what to close."""
    ours, theirs = socket.socketpair()
    client._sock = ours
    client._state = paho_client._ConnectionState.MQTT_CS_CONNECTED
    return ours, theirs


def _connack(client: Client) -> None:
    """Hand paho a successful MQTT 5 CONNACK, as its packet reader would."""
    client._in_packet = {"remaining_length": 3, "packet": bytes([0, 0, 0])}
    client._handle_connack()


def _sent_after_reconnect(client: Client, on_connect=None) -> list[tuple[str, bytes]]:
    sent: list[tuple[str, bytes]] = []
    real = client._send_publish

    def recording(mid, topic, payload, *args, **kwargs):
        sent.append((topic.decode(), payload))
        return real(mid, topic, payload, *args, **kwargs)

    client._send_publish = recording
    client.on_connect = on_connect
    client._messages_reconnect_reset()  # what paho's reconnect() does first
    return sent


def test_a_publish_made_during_a_reconnect_goes_out_after_every_older_unacknowledged_one():
    """paho resends the stored messages only after on_connect ran; a publish
    made there (or by another thread meanwhile) found the window empty and
    went out first: on a retained topic the node then kept the older record."""
    client = Client()
    ours, theirs = _connected(client)
    try:
        for value in range(3):
            client.publish(_topic("state"), DummyPayload(value=value), qos=1, wait=False)

        sent = _sent_after_reconnect(
            client, lambda *a: client.publish(_topic("state"), DummyPayload(value=3), qos=1, wait=False)
        )
        _connack(client)
    finally:
        client._sock = None
        ours.close()
        theirs.close()

    # Resent first, the new one last: the wire order is the publish order.
    assert sent == [(str(_topic("state")), DummyPayload(value=v).encode().encode()) for v in range(4)]
    assert client._inflight_messages == 4


def test_a_reconnect_resends_one_window_and_queues_the_rest_in_order():
    client = Client()
    client.max_inflight_messages_set(2)
    ours, theirs = _connected(client)
    try:
        mids = [client.publish(_topic(f"t{i}"), DummyPayload(value=i), qos=1, wait=False).mid for i in range(5)]
        sent = _sent_after_reconnect(client)
        _connack(client)
        assert [topic for topic, _ in sent] == [str(_topic("t0")), str(_topic("t1"))], "one window at once"
        assert client._inflight_messages == 2
        # Each PUBACK sends the oldest one still queued.
        for mid in mids[:3]:
            with client._out_message_mutex:
                client._do_on_publish(mid, _reason(0), None)
    finally:
        client._sock = None
        ours.close()
        theirs.close()
    assert [topic for topic, _ in sent] == [str(_topic(f"t{i}")) for i in range(5)]


def test_the_window_is_wider_than_one():
    assert Client()._max_inflight_messages > 1
