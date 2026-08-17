"""Command/ack: one command, one ack, the broker's result codes."""
import time
from dataclasses import dataclass, field

import pytest

from franzmq.client import Client
from franzmq.data_contracts import PAYLOAD_CLASSES
from franzmq.data_contracts.base import Ack, Cmd
from franzmq.message import Message
from franzmq.topic import Topic


@dataclass
class CmdThing(Cmd):
    command: dict = field(default_factory=dict)


@pytest.fixture(autouse=True)
def _register():
    PAYLOAD_CLASSES[CmdThing.get_identifier()] = CmdThing
    yield
    PAYLOAD_CLASSES.pop(CmdThing.get_identifier(), None)


class RecordingClient(Client):
    """Captures publishes instead of sending them."""

    def __init__(self):
        super().__init__()
        self.published: list[tuple[str, object]] = []

    def publish(self, topic, payload, qos=0, retain=False, wait=True):
        self.published.append((str(topic), payload))


def _cmd_topic() -> Topic:
    return Topic(payload_type=CmdThing, node_id="m1", prefix="alp", context=("m1", "go"))


def _message(expires_in: float = 30.0, correlation_id: str = "c-1") -> Message:
    payload = CmdThing(
        created_at=time.time(),
        correlation_id=correlation_id,
        expires_at=time.time() + expires_in,
        command={"speed": 5},
    )
    return Message(topic=_cmd_topic(), payload=payload)


def test_a_successful_command_gets_exactly_one_ack():
    client = RecordingClient()

    client.make_command_handler(lambda message: None)(_message())

    assert len(client.published) == 1, "colca acks once — there is no handshake ack"
    topic, ack = client.published[0]
    assert topic == "alp/v1/_Ack/m1/m1/go"
    assert (ack.result_code, ack.correlation_id) == (200, "c-1")
    assert ack.performed_at is not None


def test_a_result_tuple_is_carried_into_the_ack():
    client = RecordingClient()

    client.make_command_handler(lambda message: (409, "already bound"))(_message())

    _, ack = client.published[0]
    assert (ack.result_code, ack.message) == (409, "already bound")


def test_a_bare_result_code_is_carried_into_the_ack():
    client = RecordingClient()

    client.make_command_handler(lambda message: 422)(_message())

    assert client.published[0][1].result_code == 422


def test_an_expired_command_is_acked_498_without_running_the_handler():
    client = RecordingClient()
    ran = []

    client.make_command_handler(lambda message: ran.append(1))(_message(expires_in=-1))

    _, ack = client.published[0]
    assert ack.result_code == 498
    assert ran == [], "an expired command must not execute"


def test_a_raising_handler_is_acked_500():
    client = RecordingClient()

    def boom(message):
        raise RuntimeError("no route to device")

    client.make_command_handler(boom)(_message())

    _, ack = client.published[0]
    assert ack.result_code == 500
    assert "no route to device" in ack.message


def test_a_non_command_payload_is_ignored_without_an_ack():
    client = RecordingClient()
    message = Message(topic=_cmd_topic(), payload=Ack(correlation_id="c-1"))

    client.make_command_handler(lambda m: None)(message)

    assert client.published == []


def test_subscribe_to_command_refuses_a_non_command_topic():
    client = RecordingClient()
    with pytest.raises(ValueError, match="Cmd"):
        client.subscribe_to_command(
            Topic(payload_type=Ack, node_id="m1", prefix="alp", context=("m1", "go")),
            lambda m: None,
        )
