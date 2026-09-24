"""Commands and acks exactly as a colca node stores them decode through the
client, and whatever a contract does not declare survives the round trip."""
import json
import threading
from dataclasses import dataclass

import pytest
from paho.mqtt.client import MQTTMessage

from franzmq.client import Client
from franzmq.data_contracts import PAYLOAD_CLASSES
from franzmq.data_contracts.base import Ack, Cmd, Payload

# Recorded from a colca 0.13 node's commands stream.
SET_PRODUCT = {
    "command": {"productSku": "10042126", "recipeId": "recipe-standard"},
    "correlation_id": "07c2201e-b2b3-467e-a423-0f43f89aef2f",
    "expires_at": 1790289919890,
}
CONSTANT_UPSERT_ACK = {
    "correlation_id": "01M3ASFJN6GBM3QT7R58DA3FBP",
    "result_code": 200,
    "message": "upserted 2",
    "state_writes": [
        {"stream": "entities", "offset": 972,
         "topic": "steine/v1/_Constant/305_836/wisewoods/line1/operator/activeProductSku"},
        {"stream": "entities", "offset": 973,
         "topic": "steine/v1/_Constant/305_836/wisewoods/line1/operator/activeRecipeId"},
    ],
}
SET_PRODUCT_ACK = {
    "correlation_id": "07c2201e-b2b3-467e-a423-0f43f89aef2f",
    "result_code": 200,
    "message": "set operator/activeProductSku, set operator/activeRecipeId",
    "performed_at": 1790289890.0428104,
}


@dataclass
class CmdParam(Cmd):
    pass


@pytest.fixture(autouse=True)
def _register():
    PAYLOAD_CLASSES[CmdParam.get_identifier()] = CmdParam
    yield
    PAYLOAD_CLASSES.pop(CmdParam.get_identifier(), None)


def _raw(topic: str, payload: dict) -> MQTTMessage:
    raw = MQTTMessage(1)
    raw.topic = topic.encode()
    raw.payload = json.dumps(payload).encode()
    return raw


def test_a_command_without_created_at_decodes():
    message = Client()._decode_message(
        _raw("steine/v1/_CmdParam/305_836/wisewoods/line1/operator/setProduct", SET_PRODUCT)
    )

    cmd = message.payload
    assert isinstance(cmd, CmdParam)
    assert cmd.created_at is None
    assert cmd.correlation_id == SET_PRODUCT["correlation_id"]
    assert cmd.expires_at == SET_PRODUCT["expires_at"]
    assert cmd.command == SET_PRODUCT["command"]


def test_an_ack_with_state_writes_decodes():
    message = Client()._decode_message(
        _raw("steine/v1/_Ack/305_836/constant/upsert", CONSTANT_UPSERT_ACK)
    )

    ack = message.payload
    assert isinstance(ack, Ack)
    assert (ack.result_code, ack.message) == (200, "upserted 2")
    assert [w["offset"] for w in ack.state_writes] == [972, 973]
    assert ack.performed_at is None


def test_an_ack_without_state_writes_decodes():
    ack = Client()._decode_message(
        _raw("steine/v1/_Ack/305_836/wisewoods/line1/operator/setProduct", SET_PRODUCT_ACK)
    ).payload

    assert ack.state_writes == []
    assert ack.performed_at == SET_PRODUCT_ACK["performed_at"]


def test_node_payloads_reach_a_subscription_callback_decoded():
    """The path paho delivers through, where a decode error used to be raised
    on the network thread."""
    client = Client()
    received = []
    done = threading.Event()

    def on_message(message):
        received.append(message.payload)
        if len(received) == 2:
            done.set()

    client.subscribe("steine/v1/+/305_836/#", callback=on_message)
    client._handle_on_message(
        _raw("steine/v1/_CmdParam/305_836/wisewoods/line1/operator/setProduct", SET_PRODUCT)
    )
    client._handle_on_message(_raw("steine/v1/_Ack/305_836/constant/upsert", CONSTANT_UPSERT_ACK))

    assert done.wait(2), "a node payload never reached the callback"
    assert [type(p) for p in received] == [CmdParam, Ack]


def test_undeclared_fields_are_kept_and_re_encoded():
    record = {**SET_PRODUCT, "productSku": "10042126", "actor": {"kind": "person"}}

    cmd = CmdParam.decode(json.dumps(record), timestamp=0)

    assert cmd.productSku == "10042126"
    assert json.loads(cmd.encode()) == {**record, "created_at": None}


def test_an_undeclared_field_never_shadows_the_contract_api():
    @dataclass
    class Thing(Payload):
        value: int = 0

    thing = Thing.decode(json.dumps({"value": 1, "encode": "x"}), timestamp=0)

    assert json.loads(thing.encode()) == {"value": 1}
