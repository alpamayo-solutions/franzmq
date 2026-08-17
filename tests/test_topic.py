"""Unit tests for the v1 topic grammar beyond the shared golden vectors."""
from dataclasses import dataclass

import pytest

from franzmq.data_contracts import PAYLOAD_CLASSES
from franzmq.data_contracts.base import Ack, Cmd, Metric, Payload
from franzmq.topic import Isa95Fields, Isa95Topic, Topic


@dataclass
class Reading(Payload):
    value: float = 0.0


@dataclass
class CmdSpeed(Cmd):
    pass


@pytest.fixture(autouse=True)
def _register():
    for cls in (Reading, CmdSpeed):
        PAYLOAD_CLASSES[cls.get_identifier()] = cls
    yield
    for cls in (Reading, CmdSpeed):
        PAYLOAD_CLASSES.pop(cls.get_identifier(), None)


def test_node_id_is_emitted_at_level_four():
    topic = Topic(payload_type=Metric, node_id="m1", prefix="alp", context=("line1", "temp"))
    assert str(topic) == "alp/v1/_Metric/m1/line1/temp"


def test_missing_node_id_is_rejected():
    with pytest.raises(ValueError, match="node_id"):
        Topic(payload_type=Metric, prefix="alp", context=("temp",))


def test_node_id_must_be_one_segment():
    with pytest.raises(ValueError, match="single topic segment"):
        Topic(payload_type=Metric, node_id="site1/m1", prefix="alp", context=("temp",))


def test_contract_without_path_is_rejected():
    with pytest.raises(ValueError, match="no path"):
        Topic(payload_type=Metric, node_id="m1", prefix="alp")


def test_single_level_wildcard_is_a_valid_node_id():
    topic = Topic(payload_type=Metric, node_id="+", prefix="alp", context=("#",))
    assert str(topic) == "alp/v1/_Metric/+/#"
    assert Topic.from_str(str(topic)).node_id == "+"


def test_multi_level_wildcard_covers_every_node():
    topic = Topic(payload_type=Metric, node_id="#", prefix="alp")
    assert str(topic) == "alp/v1/_Metric/#"


def test_ack_topic_preserves_node_id():
    cmd = Topic(payload_type=CmdSpeed, node_id="m1", prefix="alp", context=("m1", "set-speed"))
    ack = cmd.to_ack_topic()
    assert ack.payload_type is Ack
    assert ack.node_id == "m1"
    assert str(ack) == "alp/v1/_Ack/m1/m1/set-speed"


def test_ack_topic_requires_a_command():
    topic = Topic(payload_type=Metric, node_id="m1", prefix="alp", context=("temp",))
    with pytest.raises(ValueError, match="ACK topics"):
        topic.to_ack_topic()


def test_unknown_contract_is_rejected():
    with pytest.raises(ValueError, match="No valid message type"):
        Topic.from_str("alp/v1/_NotRegistered/m1/temp")


def test_equality_and_hash_follow_the_string():
    a = Topic(payload_type=Reading, node_id="m1", prefix="alp", context=("temp",))
    b = Topic.from_str("alp/v1/_Reading/m1/temp")
    assert a == b
    assert a == "alp/v1/_Reading/m1/temp"
    assert len({a, b}) == 1


def test_isa95_topics_keep_their_own_shape():
    isa95 = Isa95Topic(
        payload_type=Reading,
        prefix="alp",
        context=("temp",),
        **Isa95Fields(
            enterprise="acme",
            site="site1",
            area=None,
            production_line=None,
            work_cell=None,
            origin_id=None,
        ),
    )
    assert str(isa95) == "alp/v1-isa95/acme/site1/_Reading/temp"


def test_isa95_to_topic_requires_a_node_id():
    isa95 = Isa95Topic.from_str("alp/v1-isa95/acme/site1/_Reading/temp")
    assert str(isa95.to_topic(node_id="m1")) == "alp/v1/_Reading/m1/temp"
    with pytest.raises(TypeError):
        isa95.to_topic()


def test_v1_from_str_refuses_isa95_strings():
    with pytest.raises(ValueError, match="Isa95Topic.from_str"):
        Topic.from_str("alp/v1-isa95/acme/site1/_Reading/temp")
