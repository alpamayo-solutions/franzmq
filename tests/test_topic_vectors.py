"""Golden topic-transformation vectors — the release gate for the v1 node-id scheme.

The dataset in ``tests/vectors/topic_transformations.json`` is a copy of the
canonical file shipped by ``prekit-data-contracts``
(``src/prekit_data_contracts/vectors/topic_transformations.json``). The same
cases are judged by the Go door (colca ``plugins/alp``), so the two
implementations of the grammar cannot drift apart silently: changing a case is
a protocol change and needs both suites green.

What this suite asserts on franzmq's side of the boundary:

* every ``parse`` case — accepted topics decompose into exactly the documented
  fields and re-render to the identical string; rejected topics raise;
* the outputs of the door's ``mount_insert`` / ``mount_strip`` rewrites are
  topics franzmq can still read (the rewrite never leaves the grammar);
* ``identity_rule`` cases expose the level-4 value the door matches against the
  authenticated client — franzmq does not enforce the rule, it supplies the
  field the rule is about.
"""
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from franzmq.data_contracts import PAYLOAD_CLASSES
from franzmq.data_contracts.base import Cmd, Payload
from franzmq.topic import Topic

VECTORS = json.loads(
    (Path(__file__).parent / "vectors" / "topic_transformations.json").read_text()
)


# Contracts the vectors use that are not defined by franzmq itself: they belong
# to prekit-data-contracts (the CMD classes, entities) or to the broker binary
# (_TimeSync). franzmq resolves contracts through PAYLOAD_CLASSES, so the test
# registers stand-ins with the right identifiers — the grammar under test is
# indifferent to a contract's fields.
@dataclass
class CmdParam(Cmd):
    pass


@dataclass
class CmdAdmin(Cmd):
    pass


@dataclass
class EdgeNode(Payload):
    id: str = ""


@dataclass
class TimeSync(Payload):
    now_ms: int = 0


@pytest.fixture(autouse=True)
def _register_vector_contracts():
    added = [CmdParam, CmdAdmin, EdgeNode, TimeSync]
    for cls in added:
        PAYLOAD_CLASSES[cls.get_identifier()] = cls
    yield
    for cls in added:
        PAYLOAD_CLASSES.pop(cls.get_identifier(), None)


def _parse_cases(ok: bool):
    return [c for c in VECTORS["parse"] if c["ok"] is ok]


@pytest.mark.parametrize("case", _parse_cases(ok=True), ids=lambda c: c["topic"])
def test_parse_accepts_and_decomposes(case):
    topic = Topic.from_str(case["topic"])

    assert topic.prefix == case["prefix"]
    assert topic.version == case["version"]
    assert topic.payload_type.get_identifier() == case["contract"]
    assert topic.node_id == case["node_id"]
    assert "/".join(topic.context) == case["path"]


@pytest.mark.parametrize("case", _parse_cases(ok=True), ids=lambda c: c["topic"])
def test_parse_round_trips(case):
    assert str(Topic.from_str(case["topic"])) == case["topic"]


@pytest.mark.parametrize("case", _parse_cases(ok=False), ids=lambda c: c["topic"])
def test_parse_rejects(case):
    with pytest.raises(ValueError):
        Topic.from_str(case["topic"])


@pytest.mark.parametrize(
    "case", VECTORS["mount_insert"], ids=lambda c: f"{c['topic']}+{c['mount']}"
)
def test_mount_insert_output_stays_in_the_grammar(case):
    # A rewrite that produced something franzmq cannot read would be a record no
    # publisher could ever address again. Cases whose input is below the grammar
    # (the door leaves those untouched) are excluded — they are unparseable by
    # construction, which the parse cases already pin.
    if case["out"] == case["topic"] and "why" in case:
        with pytest.raises(ValueError):
            Topic.from_str(case["topic"])
        return
    topic = Topic.from_str(case["out"])
    assert topic.node_id == Topic.from_str(case["topic"]).node_id
    assert str(topic) == case["out"]


@pytest.mark.parametrize(
    "case",
    [c for c in VECTORS["mount_strip"] if c["ok"]],
    ids=lambda c: f"{c['topic']}-{c['mount']}",
)
def test_mount_strip_output_stays_in_the_grammar(case):
    topic = Topic.from_str(case["out"])
    assert topic.node_id == Topic.from_str(case["topic"]).node_id
    assert str(topic) == case["out"]


@pytest.mark.parametrize(
    "case", VECTORS["identity_rule"], ids=lambda c: f"{c['topic']}@{c['authenticated_as']}"
)
def test_identity_rule_reads_level_four(case):
    topic = Topic.from_str(case["topic"])
    is_command = issubclass(topic.payload_type, Cmd)
    # The door accepts when the identity matches, or when the contract is a
    # command (commands are exempt — cmd grants gate them instead).
    assert (topic.node_id == case["authenticated_as"] or is_command) is case["ok"]
