from dataclasses import dataclass
from typing import Optional, Tuple, Union, TypedDict, Literal
from franzmq.data_contracts.base import Payload, Cmd, Ack
from franzmq.data_contracts import PAYLOAD_CLASSES

SINGLE_LEVEL_WILDCARD = "+"
MULTI_LEVEL_WILDCARD = "#"

class Isa95Fields(TypedDict):
    enterprise: str
    site: Optional[str]
    area: Optional[str]
    production_line: Optional[str]
    work_cell: Optional[str]
    origin_id: Optional[str]

WILDCARDS = {SINGLE_LEVEL_WILDCARD, MULTI_LEVEL_WILDCARD}

Wildcard = Literal['+', '#']

# Contracts whose wire topic carries no hierarchy path at all, i.e. the only
# ones whose valid form is exactly 4 segments (prefix/version/_Contract/node-id).
# Every other contract needs at least one path segment.
#
# This mirrors the Go door's grammar (colca `plugins/alp.Parse`). The two
# implementations are bound by the shared golden vectors
# (`vectors/topic_transformations.json`, shipped by prekit-data-contracts and
# copied into this repo's tests) — changing this set is a protocol change and
# needs both suites green.
PATHLESS_CONTRACTS = frozenset({"_TimeSync"})


@dataclass(frozen=True)
class Topic:
    """Basic topic class. Used for internal communication within an edge node.

    Wire shape (v1): ``{prefix}/{version}/_{Contract}/{node_id}/{path…}``.

    ``node_id`` is the identity of the publisher — the node or machine the
    record belongs to. It is required: a broker that enforces the identity rule
    matches this level against the authenticated client, and every hop that
    re-mounts a record rewrites only the path *after* it.
    """
    payload_type: Union[type[Payload], Wildcard] = None
    node_id: Union[str, Wildcard] = None
    prefix: str = "example"
    version: str = "v1"
    context: Tuple[Union[str, Wildcard], ...] = ()

    def __post_init__(self) -> None:
        topic_str = str(self)
        self.validate_topic(topic_str)

    def __str__(self):
        topic = f"{self.prefix}/{self.version}"
        if self.payload_type in WILDCARDS:
            topic += f"/{self.payload_type}"
        else:
            topic += f"/{self.payload_type.get_identifier()}"
        if self.node_id is not None:
            topic += f"/{self.node_id}"
        for context in self.context:
            topic += f"/{context}"
        return topic

    @classmethod
    def from_str(cls, topic: str):
        parts = topic.split("/")
        if len(parts) < 2:
            raise ValueError(f"Invalid topic {topic!r}: expected at least prefix/version.")

        version = parts[1]
        if version == "v1-isa95":
            raise ValueError(
                "ISA95 topics carry no node identity and cannot be converted implicitly. "
                "Use Isa95Topic.from_str(topic).to_topic(node_id=...) instead."
            )
        if version != "v1":
            raise ValueError(f"Invalid version: {version}")

        if len(parts) < 4:
            raise ValueError(
                f"Invalid topic {topic!r}: v1 topics need at least "
                f"prefix/version/_Contract/node-id, got {len(parts)} segments."
            )
        if not parts[2].startswith("_"):
            raise ValueError(f"No valid message type found in topic {topic}.")

        payload_type = PAYLOAD_CLASSES.get(parts[2])
        if payload_type is None:
            raise ValueError(f"No valid message type found in topic {topic}.")

        return cls(
            prefix=parts[0],
            version=version,
            payload_type=payload_type,
            node_id=parts[3],
            context=tuple(parts[4:]),
        )

    def split(self, *args, **kwargs):
        return str(self).split(*args, **kwargs)

    def startswith(self, *args, **kwargs):
        return str(self).startswith(*args, **kwargs)

    def endswith(self, *args, **kwargs):
        return str(self).endswith(*args, **kwargs)

    def __eq__(self, other):
        if isinstance(other, (Topic, str)):
            return str(self) == str(other)
        return NotImplemented

    def __ne__(self, other):
        equal = self.__eq__(other)
        if equal is NotImplemented:
            return NotImplemented
        return not equal

    def __hash__(self):
        return hash(str(self))

    def to_ack_topic(self) -> "Topic":
        if not issubclass(self.payload_type, Cmd):
            raise ValueError("ACK topics are only available for command topics.")
        return Topic(
            prefix=self.prefix,
            version=self.version,
            payload_type=Ack,
            node_id=self.node_id,
            context=self.context
        )

    def validate_topic(self, topic: str) -> None:
        """Validate the MQTT topic string for correct wildcard usage and, for
        v1 topics, for the node-id grammar."""
        if topic.count(MULTI_LEVEL_WILDCARD) > 1:
            raise ValueError("A topic can only contain one multi-level wildcard ('#').")

        if MULTI_LEVEL_WILDCARD in topic and not topic.endswith(MULTI_LEVEL_WILDCARD):
            raise ValueError("The multi-level wildcard ('#') must be at the end of the topic.")

        if any(part == MULTI_LEVEL_WILDCARD for part in topic.split("/")[:-1]):
            raise ValueError("The multi-level wildcard ('#') cannot be in the middle of the topic.")

        if self.version != "v1":
            return

        if self.node_id is None or self.node_id == "":
            raise ValueError(
                f"Topic {topic!r} has no node_id. v1 topics carry the publisher's "
                "identity at level 4: prefix/version/_Contract/node-id/path…"
            )
        if "/" in str(self.node_id):
            raise ValueError(f"node_id must be a single topic segment, got {self.node_id!r}.")

        if self.context:
            return
        # No path: only the pathless contracts and a '#' filter may end here.
        if self.node_id == MULTI_LEVEL_WILDCARD:
            return
        contract = (
            self.payload_type
            if self.payload_type in WILDCARDS
            else self.payload_type.get_identifier()
        )
        if contract not in PATHLESS_CONTRACTS:
            raise ValueError(
                f"Topic {topic!r} has no path. Only {sorted(PATHLESS_CONTRACTS)} may be "
                "published without one; every other contract needs at least one path segment."
            )

    @classmethod
    def from_isa95_topic(cls, topic: "Isa95Topic", node_id: str) -> "Topic":
        """Convert an ISA95 topic into a v1 topic under ``node_id``."""
        assert topic.version == "v1-isa95", "ISA95 topics must have version 'v1-isa95'."
        return topic.to_topic(node_id)

@dataclass(frozen=True)
class Isa95Topic(Topic):
    """ISA95 topic class. Used for external ISA95-compliant communication.

    ISA95 topics address the hierarchy through their own levels and carry no
    node identity, so ``node_id`` stays unset here — crossing into a v1 topic
    requires naming the publisher explicitly (:meth:`to_topic`).
    """
    enterprise: Union[str, Wildcard] = None
    version: str = "v1-isa95"
    site: Optional[Union[str, Wildcard]] = None
    area: Optional[Union[str, Wildcard]] = None
    production_line: Optional[Union[str, Wildcard]] = None
    work_cell: Optional[Union[str, Wildcard]] = None
    origin_id: Optional[Union[str, Wildcard]] = None

    def __str__(self):
        topic = f"{self.prefix}/{self.version}"

        isa95_levels = [
            self.enterprise,
            self.site,
            self.area,
            self.production_line,
            self.work_cell,
            self.origin_id
        ]

        for level in isa95_levels:
            if level:
                topic += f"/{level}"
            else:
                break  # Stops appending once a level is None

        if self.payload_type in WILDCARDS:
            topic += f"/{self.payload_type}"
        else:
            topic += f"/{self.payload_type.get_identifier()}"

        for ctx in self.context:
            topic += f"/{ctx}"

        return topic

    @classmethod
    def from_topic(cls, topic: Topic, isa95_fields: Isa95Fields) -> "Isa95Topic":
        """Convert a general Topic into an Isa95Topic by specifying ISA95 hierarchy fields."""
        assert topic.version == "v1", "Only v1 topics can be converted to v1-isa95 topics."
        return cls(
            prefix=topic.prefix,
            payload_type=topic.payload_type,
            context=topic.context,
            **isa95_fields
        )

    @classmethod
    def from_str(cls, topic: str):
        parts = topic.split("/")
        kwargs = {
            "prefix": parts[0],
            "version": parts[1],
            "enterprise": parts[2],
            "site": None,
            "area": None,
            "production_line": None,
            "work_cell": None,
            "origin_id": None,
            "payload_type": None,
            "context": []
        }

        attributes = ["site", "area", "production_line", "work_cell", "origin_id", "payload_type"]

        index = 3

        for attr in attributes:
            if index >= len(parts):
                break
            if parts[index][0] == "_":
                kwargs["payload_type"] = PAYLOAD_CLASSES.get(parts[index])
                kwargs["context"] = parts[index + 1:]
                break
            else:
                kwargs[attr] = parts[index]
            index += 1

        if kwargs["payload_type"] is None:
            raise ValueError(f"No valid message type found in topic {topic}.")

        return cls(**kwargs)

    def to_topic(self, node_id: str) -> Topic:
        """Convert Isa95Topic into a basic v1 Topic under ``node_id``, stripping
        the ISA95 hierarchy."""
        return Topic(
            prefix=self.prefix,
            payload_type=self.payload_type,
            node_id=node_id,
            context=self.context
        )

if __name__ == "__main__":
    from franzmq.data_contracts.base import Metric
    # Create a basic Topic
    basic_topic = Topic(prefix="example", version="v1", payload_type=Metric,
                        node_id="machine-1", context=("sensor", "temperature"))
    print("Basic Topic:", basic_topic)

    # Convert the basic Topic to an Isa95Topic
    isa95_fields = Isa95Fields(
        enterprise="enterprise1",
        site="site1",
        area="area1",
        production_line="line1",
        work_cell="cell1",
        origin_id="origin1"
    )
    isa95_topic = Isa95Topic.from_topic(basic_topic, isa95_fields)
    print("ISA95 Topic:", isa95_topic)

    # Convert back to a basic Topic
    converted_basic_topic = isa95_topic.to_topic(node_id="machine-1")
    print("Converted Basic Topic:", converted_basic_topic)
