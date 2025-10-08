from dataclasses import dataclass
from typing import Optional, Tuple, Union, TypedDict, Literal
from franzmq.data_contracts.base import Payload, PAYLOAD_CLASSES

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

@dataclass(frozen=True)
class Topic:
    """Basic topic class. Used for internal communication within an edge node."""
    payload_type: Union[type[Payload], Wildcard] = None
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
        for context in self.context:
            topic += f"/{context}"
        return topic

    @classmethod
    def from_str(cls, topic: str):
        parts = topic.split("/")
        kwargs = {
            "prefix": parts[0],
            "version": parts[1],
            "payload_type": None,
            "context": []
        }

        if kwargs["version"] == "v1-isa95":
            return Isa95Topic.from_str(topic).to_topic()
        elif kwargs["version"] != "v1":
            raise ValueError(f"Invalid version: {kwargs['version']}")

        attributes = ["payload_type"]
        
        index = 2

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
    
    def split(self, *args, **kwargs):
        return str(self).split(*args, **kwargs)

    def startswith(self, *args, **kwargs):
        return str(self).startswith(*args, **kwargs)

    def endswith(self, *args, **kwargs):
        return str(self).endswith(*args, **kwargs)

    def validate_topic(self, topic: str) -> None:
        """Validate the MQTT topic string for correct wildcard usage."""
        if topic.count(MULTI_LEVEL_WILDCARD) > 1:
            raise ValueError("A topic can only contain one multi-level wildcard ('#').")

        if MULTI_LEVEL_WILDCARD in topic and not topic.endswith(MULTI_LEVEL_WILDCARD):
            raise ValueError("The multi-level wildcard ('#') must be at the end of the topic.")

        if any(part == MULTI_LEVEL_WILDCARD for part in topic.split("/")[:-1]):
            raise ValueError("The multi-level wildcard ('#') cannot be in the middle of the topic.")

    @classmethod
    def from_isa95_topic(cls, topic: "Isa95Topic") -> "Topic":
        """Convert an ISA95 topic string into an Isa95Topic object."""
        assert topic.version == "v1-isa95", "ISA95 topics must have version 'v1-isa95'."
        return cls.from_str(topic)

@dataclass(frozen=True)
class Isa95Topic(Topic):
    """ISA95 topic class. Used for external ISA95-compliant communication."""
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

    def to_topic(self) -> Topic:
        """Convert Isa95Topic into a basic Topic, stripping the ISA95 hierarchy."""
        return Topic(
            prefix=self.prefix,
            payload_type=self.payload_type,
            context=self.context
        )

if __name__ == "__main__":
    from franzmq.data_contracts.base import Metric
    # Create a basic Topic
    basic_topic = Topic(prefix="example", version="v1", payload_type=Metric, context=("sensor", "temperature"))
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

    # Pass the ISA95 topic to the from_str method
    print("Topic from ISA95 Topic string:", Topic.from_str(str(isa95_topic)))

    # Convert back to a basic Topic
    converted_basic_topic = isa95_topic.to_topic()
    print("Converted Basic Topic:", converted_basic_topic)