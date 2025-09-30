from abc import ABC
import json
from typing import Any, Dict, List, Optional
import hashlib
import sys
import inspect
from dataclasses import dataclass, field
from enum import Enum   
import datetime
import time
import logging

class StrEnum(str, Enum):
    def __str__(self):
        return self.value

class ServiceType(StrEnum):
    CONNECTOR = "connector"
    MQTT_TO_API = "mqtt-to-api"
    MQTT_TO_GRAFANA = "mqtt-to-grafana"
    MQTT_TO_KAFKA = "mqtt-to-kafka"
    KAFKA_TO_DB = "kafka-to-db"
    KAFKA_CONNECT = "kafka-connect"
    UI = "ui"
    DATABASE = "database"
    API = "api"
    KAFKA = "kafka"
    GRAFANA = "grafana"
    BROKER = "broker"
    AGENT = "agent"
    PY_DATA_OPS = "py-data-ops"
    REVERSE_PROXY = "reverse-proxy"
    AUTH_SERVICE = "auth-service"
    MCP_SERVER="mcp-server"

class IndexType(StrEnum):
    TIME = "time"
    NUMERICAL = "numerical"
    NONE = "none"

class DataType(StrEnum):
    INT = "int"
    FLOAT = "float"
    STRING = "string"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    INT_ARRAY = "int_array"
    FLOAT_ARRAY = "float_array"
    STRING_ARRAY = "string_array"
    BOOLEAN_ARRAY = "boolean_array"
    DATETIME_ARRAY = "datetime_array"


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ServiceType):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, IndexType):
            return str(obj)
        if isinstance(obj, DataType):
            return str(obj)
        
        return super().default(obj)

@dataclass
class Payload(ABC):

    def encode(self):
        return json.dumps(self.__dict__, cls=CustomEncoder)

    @classmethod
    def decode(cls, json_str, timestamp: int):
        data = json.loads(json_str)
        data.pop("version", None)
        return cls(**data)

    @classmethod
    def get_identifier(cls) -> str:
        return "_" + cls.__name__


@dataclass
class Metric(Payload):
    value: Any
    timestamp: float = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).timestamp())

    @property
    def timestamp_dt(self) -> datetime.datetime:
        # If timestamp is from time.monotonic(), convert it to a Unix timestamp
        if isinstance(self.timestamp, str):
            return datetime.datetime.fromisoformat(self.timestamp)
        if not isinstance(self.timestamp, datetime.datetime) and self.timestamp < 1000000000:
            return datetime.datetime.fromtimestamp(time.time() - (time.monotonic() - self.timestamp))
        return datetime.datetime.fromtimestamp(self.timestamp)

    def encode(self):
        data = self.__dict__.copy()
        if isinstance(data["timestamp"], str):
            data["timestamp"] = datetime.datetime.fromisoformat(data["timestamp"]).timestamp()
        logging.info(f"Encoding Metric with timestamp: {data['timestamp']} of type {type(data['timestamp'])}")
        
        return json.dumps(data, cls=CustomEncoder)

    @classmethod
    def decode(cls, json_str, timestamp: int):
        data = json.loads(json_str)
        if isinstance(data["timestamp"], str):
            data["timestamp"] = datetime.datetime.fromisoformat(data["timestamp"]).timestamp()
        return cls(**data)


@dataclass
class DataTag(Payload):
    id: str
    name: str
    is_writable: bool
    is_readable: bool
    data_type: Optional[str] = None
    hierarchy: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataTags(Payload):
    data_tags: List[DataTag]
    connector: str

    @property
    def version(self) -> str:
        sorted_tags = sorted(self.data_tags, key=lambda x: x.id)
        tags_dict = [tag.__dict__ for tag in sorted_tags]
        return hashlib.md5(json.dumps(tags_dict, cls=CustomEncoder, sort_keys=True).encode()).hexdigest()

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'DataTags':
        data = json.loads(json_str)
        data_tags = [DataTag(**tag) for tag in data["data_tags"]]
        data["data_tags"] = data_tags
        data.pop("version", None)
        return cls(**data)

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["data_tags"] = [tag.__dict__ for tag in self.data_tags]
        d["version"] = self.version
        return d


@dataclass
class SignalData:
    id: str
    name: str
    source: str
    data_type: DataType
    index_type: IndexType
    topic_name: str
    system_element: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)
    unit: Optional[str] = None
    precision: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    @property
    def hierarchy(self) -> List[str]:
        if self.system_element:
            return self.system_element.split(" - ")
        return []

    def encode(self):
        return json.dumps(self.__dict__)

@dataclass
class DataTagContext(Payload):
    id: str
    tag_id: str
    source: str
    topic_name: str
    is_logged: bool
    is_published: bool
    signal: SignalData

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'DataTagContext':
        data = json.loads(json_str)
        data["signal"] = SignalData(**data["signal"])
        data.pop("version", None)
        return cls(**data)

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["signal"] = self.signal.__dict__
        return d


@dataclass
class DataTagContexts(Payload):
    data_tag_contexts: List[DataTagContext]

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'DataTagContexts':
        data = json.loads(json_str)
        data_tag_contexts = [DataTagContext.decode(json.dumps(context), timestamp) for context in data["data_tag_contexts"]]
        return cls(data_tag_contexts=data_tag_contexts)

    @property
    def version(self) -> str:
        sorted_contexts = sorted(self.data_tag_contexts, key=lambda x: x.tag_id)
        contexts_dict = [context.__dict__ for context in sorted_contexts]
        return hashlib.md5(json.dumps(contexts_dict, cls=CustomEncoder, sort_keys=True).encode()).hexdigest()

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["data_tag_contexts"] = [context.__dict__ for context in self.data_tag_contexts]
        d["version"] = self.version
        return d

@dataclass
class Log(Payload):
    timestamp: str
    level: str
    message: str
    logger_name: str
    module: str
    function: str
    line_no: int
    exc_info: Optional[str] = None
    extra: Optional[dict[str, Any]] = None


@dataclass
class ServiceDetails(Payload):
    id: str
    service_type: ServiceType
    display_name: str | None = None
    version: str | None = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    hierarchy: List[str] = field(default_factory=list)



@dataclass
class Computation(Payload):
    id: str
    func_name: str
    func_hash: str
    description: str
    args: List[str]
    return_data_type: DataType
    return_index_type: IndexType
    source: str

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["return_data_type"] = str(self.return_data_type)
        d["return_index_type"] = str(self.return_index_type)
        return d


@dataclass
class Computations(Payload):
    computations: List[Computation]
    service: str

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'Computations':
        data = json.loads(json_str)
        computations = [Computation(**computation) for computation in data["computations"]]
        return cls(computations=computations, service=data["service"])

    @property
    def version(self) -> str:
        sorted_computations = sorted(self.computations, key=lambda x: x.func_name)
        computations_dict = [computation.__dict__ for computation in sorted_computations]
        return hashlib.md5(json.dumps(computations_dict, cls=CustomEncoder, sort_keys=True).encode()).hexdigest()

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["computations"] = [computation.__dict__ for computation in self.computations]
        d["version"] = self.version
        return d

@dataclass
class ArgMapping:
    arg: str
    signal: SignalData | None
    is_trigger: bool


@dataclass
class ComputationContext(Payload):
    computation_id: str
    func_hash: str
    source: str
    func_name: str
    topic_name: str
    is_logged: bool
    is_published: bool
    on_interval_ms: int
    signal: SignalData
    arg_mappings: list[ArgMapping] # maps arg name to signal

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["signal"] = self.signal.__dict__
        d["arg_mappings"] = []
        for a in self.arg_mappings:
            a_dict = a.__dict__.copy()
            if a.signal is None:
                a_dict["signal"] = None
            else:
                a_dict["signal"] = a.signal.__dict__
            d["arg_mappings"].append(a_dict)
        return d


    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'ComputationContext':
        data = json.loads(json_str)
        data["signal"] = SignalData(**data["signal"])
        data["arg_mappings"] = [
            ArgMapping(
                arg=arg["arg"],
                signal=SignalData(**arg["signal"]) if arg["signal"] else None,
                is_trigger=arg["is_trigger"]
            ) for arg in data["arg_mappings"]
        ]
        return cls(**data)

@dataclass
class ComputationContexts(Payload):
    computation_contexts: List[ComputationContext]

    @classmethod
    def decode(cls, json_str: str, timestamp: int) -> 'ComputationContexts':
        data = json.loads(json_str)
        # Use the updated decode method of ComputationContext
        computation_contexts = [ComputationContext.decode(json.dumps(context), timestamp) for context in data["computation_contexts"]]
        return cls(computation_contexts=computation_contexts)

    @property
    def version(self) -> str:
        sorted_contexts = sorted(self.computation_contexts, key=lambda x: x.computation_id)
        contexts_dict = [context.__dict__ for context in sorted_contexts]
        return hashlib.md5(json.dumps(contexts_dict, cls=CustomEncoder, sort_keys=True).encode()).hexdigest()

    @property
    def __dict__(self):
        d = super().__dict__.copy()
        d["computation_contexts"] = [context.__dict__ for context in self.computation_contexts]
        d["version"] = self.version
        return d
        

@dataclass
class ValidationResults(Payload):
    service_id: str
    file_results: Dict[str, Dict[str, Any]]
    timestamp: float = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).timestamp())
    general_error: Optional[str] = None


PAYLOAD_CLASSES = {
    cls.get_identifier(): cls
    for _, cls in inspect.getmembers(sys.modules[__name__], inspect.isclass)
    if issubclass(cls, Payload) and cls != Payload
}