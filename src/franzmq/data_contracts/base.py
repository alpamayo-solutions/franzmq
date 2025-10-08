from abc import ABC
import json
from typing import Any, Optional, Dict, List
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


@dataclass
class Payload(ABC):

    def encode(self):
        return json.dumps(self.__dict__)

    @classmethod
    def decode(cls, json_str, timestamp: int):
        data = json.loads(json_str)
        data.pop("version", None)
        return cls(**data)

    @classmethod
    def get_identifier(cls) -> str:
        return "_" + cls.__name__

@dataclass
class ServiceDetails(Payload):
    name: str
    service_type: ServiceType
    metadata: Dict[str, Any] = field(default_factory=dict)
    hierarchy: List[str] = field(default_factory=list)

    # @classmethod
    # def decode(cls, json_str, timestamp: int):
    #     data = json.loads(json_str)
    #     data.pop("version", None)
    #     if "name" not in data and "id" in data:
    #         data["name"] = data.pop("id")
    #     return cls(**data)

    # @property
    # def __dict__(self):
    #     d = super().__dict__.copy()
    #     d["id"] = self.name
    #     return d


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
        
        return json.dumps(data)

    @classmethod
    def decode(cls, json_str, timestamp: int):
        data = json.loads(json_str)
        if isinstance(data["timestamp"], str):
            data["timestamp"] = datetime.datetime.fromisoformat(data["timestamp"]).timestamp()
        return cls(**data)

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


PAYLOAD_CLASSES = {
    cls.get_identifier(): cls
    for _, cls in inspect.getmembers(sys.modules[__name__], inspect.isclass)
    if issubclass(cls, Payload) and cls != Payload
}