# FranzMQ

FranzMQ is a structured MQTT communication library for edge and cloud applications. It builds on `paho-mqtt` and introduces typed payloads, hierarchical topics, priority-based callbacks, and a command/acknowledge pattern -- all with optional ISA-95 topic modeling and TLS auto-configuration.

## Features

- **Typed payloads** using Python dataclasses with automatic JSON encoding/decoding
- **Priority-based concurrent callbacks** for message handling
- **Command/acknowledge pattern** with two-phase handshake for request-response over MQTT
- **Class-based topic definitions** for type-safe, hierarchical topic construction
- **ISA-95 topic modeling** for enterprise-ready messaging structures
- **TLS support** with environment-based auto-configuration
- **MQTT-based logging** with seamless integration

## Installation

```bash
pip install franzmq
```

## Quick Start

```python
from franzmq import Client, Topic, Metric

client = Client.autocreate_and_connect(client_id="my-client")

topic = Topic(payload_type=Metric, context=("sensor", "temperature"))
metric = Metric(value=22.5)

client.publish(topic, metric)
```

## Topics

FranzMQ topics follow the structure `{prefix}/{version}/{_PayloadType}/{context...}`.

### Basic Topic

```python
from franzmq import Topic, Metric

topic = Topic(payload_type=Metric, context=("sensor", "temperature"))
# example/v1/_Metric/sensor/temperature
```

### ISA-95 Topic

For enterprise-level communication with ISA-95 hierarchy levels:

```python
from franzmq import Topic, Metric, Isa95Topic, Isa95Fields

basic_topic = Topic(payload_type=Metric, context=("sensor", "temperature"))

isa95_fields = Isa95Fields(
    enterprise="ent1",
    site="s1",
    area="a1",
    production_line="pl1",
    work_cell="wc1",
    origin_id="origin1"
)
isa95_topic = Isa95Topic.from_topic(basic_topic, isa95_fields)
# example/v1-isa95/ent1/s1/a1/pl1/wc1/origin1/_Metric/sensor/temperature
```

## Typed Payloads

All messages use structured dataclasses that encode/decode automatically to/from JSON. The following payload types are included:

| Payload | Purpose |
|---------|---------|
| `Metric` | Timestamped measurement values |
| `Log` | Structured log entries (level, message, module, etc.) |
| `ServiceDetails` | Service registration with type and metadata |
| `Cmd` | Command with correlation ID and expiration |
| `Ack` | Acknowledgement with result code and message |

Custom payloads extend the `Payload` base class:

```python
from dataclasses import dataclass
from franzmq import Payload

@dataclass
class SensorReading(Payload):
    sensor_id: str
    value: float
    unit: str
```

## Callback System

Subscribe to topics and register callbacks with optional priority. Callbacks receive a single `message: Message` argument containing the decoded topic and payload.

```python
from franzmq import Message

def on_metric(message: Message):
    print(f"Received: {message.payload.value} on {message.topic}")

client.subscribe(topic, qos=1, callback=on_metric, priority=10)
```

Callbacks are ordered by descending priority (higher numbers run first). Callbacks with the same priority are executed concurrently in separate threads.

## Command/Acknowledge Pattern

FranzMQ supports request-response semantics over MQTT using a two-phase acknowledgement flow. This avoids the need for a separate API when you need confirmed command execution.

### Flow

```
Sender                          Receiver
  |                               |
  |-- Cmd (correlation_id) ------>|
  |                               | (check expiration)
  |<-- Ack (result_code=-1) ------| (handshake)
  |                               | (execute callback)
  |<-- Ack (result_code=200) -----| (final result)
  |                               |
```

The handshake ack (`result_code=-1`) confirms the receiver is alive and processing. If the handshake arrives before the command expires, the sender extends its wait up to `max_command_duration`.

### Result codes

| Code | Meaning |
|------|---------|
| -1 | Handshake (receiver acknowledged receipt) |
| 200 | Success |
| 400 | Bad request |
| 500 | Internal error or timeout |
| 598 | Exception in command callback |

### Sending commands

`publish_command` subscribes to the ack topic, publishes the command, waits for the two-phase response, and returns the final `Ack`.

```python
from franzmq import Client, Topic, Cmd, Ack

client = Client.autocreate_and_connect(client_id="sender")

cmd_topic = Topic(
    prefix="myproject",
    payload_type=Cmd,
    context=("device1", "settings")
)

ack = client.publish_command(
    topic=cmd_topic,
    command={"enabled": True, "interval_ms": 500},
    validity_duration=30.0,
    max_command_duration=60.0,
)

if ack.result_code >= 500:
    raise Exception(f"Command failed: {ack.message}")
```

### Receiving commands

`subscribe_to_command` handles expiration checks, handshake acks, and final acks automatically. The callback receives a `Message` and returns a result code.

```python
from franzmq import Client, Topic, Cmd, Message

client = Client.autocreate_and_connect(client_id="receiver")

cmd_topic = Topic(
    prefix="myproject",
    payload_type=Cmd,
    context=("device1", "settings")
)

def handle_settings(message: Message) -> int:
    settings = message.payload.command
    apply_settings(settings)
    return 200  # success

client.subscribe_to_command(
    topic=cmd_topic,
    callback=handle_settings,
    qos=1,
)
```

The callback can return:
- `None` -- treated as 200 (success)
- An `int` result code
- A `(int, str)` tuple of (result_code, message)

Commands for the same topic are executed sequentially via an internal queue.

### Custom command payloads

Extend `Cmd` for typed command payloads:

```python
from dataclasses import dataclass, field
from franzmq import Cmd

@dataclass
class DeviceSettingsCmd(Cmd):
    command: dict = field(default_factory=dict)
```

Then use `DeviceSettingsCmd` as the topic's `payload_type`.

## Class-Based Topic Definitions

For projects with many topics, use `TopicBase` and `classproperty` to define hierarchical topic trees:

```python
from franzmq import TopicBase, classproperty, Metric
from franzmq.data_contracts.base import ServiceDetails

class DeviceTopic(TopicBase):
    prefix = "myproject"
    version = "v1"
    context = ("device1",)

    @classproperty
    def State(cls):
        return cls._topic(["state"], payload_type=ServiceDetails)

    @classproperty
    def Temperature(cls):
        return cls._topic(["temperature"], payload_type=Metric)
```

Access topics as class attributes:

```python
DeviceTopic.State        # myproject/v1/_ServiceDetails/device1/state
DeviceTopic.Temperature  # myproject/v1/_Metric/device1/temperature
```

Nested hierarchies use `_parent_class_name` and `_prefix` to compose topic paths from parent classes.

## Logging over MQTT

Enable MQTT-based logging by calling:

```python
import logging

client.configure_mqtt_logger(level=logging.INFO)
```

## Auto Configuration via Environment Variables

Uses [`python-decouple`](https://github.com/henriquebastos/python-decouple) for environment configuration.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MQTT_IP` | No | `broker` | Broker hostname |
| `MQTT_PORT` | No | `8883` (TLS) / `1883` (plain) | Broker port |
| `MQTT_USERNAME` | No | `franz` | Auth username |
| `MQTT_PASSWORD` | No | `franz` | Auth password |
| `USE_MQTTS` | No | `True` | Enable TLS |
| `CA_CERT_FILE` | If TLS | -- | CA certificate path |
| `TLS_CERT_FILE` | If TLS | -- | Client certificate path |
| `TLS_KEY_FILE` | If TLS | -- | Client private key path |

## License

MIT License
