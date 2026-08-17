# FranzMQ

FranzMQ is a structured MQTT communication library for edge and cloud applications. It builds on `paho-mqtt` and introduces typed payloads, hierarchical topics, priority-based callbacks, and a command/acknowledge pattern -- all with optional ISA-95 topic modeling and TLS auto-configuration.

## Features

- **Typed payloads** using Python dataclasses with automatic JSON encoding/decoding
- **Priority-based concurrent callbacks** for message handling
- **Command/acknowledge pattern** for confirmed request-response over MQTT
- **Class-based topic definitions** for type-safe, hierarchical topic construction
- **ISA-95 topic modeling** for enterprise-ready messaging structures
- **Pinned-key authentication** — the identity key is the credential, no CA
- **MQTT 5** with broker reason codes surfaced as exceptions
- **MQTT-based logging** with seamless integration

## Installation

```bash
pip install franzmq
```

## Quick Start

```python
from franzmq import Client, Topic, Metric

client = Client.autocreate_and_connect(client_id="my-client")

topic = Topic(payload_type=Metric, node_id="my-client", context=("sensor", "temperature"))
metric = Metric(value=22.5)

client.publish(topic, metric)
```

## Topics

FranzMQ topics follow the structure `{prefix}/{version}/{_PayloadType}/{node_id}/{context...}`.

`node_id` is the identity the message is published under — the node or machine
the record belongs to. It is required and always sits at level 4, directly after
the payload type: brokers that enforce an identity rule match that level against
the authenticated client, and every hop that re-mounts a record rewrites only the
path *after* it. Subscription filters may use `+` there to span nodes.

### Basic Topic

```python
from franzmq import Topic, Metric

topic = Topic(payload_type=Metric, node_id="machine-1", context=("sensor", "temperature"))
# example/v1/_Metric/machine-1/sensor/temperature
```

### ISA-95 Topic

For enterprise-level communication with ISA-95 hierarchy levels:

```python
from franzmq import Topic, Metric, Isa95Topic, Isa95Fields

basic_topic = Topic(payload_type=Metric, node_id="machine-1", context=("sensor", "temperature"))

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

FranzMQ supports request-response semantics over MQTT. One command gets one acknowledgement, carrying the broker's own result codes.

### Flow

```
Sender                          Receiver
  |                               |
  |-- Cmd (correlation_id) ------>|
  |                               | (check expiration)
  |                               | (execute callback)
  |<-- Ack (result_code) ---------|
  |                               |
```

The sender waits until the command's own expiry. A command that expires before it is executed is acked `498` without running the callback.

### Result codes

| Code | Meaning |
|------|---------|
| 200 | Done |
| 409 | Conflict — the request contradicts current state |
| 422 | Invalid — the request could not be understood |
| 498 | Expired before execution |
| 500 | Internal error, including an exception in the callback |

### Sending commands

`publish_command` subscribes to the ack topic, publishes the command, waits for the ack, and returns it.

```python
from franzmq import Client, Topic, Cmd, Ack

client = Client.autocreate_and_connect(client_id="sender")

cmd_topic = Topic(
    prefix="myproject",
    payload_type=Cmd,
    node_id="device1",
    context=("device1", "settings")
)

ack = client.publish_command(
    topic=cmd_topic,
    command={"enabled": True, "interval_ms": 500},
    validity_duration=30.0,
)

if ack.result_code >= 500:
    raise Exception(f"Command failed: {ack.message}")
```

### Receiving commands

`subscribe_to_command` handles expiry and acknowledgement automatically. The callback receives a `Message` and returns a result code.

```python
from franzmq import Client, Topic, Cmd, Message

client = Client.autocreate_and_connect(client_id="receiver")

cmd_topic = Topic(
    prefix="myproject",
    payload_type=Cmd,
    node_id="device1",
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
    node_id = "device1"
    context = ()

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

`node_id` may be set on the class (as above) or passed per topic:
`cls._topic(["temperature"], payload_type=Metric, node_id="device2")`.

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
| `MACHINE_KEY` | **Yes** | -- | Path to the ed25519 identity key — the credential |
| `NODE_ID` | No | client id | Identity: MQTT username, client id, and level 4 of every topic |
| `MQTT_IP` | No | `broker` | Broker hostname |
| `MQTT_PORT` | No | `1883` | Broker port |
| `MQTT_SESSION_EXPIRY` | No | never expires | Seconds the broker keeps the session after a disconnect |

## Authentication

The broker authenticates by **pinned key**, not by a certificate authority. Each
client has an ed25519 private key; on connect it presents a certificate minted
from that key in-process, and the broker looks up the public key inside among the
identities enrolled there. An unknown key is refused. Nothing validates the
broker in return, because no authority exists to validate it against — trust runs
the other way.

Generate a key with `colca-keygen` and enroll its public key at the node before
the client's first connect.

## Rejected publishes

At QoS ≥ 1 `publish()` waits for the PUBACK and raises `PublishRejected` when the
broker answers with a failure reason code:

| Code | Meaning |
|------|---------|
| `0x90` | topic name invalid — the broker does not know this contract |
| `0x99` | payload format invalid — the payload failed the contract schema |
| `0x87` | not authorized — no grant covers this topic |
| `0x89` | quota exceeded — the destination is draining |

```python
from franzmq.errors import PublishRejected

try:
    client.publish(topic, metric, qos=1)
except PublishRejected as err:
    logger.error("%s rejected: %s", err.topic, err.reason)
```

Publishing from inside a callback (`on_connect`, a message handler) cannot wait:
the PUBACK is delivered by the very thread that would be waiting. Those publishes
are sent without confirmation. Publish off the network thread when you need the
verdict.

Only one QoS ≥ 1 publish is in flight at a time. That is deliberate: unbounded
in-flight QoS-1 lets a reconnect replay an unacked message after a newer one is
already on the wire, and it is what makes a reason code attributable to the
message that earned it. Pass `wait=False` for fire-and-forget.

## License

MIT License
