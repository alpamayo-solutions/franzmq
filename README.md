# FranzMQ

**FranzMQ** is a flexible, structured MQTT communication library designed to streamline MQTT messaging for internal and external communication in edge and cloud applications. It builds on top of the `paho-mqtt` client and introduces a typed, hierarchical topic and payload system based on the ISA-95 model.

## Features

- 🧠 **Typed Payloads** using Python dataclasses
- 🧵 **Priority-based concurrent callbacks** for message handling
- 📦 **ISA-95 topic modeling** for enterprise-ready messaging structures
- 🔒 **TLS support** with environment-based auto-configuration
- 🧪 **Validation-ready** versioning and context-driven payload classes
- 📊 **MQTT-based logging** with seamless integration

## Installation

Install using pip:

```bash
pip install franzmq
```

## Quick Start

```python
from franzmq.client import Client
from franzmq.payload import Metric
from franzmq.topic import Topic

client = Client.autocreate_and_connect(client_id="my-client")

topic = Topic(payload_type=Metric, context=("sensor", "temperature"))
metric = Metric(value=22.5)

client.publish(topic, metric)
```

## Typed Payloads

All messages use structured dataclasses (e.g., `Metric`, `Log`, `ServiceDetails`, etc.) that encode/decode automatically to/from JSON.

## Logging over MQTT

Enable logging by calling:

```python
client.configure_mqtt_logger(level=logging.INFO)
```

## Auto Configuration via Environment Variables

Uses [`python-decouple`](https://github.com/henriquebastos/python-decouple) for environment configuration.

### Required Variables

- `MQTT_IP`
- `MQTT_USERNAME`
- `MQTT_PASSWORD`
- `MQTT_PORT` (optional)
- `USE_MQTTS`
- `CA_CERT_FILE` (optional)
- `TLS_CERT_FILE` (optional)
- `TLS_KEY_FILE` (optional)

## Callback System

Subscribe to topics and register priority-based callbacks:

```python
client.subscribe(topic, qos=1, callback=my_callback, priority=10)
```

Callbacks with the same priority are executed concurrently; higher-priority callbacks run first.

## Payload Types

Includes predefined payloads such as:

- `Metric`
- `Log`
- And many more are easily self-definable...

## Testing ISA-95 Topics

```python
from franzmq.topic import Isa95Topic, Topic, Isa95Fields

basic_topic = Topic(payload_type=Metric, context=("sensor", "temperature"))
isa95_fields = Isa95Fields(enterprise="ent1", site="s1", area="a1", production_line="pl1", work_cell="wc1", origin_id="origin1")
isa95_topic = Isa95Topic.from_topic(basic_topic, isa95_fields)
```

## License

MIT License
