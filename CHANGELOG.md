# Changelog

All notable changes to franzmq are documented in this file.

## [0.6.2] - 2026-08-17

### Fixed

- **A QoS ≥ 1 publish from inside a callback no longer deadlocks.** The PUBACK
  can only be read by the network thread, so waiting for it *on* that thread
  blocks until the timeout, every time — which is what publishing retained state
  from `on_connect` does. Such a publish now goes out without waiting (and says
  so at debug level). A caller that needs the broker's verdict has to publish off
  the network thread.

## [0.6.1] - 2026-08-17

### Fixed

- **A tombstone reaches its subscriber.** An empty payload retires the record
  at a topic; decoding it as a contract failed, so the message was dropped with
  a log line and a consumer kept acting on state that no longer existed.
  `message.payload` is now `None` for an empty payload, and callbacks can treat
  that as the retirement it is.

## [0.6.0] - 2026-08-17

Alignment release. franzmq is the client half of a broker contract, and the
broker changed: authentication, protocol version and acknowledgement semantics
all moved. There is no compatibility mode — services still speaking the old
model pin `0.4.1`, and that pin is their migration switch.

### Breaking Changes

- **Authentication is the pinned-key model.** The ed25519 identity key named by
  `MACHINE_KEY` is the credential: the client mints a self-signed certificate
  from it in-process and presents that, and the broker decides by looking up the
  public key among the identities enrolled there. There is no CA and nothing
  validates the broker in return — trust runs the other way.
  `MQTT_USERNAME`, `MQTT_PASSWORD`, `USE_MQTTS`, `CA_CERT_FILE` and
  `TLS_CERT_FILE` are **removed**; `TLS_KEY_FILE` is replaced by `MACHINE_KEY`.
  The MQTT username and the client id are both the identity.

- **MQTT 5 and callback API v2.** On MQTT 3.1.1 a PUBACK carries no reason
  field, so a broker that rejects a publish still acknowledges it and the
  publisher reads the rejection as success. Every client this library builds is
  now on MQTT 5, which forces paho's v2 callback signatures:
  `on_connect(client, userdata, flags, reason_code, properties)`,
  `on_publish(client, userdata, mid, reason_code, properties)`,
  `on_subscribe(client, userdata, mid, reason_code_list, properties)`.

- **QoS ≥ 1 publishes wait for their PUBACK, one at a time, and raise on
  rejection.** `publish()` raises `PublishRejected` (with the topic and the
  reason) or `PublishTimeout`. Unbounded in-flight QoS-1 is not a throughput
  knob: an ordinary reconnect replays whatever is still unacked, out of order,
  after newer messages are already on the wire. Pass `wait=False` for the old
  fire-and-forget behaviour.

- **The two-phase command handshake is gone.** One command, one ack.
  `result_code=-1` and `max_command_duration` no longer exist; the sender waits
  until the command's own expiry. Result codes are the broker's: `200` done,
  `409` conflict, `422` invalid, `498` expired before execution, `500` internal
  (including an exception in the callback — previously `598`).

- **Sessions persist.** Connections use `clean_start=False` with a session
  expiry interval (`MQTT_SESSION_EXPIRY`, default: never expires), so commands
  issued while a client was away arrive on reconnect.

### Added

- `franzmq.pinned_tls` — `load_identity_key`, `self_signed_cert_pem`,
  `self_signed_context`.
- `franzmq.errors` — `FranzmqError`, `PublishRejected`, `PublishTimeout`, and
  `REASONS` mapping each broker reason code to what it means for the publisher.
- `Client.make_command_handler(callback, qos)` — the ack rules (expiry, result
  mapping, exceptions) as a reusable handler, testable without a broker.

### Fixed

- The MQTT log handler no longer calls the deprecated `datetime.utcnow()`.

## [0.5.0] - 2026-08-17

### Breaking Changes

- **`Topic` carries the publisher's identity at level 4.** The v1 wire shape is
  now `{prefix}/{version}/_{PayloadType}/{node_id}/{context…}`. `Topic` gained a
  required `node_id` field, positioned directly after `payload_type`; `__str__`
  emits it, `from_str` reads segment 3 into it and everything from segment 4 into
  `context`, and `to_ack_topic()` preserves it. `+` is accepted as `node_id` in
  subscription filters.

  Before:
  ```python
  Topic(payload_type=Metric, context=("sensor", "temperature"))
  # example/v1/_Metric/sensor/temperature
  ```

  After:
  ```python
  Topic(payload_type=Metric, node_id="machine-1", context=("sensor", "temperature"))
  # example/v1/_Metric/machine-1/sensor/temperature
  ```

  There is no dual mode: the old shape is gone. Brokers that enforce an identity
  rule match level 4 against the authenticated client, and every hop that
  re-mounts a record rewrites only the path after it — both are impossible
  without the level. Pin `franzmq==0.4.1` for services still speaking the old
  scheme; the pin is the per-service migration switch.

- **A v1 topic must have a path.** A topic with a `node_id` and an empty
  `context` is rejected, because the level-4 identity is not a hierarchy
  position. The single exception is the pathless-contract set
  (`franzmq.topic.PATHLESS_CONTRACTS`, currently `_TimeSync`), which mirrors the
  broker-side grammar.

- **ISA-95 conversion names the publisher explicitly.** `Isa95Topic.to_topic()`
  now takes `node_id`, and `Topic.from_str()` raises on a `v1-isa95` string
  instead of silently converting: ISA-95 topics address the hierarchy through
  their own levels and carry no identity to carry over. `Isa95Topic` itself —
  its fields, its rendering, its `from_str` — is unchanged.

- **`TopicBase._topic()` takes a `node_id`**, defaulting to the class's own
  `node_id` attribute.

### Added

- **`Client.node_id`** — the identity used for topics the client builds itself
  (`publish_service_details`, the MQTT log handler). Read from the `NODE_ID`
  environment variable, and defaulted to the client id by
  `autocreate_and_connect`. `Client.require_node_id()` raises a message naming
  all three ways to set it rather than failing deep inside `Topic`.

- **Golden topic-transformation vectors** (`tests/vectors/topic_transformations.json`,
  a copy of the canonical file in `prekit-data-contracts`) drive the topic suite.
  The same cases are judged by the Go broker-side implementation, so the two
  cannot drift apart silently — changing a case is a protocol change and needs
  both suites green.

- **CI runs the test suite** on every push and pull request, and publishing is
  gated on it.

### Fixed

- `tests/test_logging.py` was a manual script that connected to a live broker at
  import time and could only ever error during collection. It is now a real unit
  test of the log handler's topic construction; the runnable demo already lived
  in `examples/log_handler.py`.

## [0.4.1] - 2026-05-12

### Added

- **`Client.publish_tombstone(topic, qos=0)`** -- publish an empty retained payload to clear a previously retained topic. Always sends `retain=True`. Use this to retract a retained `Payload` from the broker, e.g. when deleting an asset whose state was kept under a retained topic. Previously, calling `publish(topic, None)` crashed with `AttributeError: 'NoneType' object has no attribute 'encode'`.

## [0.4.0] - 2026-02-22

### Breaking Changes

- **Callback signature changed.** `subscribe()` callbacks now receive a single `message: Message` argument (a decoded franzmq `Message` object) instead of the raw paho triple `(client, userdata, message)`.

  Before:
  ```python
  def on_message(client, userdata, message):
      print(message.topic)
  client.subscribe("alp/#", callback=on_message)
  ```

  After:
  ```python
  def on_message(message: Message):
      print(message.topic)        # franzmq.Topic
      print(message.payload)      # decoded Payload instance
  client.subscribe("alp/#", callback=on_message)
  ```

- **`PAYLOAD_CLASSES` moved.** Import from `franzmq.data_contracts` instead of `franzmq.data_contracts.base`. The dict is now built by auto-discovery at import time and includes all `Payload` subclasses found in the `data_contracts` package.

- **`autocreate_and_connect` TLS defaults changed.** `CA_CERT_FILE`, `TLS_CERT_FILE`, and `TLS_KEY_FILE` now default to `None` instead of `/etc/certs/example-ca.crt`. This means TLS is only configured when certificates are explicitly provided.

### Added

- **Command/Acknowledge pattern** -- request-response semantics over MQTT with two-phase handshake:
  - `Client.publish_command(topic, command, validity_duration, ...)` -- send a command and block until acknowledged.
  - `Client.subscribe_to_command(topic, callback, qos)` -- receive commands with automatic handshake and sequential per-topic execution.
  - `Cmd` payload -- base command dataclass with `created_at`, `correlation_id`, `expires_at`, and `command` fields.
  - `Ack` payload -- acknowledgement dataclass with `correlation_id`, `result_code`, `performed_at`, and `message` fields.
  - Two-phase flow: receiver sends handshake (`result_code=-1`), then executes the callback, then sends final ack with the result code.

- **`Topic.to_ack_topic()`** -- derive the acknowledgement topic from a command topic. Raises `ValueError` if the topic's payload type is not a `Cmd` subclass.

- **`Topic.__eq__` / `__ne__` / `__hash__`** -- topics can now be compared to strings and used in sets/dicts by their string representation.

- **Class-based topic definitions** via `TopicBase` and `classproperty`:
  - `TopicBase` -- base class for hierarchical topic building with parent-chain composition.
  - `classproperty` -- descriptor for defining topic properties on classes.
  - `TopicBase.get_from_snake_case(key)` -- look up a `classproperty` topic by its snake_case name.

- **Payload auto-discovery** in `data_contracts/__init__.py` -- all `Payload` subclasses in the `data_contracts` package are automatically registered in `PAYLOAD_CLASSES` at import time. Projects extending franzmq with custom payloads benefit from this when placing modules in the `data_contracts` directory.

- **`CustomEncoder`** -- JSON encoder that handles `ServiceType`, `IndexType`, `DataType` enums and `datetime` objects. Used by `Payload.encode()`.

- **`last_will` parameter** on `Client.autocreate_and_connect(...)` -- pass a `Dict[Topic, Payload]` to set MQTT last will messages.

### Fixed

- `str_or_none` and `path_or_none` now handle `None` input without raising `AttributeError`.
- `publish_service_details` used `details.id` (non-existent attribute) instead of `details.name`.
- Removed stray `logging.info` call in `Metric.encode()` that logged every metric encoding.
- Monkey-patched `json.JSONEncoder.default` to handle `enum.Enum` subclasses, preventing `TypeError` when serializing payloads containing enum fields.

## [0.3.0] - Previous release

Initial public version with `Topic`, `Isa95Topic`, `Message`, `Client`, `Payload`, `Metric`, `Log`, `ServiceDetails`, and MQTT logging handler.
