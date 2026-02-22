# Changelog

All notable changes to franzmq are documented in this file.

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
