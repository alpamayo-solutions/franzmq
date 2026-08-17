"""Broker rejections, surfaced instead of swallowed.

On MQTT 3.1.1 a PUBACK carries no reason field, so a rejected publish is
acknowledged and reads as success — a publisher cannot tell that the broker
threw its message away. franzmq speaks MQTT 5 for exactly this reason, and
turns a rejecting PUBACK into an exception naming the topic and the cause.
"""

#: MQTT-5 PUBACK reason codes the broker uses, with what each one means for the
#: publisher. Anything not listed still raises; the code is reported raw.
REASONS = {
    0x90: "topic name invalid — the broker does not know this contract",
    0x99: "payload format invalid — the payload failed the contract schema",
    0x87: "not authorized — no grant covers this topic",
    0x89: "quota exceeded — the destination is draining and admits no new records",
}


class FranzmqError(Exception):
    """Base for every error this library raises."""


class PublishRejected(FranzmqError):
    """The broker acknowledged the publish with a failure reason code."""

    def __init__(self, reason_code: int, topic: str, reason: str = ""):
        self.reason_code = reason_code
        self.topic = topic
        self.reason = reason or REASONS.get(reason_code, "rejected by the broker")
        super().__init__(
            f"broker rejected publish to {topic}: {self.reason} (0x{reason_code:02x})"
        )


class PublishTimeout(FranzmqError):
    """No PUBACK arrived within the wait window."""

    def __init__(self, topic: str, timeout: float):
        self.topic = topic
        self.timeout = timeout
        super().__init__(f"no PUBACK for {topic} within {timeout:.1f}s")
