import threading
import time
from dataclasses import dataclass, field

from franzmq import Client, Topic, Cmd, Message

# EXAMPLE: command/ack over MQTT.
#
# Receiver subscribes to a Cmd topic. Sender publishes a command and waits for
# the Ack. franzmq handles expiry (498 without running the callback) and maps
# the callback's return value onto the ack's result code.


@dataclass
class DeviceSettingsCmd(Cmd):
    command: dict = field(default_factory=dict)


cmd_topic = Topic(
    prefix="demo",
    payload_type=DeviceSettingsCmd,
    node_id="device-1",
    context=("device-1", "settings"),
)


def run_receiver():
    receiver = Client.autocreate_and_connect(client_id="cmd-receiver")
    receiver.loop_start()

    def handle(message: Message) -> int:
        settings = message.payload.command
        print("[receiver] applying:", settings)
        time.sleep(0.5)  # simulate work
        return 200  # success

    receiver.subscribe_to_command(cmd_topic, callback=handle, qos=1)
    # Keep the receiver alive long enough for the sender to finish.
    time.sleep(10)
    receiver.loop_stop()
    receiver.disconnect()


def run_sender():
    sender = Client.autocreate_and_connect(client_id="cmd-sender")
    sender.loop_start()
    time.sleep(1)  # let the receiver subscribe first

    ack = sender.publish_command(
        topic=cmd_topic,
        command={"enabled": True, "interval_ms": 500},
        validity_duration=5.0,
    )
    print(f"[sender] ack: result_code={ack.result_code} message={ack.message!r}")

    sender.loop_stop()
    sender.disconnect()


if __name__ == "__main__":
    t = threading.Thread(target=run_receiver, daemon=True)
    t.start()
    run_sender()
    t.join()
