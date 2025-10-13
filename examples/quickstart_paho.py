import json
import time
import uuid
from datetime import datetime

import paho.mqtt.client as mqtt

# EXAMPLE: Example of a typed payload (Metric) by using standard paho mqtt client 

# --- Config you can change ---
BROKER = "test.mosquitto.org"   # public test broker, no auth, port 1883
PORT = 1883
TOPIC = "example/v1/_Metric/sensor/temp"  
QOS = 0
METRIC_VALUE = 23.7
# -----------------------------

# using uuid so we make sure that client_id is unique every time script is run
client_id = f"quick-demo-{uuid.uuid4().hex[:8]}"

def on_connect(client, userdata, flags, rc):
    print(f"[{datetime.now().isoformat(timespec='seconds')}] Connected with rc={rc}")
    if rc == 0:
        client.subscribe(TOPIC, qos=QOS)
        print(f"Subscribed to {TOPIC}")

def on_message(client, userdata, msg):
    ts = datetime.now().isoformat(timespec='seconds')
    payload_text = None
    try:
        payload_text = msg.payload.decode("utf-8")
        # Pretty print JSON if possible
        try:
            as_json = json.loads(payload_text)
            payload_text = json.dumps(as_json, indent=2, ensure_ascii=False)
        except json.JSONDecodeError:
            pass
    except Exception:
        payload_text = f"<{len(msg.payload)} bytes>"

    print(f"[{ts}] Got message on {msg.topic} (QoS {msg.qos}):\n{payload_text}")

try:
    client = mqtt.Client(
        client_id=client_id,
        protocol=mqtt.MQTTv311,
        transport="tcp",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION1,  # paho 2.x
    )
except TypeError:
    # Older paho without callback_api_version
    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311, transport="tcp")

client.on_connect = on_connect
client.on_message = on_message

# If you need auth/TLS later, uncomment and fill in:
# client.username_pw_set("username", "password")
# client.tls_set()  # and set PORT = 8883 if the broker requires TLS

print(f"Connecting to mqtt://{BROKER}:{PORT} as {client_id} ...")
client.connect(BROKER, PORT, keepalive=60)

client.loop_start()

time.sleep(1.0)

metric_payload = {"value": METRIC_VALUE}
result = client.publish(TOPIC, json.dumps(metric_payload), qos=QOS, retain=False)
status = result.rc if hasattr(result, "rc") else result[0]
if status == mqtt.MQTT_ERR_SUCCESS:
    print(f"Published to {TOPIC}: {metric_payload}")
else:
    print(f"Publish failed with status {status}")

time.sleep(2)

# Clean up
client.loop_stop()
client.disconnect()
print("Disconnected.")
