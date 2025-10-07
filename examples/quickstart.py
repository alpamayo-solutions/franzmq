import time
from franzmq.client import Client
from franzmq.topic import Topic
from franzmq.payload import Metric

# EXAMPLE: Short example of a typed payload (Metric)

# Create a client from .env file (MQTT_IP, MQTT_USERNAME, etc. are read by decouple)
client = Client.autocreate_and_connect(client_id="quick-demo")
client.loop_start()  # spin paho network loop in background

# Build a Topic that carries a 'Metric' payload, e.g., example/v1/_Metric/sensor/temp
topic = Topic(payload_type=Metric, context=("sensor", "temp"))

# Receive any franz Message (typed) through a *global* on_message
def on_any_message(client, userdata, msg):
    # Because Client overrides _handle_on_message, the msg here is a franzmq.message.Message
    print("Got:", msg.topic, "payload:", msg.payload, "at", msg.timestamp_dt)
client.on_message = on_any_message

client.subscribe(topic, qos=0) 
time.sleep(2)
client.publish(topic, Metric(value=23.7))

time.sleep(2)
client.loop_stop()
client.disconnect()
