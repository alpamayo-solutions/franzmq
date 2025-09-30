import time
from franzmq.client import Client
from franzmq.topic import Topic
from franzmq.payload import Metric

# EXAMPLE: Two callbacks with different priorities

client = Client.autocreate_and_connect(client_id="demo-subscriber")
client.loop_start()

topic = Topic(payload_type=Metric, context=("line-1", "motor-7", "speed"))

def high_priority_cb(client, userdata, msg):
    print("[P2] urgent first:", msg.payload.value)

def low_priority_cb(client, userdata, msg):
    print("[P0] later:", msg.payload.value)

# Register two callbacks on the same topic
client.subscribe(topic, qos=0, callback=low_priority_cb,  priority=0)
client.subscribe(topic, qos=0, callback=high_priority_cb, priority=2)

time.sleep(2)
# Publish a couple of values to demonstrate ordering/concurrency
client.publish(topic, Metric(value=100))
client.publish(topic, Metric(value=101))

time.sleep(2)
client.loop_stop()
client.disconnect()
