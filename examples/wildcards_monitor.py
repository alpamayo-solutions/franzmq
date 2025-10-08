import time
from franzmq.client import Client
from franzmq.topic import Topic
from franzmq.data_contracts.base import Metric

# EXAMPLE: One-liner subscribe to every Metric

client = Client.autocreate_and_connect("metric-tap")
client.loop_start()

def on_metric(client, ud, msg):
    # msg.topic is a Topic object — you can inspect its context tuple
    print("Metric@", str(msg.topic), "=", msg.payload.value)

client.on_message = on_metric
client.subscribe(Topic(payload_type=Metric, context=("#",)))  # example/v1/_Metric/#

# publish a couple for demo
client.publish(Topic(payload_type=Metric, context=("line-1","motor-7","speed")), Metric(123.4))
client.publish(Topic(payload_type=Metric, context=("line-2","oven","temp")), Metric(456.7))

time.sleep(1)
client.loop_stop(); client.disconnect()
