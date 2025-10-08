from franzmq.topic import Topic, Isa95Topic, Isa95Fields
from franzmq.payload import DataTags, DataTag, DataTagContext, DataTagContexts, Metric
from franzmq.message import Message
from franzmq.client import Client

def on_message(client, userdata, message):
    print("Callback received", type(message), type(message.payload), type(message.topic))
    print(str(message.topic), message.topic.context)

topic = Topic(payload_type=Metric, context=("this", "is", "a", "sensor"))
print("Basic Topic:", topic)

# Convert the basic Topic to an Isa95Topic
isa95_fields = Isa95Fields(
    enterprise="enterprise1",
    site="site1",
    area="area1",
    production_line="line1",
    work_cell="cell1",
    origin_id="origin1"
)

isa95_topic = Isa95Topic.from_topic(topic, isa95_fields)
print("ISA95 Topic:", isa95_topic)

# Convert the ISA95 topic back to a basic Topic
topic_from_isa95 = isa95_topic.to_topic()
print("Basic Topic (converted back from ISA95):", topic_from_isa95)

assert str(topic) == str(topic_from_isa95), "The topic should be the same"

client = Client()

# Connect to localhost MQTT broker
client.connect("localhost", 1883)

client.on_message = on_message
client.subscribe("alp/#")

topic = Topic(payload_type=Metric, context=("this", "is", "a", "sensor"))
tags = [DataTag(id="test", name="test", is_writable=True, is_readable=True, data_type="string")]
payload = DataTags(data_tags=tags)
# message = Message(topic=topic, payload=payload)
client.publish(topic, payload)

topic = Topic(payload_type=DataTagContexts)
contexts = [DataTagContext(tag_id="test", topic_name="test", is_logged=True, is_published=True)]
context_payload = DataTagContexts(data_tag_contexts=contexts)
message = Message(topic=topic, payload=context_payload)
client.publish(topic, context_payload)

topic = Topic(payload_type=Metric, context=["this", "is", "a", "sensor"])
metric_payload = Metric(value="1.0")
message = Message(topic=topic, payload=metric_payload)
client.publish(topic, metric_payload)

client.loop_forever()
