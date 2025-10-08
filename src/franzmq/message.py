from paho.mqtt.client import MQTTMessage as PahoMQTTMessage
from franzmq.data_contracts.base.topic import Topic
from franzmq.data_contracts.base.payload import Payload
from paho.mqtt.client import MQTTMessageInfo, Properties, MessageState
import datetime
import time
class Message(PahoMQTTMessage):
    _topic: Topic
    _payload: Payload

    def __init__(self, topic: Topic, payload: Payload, qos=0, retain=False, mid=0, timestamp=0, properties=None):
        self.timestamp = timestamp
        self.state = MessageState.MQTT_MS_INVALID
        self.dup = False
        self.mid = mid
        self._topic = topic
        self._payload = payload
        self.qos = 0
        self.retain = False
        self.info = MQTTMessageInfo(mid)
        self.properties: Properties | None = None

    @property
    def topic(self):
        return self._topic

    @property
    def timestamp_dt(self) -> datetime.datetime:
        # If timestamp is from time.monotonic(), convert it to a Unix timestamp
        if isinstance(self.timestamp, str):
            return datetime.datetime.fromisoformat(self.timestamp)
        if not isinstance(self.timestamp, datetime.datetime) and self.timestamp < 1000000000:
            return datetime.datetime.fromtimestamp(time.time() - (time.monotonic() - self.timestamp))
        return datetime.datetime.fromtimestamp(self.timestamp)

    @topic.setter
    def topic(self, value):
        self._topic = value

    @property
    def payload(self):
        return self._payload

    @payload.setter
    def payload(self, value):
        args = value if isinstance(value, dict) else value.__dict__
        self._payload = self.topic.payload_type(**args)
