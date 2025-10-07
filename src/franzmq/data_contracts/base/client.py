import logging
import threading
from decouple import config
import pathlib

from paho.mqtt.client import Client as PahoClient
from franzmq.data_contracts.base.topic import Topic
from franzmq.data_contracts.base.payload import Payload, ServiceType
from franzmq.data_contracts.base.message import Message
from typing import Callable


def str_or_none(value):
    if value.lower() in ["none", "null", "nan"]:
        return None
    return value

def path_or_none(value):
    if value.lower() in ["none", "null", "nan"]:
        return None
    return pathlib.Path(value)

class Client(PahoClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._on_connect_callbacks = []
        self._on_message_callbacks = []
        self._on_disconnect_callbacks = []
        self._on_subscribe_callbacks = []
        self._on_unsubscribe_callbacks = []
        self._on_log_callbacks = []

        self.subscribed_topics = set()

        self._topic_callbacks = {}  # key: topic (str), value: list of (priority, callback)
        self._topic_callbacks_lock = threading.Lock()

    def configure_mqtt_logger(
        self,
        level: int = logging.INFO,
        topic_prefix: str = "logs",
        format_string: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    ) -> None:
        from franzmq.data_contracts.base.log_handlers import configure_logging
        configure_logging(self, level, topic_prefix, format_string)
        return None

    def publish(self, topic: Topic, payload: Payload, qos=0, retain=False):
        super().publish(str(topic), payload.encode(), qos, retain)

    def subscribe(self, topic: Topic | str, qos: int = 0, callback=None, priority: int = 0):
        """
        Subscribe to a topic and (optionally) register a callback with an optional priority.
        
        * The topic is stored in `self.subscribed_topics`.
        * If a callback is supplied, it is added to an internal list along with its priority.
          When the message is received, the client’s master callback (installed on that topic)
          iterates through all registered callbacks:
            - Callbacks are ordered by descending priority (higher priority numbers first).
            - Callbacks with the same priority are executed in parallel.
        
        Parameters:
          - topic: the topic (a Topic object or string)
          - qos: the Quality-of-Service level.
          - callback: a function with signature (client, userdata, message). If None,
                      no callback is registered.
          - priority: an integer priority; higher numbers mean the callback is executed earlier.
                      Defaults to 0.
        """
        topic_str = str(topic)
        self.subscribed_topics.add(topic_str)

        if callback is not None:
            with self._topic_callbacks_lock:
                if topic_str not in self._topic_callbacks:
                    self._topic_callbacks[topic_str] = []
                    self.message_callback_add(topic_str, self._create_master_callback(topic_str))
                self._topic_callbacks[topic_str].append((priority, callback))
        return super().subscribe(topic_str, qos)

    def unsubscribe(self, topic: Topic | str) -> tuple[int, int | None]:
        """
        Unsubscribe from a topic.
        
        This method removes the topic from the set of currently subscribed topics and
        also clears any associated callbacks.
        """
        topic_str = str(topic)
        if topic_str in self.subscribed_topics:
            self.subscribed_topics.remove(topic_str)
        with self._topic_callbacks_lock:
            if topic_str in self._topic_callbacks:
                del self._topic_callbacks[topic_str]
                self.message_callback_remove(topic_str)
        return super().unsubscribe(topic_str)

    def _create_master_callback(self, topic_str: str):
        """
        Creates and returns a "master callback" function for the given topic.
        This callback is registered with the underlying paho client.
        
        When invoked, it looks up all registered callbacks for the topic, sorts them,
        and executes them in groups according to their priority.
        """
        def master_callback(client, userdata, message):
            with self._topic_callbacks_lock:
                callbacks = self._topic_callbacks.get(topic_str, []).copy()
            if not callbacks:
                return

            sorted_callbacks = sorted(callbacks, key=lambda x: x[0], reverse=True)

            current_priority = None
            group = []
            for prio, cb in sorted_callbacks:
                if current_priority is None:
                    current_priority = prio
                    group.append(cb)
                elif prio == current_priority:
                    group.append(cb)
                else:
                    self._execute_callbacks_concurrently(client, userdata, message, group)
                    current_priority = prio
                    group = [cb]
            if group:
                self._execute_callbacks_concurrently(client, userdata, message, group)
        return master_callback

    def _execute_callbacks_concurrently(self, client, userdata, message, callbacks):
        """
        Executes a list of callbacks concurrently. Each callback is run in its own thread.
        The master callback waits until all callbacks in the group have finished before returning.
        """
        threads = []
        for cb in callbacks:
            t = threading.Thread(target=cb, args=(client, userdata, message))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

    def _handle_on_message(self, message):
        topic = Topic.from_str(message.topic)
        message = Message(
            topic=topic,
            payload=topic.payload_type.decode(message.payload, message.timestamp),
            timestamp=message.timestamp,
            qos=message.qos,
            retain=message.retain,
            mid=message.mid
        )

        return super()._handle_on_message(message)

    @classmethod
    def autocreate_and_connect(cls, client_id: str, on_connect: Callable = None, on_disconnect: Callable = None):
        import ssl
        mqtt_ip = config("MQTT_IP", default="broker", cast=str_or_none)
        mqtt_username = config("MQTT_USERNAME", default="franz", cast=str_or_none)
        mqtt_password = config("MQTT_PASSWORD", default="franz", cast=str_or_none)

        mqtt_client = Client(client_id=client_id)
        mqtt_client.configure_mqtt_logger()
        mqtt_client.reconnect_on_failure = True
        mqtt_client.reconnect_on_offline = True
        mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)

        if mqtt_username and mqtt_password:
            mqtt_client.username_pw_set(mqtt_username, mqtt_password)

        ca_cert = config("CA_CERT_FILE", default="/etc/certs/example-ca.crt", cast=path_or_none)
        tls_cert = config("TLS_CERT_FILE", default="/etc/certs/example-ca.crt", cast=path_or_none)
        tls_key = config("TLS_KEY_FILE", default="/etc/certs/example-ca.crt", cast=path_or_none)
        use_mqtts = config("USE_MQTTS", default=True, cast=bool)

        if use_mqtts and ca_cert is not None:
            # Fail fast if any required files are missing
            missing = []
            for var_name, path in [("CA_CERT_FILE", ca_cert), ("TLS_CERT_FILE", tls_cert), ("TLS_KEY_FILE", tls_key)]:
                if not path:
                    missing.append(var_name)
                elif not path.exists():
                    raise RuntimeError(f"{var_name} path '{path}' does not exist")

            if missing:
                raise RuntimeError(f"Missing TLS config env vars: {', '.join(missing)}")

            mqtt_client.tls_set(
                ca_certs=ca_cert,
                certfile=tls_cert,
                keyfile=tls_key,
                tls_version=ssl.PROTOCOL_TLS_CLIENT
            )
            default_mqtt_port = 8883  # Enforce secure port
        else:
            default_mqtt_port = 1883

        mqtt_port = config("MQTT_PORT", default=default_mqtt_port, cast=int)

        mqtt_client.connect(host=mqtt_ip, port=mqtt_port)

        if on_connect is not None:
            mqtt_client.on_connect = on_connect
        if on_disconnect is not None:
            mqtt_client.on_disconnect = on_disconnect

        logger = logging.getLogger(client_id)
        logger.setLevel(logging.INFO)
        return mqtt_client

    def publish_service_details(self, details: ServiceType):
        name = [details.id] # no display name because the service does not have to know about it.
        self.publish(Topic(payload_type=ServiceType, context=details.hierarchy + name), details, retain=True)