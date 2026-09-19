import datetime
import logging
import queue
import threading
from collections import OrderedDict
import time
import uuid
import pathlib
from typing import Any, Callable, Dict, Optional, Tuple, Union

from decouple import config
from paho.mqtt.client import Client as PahoClient, CallbackAPIVersion, MQTTv5
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from franzmq.errors import PublishRejected, PublishTimeout
from franzmq.pinned_tls import self_signed_context
from franzmq.topic import Topic
from franzmq.message import Message
from franzmq.data_contracts.base import Payload, ServiceDetails, Cmd, Ack

logger = logging.getLogger(__name__)
cmd_ack_logger = logging.getLogger("franzmq.cmd_ack")


def _ts(unix_ts: float) -> str:
    """Format a unix timestamp as hh:mm:ss.fff (UTC)."""
    dt = datetime.datetime.fromtimestamp(unix_ts, tz=datetime.timezone.utc)
    return dt.strftime("%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def _now_ts() -> str:
    """Current time formatted as hh:mm:ss.fff (UTC)."""
    return _ts(time.time())


def str_or_none(value):
    if value is None:
        return None
    if value.lower() in ["none", "null", "nan"]:
        return None
    return value


def path_or_none(value):
    if value is None:
        return None
    if value.lower() in ["none", "null", "nan"]:
        return None
    return pathlib.Path(value)


#: How long a QoS≥1 publish waits for its PUBACK before giving up.
DEFAULT_PUBLISH_TIMEOUT = 10.0


class _Inflight:
    """One QoS≥1 publish awaiting its PUBACK, keyed by its mid."""

    __slots__ = ("event", "reason_code")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.reason_code: int = 0


#: PUBACKs that arrived before the publishing thread had registered the mid
#: they answer. The window is the few instructions between paho sending the
#: packet and `_publish_bytes` storing its slot, so a handful suffice; the
#: bound only keeps a lost registration from growing the dict forever.
_EARLY_ACK_LIMIT = 64


class Client(PahoClient):
    def __init__(self, *args, **kwargs):
        # MQTT 5 is not optional: on 3.1.1 a PUBACK has no reason field, so a
        # broker that rejects a publish still acknowledges it and the publisher
        # reads the rejection as success. Callback API v2 comes with it — it is
        # the only version that hands the reason code to on_publish.
        kwargs.setdefault("protocol", MQTTv5)
        super().__init__(CallbackAPIVersion.VERSION2, *args, **kwargs)

        # Identity this client publishes under — level 4 of every v1 topic it
        # builds itself (service details, logs). Brokers that enforce the
        # identity rule match this against the authenticated client, so it must
        # be the client's own id, not the tree position it sits at.
        # `autocreate_and_connect` falls back to the client_id when NODE_ID is
        # not set in the environment.
        self.node_id: Optional[str] = config("NODE_ID", default=None, cast=str_or_none)

        self.publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT
        # One QoS≥1 publish on the wire at a time, in order. Unbounded
        # in-flight QoS-1 is not just a throughput knob: an ordinary reconnect
        # replays whatever is still unacked from paho's message store, out of
        # order, after newer messages are already on the wire. paho's own
        # in-flight window closes that race without blocking anyone: a second
        # QoS≥1 publish is queued inside paho and sent once the first is
        # acked. It used to be a lock held across the PUBACK wait instead,
        # which put EVERY publisher behind the waiter -- including one inside
        # a message callback, which runs on a thread the network thread joins.
        # The callback blocked on the lock, the network thread on the
        # callback, and the waiter's PUBACK on the network thread: a deadlock
        # that ended only at the timeout.
        self.max_inflight_messages_set(1)
        # Guards the two dicts below and nothing else -- never held across a
        # wait, never held around a call into paho (whose PUBACK handler runs
        # under paho's own mutex and calls back into `_do_on_publish`).
        self._inflight_lock = threading.Lock()
        self._inflight: dict[int, _Inflight] = {}
        self._early_acks: "OrderedDict[int, int]" = OrderedDict()
        # Threads franzmq spawned to run message callbacks. The network thread
        # joins them, so a PUBACK cannot be read while one of them waits for
        # it -- the same reason the network thread itself never waits.
        self._callback_thread = threading.local()

        self._on_connect_callbacks = []
        self._on_message_callbacks = []
        self._on_disconnect_callbacks = []
        self._on_subscribe_callbacks = []
        self._on_unsubscribe_callbacks = []
        self._on_log_callbacks = []

        self.subscribed_topics = set()

        self._topic_callbacks = {}  # key: topic (str), value: list of (priority, callback)
        self._topic_callbacks_lock = threading.Lock()

        # Command/Ack state
        self._pending_commands = {}  # key: correlation_id, value: (event, ack_result)
        self._pending_commands_lock = threading.Lock()

        self._ack_topic_subscriptions = {}  # key: ack_topic_str, value: set of correlation_ids
        self._ack_topic_subscriptions_lock = threading.Lock()

        self._command_queues = {}       # key: topic_str, value: queue.Queue
        self._command_executors = {}    # key: topic_str, value: threading.Thread
        self._command_executors_lock = threading.Lock()

    def configure_mqtt_logger(
        self,
        level: int = logging.INFO,
        topic_prefix: str = "logs",
        format_string: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    ) -> None:
        from franzmq.log_handlers import configure_logging
        configure_logging(self, level, topic_prefix, format_string)
        return None

    def publish(self, topic: Topic, payload: Payload, qos=0, retain=False, wait: bool = True):
        """Publish a typed payload.

        At QoS ≥ 1 this waits for the PUBACK and raises :class:`PublishRejected`
        when the broker answers with a failure reason code — a rejected publish
        must not look like a successful one. Pass ``wait=False`` for
        fire-and-forget, accepting that rejections and ordering become your
        problem.
        """
        return self._publish_bytes(str(topic), payload.encode(), qos, retain, wait)

    def publish_tombstone(self, topic: Topic, qos: int = 0, wait: bool = True):
        """Publish an empty retained payload to clear a retained topic.

        MQTT brokers remove a topic's retained message when they receive an
        empty payload on it. Use this to retract a previously retained
        ``Payload``. Always sends ``retain=True`` because tombstones only make
        sense against retained topics.
        """
        return self._publish_bytes(str(topic), b"", qos, True, wait)

    def _publish_bytes(self, topic: str, data: bytes, qos: int, retain: bool, wait: bool):
        if qos == 0:
            # QoS 0 has no PUBACK to wait for and no reason code to report.
            return super().publish(topic, data, qos, retain)

        if wait and self._cannot_read_the_puback():
            # Publishing from the network thread, or from a callback thread it
            # is joining: the PUBACK can only be read by the network thread,
            # which is the thread now asked to wait for it (or blocked on the
            # one that is). Waiting here deadlocks until the timeout, every
            # time. The publish still goes out -- it just cannot be confirmed
            # from here, so a caller that needs the broker's verdict must
            # publish off these threads.
            logger.debug(
                "publish to %s runs on the network thread: sending without waiting for the "
                "PUBACK (a rejection cannot be reported here)", topic,
            )
            wait = False

        info = super().publish(topic, data, qos, retain)
        slot = _Inflight()
        with self._inflight_lock:
            early = self._early_acks.pop(info.mid, None)
            if early is not None:
                slot.reason_code = early
                slot.event.set()
            else:
                self._inflight[info.mid] = slot
        if not wait:
            return info
        try:
            if not slot.event.wait(self.publish_timeout):
                raise PublishTimeout(topic, self.publish_timeout)
            if slot.reason_code >= 0x80:
                raise PublishRejected(slot.reason_code, topic)
            return info
        finally:
            with self._inflight_lock:
                if self._inflight.get(info.mid) is slot:
                    del self._inflight[info.mid]

    def _cannot_read_the_puback(self) -> bool:
        return (
            threading.current_thread() is self._thread
            or getattr(self._callback_thread, "joined_by_network_thread", False)
        )

    def _do_on_publish(self, mid, reason_code, properties):
        # paho's internal PUBACK hook. Capturing here rather than on the public
        # on_publish callback keeps that callback free for the application.
        code = int(getattr(reason_code, "value", reason_code) or 0)
        with self._inflight_lock:
            slot = self._inflight.pop(mid, None)
            if slot is None:
                self._early_acks[mid] = code
                while len(self._early_acks) > _EARLY_ACK_LIMIT:
                    self._early_acks.popitem(last=False)
        if slot is not None:
            slot.reason_code = code
            slot.event.set()
        return super()._do_on_publish(mid, reason_code, properties)

    def subscribe(self, topic: Topic | str, qos: int = 0, callback=None, priority: int = 0):
        """
        Subscribe to a topic and (optionally) register a callback with an optional priority.

        Callbacks receive a single ``message: Message`` argument (a decoded franzmq Message).

        Callbacks are ordered by descending priority (higher priority numbers first).
        Callbacks with the same priority are executed in parallel.
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
        topic_str = str(topic)
        if topic_str in self.subscribed_topics:
            self.subscribed_topics.remove(topic_str)
        with self._topic_callbacks_lock:
            if topic_str in self._topic_callbacks:
                del self._topic_callbacks[topic_str]
                self.message_callback_remove(topic_str)
        return super().unsubscribe(topic_str)

    def _decode_message(self, raw_message):
        """Decode a raw paho MQTT message into a franzmq Message object.

        An empty payload is a **tombstone** — the record at that topic was
        retired — and arrives as ``message.payload is None``. Decoding it as a
        contract would fail, and a consumer that never learns about the
        retirement keeps acting on state that no longer exists.
        """
        topic = Topic.from_str(raw_message.topic)
        if isinstance(raw_message.payload, Payload):
            decoded_payload = raw_message.payload
        elif not raw_message.payload:
            decoded_payload = None
        else:
            decoded_payload = topic.payload_type.decode(raw_message.payload, getattr(raw_message, 'timestamp', time.time()))
        return Message(
            topic=topic,
            payload=decoded_payload,
            timestamp=getattr(raw_message, 'timestamp', time.time()),
            qos=raw_message.qos,
            retain=raw_message.retain,
            mid=raw_message.mid
        )

    def _create_master_callback(self, topic_str: str):
        """
        Creates a master callback that decodes the raw message and dispatches
        to all registered callbacks sorted by priority.
        """
        def master_callback(client, userdata, raw_message):
            try:
                decoded_message = self._decode_message(raw_message)
            except Exception as e:
                logger.error(f"Failed to decode message on topic {raw_message.topic}: {e}")
                return

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
                    self._execute_callbacks_concurrently(decoded_message, group)
                    current_priority = prio
                    group = [cb]
            if group:
                self._execute_callbacks_concurrently(decoded_message, group)
        return master_callback

    def _execute_callbacks_concurrently(self, message: Message, callbacks):
        """
        Execute callbacks concurrently. Each callback receives a decoded Message.
        """
        def run(cb):
            self._callback_thread.joined_by_network_thread = True
            cb(message)

        threads = []
        for cb in callbacks:
            t = threading.Thread(target=run, args=(cb,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

    def _handle_on_message(self, message):
        # Every message paho delivers comes through here, so this is where it
        # is decoded -- by the one decoder, which knows an empty payload is a
        # tombstone. A second decode here once raised on every tombstone and
        # took the network thread with it.
        return super()._handle_on_message(self._decode_message(message))

    # ── Command / Acknowledge pattern ──────────────────────────────────────

    def _create_shared_ack_callback(self, ack_topic_str: str):
        def shared_ack_callback(message: Message):
            ack_payload = message.payload
            if not isinstance(ack_payload, Ack):
                cmd_ack_logger.warning(
                    "[RECV-ACK] %s | payload on %s is %s, not an Ack — ignoring",
                    _now_ts(), ack_topic_str, type(ack_payload).__name__
                )
                return

            correlation_id = ack_payload.correlation_id
            cmd_ack_logger.info(
                "[RECV-ACK] %s | correlation_id=%s, result_code=%s, message='%s'",
                _now_ts(), correlation_id, ack_payload.result_code, ack_payload.message
            )

            with self._pending_commands_lock:
                pending = self._pending_commands.get(correlation_id)
                if pending is None:
                    cmd_ack_logger.warning(
                        "[RECV-ACK] %s | correlation_id=%s not pending (have %s) — ignoring",
                        _now_ts(), correlation_id, list(self._pending_commands)
                    )
                    return
                event, ack_result = pending
                ack_result[0] = ack_payload
                event.set()

        return shared_ack_callback

    def publish_command(
        self,
        topic: Topic,
        command: Any,
        validity_duration: float,
        qos: int = 1,
    ) -> Ack:
        """Publish a command and wait for its acknowledgement.

        One command, one ack: subscribe to the ack topic, publish, wait for the
        `Ack` carrying this correlation id, return it. The ack's `result_code`
        is the broker's own vocabulary — 200 done, 409 conflict, 422 invalid,
        498 expired, 500 internal.
        """
        if not issubclass(topic.payload_type, Cmd):
            raise ValueError(f"Topic payload type must be Cmd or a subclass of Cmd, got {topic.payload_type}")

        correlation_id = str(uuid.uuid4())
        created_at = datetime.datetime.now(datetime.timezone.utc).timestamp()
        expires_at = created_at + validity_duration

        cmd_ack_logger.info(
            "[SEND-CMD] %s | correlation_id=%s, cmd_topic=%s, expires_at=%s",
            _now_ts(), correlation_id, topic, _ts(expires_at)
        )

        command = topic.payload_type(
            created_at=created_at,
            correlation_id=correlation_id,
            expires_at=expires_at,
            command=command,
        )

        ack_topic = topic.to_ack_topic()
        ack_topic_str = str(ack_topic)

        event = threading.Event()
        ack_result: list = [None]

        with self._pending_commands_lock:
            self._pending_commands[correlation_id] = (event, ack_result)

        needs_subscription = False
        with self._ack_topic_subscriptions_lock:
            if ack_topic_str not in self._ack_topic_subscriptions:
                self._ack_topic_subscriptions[ack_topic_str] = set()
                needs_subscription = True
            self._ack_topic_subscriptions[ack_topic_str].add(correlation_id)

        if needs_subscription:
            self._subscribe_and_wait(ack_topic, qos, self._create_shared_ack_callback(ack_topic_str))

        try:
            self.publish(topic, command, qos=qos)

            timeout = expires_at - time.time()
            if timeout <= 0 or not event.wait(timeout=max(timeout, 0)):
                cmd_ack_logger.error(
                    "[SEND-CMD] %s | no ack for correlation_id=%s before expiry at %s",
                    _now_ts(), correlation_id, _ts(expires_at)
                )
                raise TimeoutError(
                    f"command {correlation_id} on {topic} expired without an ack "
                    f"(expires_at={expires_at}, now={time.time()})"
                )

            ack = ack_result[0]
            cmd_ack_logger.info(
                "[SEND-CMD] %s | COMPLETE | correlation_id=%s, result_code=%s, message='%s'",
                _now_ts(), correlation_id, ack.result_code, ack.message
            )
            return ack
        finally:
            self._release_pending(correlation_id, ack_topic, ack_topic_str)

    def _subscribe_and_wait(self, ack_topic: Topic, qos: int, callback, timeout: float = 1.0):
        """Subscribe and block until the SUBACK, so no ack can be missed."""
        confirmed = threading.Event()
        original_on_subscribe = self.on_subscribe

        def on_subscribe_wrapper(client, userdata, mid, reason_code_list, properties):
            confirmed.set()
            if original_on_subscribe:
                original_on_subscribe(client, userdata, mid, reason_code_list, properties)

        self.on_subscribe = on_subscribe_wrapper
        try:
            self.subscribe(ack_topic, qos=qos, callback=callback)
            if not confirmed.wait(timeout=timeout):
                cmd_ack_logger.warning(
                    "[SEND-CMD] %s | no SUBACK for %s within %.1fs — proceeding",
                    _now_ts(), ack_topic, timeout
                )
        finally:
            self.on_subscribe = original_on_subscribe

    def _release_pending(self, correlation_id: str, ack_topic: Topic, ack_topic_str: str) -> None:
        with self._pending_commands_lock:
            self._pending_commands.pop(correlation_id, None)

        should_unsub = False
        with self._ack_topic_subscriptions_lock:
            waiters = self._ack_topic_subscriptions.get(ack_topic_str)
            if waiters is not None:
                waiters.discard(correlation_id)
                if not waiters:
                    del self._ack_topic_subscriptions[ack_topic_str]
                    should_unsub = True

        if should_unsub:
            self.unsubscribe(ack_topic)

    def _command_worker(self, topic_str: str, q: queue.Queue):
        """Drain a per-topic command queue sequentially. Runs as a daemon thread."""
        while True:
            item = q.get()
            if item is None:
                break
            callback, message = item
            correlation_id = getattr(getattr(message, 'payload', None), 'correlation_id', '?')
            try:
                callback(message)
            except Exception as e:
                cmd_ack_logger.error(
                    "[CMD-QUEUE] %s | exception in command worker | topic=%s, correlation_id=%s, error=%s",
                    _now_ts(), topic_str, correlation_id, e, exc_info=True
                )
            finally:
                q.task_done()

    def _enqueue_command(self, topic_str: str, command_callback, message: Message):
        """Enqueue a command for off-thread execution. One worker thread per topic."""
        with self._command_executors_lock:
            if topic_str not in self._command_queues:
                q = queue.Queue()
                self._command_queues[topic_str] = q
                t = threading.Thread(target=self._command_worker, args=(topic_str, q), daemon=True)
                t.start()
                self._command_executors[topic_str] = t
            q = self._command_queues[topic_str]
        q.put((command_callback, message))

    def subscribe_to_command(
        self,
        topic: Topic,
        callback: Callable,
        qos: int = 1
    ):
        """Subscribe to a command topic and answer each command with one ack.

        The callback receives a ``Message`` and returns either ``None`` (200),
        an ``int`` result code, or a ``(code, message)`` tuple. An expired
        command is acked 498 without running the callback; an exception becomes
        500. Commands for the same topic execute sequentially.
        """
        if not issubclass(topic.payload_type, Cmd):
            raise ValueError(f"Topic payload type must be Cmd or a subclass of Cmd, got {topic.payload_type}")

        command_callback = self.make_command_handler(callback, qos)
        topic_str = str(topic)

        def dispatch_callback(message: Message):
            self._enqueue_command(topic_str, command_callback, message)

        self.subscribe(topic, qos=qos, callback=dispatch_callback)
        cmd_ack_logger.info("[RECV-CMD] %s | subscribed to cmd_topic=%s", _now_ts(), topic)

    def make_command_handler(self, callback: Callable, qos: int = 1) -> Callable[[Message], None]:
        """Wrap a user callback into the handler that answers one command with one ack.

        Exposed separately from :meth:`subscribe_to_command` so the ack rules —
        expiry, result-code mapping, exceptions — can be exercised without a
        broker, and so a caller with its own dispatch can reuse them.
        """
        def command_callback(message: Message) -> None:
            cmd_payload = message.payload
            ack_topic = message.topic.to_ack_topic()

            if not isinstance(cmd_payload, Cmd):
                cmd_ack_logger.warning(
                    "[RECV-CMD] %s | payload on %s is %s, not a Cmd — ignoring",
                    _now_ts(), message.topic, type(cmd_payload).__name__
                )
                return

            if time.time() > cmd_payload.expires_at:
                cmd_ack_logger.error(
                    "[RECV-CMD] %s | EXPIRED | correlation_id=%s, expires_at=%s",
                    _now_ts(), cmd_payload.correlation_id, _ts(cmd_payload.expires_at)
                )
                self._ack(ack_topic, cmd_payload.correlation_id, 498, "command expired before execution", qos)
                return

            try:
                result = callback(message)
                if result is None:
                    code, text = 200, ""
                elif isinstance(result, tuple) and len(result) == 2:
                    code, text = result
                elif isinstance(result, int):
                    code, text = result, ""
                else:
                    code, text = 200, ""
            except Exception as exc:
                cmd_ack_logger.error(
                    "[RECV-CMD] %s | callback raised | correlation_id=%s, error=%s",
                    _now_ts(), cmd_payload.correlation_id, exc, exc_info=True
                )
                code, text = 500, f"error processing command: {exc}"

            self._ack(ack_topic, cmd_payload.correlation_id, code, text, qos)

        return command_callback

    def _ack(self, ack_topic: Topic, correlation_id: str, result_code: int, message: str, qos: int) -> None:
        ack = Ack(
            correlation_id=correlation_id,
            performed_at=datetime.datetime.now(datetime.timezone.utc).timestamp(),
            result_code=result_code,
            message=message,
        )
        cmd_ack_logger.info(
            "[RECV-CMD] %s | ACK | correlation_id=%s, result_code=%s, ack_topic=%s",
            _now_ts(), correlation_id, result_code, ack_topic
        )
        self.publish(ack_topic, ack, qos=qos)

    # ── Factory / convenience ──────────────────────────────────────────────

    @classmethod
    def autocreate_and_connect(
        cls,
        client_id: str,
        on_connect: Callable = None,
        on_disconnect: Callable = None,
        last_will: Dict[Topic, Payload] = None,
    ):
        """Build a client configured for the broker and connect it.

        Authentication is the pinning model (:mod:`franzmq.pinned_tls`): the
        identity key named by ``MACHINE_KEY`` is the credential, the client
        presents a certificate minted from it, and the broker decides by
        looking up that public key among the identities enrolled there. There
        is no CA, no username/password pair, and no plaintext mode — the client
        id and the MQTT username are both the identity, so the broker's
        identity rule and its registry agree with the topics this client
        publishes.

        | Variable | Meaning |
        |---|---|
        | ``MACHINE_KEY`` | path to the ed25519 identity key (required) |
        | ``NODE_ID`` | identity; defaults to ``client_id`` |
        | ``MQTT_IP`` / ``MQTT_PORT`` | broker address (default ``broker:1883``) |
        | ``MQTT_SESSION_EXPIRY`` | seconds the broker keeps the session (default: never expire) |
        """
        mqtt_ip = config("MQTT_IP", default="broker", cast=str_or_none)
        mqtt_port = config("MQTT_PORT", default=1883, cast=int)

        mqtt_client = Client(client_id=client_id)
        if not mqtt_client.node_id:
            mqtt_client.node_id = client_id
        identity = mqtt_client.require_node_id()
        # The broker authenticates the connection by identity; MQTT's username
        # carries it so the two never disagree.
        mqtt_client.username_pw_set(identity)
        mqtt_client.configure_mqtt_logger()
        mqtt_client.reconnect_on_failure = True
        mqtt_client.reconnect_on_offline = True
        mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)

        key_path = config("MACHINE_KEY", default=None, cast=path_or_none)
        if key_path is None:
            raise RuntimeError(
                "MACHINE_KEY is not set: the identity key is the credential. Generate one "
                "with colca-keygen and enroll its public key at the node."
            )
        if not key_path.exists():
            raise RuntimeError(f"MACHINE_KEY path '{key_path}' does not exist")
        mqtt_client.tls_set_context(self_signed_context(key_path, identity))

        if last_will is not None:
            for topic, payload in last_will.items():
                mqtt_client.will_set(str(topic), payload.encode(), qos=1, retain=True)

        if on_connect is not None:
            mqtt_client.on_connect = on_connect
        if on_disconnect is not None:
            mqtt_client.on_disconnect = on_disconnect

        # A persistent session is what makes commands issued while this client
        # was away arrive on reconnect instead of being dropped.
        props = Properties(PacketTypes.CONNECT)
        props.SessionExpiryInterval = config("MQTT_SESSION_EXPIRY", default=0xFFFFFFFF, cast=int)
        mqtt_client.connect(host=mqtt_ip, port=mqtt_port, clean_start=False, properties=props)

        logger = logging.getLogger(client_id)
        logger.setLevel(logging.INFO)
        return mqtt_client

    def publish_service_details(self, details: ServiceDetails):
        name = [details.name]
        self.publish(
            Topic(
                payload_type=ServiceDetails,
                node_id=self.require_node_id(),
                context=tuple(details.hierarchy) + tuple(name),
            ),
            details,
            retain=True,
        )

    def require_node_id(self) -> str:
        """Return the identity this client publishes under, or explain what is missing.

        Topics built by the client itself (service details, logs) need it; a
        client that never sets ``node_id`` would otherwise fail deep inside
        ``Topic`` with no hint about where the value comes from.
        """
        if not self.node_id:
            raise RuntimeError(
                "Client.node_id is not set: v1 topics carry the publisher's identity at "
                "level 4. Set the NODE_ID environment variable, assign client.node_id, or "
                "build the client with Client.autocreate_and_connect(<id>)."
            )
        return self.node_id
