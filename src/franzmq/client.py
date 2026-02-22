import datetime
import logging
import queue
import threading
import time
import uuid
import pathlib
from typing import Any, Callable, Dict, Optional, Tuple, Union

from decouple import config
from paho.mqtt.client import Client as PahoClient

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

        # Command/Ack state
        self._pending_commands = {}  # key: correlation_id, value: (event, ack_result, handshake_received)
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

    def publish(self, topic: Topic, payload: Payload, qos=0, retain=False):
        super().publish(str(topic), payload.encode(), qos, retain)

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
        """Decode a raw paho MQTT message into a franzmq Message object."""
        topic = Topic.from_str(raw_message.topic)
        if isinstance(raw_message.payload, Payload):
            decoded_payload = raw_message.payload
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
        threads = []
        for cb in callbacks:
            t = threading.Thread(target=cb, args=(message,))
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

    # ── Command / Acknowledge pattern ──────────────────────────────────────

    def _create_shared_ack_callback(self, ack_topic_str: str):
        def shared_ack_callback(message: Message):
            now = _now_ts()
            cmd_ack_logger.info(
                "[RECV-ACK] %s | Ack callback fired on topic=%s, payload_type=%s",
                now, message.topic, type(message.payload).__name__
            )

            ack_payload = message.payload

            if not isinstance(ack_payload, Ack):
                cmd_ack_logger.warning(
                    "[RECV-ACK] %s | Payload is NOT an Ack instance (got %s), ignoring",
                    now, type(ack_payload).__name__
                )
                return

            correlation_id = ack_payload.correlation_id
            performed_at_str = _ts(ack_payload.performed_at) if ack_payload.performed_at else "None"
            cmd_ack_logger.info(
                "[RECV-ACK] %s | correlation_id=%s, result_code=%s, performed_at=%s, message='%s'",
                now, correlation_id, ack_payload.result_code, performed_at_str, ack_payload.message
            )

            with self._pending_commands_lock:
                pending_ids = list(self._pending_commands.keys())
                if correlation_id not in self._pending_commands:
                    cmd_ack_logger.warning(
                        "[RECV-ACK] %s | correlation_id=%s NOT FOUND in pending_commands (pending: %s), ignoring",
                        now, correlation_id, pending_ids
                    )
                    return

                event, ack_result, handshake_received = self._pending_commands[correlation_id]

                if ack_payload.result_code == -1:
                    handshake_received[0] = True
                    cmd_ack_logger.info(
                        "[RECV-ACK] %s | HANDSHAKE received for correlation_id=%s", now, correlation_id
                    )
                else:
                    ack_result[0] = ack_payload
                    cmd_ack_logger.info(
                        "[RECV-ACK] %s | FINAL ACK received for correlation_id=%s, result_code=%s, setting event",
                        now, correlation_id, ack_payload.result_code
                    )
                    event.set()

        return shared_ack_callback

    def publish_command(
        self,
        topic: Topic,
        command: Any,
        validity_duration: float,
        qos: int = 1,
        max_command_duration: float = 300.0
    ) -> Ack:
        """Publish a command and wait for a two-phase acknowledgement.

        1. Subscribe to the ack topic
        2. Publish the command
        3. Wait for handshake (result_code=-1)
        4. Wait for final ack (result_code>=0)
        5. Return Ack or raise TimeoutError
        """
        if not issubclass(topic.payload_type, Cmd):
            raise ValueError(f"Topic payload type must be Cmd or a subclass of Cmd, got {topic.payload_type}")

        correlation_id = str(uuid.uuid4())
        created_at = datetime.datetime.now(datetime.timezone.utc).timestamp()
        expires_at = created_at + validity_duration

        cmd_ack_logger.info(
            "[SEND-CMD] %s | BEGIN publish_command | correlation_id=%s, cmd_topic=%s, payload_type=%s",
            _now_ts(), correlation_id, topic, topic.payload_type.__name__
        )
        cmd_ack_logger.info(
            "[SEND-CMD] %s | Timing | created_at=%s, expires_at=%s, validity=%.3fs, max_cmd_duration=%.3fs",
            _now_ts(), _ts(created_at), _ts(expires_at), validity_duration, max_command_duration
        )

        command_dict = {
            "created_at": created_at,
            "correlation_id": correlation_id,
            "expires_at": expires_at,
            "command": command,
        }
        command = topic.payload_type(**command_dict)

        ack_topic = topic.to_ack_topic()
        ack_topic_str = str(ack_topic)

        cmd_ack_logger.info("[SEND-CMD] %s | Ack topic resolved: %s", _now_ts(), ack_topic_str)

        event = threading.Event()
        ack_result = [None]
        handshake_received = [False]

        with self._pending_commands_lock:
            self._pending_commands[correlation_id] = (event, ack_result, handshake_received)
            cmd_ack_logger.info(
                "[SEND-CMD] %s | Registered pending command | correlation_id=%s, total_pending=%d",
                _now_ts(), correlation_id, len(self._pending_commands)
            )

        needs_subscription = False
        with self._ack_topic_subscriptions_lock:
            if ack_topic_str not in self._ack_topic_subscriptions:
                self._ack_topic_subscriptions[ack_topic_str] = set()
                needs_subscription = True
            self._ack_topic_subscriptions[ack_topic_str].add(correlation_id)

        subscription_confirmed = threading.Event()
        original_on_subscribe = self.on_subscribe

        def on_subscribe_wrapper(client, userdata, mid, granted_qos, properties=None):
            cmd_ack_logger.info(
                "[SEND-CMD] %s | SUBACK received | mid=%s, granted_qos=%s, ack_topic=%s",
                _now_ts(), mid, granted_qos, ack_topic_str
            )
            subscription_confirmed.set()
            if original_on_subscribe:
                original_on_subscribe(client, userdata, mid, granted_qos, properties)

        if needs_subscription:
            self.on_subscribe = on_subscribe_wrapper
            shared_callback = self._create_shared_ack_callback(ack_topic_str)
            cmd_ack_logger.info(
                "[SEND-CMD] %s | Subscribing to ack_topic=%s (new subscription)", _now_ts(), ack_topic_str
            )
            result = self.subscribe(ack_topic, qos=qos, callback=shared_callback)
            cmd_ack_logger.info(
                "[SEND-CMD] %s | subscribe() returned=%s, waiting for SUBACK...", _now_ts(), result
            )

            if not subscription_confirmed.wait(timeout=1.0):
                cmd_ack_logger.warning(
                    "[SEND-CMD] %s | SUBACK TIMEOUT (1s) for ack_topic=%s, proceeding anyway",
                    _now_ts(), ack_topic_str
                )
            else:
                cmd_ack_logger.info(
                    "[SEND-CMD] %s | SUBACK confirmed for ack_topic=%s", _now_ts(), ack_topic_str
                )
            self.on_subscribe = original_on_subscribe
        else:
            cmd_ack_logger.info(
                "[SEND-CMD] %s | Already subscribed to ack_topic=%s, reusing", _now_ts(), ack_topic_str
            )

        time.sleep(0.05)
        cmd_ack_logger.info("[SEND-CMD] %s | Post-subscribe settle (50ms) done", _now_ts())

        try:
            self.publish(topic, command, qos=qos)
            cmd_ack_logger.info(
                "[SEND-CMD] %s | Command PUBLISHED to %s (qos=%d) | correlation_id=%s",
                _now_ts(), topic, qos, correlation_id
            )

            initial_timeout = expires_at - time.time()
            if initial_timeout <= 0:
                cmd_ack_logger.error(
                    "[SEND-CMD] %s | EXPIRED before wait loop | expires_at=%s, now=%s, diff=%.6fs",
                    _now_ts(), _ts(expires_at), _now_ts(), initial_timeout
                )
                raise TimeoutError(
                    f"Command expired before waiting (expires_at={expires_at}, now={time.time()})"
                )

            cmd_ack_logger.info(
                "[SEND-CMD] %s | Entering wait loop | remaining=%.3fs until expiry at %s",
                _now_ts(), initial_timeout, _ts(expires_at)
            )

            while True:
                current_time = time.time()
                remaining_initial = expires_at - current_time

                if remaining_initial <= 0:
                    current_time = time.time()
                    if handshake_received[0]:
                        elapsed_time = current_time - created_at
                        remaining_time = max_command_duration - elapsed_time
                        cmd_ack_logger.info(
                            "[SEND-CMD] %s | Validity expired but HANDSHAKE was received | elapsed=%.3fs, extended_remaining=%.3fs",
                            _now_ts(), elapsed_time, remaining_time
                        )
                        if remaining_time > 0:
                            if not event.wait(timeout=remaining_time):
                                cmd_ack_logger.error(
                                    "[SEND-CMD] %s | EXTENDED TIMEOUT | No final ACK within max_command_duration=%.3fs | correlation_id=%s",
                                    _now_ts(), max_command_duration, correlation_id
                                )
                                return Ack(
                                    correlation_id=correlation_id,
                                    performed_at=None,
                                    result_code=500,
                                    message="No response received within max_command_duration"
                                )
                        else:
                            cmd_ack_logger.error(
                                "[SEND-CMD] %s | EXTENDED TIMEOUT (no remaining) | correlation_id=%s",
                                _now_ts(), correlation_id
                            )
                            return Ack(
                                correlation_id=correlation_id,
                                performed_at=None,
                                result_code=500,
                                message="No response received within max_command_duration"
                            )
                    else:
                        cmd_ack_logger.error(
                            "[SEND-CMD] %s | TIMEOUT - NO HANDSHAKE | correlation_id=%s, expires_at=%s, now=%s",
                            _now_ts(), correlation_id, _ts(expires_at), _ts(current_time)
                        )
                        raise TimeoutError(
                            f"Command expired while waiting for ACK at {ack_topic} "
                            f"with correlation_id={correlation_id} (expires_at={expires_at}, now={current_time})"
                        )

                wait_timeout = min(remaining_initial, 0.1)
                if event.wait(timeout=wait_timeout):
                    cmd_ack_logger.info(
                        "[SEND-CMD] %s | Event SET (final ACK) | correlation_id=%s", _now_ts(), correlation_id
                    )
                    break

                if handshake_received[0]:
                    elapsed_time = time.time() - created_at
                    remaining_time = max_command_duration - elapsed_time
                    cmd_ack_logger.info(
                        "[SEND-CMD] %s | Handshake detected in poll loop | elapsed=%.3fs, extended_remaining=%.3fs | correlation_id=%s",
                        _now_ts(), elapsed_time, remaining_time, correlation_id
                    )
                    if remaining_time > 0:
                        if not event.wait(timeout=remaining_time):
                            cmd_ack_logger.error(
                                "[SEND-CMD] %s | EXTENDED TIMEOUT after handshake | correlation_id=%s",
                                _now_ts(), correlation_id
                            )
                            return Ack(
                                correlation_id=correlation_id,
                                performed_at=None,
                                result_code=500,
                                message="No response received within max_command_duration"
                            )
                    else:
                        cmd_ack_logger.error(
                            "[SEND-CMD] %s | EXTENDED TIMEOUT (no remaining) after handshake | correlation_id=%s",
                            _now_ts(), correlation_id
                        )
                        return Ack(
                            correlation_id=correlation_id,
                            performed_at=None,
                            result_code=500,
                            message="No response received within max_command_duration"
                        )
                    break

            if ack_result[0] is None:
                cmd_ack_logger.error(
                    "[SEND-CMD] %s | BUG: event was set but ack_result is None | correlation_id=%s",
                    _now_ts(), correlation_id
                )
                raise TimeoutError(f"No ACK received for correlation_id={correlation_id}")

            cmd_ack_logger.info(
                "[SEND-CMD] %s | COMPLETE | correlation_id=%s, result_code=%s, message='%s'",
                _now_ts(), correlation_id, ack_result[0].result_code, ack_result[0].message
            )
            return ack_result[0]
        finally:
            with self._pending_commands_lock:
                if correlation_id in self._pending_commands:
                    del self._pending_commands[correlation_id]
                remaining_pending = len(self._pending_commands)

            should_unsub = False
            with self._ack_topic_subscriptions_lock:
                if ack_topic_str in self._ack_topic_subscriptions:
                    self._ack_topic_subscriptions[ack_topic_str].discard(correlation_id)
                    if not self._ack_topic_subscriptions[ack_topic_str]:
                        del self._ack_topic_subscriptions[ack_topic_str]
                        should_unsub = True

            if should_unsub:
                self.unsubscribe(ack_topic)
                cmd_ack_logger.info(
                    "[SEND-CMD] %s | UNSUBSCRIBED from ack_topic=%s (no more pending) | correlation_id=%s",
                    _now_ts(), ack_topic_str, correlation_id
                )
            else:
                cmd_ack_logger.info(
                    "[SEND-CMD] %s | Cleanup done | correlation_id=%s, remaining_pending=%d",
                    _now_ts(), correlation_id, remaining_pending
                )

    def _command_worker(self, topic_str: str, q: queue.Queue):
        """Drain a per-topic command queue sequentially. Runs as a daemon thread."""
        while True:
            item = q.get()
            if item is None:
                break
            callback, message = item
            correlation_id = getattr(getattr(message, 'payload', None), 'correlation_id', '?')
            cmd_ack_logger.info(
                "[CMD-QUEUE] %s | START executing command | topic=%s, correlation_id=%s",
                _now_ts(), topic_str, correlation_id
            )
            try:
                callback(message)
            except Exception as e:
                cmd_ack_logger.error(
                    "[CMD-QUEUE] %s | Exception in command worker | topic=%s, correlation_id=%s, error=%s",
                    _now_ts(), topic_str, correlation_id, e, exc_info=True
                )
            finally:
                cmd_ack_logger.info(
                    "[CMD-QUEUE] %s | DONE executing command | topic=%s, correlation_id=%s, queue_depth=%d",
                    _now_ts(), topic_str, correlation_id, q.qsize()
                )
                q.task_done()

    def _enqueue_command(self, topic_str: str, command_callback, message: Message):
        """Enqueue a command for off-thread execution. Creates a worker thread per topic on first use."""
        with self._command_executors_lock:
            if topic_str not in self._command_queues:
                q = queue.Queue()
                self._command_queues[topic_str] = q
                t = threading.Thread(target=self._command_worker, args=(topic_str, q), daemon=True)
                t.start()
                self._command_executors[topic_str] = t
                cmd_ack_logger.info(
                    "[CMD-QUEUE] %s | Created command worker thread for topic=%s", _now_ts(), topic_str
                )
            q = self._command_queues[topic_str]
        q.put((command_callback, message))
        correlation_id = getattr(getattr(message, 'payload', None), 'correlation_id', '?')
        cmd_ack_logger.info(
            "[CMD-QUEUE] %s | Enqueued command | topic=%s, correlation_id=%s, queue_depth=%d",
            _now_ts(), topic_str, correlation_id, q.qsize()
        )

    def subscribe_to_command(
        self,
        topic: Topic,
        callback: Callable,
        qos: int = 0
    ):
        """Subscribe to a command topic and handle incoming commands with two-phase ack.

        The callback receives a ``Message`` and should return:
        - ``None`` for success (200)
        - An ``int`` result code
        - A ``(int, str)`` tuple of (result_code, message)

        Commands are executed sequentially per topic via an internal queue.
        """
        if not issubclass(topic.payload_type, Cmd):
            raise ValueError(f"Topic payload type must be Cmd or a subclass of Cmd, got {topic.payload_type}")

        cmd_ack_logger.info(
            "[RECV-CMD] %s | subscribe_to_command called | cmd_topic=%s, payload_type=%s, qos=%d",
            _now_ts(), topic, topic.payload_type.__name__, qos
        )

        def command_callback(message: Message):
            recv_time = time.time()
            cmd_payload = message.payload
            ack_topic = message.topic.to_ack_topic()

            if not isinstance(cmd_payload, Cmd):
                cmd_ack_logger.warning(
                    "[RECV-CMD] %s | Received non-Cmd payload (got %s) on topic=%s, ignoring",
                    _now_ts(), type(cmd_payload).__name__, message.topic
                )
                return

            cmd_ack_logger.info(
                "[RECV-CMD] %s | COMMAND RECEIVED | correlation_id=%s, cmd_topic=%s, ack_topic=%s",
                _now_ts(), cmd_payload.correlation_id, message.topic, ack_topic
            )
            cmd_ack_logger.info(
                "[RECV-CMD] %s | Timing | created_at=%s, expires_at=%s, received_at=%s, time_to_receive=%.3fs",
                _now_ts(), _ts(cmd_payload.created_at), _ts(cmd_payload.expires_at),
                _ts(recv_time), recv_time - cmd_payload.created_at
            )

            current_time = time.time()
            if current_time > cmd_payload.expires_at:
                cmd_ack_logger.error(
                    "[RECV-CMD] %s | EXPIRED on receiver | correlation_id=%s, expires_at=%s, now=%s, overdue=%.3fs",
                    _now_ts(), cmd_payload.correlation_id,
                    _ts(cmd_payload.expires_at), _ts(current_time),
                    current_time - cmd_payload.expires_at
                )
                ack = Ack(
                    correlation_id=cmd_payload.correlation_id,
                    performed_at=None,
                    result_code=500,
                    message="Command expired before processing"
                )
                self.publish(ack_topic, ack, qos=qos)
                cmd_ack_logger.info(
                    "[RECV-CMD] %s | Sent EXPIRED ACK (500) | correlation_id=%s, ack_topic=%s",
                    _now_ts(), cmd_payload.correlation_id, ack_topic
                )
            else:
                remaining = cmd_payload.expires_at - current_time
                cmd_ack_logger.info(
                    "[RECV-CMD] %s | Command still valid (%.3fs remaining) | correlation_id=%s",
                    _now_ts(), remaining, cmd_payload.correlation_id
                )

                handshake_ack = Ack(
                    correlation_id=cmd_payload.correlation_id,
                    performed_at=None,
                    result_code=-1,
                    message=""
                )
                cmd_ack_logger.info(
                    "[RECV-CMD] %s | PUBLISHING HANDSHAKE (-1) | correlation_id=%s, ack_topic=%s",
                    _now_ts(), cmd_payload.correlation_id, ack_topic
                )
                self.publish(ack_topic, handshake_ack, qos=qos)
                cmd_ack_logger.info(
                    "[RECV-CMD] %s | Handshake published, invoking user callback | correlation_id=%s",
                    _now_ts(), cmd_payload.correlation_id
                )

                try:
                    callback_start = time.time()
                    result = callback(message)
                    callback_end = time.time()

                    cmd_ack_logger.info(
                        "[RECV-CMD] %s | User callback returned | correlation_id=%s, result=%s, duration=%.3fs",
                        _now_ts(), cmd_payload.correlation_id, result, callback_end - callback_start
                    )

                    if result is None:
                        result_code = 200
                        message_text = ""
                    elif isinstance(result, tuple) and len(result) == 2:
                        result_code, message_text = result
                    elif isinstance(result, int):
                        result_code = result
                        message_text = ""
                    else:
                        result_code = 200
                        message_text = ""

                    performed_at = datetime.datetime.now(datetime.timezone.utc).timestamp()
                    ack = Ack(
                        correlation_id=cmd_payload.correlation_id,
                        performed_at=performed_at,
                        result_code=result_code,
                        message=message_text
                    )
                except Exception as exc:
                    cmd_ack_logger.error(
                        "[RECV-CMD] %s | User callback EXCEPTION | correlation_id=%s, error=%s",
                        _now_ts(), cmd_payload.correlation_id, exc, exc_info=True
                    )
                    ack = Ack(
                        correlation_id=cmd_payload.correlation_id,
                        performed_at=None,
                        result_code=598,
                        message=f"Error processing command: {exc}"
                    )

                cmd_ack_logger.info(
                    "[RECV-CMD] %s | PUBLISHING FINAL ACK | correlation_id=%s, result_code=%s, message='%s', ack_topic=%s",
                    _now_ts(), cmd_payload.correlation_id, ack.result_code, ack.message, ack_topic
                )
                self.publish(ack_topic, ack, qos=qos)
                cmd_ack_logger.info(
                    "[RECV-CMD] %s | Final ACK published | correlation_id=%s",
                    _now_ts(), cmd_payload.correlation_id
                )

        topic_str = str(topic)

        def dispatch_callback(message: Message):
            self._enqueue_command(topic_str, command_callback, message)

        self.subscribe(topic, qos=qos, callback=dispatch_callback)
        cmd_ack_logger.info(
            "[RECV-CMD] %s | Subscribed to cmd_topic=%s (non-blocking dispatch)", _now_ts(), topic
        )

    # ── Factory / convenience ──────────────────────────────────────────────

    @classmethod
    def autocreate_and_connect(
        cls,
        client_id: str,
        on_connect: Callable = None,
        on_disconnect: Callable = None,
        last_will: Dict[Topic, Payload] = None,
    ):
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

        ca_cert = config("CA_CERT_FILE", default=None, cast=path_or_none)
        tls_cert = config("TLS_CERT_FILE", default=None, cast=path_or_none)
        tls_key = config("TLS_KEY_FILE", default=None, cast=path_or_none)
        use_mqtts = config("USE_MQTTS", default=True, cast=bool)

        if use_mqtts and ca_cert is not None:
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
            default_mqtt_port = 8883
        else:
            default_mqtt_port = 1883

        mqtt_port = config("MQTT_PORT", default=default_mqtt_port, cast=int)

        if last_will is not None:
            for topic, payload in last_will.items():
                mqtt_client.will_set(str(topic), payload.encode(), qos=1, retain=True)

        mqtt_client.connect(host=mqtt_ip, port=mqtt_port)

        if on_connect is not None:
            mqtt_client.on_connect = on_connect
        if on_disconnect is not None:
            mqtt_client.on_disconnect = on_disconnect

        logger = logging.getLogger(client_id)
        logger.setLevel(logging.INFO)
        return mqtt_client

    def publish_service_details(self, details: ServiceDetails):
        name = [details.name]
        self.publish(Topic(payload_type=ServiceDetails, context=details.hierarchy + name), details, retain=True)
