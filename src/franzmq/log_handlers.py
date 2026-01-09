import logging
import datetime
from franzmq.data_contracts.base import Log
from franzmq.client import Client
from franzmq.topic import Topic


class MQTTHandler(logging.Handler):

    def __init__(self, mqtt_client: "Client", topic_prefix: str = "logs"):
        super().__init__()
        self.mqtt_client = mqtt_client
        self.topic_prefix = topic_prefix

    def emit(self, record: logging.LogRecord):
        try:
            payload = Log(
                timestamp=datetime.datetime.utcnow().isoformat(),
                level=record.levelname,
                message=self.format(record),
                logger_name=record.name,
                module=record.module,
                function=record.funcName,
                line_no=record.lineno,
                exc_info=self.formatter.formatException(record.exc_info) if record.exc_info and self.formatter else None,
                extra=getattr(record, 'extra', None)
            )

            topic = Topic(
                payload_type=Log,
                context=(record.name, record.levelname)
            )

            self.mqtt_client.publish(topic, payload)

        except Exception:
            self.handleError(record)

def configure_logging(
    mqtt_client: Client,
    level: int = logging.INFO,
    topic_prefix: str = "logs",
    format_string: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
) -> None:
    """
    Configures logging with MQTT handler.

    Args:
        mqtt_client: Connected MQTT client instance
        logger_name: Name of the logger
        level: Logging level
        topic_prefix: MQTT topic prefix for logs
        format_string: Log format string
    """
    logger = logging.getLogger()
    logger.setLevel(level)
    logger.handlers.clear()

    formatter = logging.Formatter(format_string)

    # Stream handler (stdout)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # MQTT handler
    mqtt_handler = MQTTHandler(mqtt_client, topic_prefix)
    mqtt_handler.setFormatter(formatter)
    logger.addHandler(mqtt_handler)
