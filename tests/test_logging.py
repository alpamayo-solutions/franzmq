
import logging

from franzmq.client import Client

mqtt_client = Client()
mqtt_client.connect(host="localhost", port=1883)
mqtt_client.configure_mqtt_logger()

logger = logging.getLogger('logging_test/new')
logger.info("MQTT Logging Initialized")

logger.error("This is an error message")
logger.warning("This is a warning message")
logger.info("This is an info message")
logger.debug("This is a debug message")