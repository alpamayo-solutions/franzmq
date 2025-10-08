import logging
from franzmq import Client
from franzmq.log_handlers import configure_logging

# EXAMPLE: Integrating Python's built in logging framework with MQTT

# Step 1: create and connect client
client = Client.autocreate_and_connect(client_id="logger-service")

# Step 2: configure logging with MQTT integration
configure_logging(
    mqtt_client=client,
    level=logging.DEBUG,  # capture DEBUG and above
    topic_prefix="logs",  # optional; included in Topic context
    format_string="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Step 3: get a logger and produce some logs
logger = logging.getLogger("myapp.component")

logger.debug("Debug message (not usually critical)")
logger.info("Hello world! This goes to console AND MQTT")
logger.warning("Something might be wrong...")
logger.error("An error occurred!")
