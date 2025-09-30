from franzmq.topic import Topic, Isa95Topic, Isa95Fields
from franzmq.payload import Metric

# EXAMPLE: Model plant hierarchy in the topic path, then convert to plain topics for internal consumers

basic = Topic(payload_type=Metric, context=("sensor", "pressure"))

isa95 = Isa95Topic.from_topic(
    basic,
    Isa95Fields(
        enterprise="acme", site="PLT-01", area="AREA-A",
        production_line="LINE-3", work_cell="CELL-2", origin_id="edge-17"
    )
)
print("ISA-95:", str(isa95))
# example/v1-isa95/acme/PLT-01/AREA-A/LINE-3/CELL-2/edge-17/_Metric/sensor/pressure

# Parse back into a basic Topic (internal routing doesn’t need the ISA-95 prefix):
parsed = Topic.from_str(str(isa95))
print("Basic  :", str(parsed))  # example/v1/_Metric/sensor/pressure
