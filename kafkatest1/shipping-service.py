import json
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC_VALIDATED = "validated_orders"

consumer = KafkaConsumer(
    TOPIC_VALIDATED, 
    bootstrap_servers=KAFKA_BROKER,
    group_id="shipping-group",
    auto_offset_reset="earliest"
)

for message in consumer:
    order = json.loads(message.value.decode("utf-8"))
    if order["status"] == "Available":
        print(f"Shipping order for {order['customer']} - {order['book']}")