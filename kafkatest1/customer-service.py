import json
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC_VALIDATED = "validated_orders"

consumer = KafkaConsumer(
    TOPIC_VALIDATED, 
    bootstrap_servers=KAFKA_BROKER,
    group_id="history-group",
    auto_offset_reset="earliest"
)

customer_orders = {}

for message in consumer:
    order = json.loads(message.value.decode("utf-8"))
    customer_orders.setdefault(order["customer"], []).append(order)
    print(f" Updated order history for {order['customer']}")