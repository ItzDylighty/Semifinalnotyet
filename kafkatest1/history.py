import json
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC_VALIDATED = "validated_orders"

consumer = KafkaConsumer(
    TOPIC_VALIDATED, 
    bootstrap_servers=KAFKA_BROKER
)

customer_orders = {}

for message in consumer:
    order = json.loads(message.value.decode("utf-8"))
    customer_orders.setdefault(order["customer"], []).append(order)

    print(f"Order history for {order['customer']}:")
    for past_order in customer_orders[order["customer"]]:
        print(f"{past_order['book']} - {past_order['status']} - {past_order['payment_option']}")