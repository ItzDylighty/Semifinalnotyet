import json
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC_VALIDATED = "validated_orders"
TOPIC_NOTIFICATIONS = "notifications"

consumer = KafkaConsumer(
    TOPIC_VALIDATED, 
    bootstrap_servers=KAFKA_BROKER,
    group_id="notification-group",
    auto_offset_reset="earliest"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER
)

for message in consumer:
    order = json.loads(message.value.decode("utf-8"))
    notification = (f"Order for {order['book']} is {order['status']}.")
    producer.send(TOPIC_NOTIFICATIONS, json.dumps({"customer": order["customer"], "message": notification}).encode("utf-8"))
    print(f"Sent notification: {notification}")
