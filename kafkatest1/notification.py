import json
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC_NOTIFICATIONS = "notifications"

consumer = KafkaConsumer(
    TOPIC_NOTIFICATIONS, 
    bootstrap_servers=KAFKA_BROKER
)

for message in consumer:
    notification = json.loads(message.value.decode("utf-8"))
    print(f"Notification: {notification['message']} for {notification['customer']}")