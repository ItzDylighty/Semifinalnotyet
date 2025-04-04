import json
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC_VALIDATED = "validated_orders"
TOPIC_NOTIFICATIONS = "notifications"

consumer = KafkaConsumer(
    TOPIC_VALIDATED, 
    bootstrap_servers=KAFKA_BROKER
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER
)

for message in consumer:
    order = json.loads(message.value.decode("utf-8"))
    if order["status"] == "Available":
        print(f"Processing payment for {order['customer']} - {order['book']} using {order['payment_option']}")
        payment_status = "Payment Confirmed"
    else:
        print(f"Payment failed for {order['customer']} - {order['book']}")
        payment_status = "Payment Failed"

    notification = {
        "customer": order["customer"], 
        "message": payment_status
    }
    producer.send(TOPIC_NOTIFICATIONS, json.dumps(notification).encode("utf-8"))