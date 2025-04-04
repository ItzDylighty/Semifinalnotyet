import json
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC_ORDERS = "book_orders"
TOPIC_VALIDATED = "validated_orders"

consumer = KafkaConsumer(
    TOPIC_ORDERS, 
    bootstrap_servers=KAFKA_BROKER,
    group_id="inventory-group",
    auto_offset_reset="earliest"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER
)

inventory = {1: {"name": "Harry Potter", "quantity": 5}, 
             2: {"name": "Pride", "quantity": 3},
             3: {"name": "Monster", "quantity": 3}}

for message in consumer:
    order = json.loads(message.value.decode("utf-8"))
    book = inventory.get(order["book_id"])

    if book and book["quantity"] >= order["quantity"]:
        book["quantity"] -= order["quantity"]
        status = "Available"
    else:
        status = "Out of Stock"

    response = {"book_id": order["book_id"], "book": book["name"] if book else "Unknown", "status": status, "customer": order["customer"]}
    producer.send(TOPIC_VALIDATED, json.dumps(response).encode("utf-8"))
    print(f"Inventory processed order: {response}")