import json
from fastapi import FastAPI
from kafka import KafkaProducer

app = FastAPI()

TOPIC_ORDERS = "book_orders"
KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER
)

@app.post("/place_order/")
def place_order(book_id: int, quantity: int, customer: str):

    order = {"book_id": book_id, 
             "quantity": quantity, 
             "customer": customer
             }
    
    producer.send(TOPIC_ORDERS, json.dumps(order).encode("utf-8"))
    return {"message": "Order placed successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5000)