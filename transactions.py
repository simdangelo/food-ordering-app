import json

from kafka import KafkaConsumer
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers="localhost:29092"
)

producer = KafkaProducer(
    bootstrap_servers="localhost:29092"
)

print("Transactions listening...\n")
while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())

        user = consumed_message["user"]
        email = consumed_message["email"]
        food_cost = consumed_message["cost"]

        data = {
            "customer_name": user,
            "customer_email": email,
            "food_cost": food_cost
        }

        print(f"Successful transaction: {consumed_message}\n")
        producer.send(
            ORDER_CONFIRMED_KAFKA_TOPIC,
            json.dumps(data).encode("utf-8")
        )