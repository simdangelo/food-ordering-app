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
        print("Ongoing transaction...")
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)

        user = consumed_message["user"]
        email = consumed_message["email"]
        food_cost = consumed_message["cost"]

        data = {
            "customer_name": user,
            "customer_email": email,
            "food_cost": food_cost
        }

        print("Successful transaction...\n")
        producer.send(
            ORDER_CONFIRMED_KAFKA_TOPIC,
            json.dumps(data).encode("utf-8")
        )