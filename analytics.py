import json

from kafka import KafkaConsumer
from kafka_topic import *



consumer = KafkaConsumer(
    bootstrap_servers="localhost:29092"
)
consumer.subscribe([ORDER_CONFIRMED_KAFKA_TOPIC, ORDER_COMPLETED_KAFKA_TOPIC])


active_orders = 0
total_orders_count = 0
total_revenue = 0

print("Analytics listening...\n")

for message in consumer:
    print("Updating analytics...")

    if message.topic == ORDER_CONFIRMED_KAFKA_TOPIC:
        consumed_message = json.loads(message.value.decode())
        total_cost = consumed_message["cost"]

        total_orders_count = total_orders_count + 1
        total_revenue = total_revenue + total_cost
        active_orders = active_orders + 1

        print(f"Orders so far today: {total_orders_count}")
        print(f"Revenue so far today: {total_revenue}")
        print(f"Orders still active: {active_orders}!!")
        print("\n")

    elif message.topic == ORDER_COMPLETED_KAFKA_TOPIC:
        active_orders = active_orders - 1
        print(f"L'ordine {message.key} è completo")
        print(f"Orders still active: {active_orders}!!")
        print("\n")

