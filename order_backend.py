import json
from flask import Flask, render_template, request, redirect, url_for
from kafka import KafkaProducer
import uuid
import datetime


app = Flask(__name__)

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 1_000_000

producer = KafkaProducer(bootstrap_servers="localhost:29092")

PRICE_DICT = {
    ("pizza", "big"): 15,
    ("pizza", "small"): 10,
    ("sandwich", "big"): 12,
    ("sandwich", "small"): 8,
}


@app.route("/place_order", methods=["GET", "POST"])
def place_order():
    if request.method == "POST":
        user = request.form.get("name")
        email = request.form.get("email")
        food = request.form.get("food")
        size = request.form.get("size")

        # Retrieve the cost from the price dictionary
        cost = PRICE_DICT.get((food.lower(), size.lower()), 0)

        if user and email and food and size:
            order = {
                "id": str(uuid.uuid4()),
                "user": user,
                "email": email,
                "food": food,
                "size": size,
                "cost": cost,
                "time": datetime.datetime.now().isoformat()
            }

            # Send order to Kafka
            producer.send(
                ORDER_KAFKA_TOPIC,
                json.dumps(order).encode("utf-8")
            )

            return redirect(url_for('order_confirmation'))  # Redirect to order confirmation page
        else:
            return "Invalid order details!"
    return render_template("place_order.html")


@app.route("/order_confirmation")
def order_confirmation():
    return render_template("order_confirmation.html")


from cassandra.cluster import Cluster
cluster = Cluster(['localhost'])
session = cluster.connect()

@app.route("/orders_db")
def display_orders():
    orders = session.execute('SELECT * FROM spark_streams.orders')  # Assuming your table name is 'orders'
    return render_template('orders_display.html', orders=orders)

if __name__ == "__main__":
    app.run(debug=True)


# import time
#
# for i in range(1_000_000):
#     order = {
#         "id": str(uuid.uuid4()),
#         "user": f"{i}_user",
#         "email": f"{i}_email",
#         "food": f"{i}_food",
#         "size": f"{i}_size",
#         "cost": 4,
#         "time": datetime.datetime.now().isoformat()
#     }
#
#     producer.send(ORDER_KAFKA_TOPIC, json.dumps(order).encode("utf-8"))
#     print(f"Done Sending..{i}")
#     time.sleep(0)