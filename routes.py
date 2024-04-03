import json
from flask import Flask, render_template, request, redirect, url_for
from kafka import KafkaProducer
import uuid
from datetime import datetime
from common.kafka_topic import *
from flask import jsonify
import psycopg2


app = Flask(__name__)

ORDER_LIMIT = 1_000_000

producer = KafkaProducer(bootstrap_servers="localhost:29092")

PRICE_DICT = {
    ("pizza", "big"): 15,
    ("pizza", "small"): 10,
    ("sandwich", "big"): 12,
    ("sandwich", "small"): 8,
}


def postgres_connection():
    conn = psycopg2.connect(
        host='localhost',
        database='database',
        user='user',
        password='password',
        port=5432
    )
    cursor = conn.cursor()
    return conn, cursor


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
                "username": user,
                "email": email,
                "food": food,
                "size": size,
                "cost": cost,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "order_completed": 0
            }

            # Send order to Kafka
            producer.send(
                ORDER_CONFIRMED_KAFKA_TOPIC,
                json.dumps(order).encode("utf-8")
            )

            return redirect(url_for('order_confirmation'))  # Redirect to order confirmation page
        else:
            return "Invalid order details!"
    return render_template("place_order.html")


@app.route("/order_confirmation")
def order_confirmation():
    return render_template("order_confirmation.html")

@app.route("/orders_db")
def display_orders():
    conn, cursor = postgres_connection()
    cursor.execute("SELECT * FROM spark_streams.orders ORDER BY time DESC LIMIT 20;")

    columns = [desc[0] for desc in cursor.description]
    orders = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return render_template('orders_display.html', orders=orders)

@app.route('/completato', methods=['POST'])
def ordine_completato():
    order_id = request.form['orderId']
    email_id = request.form['emailId']
    producer.send(ORDER_COMPLETED_KAFKA_TOPIC, json.dumps({'order_id': order_id, 'email_id': email_id}).encode('utf-8'))
    return 'OK'

@app.route('/update_order', methods=['POST'])
def update_order():
    order_id = str(request.form['orderId'])

    conn, cursor = postgres_connection()
    cursor.execute(
        """
        UPDATE spark_streams.orders SET order_completed = 1 WHERE id = %s;
        """,
        (order_id,)
    )
    cursor.close()
    conn.commit()
    conn.close()

    return jsonify(status='success')


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