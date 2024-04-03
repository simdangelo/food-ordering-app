import streamlit as st  # web development
import numpy as np  # np mean, np random
import pandas as pd  # read csv, df manipulation
import time  # to simulate a real time data, time loop
import plotly.express as px  # interactive charts
import psycopg2

st.set_page_config(
    page_title='Real-Time Food-Order Dashboard',
    page_icon='✅',
    layout='wide'
)

# dashboard title

st.title("Real-Time Food Ordering Dashboard")

def create_postgres_connection():
    conn = psycopg2.connect(
        host='localhost',
        database='database',
        user='user',
        password='password',
        port=5432
    )
    return conn


def calculate_daily_kpis():
    postgres_connection = create_postgres_connection()
    cursor = postgres_connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM spark_streams.orders WHERE DATE(time) = CURRENT_DATE;")
    total_orders = cursor.fetchone()[0]
    cursor.execute("SELECT count(*) FROM spark_streams.orders WHERE DATE(time) = CURRENT_DATE AND order_completed = 0;")
    active_orders = cursor.fetchone()[0]
    cursor.execute("SELECT SUM(cost) FROM spark_streams.orders WHERE DATE(time) = CURRENT_DATE;")
    revenues = cursor.fetchone()[0]
    cursor.execute("""
    SELECT COUNT(DISTINCT email)
        FROM (
            SELECT email, MIN(DATE(time)) AS first_order_date
            FROM spark_streams.orders
            GROUP BY email
        ) AS first_orders
        WHERE first_order_date = CURRENT_DATE;
    """)
    new_customers = cursor.fetchone()[0]


    postgres_connection.commit()
    cursor.close()
    postgres_connection.close()

    return total_orders, active_orders, revenues, new_customers


add_selectbox = st.sidebar.radio(
    "How would you like to be contacted?",
    ("Today Analytics", "Food Details", "Historical Trends")
)

placeholder = st.empty()

while True:
    if add_selectbox == "Today Analytics":
        with placeholder.container():
            total_orders, active_orders, revenues, new_customers = calculate_daily_kpis()

            st.write("## Today's Analytics")
            kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
            kpi1.metric(label="Orders today", value=total_orders)
            kpi2.metric(label="Active orders", value=active_orders)
            kpi3.metric(label="Completed orders", value=total_orders-active_orders)
            kpi4.metric(label="Total revenue", value=f"$ {revenues} ")
            kpi5.metric(label="New customers today", value=f"{new_customers}")

            # st.dataframe(pandas_df_completed(), use_container_width=True)
            # st.dataframe(pandas_df_active(), use_container_width=True)

            fig_col1, fig_col2 = st.columns(2)
            with fig_col1:
                st.markdown("### Food order Details")

                postgres_connection = create_postgres_connection()
                cursor = postgres_connection.cursor()

                cursor.execute("""
                    SELECT food, COUNT(*) AS num_orders
                    FROM spark_streams.orders
                    GROUP BY food;
                """)
                results = cursor.fetchall()

                # Convert the results to a format suitable for Plotly Express
                data = {'food': [], 'num_orders': []}
                for food, num_orders in results:
                    data['food'].append(food)
                    data['num_orders'].append(num_orders)

                # Create the pie chart using Plotly Express
                fig = px.pie(data, names='food', values='num_orders')
                fig.update_traces(textinfo='percent+label', textfont_size=14, hole=.3,
                                  textposition='auto', showlegend=False)  # Add labels with percentages
                # fig.update_layout(showlegend=False)  # Remove the legend
                st.write(fig)

            with fig_col2:
                st.markdown("### Orders by hour")

                cursor.execute("""
                                SELECT 
                                    time_series AS time_window,
                                    COUNT(orders.time) AS num_orders
                                FROM 
                                    generate_series(
                                        DATE_TRUNC('day', CURRENT_TIMESTAMP), 
                                        DATE_TRUNC('day', CURRENT_TIMESTAMP) + INTERVAL '1 day' - INTERVAL '1 minute', 
                                        INTERVAL '15 minutes'
                                    ) AS time_series
                                LEFT JOIN 
                                    spark_streams.orders 
                                ON 
                                    spark_streams.orders.time >= time_series AND spark_streams.orders.time < time_series + INTERVAL '15 minutes'
                                GROUP BY 
                                    time_window
                                ORDER BY 
                                    time_window;
                                """)
                results = cursor.fetchall()

                cursor.close()
                postgres_connection.close()

                data = {'hour_of_day': [], 'num_orders': []}
                for hour_of_day, num_orders in results:
                    data['hour_of_day'].append(hour_of_day)
                    data['num_orders'].append(num_orders)

                # Create the bar chart using Plotly Express
                fig2 = px.bar(data, x='hour_of_day', y='num_orders')
                fig2.update_layout(xaxis_tickformat='%H:%M')  # Format x-axis ticks
                st.write(fig2)

