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

postgres_connection = create_postgres_connection()

def calculate_active_orders():
    cursor = postgres_connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM spark_streams.orders WHERE order_completed = 0;")
    result = cursor.fetchone()[0]

    cursor.close()

    return result

# def calculate_orders_today():
#     result = session.execute("SELECT COUNT(*) FROM spark_streams.orders WHERE date(time)=currentDate() ALLOW FILTERING")
#     return result.one()[0] or 0
#
#
# def calculate_total_revenue():
#     result = session.execute("SELECT SUM(cost) FROM spark_streams.orders")
#     return result.one()[0] or 0
#
#
# def new_customers_today():
#     result = session.execute("SELECT * from spark_streams.orders")
#
#     data = []
#     for row in result:
#         data.append(row)
#
#     df = pd.DataFrame(data)
#     if not df.empty:
#         filtered_df = df[df['time'] > pd.Timestamp.now().floor('D')]
#         distinct_count = filtered_df["email"].nunique()
#
#     return distinct_count
#
# def total_customers():
#     result = session.execute("SELECT * from spark_streams.orders")
#
#     data = []
#     for row in result:
#         data.append(row)
#
#     df = pd.DataFrame(data)
#     if not df.empty:
#         distinct_count = df["email"].nunique()
#
#     return distinct_count
#
#
# def pandas_df_completed():
#     result = session.execute("SELECT * FROM spark_streams.orders WHERE order_completed = 1 ALLOW FILTERING")
#
#     data = []
#     for row in result:
#         data.append(row)
#
#     df = pd.DataFrame(data)
#     if not df.empty:
#         df = df.sort_values("time", ascending=False)
#         df["id"] = df["id"].astype(str)
#     return df
#
#
# def pandas_df_active():
#     result = session.execute("SELECT * FROM spark_streams.orders WHERE order_completed = 0 ALLOW FILTERING")
#
#     data = []
#     for row in result:
#         data.append(row)
#
#     df = pd.DataFrame(data)
#     if not df.empty:
#         df = df.sort_values("time", ascending=False)
#         df["id"] = df["id"].astype(str)
#     else:
#         df = pd.DataFrame(columns=["id", "user", "email", "food", "size", "cost", "time", "order_completed"])
#     return df


add_selectbox = st.sidebar.radio(
    "How would you like to be contacted?",
    ("Today Analytics", "Food Details", "Historical Trends")
)

placeholder = st.empty()

while True:

    if add_selectbox == "Today Analytics":
        with placeholder.container():

            st.write("## Today's Analytics")
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            # kpi1.metric(label="Orders today", value=calculate_orders_today(), delta=- 10 + calculate_orders_today())
            kpi2.metric(label="Active orders", value=calculate_active_orders())
            # kpi3.metric(label="Total revenue", value=f"$ {calculate_total_revenue()} ")
            # kpi4.metric(label="New customers today", value=f"{new_customers_today()}")

            # st.dataframe(pandas_df_completed(), use_container_width=True)
            # st.dataframe(pandas_df_active(), use_container_width=True)


        #     result = session.execute("SELECT * FROM spark_streams.orders")
        #     data = []
        #     for row in result:
        #         data.append(row)
        #     import duckdb
        #     df = pd.DataFrame(data)
        #     df = duckdb.sql("SELECT * FROM df where time >= '2024-04-01'").df()
        #
        #     fig_col1, fig_col2 = st.columns(2)
        #     with fig_col1:
        #         st.markdown("### Food order Details")
        #         fig = px.pie(df, names='food')
        #         st.write(fig)
        #
        #     with fig_col2:
        #     #     st.markdown("### Second Chart")
        #     #     fig2 = px.histogram(data_frame=df, x='age_new')
        #         fig2 = px.histogram(df, x='time', barmode='group')
        #         st.write(fig2)
        #     #     st.write(fig2)
        #     # st.markdown("### Detailed Data View")
        #     # st.dataframe(df)
        #     # time.sleep(1)
        # # st.empty()
