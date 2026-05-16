from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import psycopg2
import uuid

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

def extract():
    products = [
        ("Laptop", "Electronics", 1200),
        ("Phone", "Electronics", 800),
        ("Shoes", "Fashion", 120),
        ("Watch", "Accessories", 250),
        ("Headphones", "Electronics", 150),
    ]

    orders = []

    for _ in range(50):
        product = random.choice(products)
        quantity = random.randint(1, 3)

        orders.append({
            "order_id": str(uuid.uuid4()),
            "customer_id": random.randint(1, 20),
            "product_name": product[0],
            "category": product[1],
            "price": product[2],
            "quantity": quantity,
            "total_amount": product[2] * quantity,
            "order_date": datetime.now().date()
        })

    return orders


def load(**context):
    orders = context["ti"].xcom_pull(task_ids="extract_task")

    conn = psycopg2.connect(
        host="postgres",
        database="ecommerce_db",
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id TEXT PRIMARY KEY,
            customer_id INT,
            product_name TEXT,
            category TEXT,
            price FLOAT,
            quantity INT,
            total_amount FLOAT,
            order_date DATE
        )
    """)

    for order in orders:
        cursor.execute("""
            INSERT INTO fact_orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (order_id) DO NOTHING
        """, (
            order["order_id"],
            order["customer_id"],
            order["product_name"],
            order["category"],
            order["price"],
            order["quantity"],
            order["total_amount"],
            order["order_date"]
        ))

    conn.commit()

def aggregate():
    conn = psycopg2.connect(
            host = "postgres",
            databse = "ecommerce_db",
            user="airflow",
            password="airflow"
    )

    cursor = conn.cursor()

    conn.commit()
    cursor.close()
    conn.close()

def check_data_quality(**context):
    import psycopg2

    conn = psycopg2.connect(
        host="ecommerce_pg",
        data="ecommerce",
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM orders;")
    result = cursor.fetchone()
    row_count = result[0]

    cursor.close()
    conn.close()

    if row_count == 0:
        raise ValueError("Data qualty check failed: orders table is empty.")
    
    print (f"Data quality passed. {row_count} rows found.")





with DAG(
    dag_id="ecommerce_elt_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
    )

    aggregate_task = PythonOperator(
    task_id="aggregate_task",
    python_callable=aggregate,
    )

    quality_task = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )

    extract_task >> load_task >> aggregate_task >> quality_task