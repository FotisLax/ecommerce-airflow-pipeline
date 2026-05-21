from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import psycopg2
import uuid
import logging

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

logger = logging.getLogger(__name__)

def extract():
    logging.info("Starting extraction task...")

    products = [
        ("Laptop", "Electronics", 1200),
        ("Phone", "Electronics", 800),
        ("Shoes", "Fashion", 120),
        ("Watch", "Accessories", 250),
        ("Headphones", "Electronics", 150),
    ]
    customers = [
        "Alice Johnson",
        "Bob Smith",
        "Michael Brown",
        "Emma Wilson",
        "Sophia Taylor",
        "Daniel Moore",
    ]

    cities = [
        "New York",
        "London",
        "Berlin",
        "Athens",
        "Paris",
    ]

    payment_methods = [
        "Credit Card",
        "PayPal",
        "Apple Pay",
    ]

    statuses = [
        "Completed",
        "Pending",
        "Cancelled",
    ]

    orders = []

    for _ in range(50):
        product = random.choice(products)
        quantity = random.randint(1, 3)

        orders.append({
            "order_id": str(uuid.uuid4()),
            "customer_name": random.choice(customers),
            "city": random.choice(cities),
            "payment_method": random.choice(payment_methods),
            "status": random.choice(statuses),

            "product_name": product[0],
            "category": product[1],
            "price": product[2],
            "quantity": quantity,
            "total_amount": product[2] * quantity,
            "order_date": datetime.now().date()
        })
    logging.info(f"Extraction completed. Generated {len(orders)} orders.")
    return orders

def load(**context):
    orders = context["ti"].xcom_pull(task_ids="extract_task")
    logging.info(f"Loading {len(orders)} orders into database")
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
            customer_name TEXT,
            city TEXT,
            payment_method TEXT,
            status TEXT,
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
            INSERT INTO fact_orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (order_id) DO NOTHING
        """, (
            order["order_id"],
            order["customer_name"],
            order["city"],
            order["payment_method"],
            order["status"],
            order["product_name"],
            order["category"],
            order["price"],
            order["quantity"],
            order["total_amount"],
            order["order_date"]
        ))
    logging.info("Load task completed successfully")

    conn.commit()

def aggregate():
    logging.info("Starting aggregation task...")
    conn = psycopg2.connect(
            host = "postgres",
            databse = "ecommerce_db",
            user="airflow",
            password="airflow"
    )

    cursor = conn.cursor()
    logging.info("Aggregation completed")
    conn.commit()
    cursor.close()
    conn.close()

def check_data_quality(**context):
    logging.info("Starting data quality checks...")

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
    logging.info("Data quality checks passed successfully")
    cursor.close()
    conn.close()

    if row_count == 0:
        logging.error("Data quality failed: No records found")
        raise ValueError("Quality check failed")




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