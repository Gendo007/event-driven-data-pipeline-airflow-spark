from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os
import csv
import random

@dag(
    dag_id="bookings_spark_pipeline",
    # 1. Start the DAG closer to your actual data
    start_date=datetime(2026, 4, 1), 
    schedule="@monthly", # Changed to monthly to match your folder structure
    # 2. Tell Airflow NOT to run old dates from the past
    catchup=False, 
    tags=['production']
)
def bookings_spark_pipeline_func():

    # 3. DYNAMIC PATHS
    # {{ logical_date.strftime('%Y-%m') }} will now look at the current month
    dynamic_folder = "{{ logical_date.strftime('%Y-%m') }}"
    base_path = f"/tmp/data/listings/{dynamic_folder}/listings.csv"

    wait_for_listings_file = FileSensor(
        task_id="wait_for_listings_file",
        fs_conn_id="local_fs",
        filepath=base_path, 
        poke_interval=30,
        timeout=600,
        mode="reschedule"
    )

    @task
    def generate_bookings():
        # Use current time for the dummy bookings folder
        execution_date = datetime.now()
        file_date = execution_date.strftime("%Y-%m-%d_%H%M")
        file_path = f"/tmp/data/bookings/{file_date}/bookings.csv"
        
        listing_ids = [13913, 17402, 24328, 33332, 116268, 117203, 127652, 127860]
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["booking_id", "listing_id", "user_id", "booking_time", "status"])
            writer.writeheader()
            for _ in range(random.randint(30, 50)):
                writer.writerow({
                    "booking_id": random.randint(1000, 5000),
                    "listing_id": random.choice(listing_ids),
                    "user_id": random.randint(1000, 5000),
                    "booking_time": execution_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "confirmed"
                })
        return file_path

    spark_job = SparkSubmitOperator(
        task_id="process_listings_and_bookings",
        application="/home/apex007/airflow/dags/bookings_per_listing_spark.py",
        conn_id="spark_booking",
        application_args=[
            "--listings_file", base_path, # Dynamic path
            "--bookings_file", "{{ ti.xcom_pull(task_ids='generate_bookings') }}",
            "--output_path", "/tmp/data/bookings_per_listing/{{ ds }}/",
        ]
    )

    [wait_for_listings_file, generate_bookings()] >> spark_job

dag_instance = bookings_spark_pipeline_func()
