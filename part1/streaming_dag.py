"""
Фінальний проєкт — Частина 1.
Airflow DAG: shepeta_fp_streaming_pipeline.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DAG_DIR = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id="shepeta_fp_streaming_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["final_project", "streaming", "kafka", "shepeta"],
    description="End-to-End Streaming Pipeline: Kafka + Spark + MySQL",
) as dag:

    streaming_pipeline = BashOperator(
        task_id="streaming_pipeline",
        bash_command=(
            f"spark-submit --master local[2] "
            f"--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 "
            f"{os.path.join(DAG_DIR, 'streaming_pipeline.py')}"
        ),
    )
