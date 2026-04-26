"""
Фінальний проєкт — Частина 1.
Airflow DAG: shepeta_fp_streaming_pipeline.

Запускає Spark Structured Streaming job:
  MySQL athlete_bio → filter → MySQL athlete_event_results → Kafka → stream
  → join → aggregate → forEachBatch → Kafka output + MySQL
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_DIR = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id="shepeta_fp_streaming_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["final_project", "streaming", "kafka", "shepeta"],
    description="End-to-End Streaming Pipeline: Kafka + Spark + MySQL",
) as dag:

    streaming_pipeline = SparkSubmitOperator(
        task_id="streaming_pipeline",
        application=os.path.join(DAG_DIR, "streaming_pipeline.py"),
        conn_id="spark-default",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        driver_memory="1g",
        executor_memory="1g",
        executor_cores=1,
        total_executor_cores=2,
        verbose=True,
    )
