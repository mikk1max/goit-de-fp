"""
Фінальний проєкт — Частина 2, Крок 4.
Airflow DAG: shepeta_fp_datalake_pipeline.

Послідовно запускає три Spark jobs:
  landing_to_bronze → bronze_to_silver → silver_to_gold
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_DIR = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id="shepeta_fp_datalake_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["final_project", "datalake", "spark", "shepeta"],
    description="Multi-hop Data Lake: landing → bronze → silver → gold",
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=os.path.join(DAG_DIR, "landing_to_bronze.py"),
        conn_id="spark-default",
        driver_memory="512m",
        executor_memory="512m",
        executor_cores=1,
        total_executor_cores=1,
        verbose=True,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=os.path.join(DAG_DIR, "bronze_to_silver.py"),
        conn_id="spark-default",
        driver_memory="512m",
        executor_memory="512m",
        executor_cores=1,
        total_executor_cores=1,
        verbose=True,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=os.path.join(DAG_DIR, "silver_to_gold.py"),
        conn_id="spark-default",
        driver_memory="512m",
        executor_memory="512m",
        executor_cores=1,
        total_executor_cores=1,
        verbose=True,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
