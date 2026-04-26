"""
Фінальний проєкт — Частина 2, Крок 4.
Airflow DAG: shepeta_fp_datalake_pipeline.

Послідовно запускає три Spark jobs:
  landing_to_bronze → bronze_to_silver → silver_to_gold
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

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

    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command=f"spark-submit --master local[2] {os.path.join(DAG_DIR, 'landing_to_bronze.py')}",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"spark-submit --master local[2] {os.path.join(DAG_DIR, 'bronze_to_silver.py')}",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"spark-submit --master local[2] {os.path.join(DAG_DIR, 'silver_to_gold.py')}",
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
