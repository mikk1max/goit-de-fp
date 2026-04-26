"""
Фінальний проєкт — Частина 2, Крок 2.
Bronze to Silver.

Зчитує parquet-таблиці з bronze/, очищає текстові колонки,
дедублікує рядки, записує у silver/{table}.
"""

import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

TABLES     = ["athlete_bio", "athlete_event_results"]
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "silver")


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,."\'\\]', '', str(text)) if text is not None else None


clean_text_udf = udf(clean_text, StringType())


def main():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for table in TABLES:
        print("=" * 60)
        print(f"Processing: {table}")
        print("=" * 60)

        bronze_path = os.path.join(BRONZE_DIR, table)
        df = spark.read.parquet(bronze_path)
        print(f"{table} (bronze): {df.count()} rows")

        for col_name, col_type in df.dtypes:
            if col_type == "string":
                df = df.withColumn(col_name, clean_text_udf(col(col_name)))

        df = df.dropDuplicates()
        print(f"{table} (після очищення + дедублікації): {df.count()} rows")
        df.show(truncate=False)

        silver_path = os.path.join(SILVER_DIR, table)
        df.write.parquet(silver_path, mode="overwrite")
        print(f"{table}: saved to {silver_path}")

    spark.stop()
    print("\nBronze to Silver — завершено!")


if __name__ == "__main__":
    main()
