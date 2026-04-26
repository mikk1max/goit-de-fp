"""
Фінальний проєкт — Частина 2, Крок 1.
Landing to Bronze.

Завантажує CSV-файли з FTP-сервера та зберігає у форматі parquet
у папку bronze/{table}.
"""

import os
import requests
from pyspark.sql import SparkSession

BASE_URL   = "https://ftp.goit.study/neoversity/"
TABLES     = ["athlete_bio", "athlete_event_results"]
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
LAND_DIR   = os.path.join(BASE_DIR, "landing")
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")


def download_data(table_name):
    url        = f"{BASE_URL}{table_name}.csv"
    local_path = os.path.join(LAND_DIR, f"{table_name}.csv")
    os.makedirs(LAND_DIR, exist_ok=True)
    print(f"Downloading from {url}")
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(response.content)
        print(f"Saved to {local_path}")
    else:
        raise RuntimeError(f"Download failed. Status: {response.status_code}")
    return local_path


def main():
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for table in TABLES:
        print("=" * 60)
        print(f"Processing: {table}")
        print("=" * 60)

        csv_path = download_data(table)

        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        print(f"{table}: {df.count()} rows loaded from CSV")
        df.show(truncate=False)

        bronze_path = os.path.join(BRONZE_DIR, table)
        df.write.parquet(bronze_path, mode="overwrite")
        print(f"{table}: saved to {bronze_path}")

    spark.stop()
    print("\nLanding to Bronze — завершено!")


if __name__ == "__main__":
    main()
