"""
Фінальний проєкт — Частина 2, Крок 3.
Silver to Gold.

Зчитує silver/athlete_bio та silver/athlete_event_results,
робить join за athlete_id, обчислює avg(weight, height)
для кожної комбінації sport/medal/sex/country_noc,
додає timestamp і зберігає у gold/avg_stats.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp
from pyspark.sql.types import DoubleType

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
SILVER_DIR = os.path.join(BASE_DIR, "silver")
GOLD_DIR   = os.path.join(BASE_DIR, "gold")


def main():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Зчитуємо silver/athlete_bio
    print("=" * 60)
    print("Зчитуємо silver/athlete_bio")
    print("=" * 60)
    bio_df = spark.read.parquet(os.path.join(SILVER_DIR, "athlete_bio"))
    print(f"athlete_bio (silver): {bio_df.count()} rows")
    bio_df.show(5, truncate=False)

    # Зчитуємо silver/athlete_event_results
    print("=" * 60)
    print("Зчитуємо silver/athlete_event_results")
    print("=" * 60)
    events_df = spark.read.parquet(os.path.join(SILVER_DIR, "athlete_event_results"))
    print(f"athlete_event_results (silver): {events_df.count()} rows")
    events_df.show(5, truncate=False)

    # Join за athlete_id; видаляємо дублікат country_noc з bio
    print("=" * 60)
    print("Join за athlete_id")
    print("=" * 60)
    bio_for_join = bio_df.drop("country_noc")
    joined_df = events_df.join(bio_for_join, on="athlete_id", how="inner")
    print(f"Joined: {joined_df.count()} rows")

    # Приводимо weight та height до числового типу
    joined_df = (
        joined_df
        .withColumn("weight", col("weight").cast(DoubleType()))
        .withColumn("height", col("height").cast(DoubleType()))
    )

    # Агрегація
    print("=" * 60)
    print("Агрегація avg(weight, height) по sport/medal/sex/country_noc")
    print("=" * 60)
    gold_df = (
        joined_df
        .groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
        )
        .withColumn("timestamp", current_timestamp())
    )
    print(f"Gold: {gold_df.count()} rows")
    gold_df.show(truncate=False)

    gold_path = os.path.join(GOLD_DIR, "avg_stats")
    gold_df.write.parquet(gold_path, mode="overwrite")
    print(f"Saved to {gold_path}")

    spark.stop()
    print("\nSilver to Gold — завершено!")


if __name__ == "__main__":
    main()
