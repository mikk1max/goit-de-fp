"""
Фінальний проєкт — Частина 1.
End-to-End Streaming Pipeline.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, current_timestamp, from_json, struct, to_json,
)
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = "77.81.230.104:9092"
KAFKA_USERNAME   = "admin"
KAFKA_PASSWORD   = "VawEzo1ikLtrA8Ug8THa"
KAFKA_TOPIC_IN   = "athlete_event_results"
KAFKA_TOPIC_OUT  = "shepeta_athlete_enriched_agg"

JDBC_URL      = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
JDBC_DRIVER   = "com.mysql.cj.jdbc.Driver"
JDBC_USER     = "neo_data_admin"
JDBC_PASSWORD = "Proyahaxuqithab9oplp"

MYSQL_OUTPUT_URL   = "jdbc:mysql://217.61.57.46:3306/neo_data"
MYSQL_OUTPUT_TABLE = "shepeta_athlete_enriched_agg"

KAFKA_OPTS = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": (
        'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
    ),
}

# Schema of athlete_event_results rows coming from Kafka JSON
EVENT_SCHEMA = StructType([
    StructField("edition",      StringType()),
    StructField("edition_id",   StringType()),
    StructField("country_noc",  StringType()),
    StructField("sport",        StringType()),
    StructField("event",        StringType()),
    StructField("result_id",    StringType()),
    StructField("athlete",      StringType()),
    StructField("athlete_id",   StringType()),
    StructField("pos",          StringType()),
    StructField("medal",        StringType()),
    StructField("isTeamSport",  StringType()),
])


def main():
    spark = (
        SparkSession.builder
        .appName("ShepetaStreamingPipeline")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # -------------------------------------------------------------------------
    # Етап 1: Зчитати дані athlete_bio з MySQL
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Етап 1: Зчитуємо athlete_bio з MySQL")
    print("=" * 60)
    bio_df = (
        spark.read.format("jdbc")
        .option("url",      JDBC_URL)
        .option("driver",   JDBC_DRIVER)
        .option("dbtable",  "athlete_bio")
        .option("user",     JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .load()
    )
    print(f"athlete_bio: {bio_df.count()} рядків до фільтрації")

    # -------------------------------------------------------------------------
    # Етап 2: Відфільтрувати рядки з порожніми або нечисловими height / weight
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Етап 2: Фільтруємо athlete_bio (height / weight)")
    print("=" * 60)
    bio_df = (
        bio_df
        .withColumn("height", col("height").cast(DoubleType()))
        .withColumn("weight", col("weight").cast(DoubleType()))
        .filter(col("height").isNotNull() & col("weight").isNotNull())
    )
    # Залишаємо лише колонки, потрібні для join та агрегації
    bio_clean = bio_df.select(
        col("athlete_id").cast(StringType()).alias("athlete_id"),
        "sex",
        "height",
        "weight",
    )
    print(f"athlete_bio: {bio_clean.count()} рядків після фільтрації")
    bio_clean.show(5, truncate=False)

    # -------------------------------------------------------------------------
    # Етап 3а: Зчитати athlete_event_results з MySQL і записати у Kafka топік
    # -------------------------------------------------------------------------
    print("=" * 60)
    print(f"Етап 3а: Записуємо athlete_event_results → Kafka '{KAFKA_TOPIC_IN}'")
    print("=" * 60)
    events_df = (
        spark.read.format("jdbc")
        .option("url",      JDBC_URL)
        .option("driver",   JDBC_DRIVER)
        .option("dbtable",  "athlete_event_results")
        .option("user",     JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .load()
    )
    print(f"athlete_event_results: {events_df.count()} рядків")

    (
        events_df
        .select(to_json(struct(*events_df.columns)).alias("value"))
        .write
        .format("kafka")
        .options(**KAFKA_OPTS)
        .option("topic", KAFKA_TOPIC_IN)
        .save()
    )
    print(f"Записано у Kafka топік '{KAFKA_TOPIC_IN}'")

    # -------------------------------------------------------------------------
    # Етап 3б: Зчитати streaming дані з Kafka топіку та розпарсити JSON
    # -------------------------------------------------------------------------
    print("=" * 60)
    print(f"Етап 3б: Читаємо stream з Kafka топіку '{KAFKA_TOPIC_IN}'")
    print("=" * 60)
    stream_raw = (
        spark.readStream.format("kafka")
        .options(**KAFKA_OPTS)
        .option("subscribe",        KAFKA_TOPIC_IN)
        .option("startingOffsets",  "earliest")
        .load()
    )
    stream_df = (
        stream_raw
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("d"))
        .select("d.*")
    )

    # -------------------------------------------------------------------------
    # Етап 4: Join streaming подій з bio даними за athlete_id
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Етап 4: Join stream + bio за athlete_id")
    print("=" * 60)
    joined_df = stream_df.join(
        bio_clean,
        stream_df["athlete_id"] == bio_clean["athlete_id"],
        how="inner",
    ).select(
        stream_df["sport"],
        stream_df["medal"],
        stream_df["country_noc"],
        bio_clean["sex"],
        bio_clean["height"],
        bio_clean["weight"],
    )

    # -------------------------------------------------------------------------
    # Етап 5: Середній зріст і вага для кожної комбінації sport/medal/sex/country
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("Етап 5: Агрегація avg(height, weight)")
    print("=" * 60)
    agg_df = (
        joined_df
        .groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
        )
        .withColumn("timestamp", current_timestamp())
    )

    # -------------------------------------------------------------------------
    # Етап 6: forEachBatch → вихідний Kafka топік + MySQL
    # -------------------------------------------------------------------------
    def foreach_batch_function(batch_df, batch_id):
        print(f"\n--- Batch {batch_id}: {batch_df.count()} рядків ---")
        batch_df.show(truncate=False)

        # Етап 6а: Запис у вихідний Kafka топік
        (
            batch_df
            .select(to_json(struct(*batch_df.columns)).alias("value"))
            .write
            .format("kafka")
            .options(**KAFKA_OPTS)
            .option("topic", KAFKA_TOPIC_OUT)
            .save()
        )
        print(f"Batch {batch_id}: записано у Kafka '{KAFKA_TOPIC_OUT}'")

        # Етап 6б: Запис у MySQL
        (
            batch_df.write
            .format("jdbc")
            .option("url",      MYSQL_OUTPUT_URL)
            .option("driver",   JDBC_DRIVER)
            .option("dbtable",  MYSQL_OUTPUT_TABLE)
            .option("user",     JDBC_USER)
            .option("password", JDBC_PASSWORD)
            .mode("append")
            .save()
        )
        print(f"Batch {batch_id}: записано у MySQL '{MYSQL_OUTPUT_TABLE}'")

    print("=" * 60)
    print("Етап 6: Запускаємо writeStream з foreachBatch")
    print("=" * 60)
    query = (
        agg_df.writeStream
        .foreachBatch(foreach_batch_function)
        .outputMode("complete")
        .trigger(once=True)
        .start()
    )
    query.awaitTermination()

    print("\n=== Streaming pipeline завершено! ===")
    spark.stop()


if __name__ == "__main__":
    main()
