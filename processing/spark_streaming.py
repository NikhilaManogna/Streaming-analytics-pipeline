import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import psycopg2
import yaml
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import approx_count_distinct, col, count, from_json, sum as spark_sum, to_timestamp, when, window
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from utils.logger import get_logger


logger = get_logger("spark-streaming")


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", LongType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("amount", DoubleType(), False),
    ]
)


VALID_EVENT_TYPES = ("click", "order", "payment")


def load_config() -> Dict[str, Any]:
    config_path = Path(os.getenv("RTA_CONFIG_PATH", ROOT_DIR / "config" / "config.yaml"))
    with config_path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    config["kafka"]["brokers"] = os.getenv("RTA_KAFKA_BROKERS", ",".join(config["kafka"]["brokers"])).split(",")
    config["kafka"]["topic"] = os.getenv("RTA_KAFKA_TOPIC", config["kafka"]["topic"])
    config["postgres"]["host"] = os.getenv("RTA_POSTGRES_HOST", config["postgres"]["host"])
    config["postgres"]["port"] = int(os.getenv("RTA_POSTGRES_PORT", config["postgres"]["port"]))
    config["postgres"]["database"] = os.getenv("RTA_POSTGRES_DB", config["postgres"]["database"])
    config["postgres"]["user"] = os.getenv("RTA_POSTGRES_USER", config["postgres"]["user"])
    config["postgres"]["password"] = os.getenv("RTA_POSTGRES_PASSWORD", config["postgres"]["password"])
    return config


def wait_for_postgres(config: Dict[str, Any]) -> None:
    db_config = config["postgres"]
    while True:
        try:
            connection = psycopg2.connect(
                host=db_config["host"],
                port=db_config["port"],
                dbname=db_config["database"],
                user=db_config["user"],
                password=db_config["password"],
            )
            connection.close()
            return
        except psycopg2.OperationalError:
            logger.info("PostgreSQL not ready, retrying in 5 seconds")
            time.sleep(5)


def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    spark_config = config["spark"]
    return (
        SparkSession.builder.appName(spark_config["app_name"])
        .config("spark.sql.shuffle.partitions", spark_config["shuffle_partitions"])
        .config("spark.sql.streaming.stateStore.maintenanceInterval", spark_config["state_store_maintenance_interval"])
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def build_streams(spark: SparkSession, config: Dict[str, Any]) -> Tuple[DataFrame, DataFrame]:
    kafka_config = config["kafka"]

    raw_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", ",".join(kafka_config["brokers"]))
        .option("subscribe", kafka_config["topic"])
        .option("startingOffsets", kafka_config["starting_offsets"])
        .option("failOnDataLoss", str(kafka_config["fail_on_data_loss"]).lower())
        .option("maxOffsetsPerTrigger", kafka_config["max_offsets_per_trigger"])
        .load()
    )

    parsed = (
        raw_events.selectExpr("CAST(value AS STRING) AS raw_payload", "timestamp AS kafka_timestamp")
        .select(
            "raw_payload",
            "kafka_timestamp",
            from_json(col("raw_payload"), EVENT_SCHEMA).alias("event"),
        )
        .select("raw_payload", "kafka_timestamp", "event.*")
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .withColumn("user_id", col("user_id").cast("string"))
    )

    invalid_events = (
        parsed.withColumn(
            "invalid_reason",
            when(col("event_id").isNull(), "missing_event_id")
            .when(col("user_id").isNull(), "missing_user_id")
            .when(~col("event_type").isin(*VALID_EVENT_TYPES), "invalid_event_type")
            .when(col("event_timestamp").isNull(), "invalid_timestamp")
            .when(col("amount").isNull(), "missing_amount")
            .when(col("amount") < 0, "negative_amount"),
        )
        .filter(col("invalid_reason").isNotNull())
        .select("kafka_timestamp", "raw_payload", "invalid_reason")
    )

    valid_events = (
        parsed.filter(col("event_id").isNotNull())
        .filter(col("user_id").isNotNull())
        .filter(col("event_type").isin(*VALID_EVENT_TYPES))
        .filter(col("event_timestamp").isNotNull())
        .filter(col("amount").isNotNull())
        .filter(col("amount") >= 0)
        .withWatermark("event_timestamp", config["spark"]["watermark"])
        .dropDuplicates(["event_id"])
    )

    aggregations = (
        valid_events.groupBy(window(col("event_timestamp"), "1 minute"))
        .agg(
            spark_sum(when(col("event_type").isin("order", "payment"), col("amount")).otherwise(0.0)).alias("total_revenue"),
            approx_count_distinct("user_id", 0.05).alias("active_users"),
            count("event_id").alias("event_count"),
        )
        .select(
            col("window.start").alias("timestamp"),
            col("total_revenue"),
            col("active_users"),
            col("event_count"),
        )
    )

    return aggregations, invalid_events


def build_postgres_records(rows: Iterable[Any], threshold: float) -> List[Tuple[Any, float, int, int, bool]]:
    records: List[Tuple[Any, float, int, int, bool]] = []
    for row in rows:
        total_revenue = float(row["total_revenue"] or 0.0)
        active_users = int(row["active_users"] or 0)
        event_count = int(row["event_count"] or 0)
        anomaly_detected = total_revenue >= threshold
        records.append(
            (
                row["timestamp"],
                round(total_revenue, 2),
                active_users,
                event_count,
                anomaly_detected,
            )
        )
    return records


def build_dead_letter_records(rows: Iterable[Any]) -> List[Tuple[Any, str, str]]:
    records: List[Tuple[Any, str, str]] = []
    for row in rows:
        raw_payload = str(row["raw_payload"] or "")
        invalid_reason = str(row["invalid_reason"] or "unknown_invalid_record")
        records.append((row["kafka_timestamp"], raw_payload, invalid_reason))
    return records


def get_connection(db_config: Dict[str, Any]):
    return psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
    )


def write_batch_to_postgres(batch_df: DataFrame, batch_id: int, config: Dict[str, Any]) -> None:
    if batch_df.rdd.isEmpty():
        return

    db_config = config["postgres"]
    threshold = float(config["anomaly_detection"]["revenue_spike_threshold"])
    records = build_postgres_records(batch_df.collect(), threshold)

    if not records:
        return

    connection = get_connection(db_config)
    connection.autocommit = False

    try:
        with connection.cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO analytics_metrics (
                    timestamp,
                    total_revenue,
                    active_users,
                    event_count,
                    anomaly_detected
                )
                VALUES %s
                ON CONFLICT (timestamp) DO UPDATE
                SET
                    total_revenue = EXCLUDED.total_revenue,
                    active_users = EXCLUDED.active_users,
                    event_count = EXCLUDED.event_count,
                    anomaly_detected = EXCLUDED.anomaly_detected,
                    updated_at = NOW()
                """,
                records,
            )
        connection.commit()
        if any(record[-1] for record in records):
            logger.warning("Revenue spike detected in batch %s", batch_id)
        logger.info("Persisted %s aggregate rows from batch %s", len(records), batch_id)
    except Exception:
        connection.rollback()
        logger.exception("Failed to persist aggregate batch %s", batch_id)
        raise
    finally:
        connection.close()


def write_dead_letter_batch(batch_df: DataFrame, batch_id: int, config: Dict[str, Any]) -> None:
    if batch_df.rdd.isEmpty():
        return

    records = build_dead_letter_records(batch_df.collect())
    if not records:
        return

    connection = get_connection(config["postgres"])
    connection.autocommit = False

    try:
        with connection.cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO dead_letter_events (
                    kafka_timestamp,
                    raw_payload,
                    invalid_reason
                )
                VALUES %s
                """,
                records,
            )
        connection.commit()
        logger.warning("Persisted %s dead-letter events from batch %s", len(records), batch_id)
    except Exception:
        connection.rollback()
        logger.exception("Failed to persist dead-letter batch %s", batch_id)
        raise
    finally:
        connection.close()


def run() -> None:
    config = load_config()
    wait_for_postgres(config)
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel(config["spark"]["log_level"])

    aggregations, invalid_events = build_streams(spark, config)
    checkpoint_base = config["spark"]["checkpoint_dir"]

    aggregate_query = (
        aggregations.writeStream.outputMode("update")
        .option("checkpointLocation", f"{checkpoint_base}/aggregates")
        .trigger(processingTime=config["spark"]["trigger_interval"])
        .foreachBatch(lambda batch_df, batch_id: write_batch_to_postgres(batch_df, batch_id, config))
        .start()
    )

    dead_letter_query = (
        invalid_events.writeStream.outputMode("append")
        .option("checkpointLocation", f"{checkpoint_base}/dead-letter")
        .trigger(processingTime=config["spark"]["trigger_interval"])
        .foreachBatch(lambda batch_df, batch_id: write_dead_letter_batch(batch_df, batch_id, config))
        .start()
    )

    logger.info("Spark streaming job started")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()