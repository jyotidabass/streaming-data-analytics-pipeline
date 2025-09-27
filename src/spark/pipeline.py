"""Apache Spark data processing pipeline for streaming analytics."""

import logging
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, window, count, sum as spark_sum, avg, max as spark_max,
    min as spark_min, collect_list, explode, from_json, to_json,
    current_timestamp, lit, when, regexp_replace, split, size,
    row_number, rank, dense_rank, lag, lead, first, last
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType, ArrayType
)
from pyspark.sql.window import Window
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from ..config import settings

logger = logging.getLogger(__name__)


class SparkPipeline:
    """Main Spark pipeline for processing streaming data."""
    
    def __init__(self):
        """Initialize Spark session and configuration."""
        self.spark = self._create_spark_session()
        self._define_schemas()
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        spark = SparkSession.builder \
            .appName(settings.spark_app_name) \
            .master(settings.spark_master_url) \
            .config("spark.executor.memory", settings.spark_executor_memory) \
            .config("spark.driver.memory", settings.spark_driver_memory) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel(settings.log_level)
        return spark
    
    def _define_schemas(self):
        """Define schemas for different data types."""
        self.user_event_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_data", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("ip_address", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("referrer", StringType(), True)
        ])
        
        self.analytics_schema = StructType([
            StructField("metric_name", StringType(), True),
            StructField("metric_value", DoubleType(), True),
            StructField("dimensions", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("source", StringType(), True)
        ])
    
    def read_kafka_stream(self, topic: str) -> DataFrame:
        """Read streaming data from Kafka."""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
    
    def parse_json_stream(self, df: DataFrame, schema: StructType) -> DataFrame:
        """Parse JSON data from Kafka stream."""
        return df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
    
    def process_user_events(self, df: DataFrame) -> DataFrame:
        """Process user events with real-time analytics."""
        # Clean and enrich data
        processed_df = df.withColumn(
            "clean_page_url", 
            regexp_replace(col("page_url"), r"https?://[^/]+", "")
        ).withColumn(
            "device_type",
            when(col("user_agent").contains("Mobile"), "Mobile")
            .when(col("user_agent").contains("Tablet"), "Tablet")
            .otherwise("Desktop")
        ).withColumn(
            "browser",
            when(col("user_agent").contains("Chrome"), "Chrome")
            .when(col("user_agent").contains("Firefox"), "Firefox")
            .when(col("user_agent").contains("Safari"), "Safari")
            .otherwise("Other")
        )
        
        return processed_df
    
    def calculate_session_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate session-based metrics."""
        window_spec = Window.partitionBy("session_id").orderBy("timestamp")
        
        session_metrics = df.withColumn(
            "session_start", first("timestamp").over(window_spec)
        ).withColumn(
            "session_end", last("timestamp").over(window_spec)
        ).withColumn(
            "session_duration", 
            col("session_end").cast("long") - col("session_start").cast("long")
        ).withColumn(
            "page_views", count("*").over(Window.partitionBy("session_id"))
        ).withColumn(
            "unique_pages", size(collect_list("page_url").over(
                Window.partitionBy("session_id")
            ))
        )
        
        return session_metrics
    
    def calculate_real_time_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate real-time metrics using window functions."""
        # Define window specifications
        window_spec_5min = Window.partitionBy().orderBy("timestamp").rangeBetween(-300, 0)
        window_spec_1hour = Window.partitionBy().orderBy("timestamp").rangeBetween(-3600, 0)
        
        # Calculate metrics
        metrics_df = df.withColumn(
            "events_5min", count("*").over(window_spec_5min)
        ).withColumn(
            "events_1hour", count("*").over(window_spec_1hour)
        ).withColumn(
            "unique_users_5min", 
            size(collect_list("user_id").over(window_spec_5min))
        ).withColumn(
            "unique_users_1hour",
            size(collect_list("user_id").over(window_spec_1hour))
        )
        
        return metrics_df
    
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        """Detect anomalous behavior patterns."""
        # Calculate moving averages and standard deviations
        window_spec = Window.partitionBy("user_id").orderBy("timestamp").rowsBetween(-10, -1)
        
        anomaly_df = df.withColumn(
            "avg_events_per_minute",
            avg(count("*")).over(window_spec)
        ).withColumn(
            "current_events_per_minute",
            count("*").over(Window.partitionBy("user_id").orderBy("timestamp")
                          .rangeBetween(-60, 0))
        ).withColumn(
            "is_anomaly",
            when(col("current_events_per_minute") > col("avg_events_per_minute") * 3, True)
            .otherwise(False)
        )
        
        return anomaly_df
    
    def optimize_sql_queries(self, df: DataFrame) -> DataFrame:
        """Apply SQL optimization techniques."""
        # Cache frequently used DataFrames
        df.cache()
        
        # Apply predicate pushdown
        optimized_df = df.filter(
            col("timestamp") >= current_timestamp() - expr("INTERVAL 1 HOUR")
        )
        
        # Apply column pruning
        essential_columns = [
            "user_id", "session_id", "event_type", "timestamp",
            "page_url", "device_type", "browser"
        ]
        
        return optimized_df.select(*essential_columns)
    
    def write_to_database(self, df: DataFrame, table_name: str, mode: str = "append"):
        """Write DataFrame to database."""
        df.write \
            .format("jdbc") \
            .option("url", settings.database_url) \
            .option("dbtable", table_name) \
            .option("driver", "org.postgresql.Driver") \
            .mode(mode) \
            .save()
    
    def write_to_s3(self, df: DataFrame, path: str, format_type: str = "parquet"):
        """Write DataFrame to S3."""
        if settings.s3_bucket:
            s3_path = f"s3a://{settings.s3_bucket}/{path}"
            df.write \
                .format(format_type) \
                .mode("overwrite") \
                .save(s3_path)
    
    def write_to_gcs(self, df: DataFrame, path: str, format_type: str = "parquet"):
        """Write DataFrame to Google Cloud Storage."""
        if settings.gcs_bucket:
            gcs_path = f"gs://{settings.gcs_bucket}/{path}"
            df.write \
                .format(format_type) \
                .mode("overwrite") \
                .save(gcs_path)
    
    def create_streaming_query(self, df: DataFrame, output_path: str) -> Any:
        """Create a streaming query for real-time processing."""
        query = df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", f"/tmp/checkpoints/{output_path}") \
            .trigger(processingTime=f"{settings.checkpoint_interval} seconds") \
            .start()
        
        return query
    
    def run_batch_processing(self, input_path: str, output_path: str):
        """Run batch processing on historical data."""
        # Read historical data
        historical_df = self.spark.read.parquet(input_path)
        
        # Apply all transformations
        processed_df = self.process_user_events(historical_df)
        session_metrics = self.calculate_session_metrics(processed_df)
        real_time_metrics = self.calculate_real_time_metrics(session_metrics)
        anomaly_detection = self.detect_anomalies(real_time_metrics)
        optimized_df = self.optimize_sql_queries(anomaly_detection)
        
        # Write results
        optimized_df.write.mode("overwrite").parquet(output_path)
        
        logger.info(f"Batch processing completed. Results saved to {output_path}")
    
    def stop(self):
        """Stop Spark session."""
        self.spark.stop()


# Example usage and testing
if __name__ == "__main__":
    pipeline = SparkPipeline()
    
    try:
        # Example: Process streaming data
        kafka_df = pipeline.read_kafka_stream(settings.kafka_topic_user_events)
        parsed_df = pipeline.parse_json_stream(kafka_df, pipeline.user_event_schema)
        processed_df = pipeline.process_user_events(parsed_df)
        
        # Create streaming query
        query = pipeline.create_streaming_query(processed_df, "/tmp/streaming_output")
        
        # Wait for termination
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    finally:
        pipeline.stop()
