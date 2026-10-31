"""
Kafka streaming consumers for e-commerce analytics platform.

This module provides streaming consumer classes for processing e-commerce data
from Kafka topics using PySpark Structured Streaming.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    isnan,
    unix_timestamp,
    when,
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming import data_quality
from src.utils.logger import get_logger
from src.utils.spark_utils import get_secure_temp_dir


class StreamingConsumerError(Exception):
    """Exception raised by streaming consumers."""

    pass


class SchemaValidator:
    """Validates DataFrame schemas against expected schemas."""

    def __init__(self):
        """Initialize schema validator."""
        self.logger = get_logger(__name__)

    def validate_schema(self, df: DataFrame, expected_schema: StructType) -> bool:
        """
        Validate DataFrame schema against expected schema.

        Args:
            df: DataFrame to validate
            expected_schema: Expected schema structure

        Returns:
            True if schema matches, False otherwise
        """
        try:
            actual_schema = df.schema
            return self._schemas_match(actual_schema, expected_schema)
        except Exception as e:
            self.logger.error(f"Schema validation error: {e}")
            return False

    def get_schema_diff(
        self, actual_schema: StructType, expected_schema: StructType
    ) -> Dict[str, List[str]]:
        """
        Get differences between actual and expected schemas.

        Args:
            actual_schema: Actual DataFrame schema
            expected_schema: Expected schema structure

        Returns:
            Dictionary with missing_fields and extra_fields lists
        """
        actual_fields = {field.name for field in actual_schema.fields}
        expected_fields = {field.name for field in expected_schema.fields}

        return {
            "missing_fields": list(expected_fields - actual_fields),
            "extra_fields": list(actual_fields - expected_fields),
        }

    def _schemas_match(
        self, actual_schema: StructType, expected_schema: StructType
    ) -> bool:
        """Check if two schemas match."""
        if len(actual_schema.fields) != len(expected_schema.fields):
            return False

        actual_fields = {field.name: field for field in actual_schema.fields}
        expected_fields = {field.name: field for field in expected_schema.fields}

        for field_name, expected_field in expected_fields.items():
            if field_name not in actual_fields:
                return False

            actual_field = actual_fields[field_name]
            if (
                actual_field.dataType != expected_field.dataType
                or actual_field.nullable != expected_field.nullable
            ):
                return False

        return True


class DataQualityChecker:
    """Checks data quality for streaming data."""

    def __init__(self, logger=None):
        """Initialize data quality checker."""
        self.logger = logger or get_logger(__name__)

    def check_nulls(self, df: DataFrame, critical_columns: List[str]) -> DataFrame:
        """
        Check for null values in critical columns.

        Args:
            df: Input DataFrame
            critical_columns: List of critical column names

        Returns:
            DataFrame with null checks applied
        """
        for column in critical_columns:
            if column in df.columns:
                # Count null values
                null_count = (
                    df.select(
                        (col(column).isNull() | isnan(col(column))).alias("is_null")
                    )
                    .alias("null_check")
                    .collect()[0]["is_null"]
                )

                if null_count > 0:
                    self.logger.warning(
                        f"Found {null_count} null values in critical column '{column}'"
                    )

        return df

    def check_data_freshness(
        self, df: DataFrame, timestamp_column: str, max_age_seconds: int
    ) -> DataFrame:
        """
        Check data freshness and filter stale records.

        Args:
            df: Input DataFrame
            timestamp_column: Name of timestamp column
            max_age_seconds: Maximum age in seconds

        Returns:
            DataFrame with stale records filtered
        """
        if timestamp_column in df.columns:
            # Add current timestamp and calculate age
            df_with_current = df.withColumn("current_time", current_timestamp())
            df_with_age = df_with_current.withColumn(
                "age_seconds",
                unix_timestamp(col("current_time"))
                - unix_timestamp(col(timestamp_column)),
            )

            # Count stale records
            stale_count = df_with_age.filter(
                col("age_seconds") > max_age_seconds
            ).count()

            if stale_count > 0:
                self.logger.warning(
                    f"Found {stale_count} stale records older than {max_age_seconds} seconds"
                )

            # Filter out stale records
            return df_with_age.filter(col("age_seconds") <= max_age_seconds).drop(
                "current_time", "age_seconds"
            )

        return df


class BaseStreamingConsumer(ABC):
    """Abstract base class for Kafka streaming consumers."""

    def __init__(
        self,
        spark: SparkSession,
        topic: str,
        consumer_group: str,
        kafka_bootstrap_servers: str = "localhost:9092",
        enable_backpressure: bool = True,
        max_offsets_per_trigger: int = 10000,
        **kwargs,
    ):
        """
        Initialize streaming consumer.

        Args:
            spark: Spark session
            topic: Kafka topic to consume from
            consumer_group: Consumer group ID
            kafka_bootstrap_servers: Kafka bootstrap servers
            enable_backpressure: Enable adaptive query execution
            max_offsets_per_trigger: Maximum offsets per trigger
            **kwargs: Additional configuration
        """
        self.spark = spark
        self.topic = topic
        self.consumer_group = consumer_group
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.enable_backpressure = enable_backpressure
        self.max_offsets_per_trigger = max_offsets_per_trigger

        self.logger = get_logger(self.__class__.__name__)
        self.schema_validator = SchemaValidator()
        self.quality_checker = DataQualityChecker(self.logger)
        self._quality_engine: Optional[Any] = None

        # Error handling
        self.error_callback: Optional[Callable] = None
        self.streaming_query: Optional[StreamingQuery] = None

        # Configure Spark settings
        self._configure_spark_settings()

    def _configure_spark_settings(self) -> None:
        """Configure Spark settings for optimal streaming performance."""
        if self.enable_backpressure:
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            self.spark.conf.set(
                "spark.sql.streaming.adaptive.enabled", "true"
            )  # Spark 3.0+

        # Set max offsets per trigger
        self.spark.conf.set(
            "spark.sql.streaming.kafka.maxOffsetsPerTrigger",
            str(self.max_offsets_per_trigger),
        )

        # Set checkpoint configuration with secure temporary directory
        self.spark.conf.set(
            "spark.sql.streaming.checkpointLocation", get_secure_temp_dir("checkpoints")
        )

    def set_error_callback(self, callback: Callable[[Exception], None]) -> None:
        """Set error callback function."""
        self.error_callback = callback

    def error_callback(self, error: Exception) -> None:
        """Handle streaming errors."""
        self.logger.error(f"Streaming error in {self.__class__.__name__}: {error}")
        if self.error_callback:
            self.error_callback(error)

    @abstractmethod
    def get_schema(self) -> StructType:
        """Get the expected schema for the stream."""
        pass

    @abstractmethod
    def transform_stream(self, df: DataFrame) -> DataFrame:
        """Apply transformations to the stream."""
        pass

    def _assess_quality(self, df: DataFrame, stream_type: str) -> DataFrame:
        """Flag data quality issues with the streaming quality engine.

        A failure in the engine must not take the stream down, so on error the
        input DataFrame is returned unchanged.
        """
        try:
            if self._quality_engine is None:
                self._quality_engine = data_quality.DataQualityEngine(self.spark)
            quality_df, _ = self._quality_engine.assess_data_quality(
                df, stream_type, enable_profiling=False
            )
            return quality_df
        except Exception as e:
            self.logger.warning(f"Data quality assessment skipped: {e}")
            return df

    def _get_critical_columns(self) -> List[str]:
        """Get list of critical columns that should not have null values."""
        # Default critical columns - subclasses can override
        return ["timestamp"]

    def create_stream(self) -> DataFrame:
        """Create Kafka stream DataFrame."""
        try:
            # Read from Kafka
            kafka_df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                .option("subscribe", self.topic)
                .option("kafka.group.id", self.consumer_group)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
            )

            # Parse JSON data
            schema = self.get_schema()
            parsed_df = kafka_df.select(
                from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp").alias("kafka_timestamp"),
                col("partition"),
                col("offset"),
            ).select("data.*", "kafka_timestamp", "partition", "offset")

            # Validate schema
            if not self.schema_validator.validate_schema(parsed_df, schema):
                diff = self.schema_validator.get_schema_diff(parsed_df.schema, schema)
                self.logger.warning(f"Schema validation failed: {diff}")

            # Apply data quality checks
            critical_columns = self._get_critical_columns()
            quality_checked_df = self.quality_checker.check_nulls(
                parsed_df, critical_columns
            )

            # Apply freshness check if timestamp column exists
            if "timestamp" in quality_checked_df.columns:
                quality_checked_df = self.quality_checker.check_data_freshness(
                    quality_checked_df, "timestamp", 300  # 5 minutes
                )

            return quality_checked_df

        except Exception as e:
            self.error_callback(e)
            raise StreamingConsumerError(f"Failed to create stream: {e}")

    def start_stream(
        self,
        output_mode: str = "append",
        trigger_interval: str = "10 seconds",
        checkpoint_location: Optional[str] = None,
    ) -> StreamingQuery:
        """
        Start the streaming query.

        Args:
            output_mode: Output mode (append, update, complete)
            trigger_interval: Trigger interval
            checkpoint_location: Checkpoint location

        Returns:
            StreamingQuery object
        """
        try:
            # Create and transform stream
            raw_stream = self.create_stream()
            transformed_stream = self.transform_stream(raw_stream)

            # Configure checkpoint location with secure temporary directory
            if not checkpoint_location:
                base_checkpoint_dir = get_secure_temp_dir("checkpoints")
                checkpoint_location = (
                    f"{base_checkpoint_dir}/{self.topic}_{self.consumer_group}"
                )

            # Start streaming query
            query = (
                transformed_stream.writeStream.outputMode(output_mode)
                .trigger(processingTime=trigger_interval)
                .option("checkpointLocation", checkpoint_location)
                .format("console")  # Default output - can be overridden
                .start()
            )

            self.streaming_query = query
            self.logger.info(
                f"Started streaming query for topic '{self.topic}' with group '{self.consumer_group}'"
            )

            return query

        except Exception as e:
            self.error_callback(e)
            raise StreamingConsumerError(f"Failed to start stream: {e}")

    def stop_stream(self) -> None:
        """Stop the streaming query."""
        if self.streaming_query and self.streaming_query.isActive:
            self.streaming_query.stop()
            self.logger.info(f"Stopped streaming query for topic '{self.topic}'")

    def get_stream_status(self) -> Dict[str, Any]:
        """Get current stream status."""
        if not self.streaming_query:
            return {"status": "not_started"}

        try:
            progress = self.streaming_query.lastProgress
            if progress:
                return {
                    "status": "active" if self.streaming_query.isActive else "stopped",
                    "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
                    "processed_rows_per_second": progress.get(
                        "processedRowsPerSecond", 0
                    ),
                    "batch_duration": progress.get("durationMs", {}).get(
                        "triggerExecution", 0
                    ),
                    "timestamp": progress.get("timestamp"),
                }
            else:
                return {
                    "status": "active" if self.streaming_query.isActive else "stopped",
                    "input_rows_per_second": 0,
                    "processed_rows_per_second": 0,
                    "batch_duration": 0,
                }

        except Exception as e:
            self.logger.error(f"Error getting stream status: {e}")
            return {"status": "error", "error": str(e)}


class TransactionStreamConsumer(BaseStreamingConsumer):
    """Consumer for transaction stream data."""

    def __init__(
        self,
        spark: SparkSession,
        topic: str = "transactions",
        consumer_group: str = "transaction-analytics-group",
        **kwargs,
    ):
        """Initialize transaction stream consumer."""
        super().__init__(spark, topic, consumer_group, **kwargs)

    def get_schema(self) -> StructType:
        """Get transaction schema."""
        return StructType(
            [
                StructField("transaction_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("price", DoubleType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("user_location", StringType(), True),
                StructField("device_info", StringType(), True),
                StructField("payment_method", StringType(), True),
                StructField("currency", StringType(), True),
            ]
        )

    def transform_stream(self, df: DataFrame) -> DataFrame:
        """Apply transaction-specific transformations."""
        # Add derived columns
        transformed_df = df.withColumn(
            "total_amount", col("price") * col("quantity")
        ).withColumn("processing_timestamp", current_timestamp())

        # Filter out invalid transactions
        filtered_df = transformed_df.filter(
            (col("price") > 0) & (col("quantity") > 0) & col("user_id").isNotNull()
        )

        return self._assess_quality(filtered_df, "transaction")

    def _get_critical_columns(self) -> List[str]:
        """Get critical columns for transactions."""
        return [
            "transaction_id",
            "user_id",
            "product_id",
            "price",
            "quantity",
            "timestamp",
        ]


class UserBehaviorStreamConsumer(BaseStreamingConsumer):
    """Consumer for user behavior stream data."""

    def __init__(
        self,
        spark: SparkSession,
        topic: str = "user-events",
        consumer_group: str = "user-behavior-analytics-group",
        **kwargs,
    ):
        """Initialize user behavior stream consumer."""
        super().__init__(spark, topic, consumer_group, **kwargs)

    def get_schema(self) -> StructType:
        """Get user behavior schema."""
        return StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("page_url", StringType(), True),
                StructField("user_location", StringType(), True),
                StructField("device_info", StringType(), True),
                StructField("user_agent", StringType(), True),
                StructField("event_properties", StringType(), True),  # JSON string
            ]
        )

    def transform_stream(self, df: DataFrame) -> DataFrame:
        """Apply user behavior-specific transformations."""
        # Add derived columns
        transformed_df = df.withColumn("processing_timestamp", current_timestamp())

        # Add event categorization
        transformed_df = transformed_df.withColumn(
            "event_category",
            when(col("event_type").isin(["page_view", "click"]), "engagement")
            .when(col("event_type").isin(["add_to_cart", "purchase"]), "conversion")
            .otherwise("other"),
        )

        # Filter out invalid events
        filtered_df = transformed_df.filter(
            col("user_id").isNotNull()
            & col("session_id").isNotNull()
            & col("event_type").isNotNull()
        )

        return self._assess_quality(filtered_df, "user_behavior")

    def _get_critical_columns(self) -> List[str]:
        """Get critical columns for user behavior."""
        return [
            "event_id",
            "user_id",
            "session_id",
            "event_type",
            "timestamp",
        ]
