"""
Unit tests for streaming data quality framework.
"""

from datetime import datetime
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

from src.streaming.data_quality import (
    CompletenessChecker,
    DataQualityEngine,
    StreamingAnomalyDetector,
    StreamingDataProfiler,
    StreamingDataValidator,
)
from src.streaming.data_quality.anomaly_detector import AnomalyRule, AnomalyType
from src.streaming.data_quality.completeness_checker import CompletenessRule
from src.streaming.data_quality.quality_engine import QualityLevel
from src.streaming.data_quality.validator import ValidationRule


@pytest.fixture(scope="module", autouse=True)
def mock_pyspark_functions():
    """Mock all PySpark functions globally for all tests."""
    # Create a mock SparkContext
    mock_spark_context = Mock()

    patches = [
        patch("pyspark.SparkContext._active_spark_context", mock_spark_context),
        patch("pyspark.sql.functions.current_timestamp", return_value=Mock()),
        patch("pyspark.sql.functions.col", side_effect=lambda x: Mock(name=f"col_{x}")),
        patch("pyspark.sql.functions.lit", side_effect=lambda x: Mock(name=f"lit_{x}")),
        patch("pyspark.sql.functions.when", return_value=Mock()),
        patch("pyspark.sql.functions.isnan", return_value=Mock()),
        patch("pyspark.sql.functions.isnull", return_value=Mock()),
        patch("pyspark.sql.functions.count", return_value=Mock()),
        patch("pyspark.sql.functions.mean", return_value=Mock()),
        patch("pyspark.sql.functions.stddev", return_value=Mock()),
        patch("pyspark.sql.functions.length", return_value=Mock()),
        patch("pyspark.sql.functions.trim", return_value=Mock()),
        patch("pyspark.sql.functions.coalesce", return_value=Mock()),
        patch("pyspark.sql.functions.percentile_approx", return_value=Mock()),
    ]

    # Start all patches
    started_patches = [p.start() for p in patches]

    try:
        yield
    finally:
        # Stop all patches
        for p in patches:
            p.stop()


@pytest.fixture
def mock_dataframe():
    """Create a mock DataFrame that supports chaining and column access."""
    mock_df = Mock(spec=DataFrame)

    # Make all DataFrame methods return the same mock for chaining
    mock_df.withColumn.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.groupBy.return_value = mock_df
    mock_df.agg.return_value = mock_df
    mock_df.orderBy.return_value = mock_df
    mock_df.limit.return_value = mock_df
    mock_df.collect.return_value = []
    mock_df.count.return_value = 100

    # Mock columns attribute
    mock_df.columns = ["transaction_id", "user_id", "total_amount", "timestamp"]
    mock_df.dtypes = [
        ("transaction_id", "string"),
        ("user_id", "int"),
        ("total_amount", "double"),
        ("timestamp", "timestamp"),
    ]

    return mock_df


@pytest.fixture
def mock_spark():
    """Create mock SparkSession."""
    return Mock(spec=SparkSession)


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestStreamingDataValidator:
    """Test cases for streaming data validator."""

    @pytest.fixture
    def validator(self, mock_spark):
        """Create StreamingDataValidator instance."""
        return StreamingDataValidator(mock_spark)

    def test_validator_initialization(self, mock_spark):
        """Test validator initialization."""
        validator = StreamingDataValidator(mock_spark)
        assert validator.spark == mock_spark
        assert validator.logger is not None
        assert "transaction" in validator.validation_rules
        assert "user_behavior" in validator.validation_rules

    def test_validation_rules_structure(self, validator):
        """Test validation rules structure."""
        transaction_rules = validator.validation_rules["transaction"]
        assert len(transaction_rules) > 0

        for rule in transaction_rules:
            assert isinstance(rule, ValidationRule)
            assert rule.name is not None
            assert rule.description is not None
            assert rule.condition is not None
            assert rule.severity in ["error", "warning", "info"]

    def test_validate_stream_with_rules(self, validator, mock_dataframe):
        """Test stream validation with rules."""
        validated_df, validation_results = validator.validate_stream(
            mock_dataframe, "transaction"
        )

        assert validated_df is not None
        assert isinstance(validation_results, dict)
        assert "stream_type" in validation_results
        assert "total_count" in validation_results
        assert "valid_count" in validation_results
        assert "invalid_count" in validation_results

    def test_validate_stream_unknown_type(self, validator, mock_dataframe):
        """Test validation with unknown stream type."""
        validated_df, validation_results = validator.validate_stream(
            mock_dataframe, "unknown_type"
        )

        assert validated_df is not None
        assert validation_results["valid_count"] == mock_dataframe.count()
        assert validation_results["invalid_count"] == 0

    def test_add_custom_validation_rule(self, validator):
        """Test adding custom validation rules."""
        custom_rule = ValidationRule(
            name="custom_test_rule",
            description="Test custom rule",
            condition="test_column IS NOT NULL",
            severity="error",
            category="custom",
        )

        validator.add_custom_validation_rule("transaction", custom_rule)

        transaction_rules = validator.validation_rules["transaction"]
        custom_rule_found = any(
            rule.name == "custom_test_rule" for rule in transaction_rules
        )
        assert custom_rule_found

    def test_get_validation_summary(self, validator, mock_dataframe):
        """Test validation summary generation."""
        # Mock the dataframe to have validation columns
        mock_dataframe.columns = ["validation_passed", "rule_test"]

        summary = validator.get_validation_summary(mock_dataframe)

        assert isinstance(summary, dict)
        # Summary should be empty or have basic structure due to mocking

    def test_create_validation_report_stream(self, validator, mock_dataframe):
        """Test validation report stream creation."""
        mock_dataframe.columns = [
            "validation_passed",
            "validation_timestamp",
            "rule_test",
        ]

        report_stream = validator.create_validation_report_stream(mock_dataframe)

        assert report_stream is not None
        mock_dataframe.withColumn.assert_called()


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestStreamingAnomalyDetector:
    """Test cases for streaming anomaly detector."""

    @pytest.fixture
    def anomaly_detector(self, mock_spark):
        """Create StreamingAnomalyDetector instance."""
        return StreamingAnomalyDetector(mock_spark)

    def test_anomaly_detector_initialization(self, mock_spark):
        """Test anomaly detector initialization."""
        detector = StreamingAnomalyDetector(mock_spark)
        assert detector.spark == mock_spark
        assert detector.logger is not None
        assert detector.z_score_threshold == 3.0
        assert "transaction" in detector.anomaly_rules

    def test_anomaly_rules_structure(self, anomaly_detector):
        """Test anomaly rules structure."""
        transaction_rules = anomaly_detector.anomaly_rules["transaction"]
        assert len(transaction_rules) > 0

        for rule in transaction_rules:
            assert isinstance(rule, AnomalyRule)
            assert rule.name is not None
            assert rule.description is not None
            assert isinstance(rule.anomaly_type, AnomalyType)
            # zero_amount legitimately uses a threshold of 0.0
            assert rule.threshold >= 0

    def test_detect_anomalies(self, anomaly_detector, mock_dataframe):
        """Test anomaly detection."""
        anomaly_df, anomaly_results = anomaly_detector.detect_anomalies(
            mock_dataframe, "transaction"
        )

        assert anomaly_df is not None
        assert isinstance(anomaly_results, dict)
        assert "stream_type" in anomaly_results
        assert "total_count" in anomaly_results
        assert "anomaly_count" in anomaly_results
        assert "anomaly_rate" in anomaly_results

    def test_detect_anomalies_unknown_type(self, anomaly_detector, mock_dataframe):
        """Test anomaly detection with unknown stream type."""
        anomaly_df, anomaly_results = anomaly_detector.detect_anomalies(
            mock_dataframe, "unknown_type"
        )

        assert anomaly_df is not None
        assert anomaly_results["anomaly_count"] == 0

    def test_add_custom_anomaly_rule(self, anomaly_detector):
        """Test adding custom anomaly rules."""
        custom_rule = AnomalyRule(
            name="custom_anomaly_rule",
            description="Test custom anomaly rule",
            anomaly_type=AnomalyType.STATISTICAL_OUTLIER,
            threshold=2.5,
            column="test_column",
        )

        anomaly_detector.add_custom_anomaly_rule("transaction", custom_rule)

        transaction_rules = anomaly_detector.anomaly_rules["transaction"]
        custom_rule_found = any(
            rule.name == "custom_anomaly_rule" for rule in transaction_rules
        )
        assert custom_rule_found

    def test_get_anomaly_summary(self, anomaly_detector, mock_dataframe):
        """Test anomaly summary generation."""
        mock_dataframe.columns = ["is_anomaly", "anomaly_test"]

        summary = anomaly_detector.get_anomaly_summary(mock_dataframe)

        assert isinstance(summary, dict)

    def test_create_anomaly_alert_stream(self, anomaly_detector, mock_dataframe):
        """Test anomaly alert stream creation."""
        mock_dataframe.columns = ["is_anomaly", "timestamp"]

        alert_stream = anomaly_detector.create_anomaly_alert_stream(mock_dataframe)

        assert alert_stream is not None
        mock_dataframe.filter.assert_called()


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestCompletenessChecker:
    """Test cases for completeness checker."""

    @pytest.fixture
    def completeness_checker(self, mock_spark):
        """Create CompletenessChecker instance."""
        return CompletenessChecker(mock_spark)

    def test_completeness_checker_initialization(self, mock_spark):
        """Test completeness checker initialization."""
        checker = CompletenessChecker(mock_spark)
        assert checker.spark == mock_spark
        assert checker.logger is not None
        assert checker.excellent_threshold == 0.98
        assert "transaction" in checker.completeness_rules

    def test_completeness_rules_structure(self, completeness_checker):
        """Test completeness rules structure."""
        transaction_rules = completeness_checker.completeness_rules["transaction"]
        assert len(transaction_rules) > 0

        for rule in transaction_rules:
            assert isinstance(rule, CompletenessRule)
            assert rule.column is not None
            assert isinstance(rule.required, bool)
            assert rule.weight > 0

    def test_check_completeness(self, completeness_checker, mock_dataframe):
        """Test completeness checking."""
        completeness_df, completeness_results = completeness_checker.check_completeness(
            mock_dataframe, "transaction"
        )

        assert completeness_df is not None
        assert isinstance(completeness_results, dict)
        assert "stream_type" in completeness_results
        assert "total_records" in completeness_results
        assert "overall_completeness" in completeness_results

    def test_check_completeness_unknown_type(
        self, completeness_checker, mock_dataframe
    ):
        """Test completeness checking with unknown stream type."""
        completeness_df, completeness_results = completeness_checker.check_completeness(
            mock_dataframe, "unknown_type"
        )

        assert completeness_df is not None
        assert completeness_results["overall_completeness"] == 1.0

    def test_add_custom_completeness_rule(self, completeness_checker):
        """Test adding custom completeness rules."""
        custom_rule = CompletenessRule(
            column="custom_column", required=True, min_length=5, weight=1.5
        )

        completeness_checker.add_custom_completeness_rule("transaction", custom_rule)

        transaction_rules = completeness_checker.completeness_rules["transaction"]
        custom_rule_found = any(
            rule.column == "custom_column" for rule in transaction_rules
        )
        assert custom_rule_found

    def test_get_completeness_dashboard_metrics(
        self, completeness_checker, mock_dataframe
    ):
        """Test completeness dashboard metrics generation."""
        mock_dataframe.columns = [
            "overall_completeness_score",
            "completeness_category",
            "completeness_passed",
        ]

        metrics = completeness_checker.get_completeness_dashboard_metrics(
            mock_dataframe
        )

        assert isinstance(metrics, dict)

    def test_create_completeness_monitoring_stream(
        self, completeness_checker, mock_dataframe
    ):
        """Test completeness monitoring stream creation."""
        mock_dataframe.columns = [
            "completeness_check_timestamp",
            "overall_completeness_score",
            "completeness_category",
        ]

        monitoring_stream = completeness_checker.create_completeness_monitoring_stream(
            mock_dataframe
        )

        assert monitoring_stream is not None


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestStreamingDataProfiler:
    """Test cases for streaming data profiler."""

    @pytest.fixture
    def profiler(self, mock_spark):
        """Create StreamingDataProfiler instance."""
        return StreamingDataProfiler(mock_spark)

    def test_profiler_initialization(self, mock_spark):
        """Test profiler initialization."""
        profiler = StreamingDataProfiler(mock_spark)
        assert profiler.spark == mock_spark
        assert profiler.logger is not None
        assert profiler.max_distinct_values == 100
        assert profiler.sample_size == 10000

    def test_profile_stream(self, profiler, mock_dataframe):
        """Test stream profiling."""
        with patch.object(
            profiler, "_sample_data_for_profiling", return_value=mock_dataframe
        ), patch.object(profiler, "_profile_column", return_value=Mock()), patch.object(
            profiler, "_generate_dataset_profile", return_value={}
        ), patch.object(
            profiler, "_detect_correlations", return_value={}
        ), patch.object(
            profiler, "_generate_quality_insights", return_value=[]
        ), patch.object(
            profiler, "_generate_recommendations", return_value=[]
        ), patch.object(
            profiler, "_column_profile_to_dict", return_value={}
        ):
            profile_results = profiler.profile_stream(mock_dataframe, "transaction")

            assert isinstance(profile_results, dict)
            assert "profiling_timestamp" in profile_results
            assert "stream_type" in profile_results
            assert "dataset_profile" in profile_results
            assert "column_profiles" in profile_results

    def test_sample_data_for_profiling(self, profiler, mock_dataframe):
        """Test data sampling for profiling."""
        # Mock count to return a large number
        mock_dataframe.count.return_value = 50000

        sampled_df = profiler._sample_data_for_profiling(mock_dataframe)

        # Should call sample when data is larger than sample_size
        assert sampled_df is not None

    def test_is_numeric_type(self, profiler):
        """Test numeric type checking."""
        assert profiler._is_numeric_type("int") == True
        assert profiler._is_numeric_type("double") == True
        assert profiler._is_numeric_type("string") == False

    def test_is_string_type(self, profiler):
        """Test string type checking."""
        assert profiler._is_string_type("string") == True
        assert profiler._is_string_type("varchar") == True
        assert profiler._is_string_type("int") == False

    def test_create_profiling_summary_stream(self, profiler, mock_dataframe):
        """Test profiling summary stream creation."""
        profile_results = {
            "profiling_timestamp": datetime.now(),
            "stream_type": "transaction",
            "dataset_profile": {"row_count": 100, "column_count": 4},
            "quality_insights": ["insight1", "insight2"],
            "recommendations": ["rec1", "rec2"],
        }

        with patch.object(
            profiler.spark, "createDataFrame", return_value=mock_dataframe
        ):
            summary_stream = profiler.create_profiling_summary_stream(
                mock_dataframe, profile_results
            )

            assert summary_stream is not None


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
class TestDataQualityEngine:
    """Test cases for data quality engine."""

    @pytest.fixture
    def quality_engine(self, mock_spark):
        """Create DataQualityEngine instance."""
        return DataQualityEngine(mock_spark)

    def test_quality_engine_initialization(self, mock_spark):
        """Test quality engine initialization."""
        engine = DataQualityEngine(mock_spark)
        assert engine.spark == mock_spark
        assert engine.logger is not None
        assert engine.validator is not None
        assert engine.anomaly_detector is not None
        assert engine.completeness_checker is not None
        assert engine.profiler is not None

    def test_quality_engine_with_config(self, mock_spark):
        """Test quality engine initialization with config."""
        config = {
            "validation": {"custom_setting": True},
            "anomaly": {"threshold": 2.5},
            "completeness": {"excellent_threshold": 0.99},
            "profiling": {"max_distinct_values": 200},
        }

        engine = DataQualityEngine(mock_spark, config)
        assert engine.config == config

    def test_assess_data_quality(self, quality_engine, mock_dataframe):
        """Test comprehensive data quality assessment."""
        # Mock the component methods
        with patch.object(
            quality_engine.validator,
            "validate_stream",
            return_value=(mock_dataframe, {"valid_count": 80, "invalid_count": 20}),
        ), patch.object(
            quality_engine.anomaly_detector,
            "detect_anomalies",
            return_value=(mock_dataframe, {"anomaly_count": 5}),
        ), patch.object(
            quality_engine.completeness_checker,
            "check_completeness",
            return_value=(mock_dataframe, {"overall_completeness": 0.95}),
        ), patch.object(
            quality_engine.profiler, "profile_stream", return_value={}
        ), patch.object(
            quality_engine, "_add_quality_summary", return_value=mock_dataframe
        ), patch.object(
            quality_engine, "_generate_quality_report"
        ) as mock_report:
            mock_report.return_value = Mock()
            mock_report.return_value.quality_level = QualityLevel.GOOD

            quality_df, quality_report = quality_engine.assess_data_quality(
                mock_dataframe, "transaction"
            )

            assert quality_df is not None
            assert quality_report is not None

    def test_determine_quality_level(self, quality_engine):
        """Test quality level determination."""
        # Test excellent quality
        level = quality_engine._determine_quality_level(0.99, 0.98, 0.005)
        assert level == QualityLevel.EXCELLENT

        # Test good quality
        level = quality_engine._determine_quality_level(0.96, 0.96, 0.03)
        assert level == QualityLevel.GOOD

        # Test critical quality
        level = quality_engine._determine_quality_level(0.70, 0.70, 0.30)
        assert level == QualityLevel.CRITICAL

    def test_generate_recommendations(self, quality_engine):
        """Test recommendation generation."""
        recommendations = quality_engine._generate_recommendations(
            0.85, 0.80, 0.15, "transaction"
        )

        assert isinstance(recommendations, list)
        assert len(recommendations) > 0

    def test_create_quality_monitoring_stream(self, quality_engine, mock_dataframe):
        """Test quality monitoring stream creation."""
        mock_dataframe.columns = [
            "data_quality_score",
            "quality_level",
            "validation_passed",
            "completeness_passed",
            "is_anomaly",
        ]

        monitoring_stream = quality_engine.create_quality_monitoring_stream(
            mock_dataframe
        )

        assert monitoring_stream is not None
        mock_dataframe.withColumn.assert_called()

    def test_get_quality_dashboard_metrics(self, quality_engine, mock_dataframe):
        """Test quality dashboard metrics generation."""
        # Mock the necessary DataFrame operations
        mock_dataframe.groupBy.return_value = mock_dataframe
        mock_dataframe.agg.return_value = mock_dataframe
        mock_dataframe.collect.return_value = [
            {"quality_level": "good", "record_count": 80, "percentage": 80.0}
        ]

        # Mock additional operations for other metrics
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value=85.5)
        mock_dataframe.agg.return_value.collect.return_value = [mock_row]

        metrics = quality_engine.get_quality_dashboard_metrics(mock_dataframe)

        assert isinstance(metrics, dict)


class TestDataQualityIntegration:
    """Test cases for data quality module integration."""

    def test_data_quality_module_import(self):
        """Test that data quality module can be imported."""
        import src.streaming.data_quality as data_quality

        assert data_quality is not None

    def test_data_quality_module_exports(self):
        """Test data quality module exports."""
        from src.streaming.data_quality import (
            CompletenessChecker,
            DataQualityEngine,
            StreamingAnomalyDetector,
            StreamingDataProfiler,
            StreamingDataValidator,
        )

        assert DataQualityEngine is not None
        assert StreamingDataValidator is not None
        assert StreamingAnomalyDetector is not None
        assert CompletenessChecker is not None
        assert StreamingDataProfiler is not None

    def test_validation_rule_creation(self):
        """Test ValidationRule creation."""
        rule = ValidationRule(
            name="test_rule",
            description="Test validation rule",
            condition="column IS NOT NULL",
            severity="error",
            category="format",
        )

        assert rule.name == "test_rule"
        assert rule.enabled == True  # Default value

    def test_anomaly_rule_creation(self):
        """Test AnomalyRule creation."""
        rule = AnomalyRule(
            name="test_anomaly",
            description="Test anomaly rule",
            anomaly_type=AnomalyType.STATISTICAL_OUTLIER,
            threshold=3.0,
            column="test_column",
        )

        assert rule.name == "test_anomaly"
        assert rule.anomaly_type == AnomalyType.STATISTICAL_OUTLIER
        assert rule.enabled == True  # Default value

    def test_completeness_rule_creation(self):
        """Test CompletenessRule creation."""
        rule = CompletenessRule(
            column="test_column",
            required=True,
            min_length=5,
            max_length=100,
            weight=1.5,
        )

        assert rule.column == "test_column"
        assert rule.required == True
        assert rule.weight == 1.5

    def test_quality_level_enum(self):
        """Test QualityLevel enum."""
        assert QualityLevel.EXCELLENT.value == "excellent"
        assert QualityLevel.GOOD.value == "good"
        assert QualityLevel.FAIR.value == "fair"
        assert QualityLevel.POOR.value == "poor"
        assert QualityLevel.CRITICAL.value == "critical"

    def test_anomaly_type_enum(self):
        """Test AnomalyType enum."""
        assert AnomalyType.STATISTICAL_OUTLIER.value == "statistical_outlier"
        assert AnomalyType.PATTERN_DEVIATION.value == "pattern_deviation"
        assert AnomalyType.VOLUME_ANOMALY.value == "volume_anomaly"
        assert AnomalyType.TEMPORAL_ANOMALY.value == "temporal_anomaly"
        assert AnomalyType.BUSINESS_RULE_VIOLATION.value == "business_rule_violation"
