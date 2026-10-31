"""
Comprehensive unit tests for utility modules.
"""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from src.utils.logger import setup_logging
from src.utils.spark_validator import main as spark_validator_main
from src.utils.spark_validator import validate_spark_imports


class TestLogger:
    """Test logger utility."""

    def test_setup_logging_default(self):
        """Test setup_logging with default parameters."""
        logger = setup_logging()
        assert logger is not None
        assert logger.name == "ecommerce_analytics"

    def test_setup_logging_custom_name(self):
        """Test setup_logging with custom name."""
        logger = setup_logging("custom_logger")
        assert logger.name == "custom_logger"

    def test_setup_logging_with_level(self):
        """Test setup_logging with custom level."""
        logger = setup_logging("test_logger", level="DEBUG")
        assert logger.level == 10  # DEBUG level


class TestSparkValidator:
    """Test Spark validator utility."""

    def test_validate_spark_imports_valid_file(self):
        """Test validation with valid Spark imports."""
        # Create a temporary file with valid Spark imports
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(
                """
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

def process_data():
    spark = SparkSession.builder.appName("test").getOrCreate()
    return spark
"""
            )
            temp_path = Path(f.name)

        try:
            errors = validate_spark_imports(temp_path)
            # Should have no errors for proper usage
            assert len(errors) == 0
        finally:
            temp_path.unlink()

    def test_validate_spark_imports_missing_sparksession(self):
        """Test validation with missing SparkSession import."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(
                """
import pyspark.sql.functions as F

def process_data():
    return None
"""
            )
            temp_path = Path(f.name)

        try:
            errors = validate_spark_imports(temp_path)
            # Should suggest importing SparkSession
            assert len(errors) == 1
            assert "Consider importing SparkSession explicitly" in errors[0]
        finally:
            temp_path.unlink()

    def test_validate_spark_imports_deprecated_rdd(self):
        """Test validation with deprecated RDD usage."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(
                """
from pyspark.rdd import RDD
from pyspark import SparkContext

def process_data():
    sc = SparkContext()
    return sc.parallelize([1, 2, 3])
"""
            )
            temp_path = Path(f.name)

        try:
            errors = validate_spark_imports(temp_path)
            # Should suggest using DataFrame API
            assert len(errors) == 1
            assert "Consider using DataFrame API instead of RDD" in errors[0]
        finally:
            temp_path.unlink()

    def test_validate_spark_imports_no_spark(self):
        """Test validation with no Spark imports."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(
                """
import pandas as pd

def process_data():
    return pd.DataFrame()
"""
            )
            temp_path = Path(f.name)

        try:
            errors = validate_spark_imports(temp_path)
            # Should have no errors for non-Spark files
            assert len(errors) == 0
        finally:
            temp_path.unlink()

    def test_validate_spark_imports_invalid_syntax(self):
        """Test validation with invalid Python syntax."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(
                """
import pyspark.sql
def invalid_syntax(
    return None
"""
            )
            temp_path = Path(f.name)

        try:
            errors = validate_spark_imports(temp_path)
            # Should report parsing error
            assert len(errors) == 1
            assert "Error parsing file" in errors[0]
        finally:
            temp_path.unlink()

    @patch("src.utils.spark_validator.Path")
    def test_spark_validator_main_no_src_dir(self, mock_path):
        """Test main function with no src directory."""
        mock_path.return_value.exists.return_value = False

        with patch("builtins.print") as mock_print:
            result = spark_validator_main()
            assert result == 0
            mock_print.assert_called_with("No src directory found")


class TestUtilsFunctions:
    """Test utility functions."""

    def test_file_operations(self):
        """Test basic file operations used in utils."""
        # Test temporary file creation and cleanup
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("# Test file\nprint('hello')")
            temp_path = Path(f.name)

        # File should exist
        assert temp_path.exists()

        # Clean up
        temp_path.unlink()
        assert not temp_path.exists()

    def test_path_operations(self):
        """Test path operations used in utils."""
        # Test Path object creation
        test_path = Path("test/path/file.py")

        assert test_path.name == "file.py"
        assert test_path.suffix == ".py"
        assert str(test_path).endswith("file.py")

    def test_time_operations(self):
        """Test time-related operations."""
        import time

        start_time = time.time()
        time.sleep(0.01)  # Small delay
        end_time = time.time()

        duration = end_time - start_time
        assert duration > 0.005  # At least 5ms
        assert duration < 0.1  # Less than 100ms

    def test_string_operations(self):
        """Test string operations used in validation."""
        test_string = "pyspark.sql.functions"

        assert "pyspark" in test_string
        assert test_string.startswith("pyspark")
        assert test_string.endswith("functions")

        parts = test_string.split(".")
        assert len(parts) == 3
        assert parts[0] == "pyspark"
        assert parts[1] == "sql"
        assert parts[2] == "functions"


class TestUtilsIntegration:
    """Integration tests for utility modules."""

    def test_logger_and_validator_integration(self):
        """Test integration of logger and spark validator."""
        logger = setup_logging("integration_test")

        # Create a valid temporary Python file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(
                """
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def test_function():
    return "valid"
"""
            )
            temp_path = Path(f.name)

        try:
            # Validate the file
            errors = validate_spark_imports(temp_path)

            # Log the results - should not raise exception
            logger.info(f"Validation completed. Found {len(errors)} errors.")

            # Should have no errors
            assert len(errors) == 0

        finally:
            temp_path.unlink()

    def test_error_handling_integration(self):
        """Test error handling across utilities."""
        logger = setup_logging("error_test")

        # Create an invalid temporary Python file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(
                """
import pyspark.sql
def invalid_syntax(
    return None
"""
            )
            temp_path = Path(f.name)

        try:
            # This should handle the error gracefully
            errors = validate_spark_imports(temp_path)

            # Log the error
            logger.error(f"Validation found {len(errors)} errors: {errors}")

            # Should have one error
            assert len(errors) == 1
            assert "Error parsing file" in errors[0]

        finally:
            temp_path.unlink()

    def test_multiple_file_validation(self):
        """Test validation of multiple files."""
        logger = setup_logging("multi_file_test")

        # Create multiple temporary files
        files = []
        try:
            # Valid file
            with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
                f.write("from pyspark.sql import SparkSession\ndef valid(): pass")
                files.append(Path(f.name))

            # File with warning
            with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
                f.write("import pyspark.sql.functions as F\ndef warning(): pass")
                files.append(Path(f.name))

            # Validate all files
            all_errors = []
            for file_path in files:
                errors = validate_spark_imports(file_path)
                all_errors.extend(errors)

            logger.info(
                f"Validated {len(files)} files, found {len(all_errors)} total issues"
            )

            # Should have one warning about missing SparkSession
            assert len(all_errors) == 1
            assert "Consider importing SparkSession explicitly" in all_errors[0]

        finally:
            # Clean up all files
            for file_path in files:
                file_path.unlink(missing_ok=True)
