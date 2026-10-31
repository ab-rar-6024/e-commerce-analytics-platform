"""
Spark job validation utility for pre-commit hooks.
"""
import ast
import sys
from pathlib import Path
from typing import List, Set


def validate_spark_imports(file_path: Path) -> List[str]:
    """Validate Spark-related imports in Python files."""
    errors = []

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        tree = ast.parse(content)

        # Check for common Spark import patterns
        spark_imports = set()
        imported_names = set()

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if "pyspark" in alias.name:
                        spark_imports.add(alias.name)
                        imported_names.add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module and "pyspark" in node.module:
                    spark_imports.add(node.module)
                    for alias in node.names:
                        imported_names.add(alias.name)

        # Validate common patterns
        if spark_imports:
            # Check for proper SparkSession usage
            if any(
                imp == "pyspark.sql" or imp.startswith("pyspark.sql.")
                for imp in spark_imports
            ):
                if not any("SparkSession" in name for name in imported_names):
                    errors.append(
                        f"{file_path}: Consider importing SparkSession explicitly"
                    )

            # Check for deprecated RDD usage
            if any("pyspark.rdd" in imp for imp in spark_imports):
                errors.append(
                    f"{file_path}: Consider using DataFrame API instead of RDD"
                )

    except Exception as e:
        errors.append(f"{file_path}: Error parsing file: {e}")

    return errors


def main():
    """Main validation function."""
    src_path = Path("src")

    if not src_path.exists():
        print("No src directory found")
        return 0

    errors = []

    # Find all Python files in src directory
    python_files = list(src_path.rglob("*.py"))

    for file_path in python_files:
        if file_path.name.startswith("__") or "test" in file_path.name:
            continue

        file_errors = validate_spark_imports(file_path)
        errors.extend(file_errors)

    if errors:
        print("Spark validation errors:")
        for error in errors:
            print(f"  {error}")
        return 1

    print("Spark validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
