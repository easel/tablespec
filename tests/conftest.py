"""Pytest configuration for tablespec tests."""

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Set Spark/Java environment variables at module level so that module-level
# ``try: import pyspark`` blocks in source code (e.g. casting_utils.py,
# output_formatting.py) see the correct SPARK_HOME / JAVA_HOME *before*
# pytest collects test files and triggers those imports.
# ---------------------------------------------------------------------------
if "DATABRICKS_RUNTIME_VERSION" not in os.environ and "SPARK_HOME" not in os.environ:
    _project_root = Path(__file__).parent.parent
    _local_spark = _project_root / ".local" / "spark-4.0.0-bin-hadoop3"
    _local_java = _project_root / ".local" / "share" / "java"

    if _local_spark.exists():
        os.environ["SPARK_HOME"] = str(_local_spark)
        os.environ["JAVA_HOME"] = str(_local_java)
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "no_spark: mark test as not requiring Spark (skips Spark setup)"
    )


# ---------------------------------------------------------------------------
# Spark environment setup (autouse) and session-scoped spark_session fixture
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _setup_spark_environment_per_test(request):
    """Set up Spark environment variables for local testing.

    Skipped for tests/modules marked with @pytest.mark.no_spark.
    This is function-scoped so it can check individual test markers.
    """
    if request.node.get_closest_marker("no_spark"):
        return

    # Skip if in Databricks
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return

    # Skip if SPARK_HOME is already set
    if "SPARK_HOME" in os.environ:
        return

    project_root = Path(__file__).parent.parent
    local_spark = project_root / ".local" / "spark-4.0.0-bin-hadoop3"
    local_java = project_root / ".local" / "share" / "java"

    if not local_spark.exists():
        # Silently skip env setup -- spark_session fixture will pytest.skip if needed
        return

    os.environ["SPARK_HOME"] = str(local_spark)
    os.environ["JAVA_HOME"] = str(local_java)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for integration tests.

    Uses file-based locking so only one Spark session is active at a time,
    even when running tests in parallel with pytest-xdist.

    The session is protected against accidental stops by test code.
    """
    # Set up environment for the session
    project_root = Path(__file__).parent.parent
    local_spark = project_root / ".local" / "spark-4.0.0-bin-hadoop3"
    local_java = project_root / ".local" / "share" / "java"

    if not local_spark.exists():
        pytest.skip(f"Local Spark installation not found at {local_spark}. Run 'make setup-spark' first.")

    os.environ.setdefault("SPARK_HOME", str(local_spark))
    os.environ.setdefault("JAVA_HOME", str(local_java))
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    try:
        from tablespec.spark_factory import create_delta_spark_session
    except ImportError:
        pytest.skip("PySpark not available -- install with: uv sync --extra spark --group dev")

    import fcntl

    lock_file = Path(tempfile.gettempdir()) / "tablespec_spark_test.lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)

    lock_fd = open(lock_file, "w")  # noqa: SIM115 -- lock must be held for entire session
    try:
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)

        try:
            spark = create_delta_spark_session(
                "tablespec-test",
                {
                    "spark.master": "local[2]",
                    "spark.default.parallelism": "2",
                    "spark.dynamicAllocation.enabled": "false",
                    "spark.sql.shuffle.partitions": "2",
                    "spark.executor.cores": "1",
                    "spark.executor.instances": "1",
                    "spark.ui.enabled": "false",
                },
            )
        except Exception as e:
            pytest.skip(f"Spark not available: {e}")

        # Protect against accidental stops during tests
        original_stop = spark.stop

        def protected_stop():
            import logging
            logging.getLogger(__name__).warning(
                "spark.stop() called during tests -- ignoring to preserve session"
            )

        spark.stop = protected_stop  # type: ignore[method-assign]

        try:
            yield spark
        finally:
            spark.stop = original_stop  # type: ignore[method-assign]
            spark.stop()
    finally:
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
        lock_fd.close()


@pytest.fixture
def anyio_backend():
    """Configure anyio to only use asyncio backend, not trio."""
    return "asyncio"


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test outputs."""
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def _mock_test_mode(monkeypatch):
    """Set environment variables for deterministic test execution."""
    monkeypatch.setenv("TABLESPEC_TEST_MODE", "1")
    monkeypatch.setenv("TABLESPEC_RANDOM_SEED", "42")


class FixtureDataLoader:
    """Helper class for loading and comparing fixture data."""

    @staticmethod
    def load_json(path: Path) -> dict[str, Any]:
        """Load JSON file with error handling."""
        try:
            with open(path) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            pytest.fail(f"Failed to load JSON from {path}: {e}")

    @staticmethod
    def compare_json_structure(
        actual: dict[str, Any], expected: dict[str, Any], path: str = ""
    ) -> None:
        """Compare JSON structure, ignoring specific timestamp/ID fields."""
        ignore_fields = {
            "timestamp",
            "generated_at",
            "processing_time",
            "extraction_id",
            "extraction_timestamp",
            "source_file_modified",
            "output_file",
            "source_file",
            "input_file",
        }

        if isinstance(expected, dict) and isinstance(actual, dict):  # pyright: ignore[reportUnnecessaryIsInstance]
            for key, expected_value in expected.items():
                current_path = f"{path}.{key}" if path else key

                if key in ignore_fields:
                    continue  # Skip timestamp fields

                if key not in actual:
                    pytest.fail(f"Missing key in actual data: {current_path}")

                FixtureDataLoader.compare_json_structure(actual[key], expected_value, current_path)

        elif isinstance(expected, list) and isinstance(actual, list):
            if len(actual) != len(expected):
                pytest.fail(
                    f"List length mismatch at {path}: expected {len(expected)}, got {len(actual)}"
                )

            for i, (actual_item, expected_item) in enumerate(zip(actual, expected, strict=False)):
                FixtureDataLoader.compare_json_structure(actual_item, expected_item, f"{path}[{i}]")

        elif actual != expected:
            pytest.fail(f"Value mismatch at {path}: expected {expected}, got {actual}")


@pytest.fixture
def fixture_loader():
    """Provide access to the fixture data loader."""
    return FixtureDataLoader
