"""Tests for GXSuiteExecutor — suite-level GX execution with staged validation."""

import pytest

pytestmark = [
    pytest.mark.no_spark,
    pytest.mark.filterwarnings("ignore::ResourceWarning"),
    pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning"),
]

# Skip all tests if GX not available
try:
    import great_expectations  # noqa: F401

    HAS_GX = True
except (ImportError, ValueError):
    HAS_GX = False

pytestmark_gx = pytest.mark.skipif(not HAS_GX, reason="great_expectations not installed")


@pytestmark_gx
class TestGXSuiteExecutor:
    def test_empty_suite(self):
        import pandas as pd

        from tablespec.validation.gx_executor import GXSuiteExecutor

        executor = GXSuiteExecutor()
        result = executor.execute_suite(pd.DataFrame({"id": [1, 2, 3]}), [])
        assert result.success
        assert result.total == 0

    def test_single_expectation_passes(self):
        import pandas as pd

        from tablespec.validation.gx_executor import GXSuiteExecutor

        executor = GXSuiteExecutor()
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = executor.execute_suite(
            df,
            [{"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}}],
        )
        assert result.success
        assert result.total == 1
        assert result.passed == 1

    def test_single_expectation_fails(self):
        import pandas as pd

        from tablespec.validation.gx_executor import GXSuiteExecutor

        executor = GXSuiteExecutor()
        df = pd.DataFrame({"id": [1, None, 3]})
        result = executor.execute_suite(
            df,
            [{"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}}],
        )
        assert not result.success
        assert result.failed == 1
        assert result.results[0].unexpected_count == 1

    def test_multiple_expectations_mixed(self):
        import pandas as pd

        from tablespec.validation.gx_executor import GXSuiteExecutor

        executor = GXSuiteExecutor()
        df = pd.DataFrame({"id": [1, None, 3], "name": ["a", "bb", "ccc"]})
        result = executor.execute_suite(
            df,
            [
                {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}},
                {
                    "type": "expect_column_value_lengths_to_be_between",
                    "kwargs": {"column": "name", "min_value": 1, "max_value": 2},
                },
            ],
        )
        assert not result.success
        assert result.total == 2
        assert result.failed == 2  # null violation + length violation

    def test_result_includes_column(self):
        import pandas as pd

        from tablespec.validation.gx_executor import GXSuiteExecutor

        executor = GXSuiteExecutor()
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = executor.execute_suite(
            df,
            [{"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}}],
        )
        assert result.results[0].column == "id"

    def test_result_includes_expectation_type(self):
        import pandas as pd

        from tablespec.validation.gx_executor import GXSuiteExecutor

        executor = GXSuiteExecutor()
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = executor.execute_suite(
            df,
            [{"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}}],
        )
        assert result.results[0].expectation_type == "expect_column_values_to_not_be_null"


@pytestmark_gx
class TestStagedExecution:
    def test_routes_raw_and_ingested(self):
        import pandas as pd

        from tablespec.validation.gx_executor import GXSuiteExecutor

        executor = GXSuiteExecutor()
        raw_df = pd.DataFrame({"age": ["25", "abc", "30"]})
        ingested_df = pd.DataFrame({"age": [25, 0, 30]})

        expectations = [
            {
                "type": "expect_column_values_to_match_regex",
                "kwargs": {"column": "age", "regex": r"^\d+$"},
            },
            {
                "type": "expect_column_values_to_be_between",
                "kwargs": {"column": "age", "min_value": 1, "max_value": 150},
            },
        ]
        result = executor.execute_staged(raw_df, ingested_df, expectations)
        assert len(result.raw.results) == 1  # regex is RAW
        assert len(result.ingested.results) == 1  # between is INGESTED

    def test_skips_redundant(self):
        import pandas as pd

        from tablespec.validation.gx_executor import GXSuiteExecutor

        executor = GXSuiteExecutor()
        df = pd.DataFrame({"id": [1]})
        result = executor.execute_staged(
            df,
            df,
            [
                {"type": "expect_column_to_exist", "kwargs": {"column": "id"}},
            ],
        )
        assert len(result.skipped) == 1
        assert result.skipped[0]["reason"] == "redundant"

    def test_empty_staged(self):
        import pandas as pd

        from tablespec.validation.gx_executor import GXSuiteExecutor

        executor = GXSuiteExecutor()
        df = pd.DataFrame({"id": [1]})
        result = executor.execute_staged(df, df, [])
        assert result.raw.total == 0
        assert result.ingested.total == 0

    def test_honors_explicit_stage_in_meta(self):
        import pandas as pd

        from tablespec.validation.gx_executor import GXSuiteExecutor

        executor = GXSuiteExecutor()
        raw_df = pd.DataFrame({"val": ["1", "2", "3"]})
        ingested_df = pd.DataFrame({"val": [1, 2, 3]})

        # Force a normally-raw expectation to run on ingested via meta override
        expectations = [
            {
                "type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "val"},
                "meta": {"validation_stage": "ingested"},
            },
        ]
        result = executor.execute_staged(raw_df, ingested_df, expectations)
        assert result.raw.total == 0
        assert result.ingested.total == 1
