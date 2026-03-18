"""Spike test: verify Great Expectations 1.6+ works with DuckDB.

This spike intentionally uses pandas via DuckDB export -- not part of the
Spark/Sail validation pipeline.

This is a proof-of-concept to validate that GX can query data stored in
DuckDB.  Two approaches are tested:

1. **SqlAlchemy datasource** (`duckdb-engine`) -- the ideal path.
2. **Pandas datasource** fed from `duckdb.sql().df()` -- the fallback.

Findings are documented in the test docstrings.
"""

from __future__ import annotations

import pytest

try:
    import duckdb
    import sqlalchemy  # noqa: F401

    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False

pytestmark = [
    pytest.mark.fast,
    pytest.mark.no_spark,
    pytest.mark.skipif(not HAS_DUCKDB, reason="duckdb/duckdb-engine not installed"),
    pytest.mark.filterwarnings("ignore::ResourceWarning"),
    pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning"),
]


# ---------------------------------------------------------------------------
# Approach 1: SqlAlchemy datasource (ideal, but currently broken)
# ---------------------------------------------------------------------------


class TestGxDuckdbSqlAlchemy:
    """Attempt to use GX's SqlAlchemy execution engine with DuckDB.

    FINDING: GX 1.15.1 hits an IndexError in
    ``SqlAlchemyExecutionEngine.resolve_metric_bundle`` when DuckDB is the
    backend.  The bundled SQL query returns an empty result set, causing
    ``res[0]`` to fail.  This appears to be a dialect-compatibility gap in
    GX rather than a duckdb-engine bug.

    These tests are marked ``xfail`` to document the current state.
    """

    @pytest.fixture()
    def gx_sqla_batch(self, tmp_path):
        import great_expectations as gx

        db_path = tmp_path / "spike.duckdb"
        connection_string = f"duckdb:///{db_path}"

        engine = sqlalchemy.create_engine(connection_string)
        with engine.connect() as conn:
            conn.execute(
                sqlalchemy.text(
                    """
                    CREATE TABLE sample_data (
                        id INTEGER,
                        state_code VARCHAR,
                        full_name VARCHAR,
                        age INTEGER
                    )
                    """
                )
            )
            conn.execute(
                sqlalchemy.text(
                    """
                    INSERT INTO sample_data VALUES
                        (1, 'CA', 'Alice Smith', 30),
                        (2, 'TX', 'Bob Jones', NULL),
                        (3, 'NY', 'Charlie Brown', 25),
                        (4, 'CA', NULL, 40),
                        (5, 'FL', 'Eve Davis', 35)
                    """
                )
            )
            conn.commit()
        engine.dispose()

        context = gx.get_context()
        datasource = context.data_sources.add_sql(
            name="duckdb_spike_sqla",
            connection_string=connection_string,
            create_temp_table=False,
        )
        asset = datasource.add_table_asset(
            name="sample_data", table_name="sample_data"
        )
        batch_def = asset.add_batch_definition_whole_table("full_table")
        return batch_def.get_batch()

    @pytest.mark.xfail(
        reason="GX SqlAlchemy engine MetricResolutionError with DuckDB dialect",
        strict=True,
    )
    def test_sqla_not_null(self, gx_sqla_batch):
        import great_expectations as gx

        result = gx_sqla_batch.validate(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="id")
        )
        assert result.success is True


# ---------------------------------------------------------------------------
# Approach 2: Pandas datasource (working fallback)
# ---------------------------------------------------------------------------


class TestGxDuckdbPandas:
    """Use DuckDB to query data, then hand a DataFrame to GX's Pandas engine.

    This is the recommended approach until GX adds first-class DuckDB
    dialect support in its SqlAlchemy engine.
    """

    @pytest.fixture()
    def gx_pandas_batch(self):
        import great_expectations as gx

        # Build sample data in DuckDB, export as pandas DataFrame
        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE TABLE sample_data (
                id INTEGER,
                state_code VARCHAR,
                full_name VARCHAR,
                age INTEGER
            )
            """
        )
        con.execute(
            """
            INSERT INTO sample_data VALUES
                (1, 'CA', 'Alice Smith', 30),
                (2, 'TX', 'Bob Jones', NULL),
                (3, 'NY', 'Charlie Brown', 25),
                (4, 'CA', NULL, 40),
                (5, 'FL', 'Eve Davis', 35)
            """
        )
        df = con.execute("SELECT * FROM sample_data").df()
        con.close()

        context = gx.get_context()
        datasource = context.data_sources.add_pandas(name="duckdb_pandas_spike")
        asset = datasource.add_dataframe_asset(name="sample_data")
        batch_def = asset.add_batch_definition_whole_dataframe("full_table")
        batch = batch_def.get_batch(batch_parameters={"dataframe": df})
        return batch

    # -- expect_column_values_to_not_be_null --------------------------------

    def test_not_null_passes(self, gx_pandas_batch):
        """Column 'id' has no nulls -- expectation should pass."""
        import great_expectations as gx

        result = gx_pandas_batch.validate(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="id")
        )
        assert result.success is True

    def test_not_null_fails(self, gx_pandas_batch):
        """Column 'full_name' has one null -- expectation should fail."""
        import great_expectations as gx

        result = gx_pandas_batch.validate(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="full_name")
        )
        assert result.success is False

    # -- expect_column_values_to_be_in_set ----------------------------------

    def test_in_set_passes(self, gx_pandas_batch):
        """All state_code values are in the allowed set."""
        import great_expectations as gx

        result = gx_pandas_batch.validate(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="state_code",
                value_set=["CA", "TX", "NY", "FL", "WA"],
            )
        )
        assert result.success is True

    def test_in_set_fails(self, gx_pandas_batch):
        """state_code='FL' is not in the restricted set -- should fail."""
        import great_expectations as gx

        result = gx_pandas_batch.validate(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="state_code",
                value_set=["CA", "TX", "NY"],
            )
        )
        assert result.success is False

    # -- expect_column_value_lengths_to_be_between --------------------------

    def test_lengths_between_passes(self, gx_pandas_batch):
        """state_code is always 2 chars."""
        import great_expectations as gx

        result = gx_pandas_batch.validate(
            gx.expectations.ExpectColumnValueLengthsToBeBetween(
                column="state_code",
                min_value=2,
                max_value=2,
            )
        )
        assert result.success is True

    def test_lengths_between_fails(self, gx_pandas_batch):
        """full_name lengths vary and exceed max_value=5 -- should fail."""
        import great_expectations as gx

        result = gx_pandas_batch.validate(
            gx.expectations.ExpectColumnValueLengthsToBeBetween(
                column="full_name",
                min_value=1,
                max_value=5,
            )
        )
        assert result.success is False
