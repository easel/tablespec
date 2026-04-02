"""Suite-level GX execution with staged validation support.

Executes entire expectation suites in a single batch pass via the GX Spark
engine, replacing the per-expectation validator pattern in gx_wrapper.py.

Requires a Spark or Sail session — use ``get_session()`` from
``tablespec.session`` to obtain one.

Supports staged execution where raw (string) and ingested (typed)
expectations route to different DataFrames.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ExpectationResult:
    """Result of a single expectation evaluation."""

    expectation_type: str
    success: bool
    column: str | None = None
    observed_value: Any = None
    unexpected_count: int = 0
    unexpected_values: list[Any] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class SuiteExecutionResult:
    """Result of executing an entire expectation suite."""

    results: list[ExpectationResult]
    success: bool  # True if all expectations passed
    total: int = 0
    passed: int = 0
    failed: int = 0

    @classmethod
    def from_results(cls, results: list[ExpectationResult]) -> SuiteExecutionResult:
        passed = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        return cls(
            results=results,
            success=all(r.success for r in results) if results else True,
            total=len(results),
            passed=passed,
            failed=failed,
        )


@dataclass
class StagedExecutionResult:
    """Result of staged (raw + ingested) execution."""

    raw: SuiteExecutionResult
    ingested: SuiteExecutionResult
    skipped: list[dict[str, Any]]  # Redundant/unknown expectations that were skipped


class GXSuiteExecutor:
    """Execute GX expectation suites against Spark DataFrames.

    Requires a Spark or Sail session. All validation runs through the GX
    Spark execution engine.

    Supports two execution modes:
    - execute_suite(): Run all expectations against a single DataFrame
    - execute_staged(): Classify and route expectations to raw/ingested DataFrames
    """

    def __init__(self, spark: Any | None = None) -> None:
        """Initialise the executor.

        Args:
            spark: A ``SparkSession`` (from Spark or Sail).
        """
        self._spark = spark
        self._context: Any | None = None

    def _get_context(self) -> Any:
        if self._context is None:
            import great_expectations as gx

            self._context = gx.get_context()  # type: ignore[attr-defined]
        return self._context

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute_suite(
        self,
        df: Any,
        expectations: list[dict[str, Any]],
    ) -> SuiteExecutionResult:
        """Execute all expectations against a Spark DataFrame in one batch.

        Args:
            df: A PySpark DataFrame (from Spark or Sail session).
            expectations: List of expectation dicts with 'type', 'kwargs', and
                optional 'meta' keys.

        Returns:
            SuiteExecutionResult with per-expectation results and summary counts.
        """
        if not expectations:
            return SuiteExecutionResult.from_results([])

        return self._execute_spark(df, expectations)

    def validate_expectation(
        self,
        exp_type: str,
        kwargs: dict[str, Any],
        meta: dict[str, Any] | None = None,
    ) -> tuple[bool, str | None]:
        """Validate a single expectation configuration without executing it."""
        from great_expectations.core import ExpectationSuite as GXSuite
        from great_expectations.expectations.expectation_configuration import (
            ExpectationConfiguration,
        )

        try:
            suite = GXSuite(name="validation_test")
            suite.add_expectation_configuration(
                ExpectationConfiguration(type=exp_type, kwargs=kwargs, meta=meta or {})
            )
            return (True, None)
        except Exception as exc:
            return (False, str(exc))

    def execute_staged(
        self,
        raw_df: Any,
        ingested_df: Any,
        expectations: list[dict[str, Any]],
    ) -> StagedExecutionResult:
        """Classify expectations by stage and execute against appropriate DataFrame.

        Raw expectations run against raw_df (string data).
        Ingested expectations run against ingested_df (typed data).
        Redundant/unknown expectations are skipped.

        Args:
            raw_df: Spark DataFrame with string columns representing raw/bronze data.
            ingested_df: Spark DataFrame with typed columns representing ingested data.
            expectations: List of expectation dicts to classify and execute.

        Returns:
            StagedExecutionResult with separate raw/ingested results and skipped list.
        """
        from tablespec.models.umf import (
            REDUNDANT_VALIDATION_TYPES,
            classify_validation_type,
        )

        raw_exps: list[dict[str, Any]] = []
        ingested_exps: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        for exp in expectations:
            exp_type = exp.get("type", exp.get("expectation_type", ""))

            if exp_type in REDUNDANT_VALIDATION_TYPES:
                skipped.append({"expectation": exp, "reason": "redundant"})
                continue

            # Honor explicit stage from meta, fall back to classification
            stage: str | None = exp.get("meta", {}).get("validation_stage")
            if not stage:
                stage = classify_validation_type(exp_type)

            if stage == "raw":
                raw_exps.append(exp)
            elif stage == "ingested":
                ingested_exps.append(exp)
            else:
                skipped.append({"expectation": exp, "reason": f"unknown stage: {stage}"})

        raw_result = (
            self.execute_suite(raw_df, raw_exps) if raw_exps else SuiteExecutionResult.from_results([])
        )
        ingested_result = (
            self.execute_suite(ingested_df, ingested_exps)
            if ingested_exps
            else SuiteExecutionResult.from_results([])
        )

        return StagedExecutionResult(
            raw=raw_result,
            ingested=ingested_result,
            skipped=skipped,
        )

    # ------------------------------------------------------------------
    # Internal: Spark execution (works with Spark & Sail)
    # ------------------------------------------------------------------

    def _execute_spark(
        self,
        df: Any,
        expectations: list[dict[str, Any]],
    ) -> SuiteExecutionResult:
        """Execute expectations against a Spark DataFrame via the GX Spark engine."""
        from great_expectations.core import ExpectationSuite as GXSuite
        from great_expectations.core import ValidationDefinition
        from great_expectations.expectations.expectation_configuration import (
            ExpectationConfiguration,
        )

        context = self._get_context()
        run_id = uuid.uuid4().hex[:8]

        # Build suite
        suite = GXSuite(name=f"suite_{run_id}")
        for exp in expectations:
            exp_type = exp.get("type", exp.get("expectation_type", ""))
            kwargs = exp.get("kwargs", {})
            meta = exp.get("meta", {})
            suite.add_expectation_configuration(
                ExpectationConfiguration(type=exp_type, kwargs=kwargs, meta=meta)
            )
        suite = context.suites.add(suite)

        # Set up Spark datasource and asset
        ds_name = f"spark_ds_{run_id}"
        asset_name = f"spark_asset_{run_id}"
        batch_name = f"spark_batch_{run_id}"

        ds = None
        vd_name = f"vd_{run_id}"
        try:
            ds = context.data_sources.add_spark(name=ds_name)
            asset = ds.add_dataframe_asset(name=asset_name)
            batch_def = asset.add_batch_definition_whole_dataframe(batch_name)

            vd = context.validation_definitions.add(
                ValidationDefinition(name=vd_name, suite=suite, data=batch_def)
            )

            validation_result = vd.run(batch_parameters={"dataframe": df})
            return self._parse_validation_result(validation_result)
        finally:
            self._cleanup(context, suite, ds, ds_name, asset_name, vd_name)

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_validation_result(validation_result: Any) -> SuiteExecutionResult:
        """Convert a GX ValidationResult into our SuiteExecutionResult."""
        results: list[ExpectationResult] = []
        for res in validation_result.results:
            result_dict = res.to_json_dict() if hasattr(res, "to_json_dict") else {}
            result_obj = result_dict.get("result", {})
            exp_config = result_dict.get("expectation_config", {})

            results.append(
                ExpectationResult(
                    expectation_type=exp_config.get("type", ""),
                    success=result_dict.get("success", False),
                    column=exp_config.get("kwargs", {}).get("column"),
                    observed_value=result_obj.get("observed_value"),
                    unexpected_count=result_obj.get("unexpected_count", 0),
                    unexpected_values=result_obj.get("partial_unexpected_list", []),
                    details=result_obj,
                )
            )

        return SuiteExecutionResult.from_results(results)

    @staticmethod
    def _cleanup(
        context: Any,
        suite: Any,
        ds: Any | None,
        ds_name: str,
        asset_name: str,
        vd_name: str | None = None,
    ) -> None:
        """Clean up ephemeral GX resources."""
        if vd_name is not None:
            try:
                context.validation_definitions.delete(vd_name)
            except Exception:
                pass
        try:
            context.suites.delete(suite.name)
        except Exception:
            pass
        if ds is not None:
            try:
                ds.delete_asset(asset_name)
            except Exception:
                pass
        try:
            context.data_sources.delete(ds_name)
        except Exception:
            pass
