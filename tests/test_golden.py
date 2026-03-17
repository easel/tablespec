"""Golden file test runner with auto-discovery of test cases."""

from difflib import unified_diff
from pathlib import Path

import pytest

GOLDEN_DIR = Path(__file__).parent / "golden"


def discover_golden_cases(feature: str) -> list[tuple[str, Path, Path]]:
    """Auto-discover input/expected pairs for a feature."""
    feature_dir = GOLDEN_DIR / feature
    if not feature_dir.exists():
        return []
    cases = []
    for input_file in sorted(feature_dir.glob("*.input.*")):
        case_name = input_file.name.split(".input.")[0]
        # Find matching expected file with any extension
        expected_files = list(feature_dir.glob(f"{case_name}.expected.*"))
        if expected_files:
            cases.append((case_name, input_file, expected_files[0]))
    return cases


class GoldenCase:
    def __init__(self, name: str, input_path: Path, expected_path: Path):
        self.name = name
        self.input_path = input_path
        self.expected_path = expected_path
        self.expected = expected_path.read_text()

    def assert_matches(self, actual: str):
        if actual.strip() != self.expected.strip():
            diff = list(
                unified_diff(
                    self.expected.splitlines(keepends=True),
                    actual.splitlines(keepends=True),
                    fromfile=f"expected ({self.expected_path.name})",
                    tofile="actual",
                )
            )
            pytest.fail(f"Golden file mismatch for {self.name}:\n{''.join(diff)}")


@pytest.mark.no_spark
@pytest.mark.fast
class TestGoldenSQLDDL:
    @pytest.mark.parametrize(
        "case_name,input_path,expected_path",
        discover_golden_cases("sql_ddl"),
        ids=lambda x: x if isinstance(x, str) else "",
    )
    def test_sql_ddl(self, case_name, input_path, expected_path):
        import yaml

        from tablespec.schemas.generators import generate_sql_ddl

        case = GoldenCase(case_name, input_path, expected_path)
        with open(input_path) as f:
            umf_data = yaml.safe_load(f)
        actual = generate_sql_ddl(umf_data)
        case.assert_matches(actual)
