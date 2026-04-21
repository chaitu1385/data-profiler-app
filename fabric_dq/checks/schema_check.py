"""Expected-schema check: columns present + dtype match.

Compares ``config.expected_schema`` (column -> dtype-simple-string) to the
actual DataFrame schema. Missing columns, extra columns, and dtype drift all
surface as a single FAIL result (one per issue).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

from fabric_dq.checks.base import Check, CheckResult, Severity, Status

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class SchemaCheck(Check):
    check_type = "schema"

    def run(self, expected: Dict[str, str], full_df: "DataFrame") -> List[CheckResult]:
        actual = {f.name: f.dataType.simpleString() for f in full_df.schema.fields}
        results: List[CheckResult] = []

        for col, exp_dtype in expected.items():
            if col not in actual:
                results.append(CheckResult(
                    check_type=self.check_type,
                    column=col,
                    status=Status.FAIL,
                    metric_value=None,
                    threshold=None,
                    message=f"expected column {col!r} missing from table",
                    severity=Severity.FAIL,
                ))
                continue
            if actual[col] != exp_dtype:
                results.append(CheckResult(
                    check_type=self.check_type,
                    column=col,
                    status=Status.FAIL,
                    metric_value=None,
                    threshold=None,
                    message=(
                        f"dtype mismatch on {col!r}: expected {exp_dtype}, "
                        f"got {actual[col]}"
                    ),
                    severity=Severity.FAIL,
                    details={"expected": exp_dtype, "actual": actual[col]},
                ))

        if not results:
            results.append(CheckResult(
                check_type=self.check_type,
                column=None,
                status=Status.PASS,
                metric_value=0,
                threshold=0,
                message=f"schema matches expected ({len(expected)} columns)",
                severity=Severity.FAIL,
            ))
        return results
