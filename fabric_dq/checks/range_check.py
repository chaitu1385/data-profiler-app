"""Numeric range check. Reads min/max from the column profile."""
from __future__ import annotations

from typing import Any, Dict

from fabric_dq.checks.base import Check, CheckResult, Status
from fabric_dq.config import RangeCheckCfg


class RangeCheck(Check):
    check_type = "range"

    def run(self, cfg: RangeCheckCfg, profile: Dict[str, Dict[str, Any]]) -> CheckResult:
        col = profile.get(cfg.column)
        if col is None:
            return CheckResult(
                check_type=self.check_type,
                column=cfg.column,
                status=Status.FAIL,
                metric_value=None,
                threshold=None,
                message=f"column {cfg.column!r} not present",
                severity=cfg.severity,
            )
        actual_min = col.get("min")
        actual_max = col.get("max")

        violations = []
        if cfg.min is not None and actual_min is not None and actual_min < cfg.min:
            violations.append(f"min={actual_min} < {cfg.min}")
        if cfg.max is not None and actual_max is not None and actual_max > cfg.max:
            violations.append(f"max={actual_max} > {cfg.max}")

        if violations:
            status = Status.FAIL if cfg.severity == "FAIL" else Status.WARN
            msg = f"{cfg.column}: " + "; ".join(violations)
        else:
            status = Status.PASS
            msg = (
                f"{cfg.column} within [{cfg.min}, {cfg.max}] "
                f"(actual [{actual_min}, {actual_max}])"
            )

        return CheckResult(
            check_type=self.check_type,
            column=cfg.column,
            status=status,
            metric_value=None,
            threshold=None,
            message=msg,
            severity=cfg.severity,
            details={"min": actual_min, "max": actual_max,
                     "expected_min": cfg.min, "expected_max": cfg.max},
        )
