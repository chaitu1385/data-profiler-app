"""Null-percentage check. Reads from the column profile; does not re-scan."""
from __future__ import annotations

from typing import Any, Dict

from fabric_dq.checks.base import Check, CheckResult, Status, classify
from fabric_dq.config import NullCheckCfg


class NullCheck(Check):
    check_type = "null"

    def run(self, cfg: NullCheckCfg, profile: Dict[str, Dict[str, Any]]) -> CheckResult:
        col_metrics = profile.get(cfg.column)
        if col_metrics is None:
            return CheckResult(
                check_type=self.check_type,
                column=cfg.column,
                status=Status.FAIL,
                metric_value=None,
                threshold=cfg.max_null_pct,
                message=f"column {cfg.column!r} not present in table",
                severity=cfg.severity,
            )
        null_pct = float(col_metrics.get("null_pct") or 0.0)
        status = classify(null_pct, cfg.max_null_pct, cfg.severity)
        msg = (
            f"{cfg.column}: null_pct={null_pct:.4f} "
            f"(threshold={cfg.max_null_pct:.4f})"
        )
        return CheckResult(
            check_type=self.check_type,
            column=cfg.column,
            status=status,
            metric_value=null_pct,
            threshold=cfg.max_null_pct,
            message=msg,
            severity=cfg.severity,
            details={"null_count": col_metrics.get("null_count")},
        )
