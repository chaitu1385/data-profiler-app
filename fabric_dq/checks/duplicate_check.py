"""Duplicate check. Runs on the full DataFrame (never a sample)."""
from __future__ import annotations

from typing import TYPE_CHECKING

from fabric_dq.checks.base import Check, CheckResult, Status, classify
from fabric_dq.config import DuplicateCheckCfg
from fabric_dq.profile.table_profiler import duplicate_stats

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DuplicateCheck(Check):
    check_type = "duplicate"

    def run(self, cfg: DuplicateCheckCfg, full_df: "DataFrame") -> CheckResult:
        stats = duplicate_stats(full_df, cfg.subset)
        total = stats["total"]
        dup_rows = stats["duplicate_rows"]
        dup_pct = (dup_rows / total) if total else 0.0
        status = classify(dup_pct, cfg.max_duplicate_pct, cfg.severity)
        return CheckResult(
            check_type=self.check_type,
            column=",".join(cfg.subset),
            status=status,
            metric_value=dup_pct,
            threshold=cfg.max_duplicate_pct,
            message=(
                f"subset={cfg.subset}: duplicate_rows={dup_rows} / {total} "
                f"({dup_pct:.4f}), threshold={cfg.max_duplicate_pct:.4f}"
            ),
            severity=cfg.severity,
            details={"duplicate_rows": dup_rows, "total": total},
        )
