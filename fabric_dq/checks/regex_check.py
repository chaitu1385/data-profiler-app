"""Regex / format check.

Uses ``rlike`` and samples up to 10 failing values for triage context. Runs
against the provided DataFrame (typically the sample, since this is a
distributional check).
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from fabric_dq.checks.base import Check, CheckResult, Status, classify
from fabric_dq.config import RegexCheckCfg

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class RegexCheck(Check):
    check_type = "regex"

    def run(self, cfg: RegexCheckCfg, df: "DataFrame") -> CheckResult:
        from pyspark.sql import functions as F

        col = F.col(cfg.column)
        agg = df.agg(
            F.count(col).alias("non_null"),
            F.sum(F.when(col.isNotNull() & ~col.rlike(cfg.pattern), 1).otherwise(0)).alias("bad"),
        ).collect()[0]
        non_null = int(agg["non_null"] or 0)
        bad = int(agg["bad"] or 0)
        bad_pct = (bad / non_null) if non_null else 0.0

        status = classify(bad_pct, cfg.max_fail_pct, cfg.severity)

        examples = []
        if bad > 0:
            examples = [
                r[0] for r in
                df.filter(col.isNotNull() & ~col.rlike(cfg.pattern))
                  .select(col).limit(10).collect()
            ]

        return CheckResult(
            check_type=self.check_type,
            column=cfg.column,
            status=status,
            metric_value=bad_pct,
            threshold=cfg.max_fail_pct,
            message=(
                f"{cfg.column}: {bad}/{non_null} fail pattern ({bad_pct:.4f}), "
                f"threshold={cfg.max_fail_pct:.4f}"
            ),
            severity=cfg.severity,
            details={"pattern": cfg.pattern, "examples": examples},
        )
