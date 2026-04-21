"""Freshness / recency check: ``max(col)`` vs. ``current_timestamp``."""
from __future__ import annotations

from typing import TYPE_CHECKING

from fabric_dq.checks.base import Check, CheckResult, Status, classify
from fabric_dq.config import FreshnessCheckCfg

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class FreshnessCheck(Check):
    check_type = "freshness"

    def run(self, cfg: FreshnessCheckCfg, df: "DataFrame") -> CheckResult:
        from pyspark.sql import functions as F

        row = df.agg(
            (
                (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max(F.col(cfg.column))))
                / 3600.0
            ).alias("age_hours"),
            F.max(F.col(cfg.column)).alias("max_ts"),
        ).collect()[0]
        age_hours = float(row["age_hours"]) if row["age_hours"] is not None else float("inf")

        status = classify(age_hours, cfg.max_age_hours, cfg.severity)
        return CheckResult(
            check_type=self.check_type,
            column=cfg.column,
            status=status,
            metric_value=age_hours,
            threshold=cfg.max_age_hours,
            message=(
                f"{cfg.column}: max={row['max_ts']}, age={age_hours:.2f}h, "
                f"threshold={cfg.max_age_hours:.2f}h"
            ),
            severity=cfg.severity,
            details={"max_ts": str(row["max_ts"])},
        )
