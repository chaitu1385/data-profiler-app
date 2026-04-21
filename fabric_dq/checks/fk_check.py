"""Foreign-key / referential-integrity check.

Uses ``LEFT ANTI JOIN`` so Spark's Catalyst can push through a broadcast hash
join when the reference table fits in ``spark.sql.autoBroadcastJoinThreshold``.
Samples up to 10 orphan key values for triage.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from fabric_dq.checks.base import Check, CheckResult, Status, classify
from fabric_dq.config import FKCheckCfg

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame


class FKCheck(Check):
    check_type = "foreign_key"

    def run(self, cfg: FKCheckCfg, full_df: "DataFrame", spark: "SparkSession") -> CheckResult:
        from pyspark.sql import functions as F

        try:
            ref = spark.table(cfg.ref_table).select(F.col(cfg.ref_column).alias("__ref__"))
        except Exception as e:  # noqa: BLE001
            return CheckResult(
                check_type=self.check_type,
                column=cfg.column,
                status=Status.FAIL,
                metric_value=None,
                threshold=cfg.max_orphan_pct,
                message=f"could not read ref table {cfg.ref_table!r}: {e}",
                severity=cfg.severity,
            )

        left = full_df.select(F.col(cfg.column).alias("__fk__")).filter(F.col("__fk__").isNotNull())
        total = left.count()
        orphans_df = left.join(ref, left["__fk__"] == ref["__ref__"], how="left_anti")
        orphan_count = orphans_df.count()
        orphan_pct = (orphan_count / total) if total else 0.0

        status = classify(orphan_pct, cfg.max_orphan_pct, cfg.severity)
        samples = [r[0] for r in orphans_df.limit(10).collect()]

        return CheckResult(
            check_type=self.check_type,
            column=cfg.column,
            status=status,
            metric_value=orphan_pct,
            threshold=cfg.max_orphan_pct,
            message=(
                f"{cfg.column} -> {cfg.ref_table}.{cfg.ref_column}: "
                f"{orphan_count}/{total} orphans ({orphan_pct:.4f}), "
                f"threshold={cfg.max_orphan_pct:.4f}"
            ),
            severity=cfg.severity,
            details={"orphan_examples": samples, "ref_table": cfg.ref_table},
        )
