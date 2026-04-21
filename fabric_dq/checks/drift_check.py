"""Distribution-drift check against historical profile runs.

Reads the ``dq_profile_runs`` Delta table, pulls the last N baseline values
for ``(table, column, metric)``, and flags percent-delta beyond the threshold.
Emits a WARN (not FAIL) when insufficient baseline exists.
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Dict, Optional

from fabric_dq.checks.base import Check, CheckResult, Severity, Status
from fabric_dq.config import DriftCheckCfg

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class DriftCheck(Check):
    check_type = "drift"

    def run(
        self,
        cfg: DriftCheckCfg,
        table: str,
        current_profile: Dict[str, Dict[str, Any]],
        spark: "SparkSession",
        runs_table: str,
    ) -> CheckResult:
        col_metrics = current_profile.get(cfg.column) or {}
        current_value = col_metrics.get(cfg.metric)
        if current_value is None:
            return CheckResult(
                check_type=self.check_type,
                column=cfg.column,
                status=Status.WARN,
                metric_value=None,
                threshold=cfg.max_pct_delta,
                message=f"metric {cfg.metric!r} not available for {cfg.column!r}",
                severity=cfg.severity,
            )

        baselines = _load_baselines(spark, runs_table, table, cfg.column, cfg.metric, cfg.n)
        if len(baselines) < cfg.n:
            return CheckResult(
                check_type=self.check_type,
                column=cfg.column,
                status=Status.WARN,
                metric_value=float(current_value),
                threshold=cfg.max_pct_delta,
                message=(
                    f"insufficient baseline for drift: have {len(baselines)} runs, "
                    f"need {cfg.n}"
                ),
                severity=cfg.severity,
                details={"baselines": baselines},
            )

        baseline_mean = sum(baselines) / len(baselines)
        if baseline_mean == 0:
            pct_delta = 0.0 if current_value == 0 else float("inf")
        else:
            pct_delta = abs((current_value - baseline_mean) / baseline_mean)

        if pct_delta > cfg.max_pct_delta:
            status = Status.FAIL if cfg.severity == Severity.FAIL else Status.WARN
        else:
            status = Status.PASS

        return CheckResult(
            check_type=self.check_type,
            column=cfg.column,
            status=status,
            metric_value=pct_delta,
            threshold=cfg.max_pct_delta,
            message=(
                f"{cfg.column}.{cfg.metric}: current={current_value}, "
                f"baseline_mean(last {cfg.n})={baseline_mean:.4g}, "
                f"pct_delta={pct_delta:.4f}, threshold={cfg.max_pct_delta:.4f}"
            ),
            severity=cfg.severity,
            details={"baseline_mean": baseline_mean, "baselines": baselines,
                     "current": float(current_value)},
        )


def _load_baselines(
    spark: "SparkSession",
    runs_table: str,
    table: str,
    column: str,
    metric: str,
    n: int,
) -> list:
    """Pull the last ``n`` baseline metric values for ``(table, column, metric)``.

    Returns an empty list if the runs table does not exist (first-ever run).
    """
    try:
        df = spark.table(runs_table)
    except Exception:
        return []

    rows = (
        df.filter(f"table = '{table}'")
          .orderBy("run_ts", ascending=False)
          .limit(n)
          .select("profile_json")
          .collect()
    )
    values = []
    for r in rows:
        try:
            profile = json.loads(r["profile_json"])
            v = (profile.get(column) or {}).get(metric)
            if v is not None:
                values.append(float(v))
        except Exception:  # noqa: BLE001
            continue
    return values
