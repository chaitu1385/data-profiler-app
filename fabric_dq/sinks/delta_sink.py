"""Delta-table sink for profile + check results.

Schema (append-only; adding a column here is a breaking change):

``dq_profile_runs``
    run_id STRING, table STRING, run_ts TIMESTAMP, row_count BIGINT,
    sample_pct DOUBLE, config_hash STRING, schema_fingerprint STRING,
    profile_json STRING

``dq_check_results``
    run_id STRING, table STRING, run_ts TIMESTAMP, check_type STRING,
    column STRING, status STRING, severity STRING, metric_value DOUBLE,
    threshold DOUBLE, message STRING, details STRING
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from fabric_dq.checks.base import CheckResult
    from fabric_dq.sampling import SampleMeta


DEFAULT_RUNS_TABLE = "dq_profile_runs"
DEFAULT_RESULTS_TABLE = "dq_check_results"


@dataclass
class DeltaSink:
    spark: "SparkSession"
    runs_table: str = DEFAULT_RUNS_TABLE
    results_table: str = DEFAULT_RESULTS_TABLE

    def write(
        self,
        *,
        run_id: str,
        table: str,
        row_count: int,
        sample_meta: "SampleMeta",
        config_hash: str,
        schema_fingerprint: str,
        profile: Dict[str, Dict[str, Any]],
        check_results: Iterable["CheckResult"],
    ) -> None:
        from pyspark.sql import Row, functions as F

        run_ts = F.current_timestamp()

        runs_row = [Row(
            run_id=run_id,
            table=table,
            row_count=int(row_count),
            sample_pct=float(sample_meta.fraction),
            config_hash=config_hash,
            schema_fingerprint=schema_fingerprint,
            profile_json=json.dumps(profile, default=str),
        )]
        runs_df = self.spark.createDataFrame(runs_row).withColumn("run_ts", run_ts) \
            .select("run_id", "table", "run_ts", "row_count", "sample_pct",
                    "config_hash", "schema_fingerprint", "profile_json")
        _append(runs_df, self.runs_table)

        rows: List[Row] = []
        for r in check_results:
            d = r.to_row()
            rows.append(Row(
                run_id=run_id,
                table=table,
                check_type=d["check_type"],
                column=d["column"],
                status=d["status"],
                severity=d["severity"],
                metric_value=d["metric_value"],
                threshold=d["threshold"],
                message=d["message"],
                details=d["details"],
            ))
        if rows:
            results_df = self.spark.createDataFrame(rows).withColumn("run_ts", run_ts) \
                .select("run_id", "table", "run_ts", "check_type", "column",
                        "status", "severity", "metric_value", "threshold",
                        "message", "details")
            _append(results_df, self.results_table)


def _append(df, table: str) -> None:
    """Append to Delta if available, fall back to table save otherwise.

    Fabric Lakehouses expose Delta by default; the fallback path is only here
    so unit tests can run against a local Spark without delta-spark installed.
    """
    writer = df.write.mode("append")
    try:
        writer.format("delta").saveAsTable(table)
    except Exception:
        writer.saveAsTable(table)
