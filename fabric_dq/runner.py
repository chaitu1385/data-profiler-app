"""Orchestrator: the one function notebooks call.

Flow:
    load_config -> read table -> pick_sample -> profile columns ->
    run all configured checks -> build DQReport -> persist via sink
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from fabric_dq.checks.base import CheckResult
from fabric_dq.checks.drift_check import DriftCheck
from fabric_dq.checks.duplicate_check import DuplicateCheck
from fabric_dq.checks.fk_check import FKCheck
from fabric_dq.checks.freshness_check import FreshnessCheck
from fabric_dq.checks.null_check import NullCheck
from fabric_dq.checks.range_check import RangeCheck
from fabric_dq.checks.regex_check import RegexCheck
from fabric_dq.checks.schema_check import SchemaCheck
from fabric_dq.config import Config, ConfigLike, load_config
from fabric_dq.profile.column_profiler import profile_columns
from fabric_dq.profile.table_profiler import profile_table
from fabric_dq.reporting.summary import DQReport
from fabric_dq.sampling import pick_sample
from fabric_dq.sinks.delta_sink import DEFAULT_RESULTS_TABLE, DEFAULT_RUNS_TABLE, DeltaSink

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def run_profile(
    table: Optional[str] = None,
    config: Optional[ConfigLike] = None,
    *,
    run_id: Optional[str] = None,
    sink: Optional[str] = "delta",
    spark: Optional["SparkSession"] = None,
    runs_table: str = DEFAULT_RUNS_TABLE,
    results_table: str = DEFAULT_RESULTS_TABLE,
) -> DQReport:
    """Profile *table* and run all configured checks.

    Either ``table`` or ``config`` must be supplied. If both are given and the
    config also has a ``table`` key, ``table`` argument wins.
    """
    if config is None and table is None:
        raise ValueError("run_profile requires either `table` or `config`")

    cfg: Config = _resolve_config(table, config)
    spark = spark or _active_spark()
    run_id = run_id or uuid.uuid4().hex

    full_df = spark.table(cfg.table)
    sample_df, full_df, sample_meta = pick_sample(full_df, cfg.sampling)

    table_prof = profile_table(full_df, row_count=sample_meta.row_count)
    col_profile = profile_columns(sample_df)

    results: List[CheckResult] = []

    if cfg.expected_schema:
        results.extend(SchemaCheck().run(cfg.expected_schema, full_df))

    for nc in cfg.nulls:
        results.append(NullCheck().run(nc, col_profile))

    for dc in cfg.duplicates:
        results.append(DuplicateCheck().run(dc, full_df))

    for rc in cfg.ranges:
        results.append(RangeCheck().run(rc, col_profile))

    for rex in cfg.regex:
        results.append(RegexCheck().run(rex, sample_df))

    for fk in cfg.foreign_keys:
        results.append(FKCheck().run(fk, full_df, spark))

    for fr in cfg.freshness:
        results.append(FreshnessCheck().run(fr, full_df))

    for dr in cfg.drift:
        results.append(DriftCheck().run(dr, cfg.table, col_profile, spark, runs_table))

    report = DQReport(
        run_id=run_id,
        table=cfg.table,
        row_count=sample_meta.row_count,
        sample_pct=sample_meta.fraction,
        profile=col_profile,
        check_results=results,
    )

    if sink == "delta":
        DeltaSink(spark=spark, runs_table=runs_table, results_table=results_table).write(
            run_id=run_id,
            table=cfg.table,
            row_count=sample_meta.row_count,
            sample_meta=sample_meta,
            config_hash=_config_hash(cfg),
            schema_fingerprint=table_prof.schema_fingerprint,
            profile=col_profile,
            check_results=results,
        )

    return report


def _resolve_config(table: Optional[str], config: Optional[ConfigLike]) -> Config:
    if config is None:
        return Config(table=table)  # defaults everywhere else
    cfg = load_config(config)
    if table:
        cfg.table = table
    return cfg


def _active_spark() -> "SparkSession":
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark


def _config_hash(cfg: Config) -> str:
    blob = json.dumps(_to_serializable(cfg), sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]


def _to_serializable(obj: Any) -> Any:
    if hasattr(obj, "__dataclass_fields__"):
        return {k: _to_serializable(v) for k, v in asdict(obj).items()}
    if isinstance(obj, (list, tuple)):
        return [_to_serializable(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _to_serializable(v) for k, v in obj.items()}
    return obj
