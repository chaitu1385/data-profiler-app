"""Single-pass column profiling.

Builds one ``df.agg(*exprs)`` expression covering every column and collects
all metrics in a single Spark action. Per-column ``.agg()`` calls are the
most expensive thing you can do on wide tables, so we avoid them.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


_NUMERIC_TYPES = {"byte", "short", "int", "integer", "long", "bigint", "float", "double", "decimal"}
_TEMPORAL_TYPES = {"date", "timestamp", "timestamp_ntz"}


def profile_columns(df: "DataFrame") -> Dict[str, Dict[str, Any]]:
    """Return ``{column_name: {metric: value, ...}}`` for every column in *df*.

    Metrics by type:
        numeric:  count, null_count, null_pct, distinct_approx, min, max,
                  mean, stddev, p25, p50, p75
        string:   count, null_count, null_pct, distinct_approx, min_len, max_len, avg_len
        temporal: count, null_count, null_pct, distinct_approx, min, max
        boolean:  count, null_count, null_pct, true_count, false_count
    """
    from pyspark.sql import functions as F

    schema = df.schema
    exprs: List = []
    aliases: List[tuple] = []  # (col_name, metric, alias)

    def add(col: str, metric: str, expr) -> None:
        alias = f"__{col}__{metric}__"
        aliases.append((col, metric, alias))
        exprs.append(expr.alias(alias))

    total_rows_alias = "__total_rows__"
    exprs.append(F.count(F.lit(1)).alias(total_rows_alias))

    for field in schema.fields:
        name = field.name
        dtype = field.dataType.simpleString().split("(")[0]  # strip decimal(10,2) etc.

        add(name, "count", F.count(F.col(name)))
        add(name, "null_count", F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)))
        add(name, "distinct_approx", F.approx_count_distinct(F.col(name)))

        if dtype in _NUMERIC_TYPES:
            add(name, "min", F.min(F.col(name)))
            add(name, "max", F.max(F.col(name)))
            add(name, "mean", F.avg(F.col(name)))
            add(name, "stddev", F.stddev(F.col(name)))
            add(name, "p25", F.expr(f"percentile_approx(`{name}`, 0.25)"))
            add(name, "p50", F.expr(f"percentile_approx(`{name}`, 0.50)"))
            add(name, "p75", F.expr(f"percentile_approx(`{name}`, 0.75)"))
        elif dtype == "string":
            add(name, "min_len", F.min(F.length(F.col(name))))
            add(name, "max_len", F.max(F.length(F.col(name))))
            add(name, "avg_len", F.avg(F.length(F.col(name))))
        elif dtype in _TEMPORAL_TYPES:
            add(name, "min", F.min(F.col(name)))
            add(name, "max", F.max(F.col(name)))
        elif dtype == "boolean":
            add(name, "true_count", F.sum(F.when(F.col(name) == True, 1).otherwise(0)))  # noqa: E712
            add(name, "false_count", F.sum(F.when(F.col(name) == False, 1).otherwise(0)))  # noqa: E712

    row = df.agg(*exprs).collect()[0].asDict()
    total = row[total_rows_alias] or 0

    result: Dict[str, Dict[str, Any]] = {}
    for col, metric, alias in aliases:
        result.setdefault(col, {})
        value = row[alias]
        result[col][metric] = value

    # Derived metrics that depend on totals.
    for col, metrics in result.items():
        nulls = metrics.get("null_count") or 0
        metrics["null_pct"] = (nulls / total) if total else 0.0
        metrics["row_count"] = total
        metrics["dtype"] = next(
            (f.dataType.simpleString() for f in schema.fields if f.name == col), None
        )

    return result
