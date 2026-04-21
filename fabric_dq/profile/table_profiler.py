"""Table-level profiling: row count, duplicate count, schema fingerprint."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class TableProfile:
    row_count: int
    schema: Dict[str, str]
    schema_fingerprint: str


def profile_table(df: "DataFrame", row_count: Optional[int] = None) -> TableProfile:
    if row_count is None:
        row_count = df.count()
    schema = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    fingerprint = hashlib.sha256(
        json.dumps(schema, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    return TableProfile(row_count=row_count, schema=schema, schema_fingerprint=fingerprint)


def duplicate_stats(df: "DataFrame", subset: List[str]) -> Dict[str, int]:
    """Return ``{'total': N, 'duplicate_rows': D}`` where D is the number of
    rows that are part of a duplicate group (group_size - 1 summed across groups).

    Always runs on the full DataFrame: duplicate counts from a sample are not
    meaningful.
    """
    from pyspark.sql import functions as F

    grouped = df.groupBy(*subset).agg(F.count(F.lit(1)).alias("__n__"))
    agg = grouped.agg(
        F.sum("__n__").alias("total"),
        F.sum(F.when(F.col("__n__") > 1, F.col("__n__") - 1).otherwise(0)).alias("duplicate_rows"),
    ).collect()[0]
    return {
        "total": int(agg["total"] or 0),
        "duplicate_rows": int(agg["duplicate_rows"] or 0),
    }
