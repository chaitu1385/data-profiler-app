"""End-to-end smoke test against a local SparkSession.

Builds a small DataFrame with known issues (null account_id, duplicate
account_id, out-of-range balance, bad email, stale timestamp) and asserts
that each check surfaces. Requires pyspark; skipped otherwise.
"""
import datetime as dt

import pytest


pytest.importorskip("pyspark")


def _seeded_df(spark):
    from pyspark.sql import Row
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType,
    )

    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("balance", DoubleType(), True),
        StructField("updated_at", TimestampType(), True),
    ])
    old = dt.datetime(2020, 1, 1, 0, 0, 0)
    now = dt.datetime.utcnow()

    rows = [
        Row("A1", "C1", "a@example.com",    100.0, now),
        Row("A1", "C1", "a@example.com",    100.0, now),  # duplicate account_id
        Row(None, "C2", "b@example.com",     50.0, now),  # null account_id
        Row("A3", "C3", "not-an-email",      10.0, now),  # bad email
        Row("A4", "C4", "c@example.com",   -10.0, now),   # below range
        Row("A5", "C5", "d@example.com",    20.0, old),   # stale
    ]
    return spark.createDataFrame(rows, schema)


def test_runner_surfaces_seeded_issues(spark, tmp_path):
    from fabric_dq import run_profile

    df = _seeded_df(spark)
    df.createOrReplaceTempView("silver_accounts")

    cfg = {
        "table": "silver_accounts",
        "sampling": {"strategy": "full"},
        "expected_schema": {
            "account_id": "string",
            "customer_id": "string",
            "email": "string",
            "balance": "double",
            "updated_at": "timestamp",
        },
        "checks": {
            "nulls": [
                {"column": "account_id", "max_null_pct": 0.0, "severity": "FAIL"},
            ],
            "duplicates": [
                {"subset": ["account_id"], "max_duplicate_pct": 0.0, "severity": "FAIL"},
            ],
            "ranges": [
                {"column": "balance", "min": 0, "max": 10000, "severity": "FAIL"},
            ],
            "regex": [
                {"column": "email",
                 "pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
                 "max_fail_pct": 0.0,
                 "severity": "FAIL"},
            ],
            "freshness": [
                {"column": "updated_at", "max_age_hours": 24, "severity": "WARN"},
            ],
        },
    }

    report = run_profile(config=cfg, spark=spark, sink=None)

    by_type = {}
    for r in report.check_results:
        by_type.setdefault(r.check_type, []).append(r)

    def assert_any_fail(check_type):
        assert check_type in by_type, f"no {check_type} check ran"
        assert any(r.status == "FAIL" for r in by_type[check_type]), (
            f"expected at least one FAIL for {check_type}, "
            f"got {[r.status for r in by_type[check_type]]}"
        )

    assert_any_fail("null")
    assert_any_fail("duplicate")
    assert_any_fail("range")
    assert_any_fail("regex")
    assert report.overall_status == "FAIL"

    md = report.summary_markdown()
    assert "DQ Report" in md
    assert "FAIL" in md
