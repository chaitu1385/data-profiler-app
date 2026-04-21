"""Unit tests for individual check classes that don't need Spark.

Checks that consume the pre-built profile dict (null, range) can be tested
without a SparkSession — keeps feedback fast.
"""
import pytest

from fabric_dq.checks.base import Status, worst
from fabric_dq.checks.null_check import NullCheck
from fabric_dq.checks.range_check import RangeCheck
from fabric_dq.config import NullCheckCfg, RangeCheckCfg


def test_null_check_pass():
    profile = {"x": {"null_pct": 0.0, "null_count": 0}}
    r = NullCheck().run(NullCheckCfg(column="x", max_null_pct=0.0), profile)
    assert r.status == Status.PASS


def test_null_check_fail():
    profile = {"x": {"null_pct": 0.1, "null_count": 10}}
    r = NullCheck().run(NullCheckCfg(column="x", max_null_pct=0.0), profile)
    assert r.status == Status.FAIL
    assert r.metric_value == 0.1


def test_null_check_warn():
    profile = {"x": {"null_pct": 0.1, "null_count": 10}}
    r = NullCheck().run(
        NullCheckCfg(column="x", max_null_pct=0.0, severity="WARN"), profile,
    )
    assert r.status == Status.WARN


def test_null_check_missing_column():
    r = NullCheck().run(NullCheckCfg(column="nope"), profile={})
    assert r.status == Status.FAIL
    assert "not present" in r.message


def test_range_check_pass():
    profile = {"x": {"min": 0, "max": 10}}
    r = RangeCheck().run(RangeCheckCfg(column="x", min=0, max=10), profile)
    assert r.status == Status.PASS


def test_range_check_fail():
    profile = {"x": {"min": -1, "max": 100}}
    r = RangeCheck().run(RangeCheckCfg(column="x", min=0, max=10), profile)
    assert r.status == Status.FAIL
    assert "min=-1" in r.message
    assert "max=100" in r.message


def test_worst_status():
    assert worst("PASS", "WARN", "FAIL") == "FAIL"
    assert worst("PASS", "WARN") == "WARN"
    assert worst("PASS") == "PASS"
    assert worst() == "PASS"
