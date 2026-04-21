"""Config loader tests — no Spark needed, so they run in plain CI."""
import json
import os
import tempfile

import pytest

from fabric_dq.config import Config, load_config


def test_minimal_dict_config():
    cfg = load_config({"table": "db.schema.t"})
    assert isinstance(cfg, Config)
    assert cfg.table == "db.schema.t"
    assert cfg.sampling.strategy == "fraction"
    assert cfg.nulls == []


def test_full_dict_config():
    cfg = load_config({
        "table": "db.t",
        "sampling": {"strategy": "fraction", "fraction": 0.1},
        "expected_schema": {"id": "string"},
        "checks": {
            "nulls": [{"column": "id", "max_null_pct": 0.0, "severity": "FAIL"}],
            "duplicates": [{"subset": ["id"], "max_duplicate_pct": 0.0}],
            "ranges": [{"column": "x", "min": 0, "max": 10}],
            "regex": [{"column": "email", "pattern": "@"}],
            "foreign_keys": [{"column": "cid", "ref_table": "db.c", "ref_column": "id"}],
            "freshness": [{"column": "ts", "max_age_hours": 24}],
            "drift": [{"column": "x", "metric": "mean", "n": 3, "max_pct_delta": 0.1}],
        },
    })
    assert len(cfg.nulls) == 1 and cfg.nulls[0].column == "id"
    assert cfg.duplicates[0].subset == ["id"]
    assert cfg.foreign_keys[0].ref_table == "db.c"
    assert cfg.drift[0].n == 3


def test_missing_table_raises():
    with pytest.raises(ValueError, match="table"):
        load_config({})


def test_unknown_top_level_key_raises():
    with pytest.raises(ValueError, match="Unknown keys"):
        load_config({"table": "t", "bogus": 1})


def test_unknown_check_type_raises():
    with pytest.raises(ValueError, match="Unknown check types"):
        load_config({"table": "t", "checks": {"made_up": []}})


def test_unknown_check_field_raises():
    with pytest.raises(ValueError, match="Unknown keys"):
        load_config({"table": "t", "checks": {"nulls": [{"column": "x", "oops": 1}]}})


def test_invalid_sampling_strategy_raises():
    with pytest.raises(ValueError, match="strategy"):
        load_config({"table": "t", "sampling": {"strategy": "made_up"}})


def test_invalid_severity_raises():
    with pytest.raises(ValueError, match="severity"):
        load_config({
            "table": "t",
            "checks": {"nulls": [{"column": "x", "severity": "LOUD"}]},
        })


def test_json_file_roundtrip(tmp_path):
    p = tmp_path / "c.json"
    p.write_text(json.dumps({"table": "db.t"}))
    cfg = load_config(str(p))
    assert cfg.table == "db.t"


def test_yaml_file_roundtrip(tmp_path):
    yaml = pytest.importorskip("yaml")  # noqa: F841
    p = tmp_path / "c.yaml"
    p.write_text("table: db.t\nsampling:\n  strategy: full\n")
    cfg = load_config(str(p))
    assert cfg.sampling.strategy == "full"
