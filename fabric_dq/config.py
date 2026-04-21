"""Config schema + loader for fabric_dq.

A config can be supplied as a Python dict, a YAML file path, or a JSON file
path. Unknown keys raise so typos fail loudly.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, fields
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union


ConfigLike = Union[str, Mapping[str, Any]]


_ALLOWED_SAMPLING_STRATEGIES = {"fraction", "rows", "full", "aggregate_only"}
_ALLOWED_SEVERITIES = {"WARN", "FAIL"}


@dataclass
class SamplingConfig:
    strategy: str = "fraction"
    fraction: float = 0.05
    rows: Optional[int] = None
    seed: int = 42
    min_rows: int = 100_000

    def __post_init__(self) -> None:
        if self.strategy not in _ALLOWED_SAMPLING_STRATEGIES:
            raise ValueError(
                f"sampling.strategy must be one of {_ALLOWED_SAMPLING_STRATEGIES}, "
                f"got {self.strategy!r}"
            )
        if self.strategy == "fraction" and not 0 < self.fraction <= 1.0:
            raise ValueError("sampling.fraction must be in (0, 1]")
        if self.strategy == "rows" and (self.rows is None or self.rows <= 0):
            raise ValueError("sampling.rows must be a positive int when strategy=rows")


@dataclass
class NullCheckCfg:
    column: str
    max_null_pct: float = 0.0
    severity: str = "FAIL"


@dataclass
class DuplicateCheckCfg:
    subset: List[str]
    max_duplicate_pct: float = 0.0
    severity: str = "FAIL"


@dataclass
class RangeCheckCfg:
    column: str
    min: Optional[float] = None
    max: Optional[float] = None
    severity: str = "FAIL"


@dataclass
class RegexCheckCfg:
    column: str
    pattern: str
    max_fail_pct: float = 0.0
    severity: str = "FAIL"


@dataclass
class FKCheckCfg:
    column: str
    ref_table: str
    ref_column: str
    max_orphan_pct: float = 0.0
    severity: str = "FAIL"


@dataclass
class FreshnessCheckCfg:
    column: str
    max_age_hours: float
    severity: str = "FAIL"


@dataclass
class DriftCheckCfg:
    column: str
    metric: str = "mean"  # mean | null_pct | distinct_count
    baseline: str = "last_n_runs"
    n: int = 5
    max_pct_delta: float = 0.20
    severity: str = "WARN"


@dataclass
class Thresholds:
    warn_default: float = 0.01
    fail_default: float = 0.05


@dataclass
class Config:
    table: str
    sampling: SamplingConfig = field(default_factory=SamplingConfig)
    expected_schema: Dict[str, str] = field(default_factory=dict)
    nulls: List[NullCheckCfg] = field(default_factory=list)
    duplicates: List[DuplicateCheckCfg] = field(default_factory=list)
    ranges: List[RangeCheckCfg] = field(default_factory=list)
    regex: List[RegexCheckCfg] = field(default_factory=list)
    foreign_keys: List[FKCheckCfg] = field(default_factory=list)
    freshness: List[FreshnessCheckCfg] = field(default_factory=list)
    drift: List[DriftCheckCfg] = field(default_factory=list)
    thresholds: Thresholds = field(default_factory=Thresholds)
    output: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.table or not isinstance(self.table, str):
            raise ValueError("config.table must be a non-empty string")
        for c in self.nulls + self.duplicates + self.ranges + self.regex \
                + self.foreign_keys + self.freshness + self.drift:
            if c.severity not in _ALLOWED_SEVERITIES:
                raise ValueError(
                    f"severity must be one of {_ALLOWED_SEVERITIES}, got {c.severity!r}"
                )


_TOP_LEVEL_KEYS = {f.name for f in fields(Config)} | {"checks"}


def load_config(source: ConfigLike) -> Config:
    """Load a Config from a dict, YAML path, or JSON path."""
    if isinstance(source, Mapping):
        raw: Dict[str, Any] = dict(source)
    elif isinstance(source, str):
        raw = _read_file(source)
    else:
        raise TypeError(f"Unsupported config source type: {type(source).__name__}")

    _reject_unknown_keys(raw, _TOP_LEVEL_KEYS, path="<root>")

    # `checks` block is flattened into the top-level Config dataclass.
    checks = raw.pop("checks", {}) or {}
    if not isinstance(checks, Mapping):
        raise ValueError("config.checks must be a mapping")

    sampling = _build_dc(SamplingConfig, raw.get("sampling") or {}, "sampling")
    thresholds = _build_dc(Thresholds, raw.get("thresholds") or {}, "thresholds")

    null_cfgs = [_build_dc(NullCheckCfg, x, "checks.nulls[]") for x in checks.get("nulls", []) or []]
    dup_cfgs = [_build_dc(DuplicateCheckCfg, x, "checks.duplicates[]") for x in checks.get("duplicates", []) or []]
    range_cfgs = [_build_dc(RangeCheckCfg, x, "checks.ranges[]") for x in checks.get("ranges", []) or []]
    regex_cfgs = [_build_dc(RegexCheckCfg, x, "checks.regex[]") for x in checks.get("regex", []) or []]
    fk_cfgs = [_build_dc(FKCheckCfg, x, "checks.foreign_keys[]") for x in checks.get("foreign_keys", []) or []]
    fresh_cfgs = [_build_dc(FreshnessCheckCfg, x, "checks.freshness[]") for x in checks.get("freshness", []) or []]
    drift_cfgs = [_build_dc(DriftCheckCfg, x, "checks.drift[]") for x in checks.get("drift", []) or []]

    unknown_checks = set(checks) - {
        "nulls", "duplicates", "ranges", "regex",
        "foreign_keys", "freshness", "drift",
    }
    if unknown_checks:
        raise ValueError(f"Unknown check types in config.checks: {sorted(unknown_checks)}")

    return Config(
        table=raw.get("table"),
        sampling=sampling,
        expected_schema=dict(raw.get("expected_schema") or {}),
        nulls=null_cfgs,
        duplicates=dup_cfgs,
        ranges=range_cfgs,
        regex=regex_cfgs,
        foreign_keys=fk_cfgs,
        freshness=fresh_cfgs,
        drift=drift_cfgs,
        thresholds=thresholds,
        output=dict(raw.get("output") or {}),
    )


def _read_file(path: str) -> Dict[str, Any]:
    _, ext = os.path.splitext(path.lower())
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()
    if ext in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore
        except ImportError as e:
            raise ImportError("PyYAML is required to load YAML configs") from e
        return yaml.safe_load(text) or {}
    if ext == ".json":
        return json.loads(text)
    raise ValueError(f"Unsupported config file extension: {ext}")


def _build_dc(cls, data: Mapping[str, Any], path: str):
    if not isinstance(data, Mapping):
        raise ValueError(f"{path} must be a mapping, got {type(data).__name__}")
    allowed = {f.name for f in fields(cls)}
    _reject_unknown_keys(data, allowed, path=path)
    return cls(**{k: v for k, v in data.items() if k in allowed})


def _reject_unknown_keys(data: Mapping[str, Any], allowed: set, *, path: str) -> None:
    unknown = set(data) - allowed
    if unknown:
        raise ValueError(f"Unknown keys in {path}: {sorted(unknown)}")
