# fabric_dq — Data Quality & Profiling Framework for Azure Fabric

Config-driven PySpark profiling + quality checks, designed to run inside
Fabric notebooks against Lakehouse tables. Results are persisted to
append-only Delta tables so they're queryable, trendable, and easy to share
with the science team.

## Why

Recurring issues in data → science handoffs — duplicate accounts, null
fields, silent type drift — cost both teams days of triage. This framework
replaces ad-hoc per-notebook checks with one reusable entrypoint.

## Quick start

```python
from fabric_dq import run_profile

report = run_profile(
    config="/lakehouse/default/Files/dq/configs/accounts.yaml",
)
print(report.summary_markdown())
# report.overall_status -> "PASS" | "WARN" | "FAIL"
```

Or pass a dict for ad-hoc use:

```python
report = run_profile(config={
    "table": "lakehouse.silver.accounts",
    "sampling": {"strategy": "fraction", "fraction": 0.05},
    "checks": {
        "nulls": [{"column": "account_id", "max_null_pct": 0.0}],
        "duplicates": [{"subset": ["account_id"]}],
    },
})
```

## Built-in checks

| Check | Purpose | Runs on |
|---|---|---|
| `nulls` | Column null % vs threshold | profile (any sample) |
| `duplicates` | Duplicate rows for key subset | full DF |
| `schema` | Expected columns + dtypes | full DF |
| `ranges` | Numeric min/max bounds | profile |
| `regex` | Column value format | sample |
| `foreign_keys` | Orphan rows vs. reference table | full DF |
| `freshness` | `max(ts)` vs. now | full DF |
| `drift` | Metric vs. last N historical runs | profile + Delta history |

## Design invariants

1. **Sampling for distribution stats only.** Row-count-dependent checks
   (duplicates, FKs) always run on the full DataFrame — sampling them would
   silently underreport issues.
2. **Single-pass profiling.** All column metrics are collected in one
   `df.agg(...)` action. Adding a per-column `.agg()` to the hot path is the
   biggest perf regression you can make.
3. **Never raise.** Checks return PASS / WARN / FAIL. The caller decides
   whether to gate downstream pipelines.
4. **Append-only Delta.** `dq_profile_runs` + `dq_check_results` are the
   source of truth for trend queries and the drift check's baseline.

## Adding a new check

1. Add a config dataclass in `fabric_dq/config.py`.
2. Add a loader line to `load_config()` so YAML/dict configs accept it.
3. Create `fabric_dq/checks/<name>_check.py` subclassing `Check`, returning
   one or more `CheckResult`.
4. Wire it into `runner.py` alongside the existing checks.

## Testing

```bash
pytest tests/ -v
```

Config tests and Spark-free check unit tests run in plain CI. The runner
smoke test is gated on `pyspark` being importable — install pyspark
locally to run it, otherwise let Fabric serve as the integration environment.
