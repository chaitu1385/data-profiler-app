# fabric_dq

A generic, config-driven data quality & profiling framework for Azure
Fabric notebooks. Point it at a Lakehouse table and a config, get a
column profile, per-check PASS/WARN/FAIL results, and append-only Delta
history for trend queries and drift baselines.

Built to replace ad-hoc per-notebook checks that let duplicate accounts,
null fields, and silent type drift leak through data → science handoffs.

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
   (duplicates, FKs) always run on the full DataFrame — sampling them
   would silently underreport issues.
2. **Single-pass profiling.** All column metrics are collected in one
   `df.agg(...)` action per sample.
3. **Never raises.** Checks return PASS / WARN / FAIL; the caller decides
   whether to gate downstream work.
4. **Append-only Delta.** `dq_profile_runs` + `dq_check_results` are the
   source of truth for trend queries and the drift check's baseline.

## Repo layout

```
fabric_dq/            # the package
  runner.py           # run_profile() entrypoint
  config.py           # dataclass schema + YAML/JSON/dict loader
  sampling.py
  profile/            # column_profiler, table_profiler
  checks/             # null, duplicate, schema, range, regex, fk,
                      # freshness, drift
  sinks/delta_sink.py
  reporting/summary.py
  configs/
    example_accounts.yaml
notebooks/
  01_run_profile.py   # Fabric notebook entry point
  02_view_results.py  # Delta-history queries for trend + triage
tests/
  test_config.py
  test_checks.py
  test_runner_smoke.py
```

See [`fabric_dq/README.md`](fabric_dq/README.md) for detailed package
documentation and the "how to add a new check" workflow.

## Testing

```bash
pytest tests/ -v
```

Config tests and Spark-free check unit tests run in plain CI. The runner
smoke test is gated on `pyspark` being importable — install pyspark
locally to run it, otherwise let Fabric serve as the integration
environment.

## License

This project is provided as-is for internal use.
