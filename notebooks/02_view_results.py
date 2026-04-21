# Fabric notebook: query DQ result history for trends and triage.

# CELL ----------------------------------------------------------------------
# Latest run per table.
display(spark.sql("""
    SELECT table, MAX(run_ts) AS latest_run, COUNT(*) AS total_runs
    FROM dq_profile_runs
    GROUP BY table
    ORDER BY latest_run DESC
"""))

# CELL ----------------------------------------------------------------------
# All FAIL results from the last 7 days.
display(spark.sql("""
    SELECT run_ts, table, check_type, column, metric_value, threshold, message
    FROM dq_check_results
    WHERE status = 'FAIL'
      AND run_ts >= current_timestamp() - INTERVAL 7 DAYS
    ORDER BY run_ts DESC
"""))

# CELL ----------------------------------------------------------------------
# Drift trend for balance.mean over last 30 runs.
display(spark.sql("""
    SELECT run_ts,
           get_json_object(profile_json, '$.balance.mean') AS balance_mean,
           get_json_object(profile_json, '$.balance.null_pct') AS balance_null_pct
    FROM dq_profile_runs
    WHERE table = 'lakehouse.silver.accounts'
    ORDER BY run_ts DESC
    LIMIT 30
"""))
