# Fabric notebook: run the DQ profile against a silver-layer table.
#
# Attach a Lakehouse that contains the target table and upload the
# `fabric_dq` package + `configs/example_accounts.yaml` to the attached
# Lakehouse Files/ path.
#
# In a Fabric notebook, each `# CELL` marker below becomes a separate cell.

# CELL ----------------------------------------------------------------------
# If the package isn't installed, add the Lakehouse Files path to sys.path.
import sys
sys.path.insert(0, "/lakehouse/default/Files/dq")

from fabric_dq import run_profile

# CELL ----------------------------------------------------------------------
report = run_profile(
    config="/lakehouse/default/Files/dq/configs/example_accounts.yaml",
)

# CELL ----------------------------------------------------------------------
print(report.summary_markdown())

# CELL ----------------------------------------------------------------------
# Also render the results as a DataFrame for Fabric's `display()` widget.
from pyspark.sql import Row
results_df = spark.createDataFrame([Row(**r) for r in report.results_as_rows()])
display(results_df)

# CELL ----------------------------------------------------------------------
# Gate downstream work on overall status (never raises from the framework).
if report.overall_status == "FAIL":
    print("DQ FAILED — notify the science team before promoting to gold.")
