"""Generic data quality & profiling framework for Azure Fabric notebooks.

Public entrypoint: :func:`run_profile`.
"""
from fabric_dq.runner import run_profile
from fabric_dq.checks.base import CheckResult, Severity, Status
from fabric_dq.reporting.summary import DQReport

__all__ = ["run_profile", "CheckResult", "Severity", "Status", "DQReport"]
__version__ = "0.1.0"
