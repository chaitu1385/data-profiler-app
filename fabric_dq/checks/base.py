"""Check base class + result dataclass.

Every check returns a :class:`CheckResult` carrying a status (PASS / WARN / FAIL),
the metric value, the threshold it was compared against, and a human-readable
message. The runner aggregates these into the final report.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


class Severity:
    WARN = "WARN"
    FAIL = "FAIL"


class Status:
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


_STATUS_RANK = {Status.PASS: 0, Status.WARN: 1, Status.FAIL: 2}


def worst(*statuses: str) -> str:
    """Return the worst status among the given statuses (FAIL > WARN > PASS)."""
    if not statuses:
        return Status.PASS
    return max(statuses, key=lambda s: _STATUS_RANK.get(s, 0))


@dataclass
class CheckResult:
    check_type: str
    column: Optional[str]
    status: str
    metric_value: Optional[float]
    threshold: Optional[float]
    message: str
    severity: str = Severity.FAIL
    details: Dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> Dict[str, Any]:
        import json
        return {
            "check_type": self.check_type,
            "column": self.column,
            "status": self.status,
            "metric_value": float(self.metric_value) if self.metric_value is not None else None,
            "threshold": float(self.threshold) if self.threshold is not None else None,
            "message": self.message,
            "severity": self.severity,
            "details": json.dumps(self.details, default=str),
        }


class Check:
    """Marker base class for checks. Implementations override :meth:`run`."""

    check_type: str = "base"

    def run(self, *args, **kwargs) -> CheckResult:  # pragma: no cover - abstract
        raise NotImplementedError


def classify(metric_value: float, threshold: float, severity: str) -> str:
    """Compare ``metric_value`` to ``threshold`` and return a status.

    A check FAILS/WARNS (per configured severity) when metric > threshold,
    otherwise PASSes. Keeping this single comparator in one place makes
    per-check code uniform.
    """
    if metric_value <= threshold:
        return Status.PASS
    return Status.FAIL if severity == Severity.FAIL else Status.WARN
