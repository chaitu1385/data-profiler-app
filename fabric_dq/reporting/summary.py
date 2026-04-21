"""Report assembly: ``DQReport`` wraps profile + check results and renders
a markdown summary notebooks can print directly."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from fabric_dq.checks.base import CheckResult, Status, worst


_STATUS_ORDER = {Status.FAIL: 0, Status.WARN: 1, Status.PASS: 2}


@dataclass
class DQReport:
    run_id: str
    table: str
    row_count: int
    sample_pct: float
    profile: Dict[str, Dict[str, Any]]
    check_results: List[CheckResult] = field(default_factory=list)

    @property
    def overall_status(self) -> str:
        if not self.check_results:
            return Status.PASS
        return worst(*(r.status for r in self.check_results))

    def counts(self) -> Dict[str, int]:
        out = {Status.PASS: 0, Status.WARN: 0, Status.FAIL: 0}
        for r in self.check_results:
            out[r.status] = out.get(r.status, 0) + 1
        return out

    def summary_markdown(self, max_msg: int = 160) -> str:
        counts = self.counts()
        lines: List[str] = []
        lines.append(f"# DQ Report — {self.table}")
        lines.append("")
        lines.append(f"- **run_id:** `{self.run_id}`")
        lines.append(f"- **row_count:** {self.row_count:,}")
        lines.append(f"- **sample_pct:** {self.sample_pct:.4f}")
        lines.append(f"- **overall_status:** **{self.overall_status}**")
        lines.append(
            f"- **checks:** {counts[Status.PASS]} PASS, "
            f"{counts[Status.WARN]} WARN, {counts[Status.FAIL]} FAIL"
        )
        lines.append("")
        lines.append("| status | check | column | metric | threshold | message |")
        lines.append("|---|---|---|---|---|---|")

        sorted_results = sorted(
            self.check_results,
            key=lambda r: (_STATUS_ORDER.get(r.status, 99), r.check_type, r.column or ""),
        )
        for r in sorted_results:
            msg = r.message if len(r.message) <= max_msg else r.message[:max_msg - 1] + "…"
            lines.append(
                f"| {r.status} | {r.check_type} | {r.column or ''} | "
                f"{_fmt(r.metric_value)} | {_fmt(r.threshold)} | {msg} |"
            )
        return "\n".join(lines)

    def results_as_rows(self) -> List[Dict[str, Any]]:
        return [r.to_row() for r in self.check_results]


def _fmt(x) -> str:
    if x is None:
        return ""
    if isinstance(x, float):
        return f"{x:.4f}"
    return str(x)
