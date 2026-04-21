"""Sampling helpers.

Key invariant: distribution stats can be sampled, count-dependent checks
(duplicates, FKs) must always run on the full DataFrame. ``pick_sample``
returns both so callers can pick the right one.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Tuple

from fabric_dq.config import SamplingConfig

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class SampleMeta:
    strategy: str
    fraction: float
    row_count: int
    sampled: bool


def pick_sample(df: "DataFrame", cfg: SamplingConfig) -> Tuple["DataFrame", "DataFrame", SampleMeta]:
    """Return ``(sample_df, full_df, meta)``.

    Small tables (< cfg.min_rows) are never sampled even under ``strategy=fraction``;
    the overhead isn't worth the accuracy loss.
    """
    row_count = df.count()

    if cfg.strategy == "full" or cfg.strategy == "aggregate_only" or row_count < cfg.min_rows:
        return df, df, SampleMeta(
            strategy=cfg.strategy,
            fraction=1.0,
            row_count=row_count,
            sampled=False,
        )

    if cfg.strategy == "fraction":
        sample = df.sample(withReplacement=False, fraction=cfg.fraction, seed=cfg.seed)
        return sample, df, SampleMeta(
            strategy="fraction",
            fraction=cfg.fraction,
            row_count=row_count,
            sampled=True,
        )

    if cfg.strategy == "rows":
        assert cfg.rows is not None
        fraction = min(1.0, max(cfg.rows / max(row_count, 1), 0.0))
        sample = df.sample(withReplacement=False, fraction=fraction, seed=cfg.seed).limit(cfg.rows)
        return sample, df, SampleMeta(
            strategy="rows",
            fraction=fraction,
            row_count=row_count,
            sampled=True,
        )

    raise ValueError(f"Unhandled sampling strategy: {cfg.strategy}")
