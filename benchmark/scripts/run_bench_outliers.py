"""Outlier rejection for benchmark runs.

Supports three methods:
  - none: keep all runs
  - middle-N: sort M runs by each metric, keep middle N (conservative intersection)
  - std-dev: drop runs where |value - mean| > threshold * stdev
"""

import statistics
from typing import Any

METRIC_KEYS = [
    "consensus_tps",
    "consensus_latency_ms",
    "end_to_end_tps",
    "end_to_end_latency_ms",
]


def apply_outlier_rejection(
    runs: list[dict[str, Any]],
    method: str = "none",
    middle_n: int = 3,
    std_dev_threshold: float = 2.0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Filter a list of per-run metric dicts, returning (kept_runs, stats)."""
    if method == "none" or len(runs) <= middle_n:
        return runs, {
            "method": method,
            "total_runs": len(runs),
            "kept_runs": len(runs),
            "rejected_indices": [],
        }

    if method == "middle-n":
        return _reject_middle_n(runs, middle_n)

    if method == "std-dev":
        return _reject_std_dev(runs, std_dev_threshold)

    raise ValueError(f"Unknown outlier method: {method}")


def _reject_middle_n(
    runs: list[dict[str, Any]], n: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    m = len(runs)
    if n >= m:
        return runs, {
            "method": "middle-N",
            "total_runs": m,
            "kept_runs": m,
            "rejected_indices": [],
        }

    start = (m - n) // 2
    end = start + n
    kept_by_metric: list[set[int]] = []

    for key in METRIC_KEYS:
        sorted_indices = sorted(
            range(m),
            key=lambda i: runs[i].get(key, 0.0),
        )
        kept_by_metric.append(set(sorted_indices[start:end]))

    # Conservative intersection: a run must be an outlier for ALL metrics
    kept_indices = set.intersection(*kept_by_metric)
    # Fallback: keep middle-N by consensus_tps alone
    if not kept_indices:
        kept_indices = kept_by_metric[0]

    rejected = [i for i in range(m) if i not in kept_indices]
    return [runs[i] for i in sorted(kept_indices)], {
        "method": "middle-N",
        "total_runs": m,
        "kept_runs": len(kept_indices),
        "rejected_indices": rejected,
    }


def _reject_std_dev(
    runs: list[dict[str, Any]], threshold: float
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    m = len(runs)
    if m < 3:
        return runs, {
            "method": "std-dev",
            "total_runs": m,
            "kept_runs": m,
            "rejected_indices": [],
        }

    outlier_sets: list[set[int]] = []
    for key in METRIC_KEYS:
        vals = [r.get(key, 0.0) for r in runs]
        mean = statistics.mean(vals)
        stdev = statistics.stdev(vals)
        if stdev == 0:
            outlier_sets.append(set())
            continue
        outlier_sets.append(
            {i for i, v in enumerate(vals) if abs(v - mean) > threshold * stdev}
        )

    rejected = set.intersection(*outlier_sets) if outlier_sets else set()
    kept = [runs[i] for i in range(m) if i not in rejected]

    return kept, {
        "method": "std-dev",
        "total_runs": m,
        "kept_runs": len(kept),
        "rejected_indices": sorted(rejected),
    }


def aggregate_metrics(
    runs: list[dict[str, Any]],
) -> dict[str, float]:
    """Compute mean of each metric across kept runs."""
    result: dict[str, float] = {}
    for key in METRIC_KEYS:
        vals = [r[key] for r in runs if key in r and r[key] is not None]
        result[key] = statistics.mean(vals) if vals else 0.0
    return result
