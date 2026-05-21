"""YAML config loader, schema validation, and test-matrix resolver.

Test matrix = Cartesian product of protocols x rates x faults [x delays] x repeat(runs).
Group-level keys deep-merge over defaults.
"""

from __future__ import annotations

import copy
from itertools import product
from pathlib import Path
from typing import Any

import yaml


# ── Default values ──────────────────────────────────────────────
DEFAULT_BENCH: dict[str, Any] = {
    "nodes": 10,
    "workers": 1,
    "collocate": True,
    "tx_size": 512,
    "duration": 30,
    "runs": 3,
}

DEFAULT_NODE: dict[str, Any] = {
    "header_size": 1000,
    "max_header_delay": 2000,
    "gc_depth": 50,
    "sync_retry_delay": 10000,
    "sync_retry_nodes": 3,
    "batch_size": 500000,
    "max_batch_delay": 200,
    "consensus_protocol": "round_robin",
    "dag_protocol": "noveldag",
}

DEFAULT_LOCAL: dict[str, Any] = {
    "delays": [0],
}

DEFAULT_OUTLIER: dict[str, Any] = {
    "method": "none",
    "middle_n": 3,
    "std_dev_threshold": 2.0,
}

DEFAULT_OUTPUT: dict[str, Any] = {
    "csv_dir": "csv_plots",
    "plot_dir": "plots",
    "checkpoint_dir": ".checkpoints",
}


def load_config(path: str | Path) -> dict[str, Any]:
    """Load and minimally validate a YAML config file."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    if raw is None:
        raise ConfigError(f"Config file is empty: {path}")
    if "groups" not in raw or not raw["groups"]:
        raise ConfigError("Config must define at least one 'groups' entry")
    return raw


def resolve_groups(
    raw: dict[str, Any],
    group_name: str | None = None,
    cli_overrides: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Resolve one or all groups into lists of (bench, node, delay, label) dicts.

    Returns {group_name: [point_dict, ...]}.
    Each point_dict has keys: bench, node, delay, protocol, faults, rate, run_index.
    """
    overrides = cli_overrides or {}

    # Merge defaults
    defs_bench = deep_merge(DEFAULT_BENCH.copy(), raw.get("bench_defaults", {}) or {})
    defs_node = deep_merge(DEFAULT_NODE.copy(), raw.get("node_defaults", {}) or {})
    defs_local = deep_merge(DEFAULT_LOCAL.copy(), raw.get("local_defaults", {}) or {})

    selected_groups: dict[str, Any] = {}
    if group_name:
        if group_name not in raw["groups"]:
            raise ConfigError(f"Group '{group_name}' not found in config")
        selected_groups[group_name] = raw["groups"][group_name]
    else:
        selected_groups = raw["groups"]

    result: dict[str, list[dict[str, Any]]] = {}
    for gname, gcfg in selected_groups.items():
        result[gname] = _resolve_one_group(
            gname, gcfg, defs_bench, defs_node, defs_local, overrides
        )
    return result


def _resolve_one_group(
    gname: str,
    gcfg: dict[str, Any],
    defs_bench: dict[str, Any],
    defs_node: dict[str, Any],
    defs_local: dict[str, Any],
    overrides: dict[str, Any],
) -> list[dict[str, Any]]:
    bench = deep_merge(defs_bench.copy(), gcfg.get("bench", {}) or {})
    node = deep_merge(defs_node.copy(), gcfg.get("node", {}) or {})
    local = deep_merge(defs_local.copy(), gcfg.get("local", {}) or {})

    protocols = _to_list(
        overrides.get("protocols") or gcfg.get("protocols") or [node["dag_protocol"]]
    )
    rates = _to_list(overrides.get("rates") or gcfg.get("rates", [60000]))
    faults = _to_list(overrides.get("faults") or gcfg.get("faults", [0]))
    delays = _to_list(
        overrides.get("delays") or gcfg.get("local", {}).get("delays") or local["delays"]
    )
    runs = int(overrides.get("runs") or bench.get("runs", 1))

    # Apply CLI overrides to bench/node
    for k in ("nodes", "workers", "tx_size", "duration", "runs"):
        if k in overrides and overrides[k] is not None:
            bench[k] = overrides[k]
    for k in (
        "header_size",
        "max_header_delay",
        "gc_depth",
        "sync_retry_delay",
        "sync_retry_nodes",
        "batch_size",
        "max_batch_delay",
        "consensus_protocol",
        "dag_protocol",
    ):
        if k in overrides and overrides[k] is not None:
            node[k] = overrides[k]

    bench["runs"] = runs

    points: list[dict[str, Any]] = []
    for proto, rate, fault, delay in product(protocols, rates, faults, delays):
        pt_node = node.copy()
        pt_node["dag_protocol"] = proto
        for run_idx in range(1, runs + 1):
            points.append(
                {
                    "bench": bench.copy(),
                    "node": pt_node,
                    "delay": delay,
                    "protocol": proto,
                    "faults": fault,
                    "rate": rate,
                    "run_index": run_idx,
                    "group": gname,
                    "label": f"d{delay}_f{fault}_{proto}_r{rate}",
                }
            )
    return points


def get_outlier_config(raw: dict[str, Any]) -> dict[str, Any]:
    oc = raw.get("outlier_rejection", None)
    if oc is None:
        return DEFAULT_OUTLIER.copy()
    return deep_merge(DEFAULT_OUTLIER.copy(), oc)


def get_output_config(raw: dict[str, Any]) -> dict[str, Any]:
    oc = raw.get("output", None)
    if oc is None:
        return DEFAULT_OUTPUT.copy()
    return deep_merge(DEFAULT_OUTPUT.copy(), oc)


# ── Helpers ─────────────────────────────────────────────────────


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge override into base. Returns base (mutated)."""
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            deep_merge(base[k], v)
        else:
            base[k] = copy.deepcopy(v)
    return base


def _to_list(v: Any) -> list[Any]:
    if isinstance(v, list):
        return v
    return [v]


class ConfigError(Exception):
    """Configuration validation error."""
