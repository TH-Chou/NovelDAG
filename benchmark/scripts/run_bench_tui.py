#!/usr/bin/env python3
"""Interactive TUI benchmark launcher with cursor navigation.

Usage: python run_bench_tui.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import questionary
from questionary import Style

BENCH_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = Path(__file__).resolve().parent

# ── Style ──────────────────────────────────────────────────────
STYLE = Style(
    [
        ("qmark", "fg:#2196F3 bold"),
        ("question", "bold"),
        ("answer", "fg:#4CAF50 bold"),
        ("pointer", "fg:#FF9800 bold"),
        ("highlighted", "fg:#FF9800 bold"),
        ("selected", "fg:#4CAF50"),
        ("separator", "fg:#666"),
        ("instruction", "fg:#888"),
        ("text", ""),
    ]
)

# ── Constants ──────────────────────────────────────────────────
MODES = [
    {"name": "🖥  本机 (local)", "value": "local"},
    {"name": "☁️  AWS 云端", "value": "aws"},
    {"name": "☁️  GCP 云端", "value": "gcp"},
]

PROTOCOLS = [
    questionary.Choice("narwhal   — 经典 Narwhal DAG", value="narwhal"),
    questionary.Choice("noveldag  — NovelDAG (优化版)", value="noveldag"),
    questionary.Choice("wahoo     — Wahoo 双路径", value="wahoo"),
]

RATES = [
    questionary.Choice("60K", value=60000),
    questionary.Choice("130K", value=130000),
    questionary.Choice("200K", value=200000),
    questionary.Choice("270K", value=270000),
    questionary.Choice("340K", value=340000),
]

FAULTS_LIST = [
    questionary.Choice("f=0 (无故障)", value=0),
    questionary.Choice("f=1", value=1),
    questionary.Choice("f=3", value=3),
]

DELAYS = [
    questionary.Choice("0ms (无延迟)", value=0),
    questionary.Choice("100ms (跨洲延迟 RTT=200ms)", value=100),
]

OUTLIER_METHODS = [
    questionary.Choice("无剔除 (保留全部)", value="none"),
    questionary.Choice("Middle-N (取中间N轮)", value="middle-N"),
    questionary.Choice("Std-Dev (超出Nσ丢弃)", value="std-dev"),
]

CONFIG_TEMPLATES = [
    questionary.Choice("smoke — 快速验证 (1轮, 4节点, 60000 rate)", value="smoke"),
    questionary.Choice("rtt_sweep — 延迟对比 (0/100ms, 3协议×4速率×3故障)", value="rtt_sweep"),
    questionary.Choice("自定义 — 自由配置所有参数", value="custom"),
]


def main():
    print_header()
    try:
        while True:
            wizard()
            again = questionary.select(
                "要继续操作吗？",
                choices=[
                    questionary.Choice("🔄 返回主菜单", value="continue"),
                    questionary.Choice("👋 退出", value="quit"),
                ],
                style=STYLE,
            ).ask()
            if again == "quit":
                break
            print("\n" + "=" * 62 + "\n")
    except KeyboardInterrupt:
        print("\n\n  已取消。")
    print("\n  再见！\n")
    sys.exit(0)


def print_header():
    print("\n" + "=" * 62)
    print("  🧬  NovelDAG Benchmark  —  交互式测试控制台")
    print("=" * 62)


# ═══════════════════════════════════════════════════════════════
# Main wizard flow
# ═══════════════════════════════════════════════════════════════


def wizard():
    # ── Step 1: Select mode ──
    mode = questionary.select(
        "选择运行模式",
        choices=MODES,
        style=STYLE,
    ).ask()
    if mode is None:
        return

    # ── Step 2: Load config or build from scratch ──
    configs_dir = SCRIPTS_DIR / "configs"
    config_files = sorted(configs_dir.glob("*.yaml")) if configs_dir.exists() else []

    config_choices = [
        questionary.Choice(f"📄 {f.name}", value=str(f)) for f in config_files
    ]
    config_choices.append(questionary.Choice("✨ 不使用配置文件，交互式构建", value="__inline__"))
    config_choices.insert(
        0, questionary.Choice("⚡ 快速模板 (smoke / rtt_sweep)", value="__template__")
    )

    config_source = questionary.select(
        "选择配置来源",
        choices=config_choices,
        style=STYLE,
    ).ask()
    if config_source is None:
        return

    # ── Build the run config ──
    if config_source == "__inline__":
        cfg = build_inline_config(mode)
    elif config_source == "__template__":
        cfg = build_from_template(mode)
    else:
        cfg = {"config_file": config_source}

    if cfg is None:
        return

    # ── Step 3: Action ──
    actions = [
        questionary.Choice("🚀 运行测试", value="run"),
        questionary.Choice("📥 收集云端日志 (仅云端)", value="collect"),
        questionary.Choice("📊 解析日志 → CSV", value="parse"),
        questionary.Choice("📈 生成图表", value="plot"),
        questionary.Separator(),
        questionary.Choice("🔥 完整流水线 (运行→解析→图表)", value="full"),
    ]

    action = questionary.select(
        "选择操作",
        choices=actions,
        style=STYLE,
    ).ask()
    if action is None:
        return

    # ── Execute ──
    execute_action(mode, cfg, action)


# ═══════════════════════════════════════════════════════════════
# Config builders
# ═══════════════════════════════════════════════════════════════


def build_from_template(mode: str) -> dict[str, Any] | None:
    template = questionary.select(
        "选择预设模板",
        choices=CONFIG_TEMPLATES,
        style=STYLE,
    ).ask()
    if template is None:
        return None

    if template == "custom":
        return build_inline_config(mode)

    # Templates
    tpl_dir = SCRIPTS_DIR / "configs"
    if template == "smoke":
        return {"config_file": str(tpl_dir / "smoke.yaml")}
    if template == "rtt_sweep":
        return {"config_file": str(tpl_dir / "full_sweep.yaml"), "group": "rtt_sweep"}


def build_inline_config(mode: str) -> dict[str, Any] | None:
    """Interactively build a benchmark configuration step by step."""
    print("\n  ── 配置测试矩阵 ──\n")

    # Protocols
    protocols = questionary.checkbox(
        "选择 DAG 协议 (空格选中/取消，回车确认)",
        choices=PROTOCOLS,
        style=STYLE,
    ).ask()
    if not protocols:
        print("  至少选择一个协议。")
        return None

    # Rates
    rates = questionary.checkbox(
        "选择注入速率",
        choices=RATES + [questionary.Choice("自定义输入...", value="__custom__")],
        style=STYLE,
    ).ask()
    if not rates:
        return None
    if "__custom__" in rates:
        rates.remove("__custom__")
        custom = questionary.text("输入自定义速率 (逗号分隔，如 80000,150000):", style=STYLE).ask()
        if custom:
            rates.extend([int(x.strip()) for x in custom.split(",") if x.strip()])

    # Faults
    faults = questionary.checkbox(
        "选择故障节点数",
        choices=FAULTS_LIST,
        style=STYLE,
    ).ask()
    if not faults:
        return None

    # Delays (local only)
    delays = [0]
    if mode == "local":
        delay_answer = questionary.checkbox(
            "选择网络延迟 (本地 dummynet)",
            choices=DELAYS,
            style=STYLE,
        ).ask()
        delays = delay_answer if delay_answer else [0]

    # Runs
    runs_str = questionary.text(
        "每配置重复轮数:",
        default="3",
        style=STYLE,
        validate=lambda v: v.isdigit() and int(v) > 0,
    ).ask()
    runs = int(runs_str or 3)

    # Duration
    dur_str = questionary.text(
        "每轮持续时间 (秒):",
        default="20",
        style=STYLE,
        validate=lambda v: v.isdigit() and int(v) > 0,
    ).ask()
    duration = int(dur_str or 20)

    # Nodes
    nodes_str = questionary.text(
        "节点总数:",
        default="10",
        style=STYLE,
        validate=lambda v: v.isdigit() and int(v) > 0,
    ).ask()
    nodes = int(nodes_str or 10)

    # Max header delay
    mhd_str = questionary.text(
        "max_header_delay (ms):",
        default="170",
        style=STYLE,
    ).ask()
    max_header_delay = int(mhd_str or 170)

    # Outlier rejection
    print("\n  ── 离群值剔除 ──\n")
    outlier_method = questionary.select(
        "离群值处理方法",
        choices=OUTLIER_METHODS,
        style=STYLE,
    ).ask()

    outlier_cfg = {"method": outlier_method}
    if outlier_method == "middle-n":
        n = questionary.text("保留中间N轮:", default="3", style=STYLE).ask()
        outlier_cfg["middle_n"] = int(n or 3)
    elif outlier_method == "std-dev":
        t = questionary.text("标准差倍数阈值:", default="2.0", style=STYLE).ask()
        outlier_cfg["std_dev_threshold"] = float(t or 2.0)

    # Summary
    total = len(protocols) * len(rates) * len(faults) * len(delays) * runs
    print(f"\n  ── 矩阵: {len(protocols)}协议 × {len(rates)}速率 × {len(faults)}故障 × {len(delays)}延迟 × {runs}轮 = {total} 次运行 ──\n")

    confirm = questionary.confirm("确认开始?", default=True, style=STYLE).ask()
    if not confirm:
        return None

    return {
        "inline": {
            "protocols": protocols,
            "rates": rates,
            "faults": faults,
            "delays": delays,
            "runs": runs,
            "duration": duration,
            "nodes": nodes,
            "max_header_delay": max_header_delay,
        },
        "outlier": outlier_cfg,
    }


# ═══════════════════════════════════════════════════════════════
# Executor
# ═══════════════════════════════════════════════════════════════


def execute_action(mode: str, cfg: dict[str, Any], action: str) -> None:
    cmd = [sys.executable, str(SCRIPTS_DIR / "run_bench.py"), "--mode", mode]

    # Config file
    if config_file := cfg.get("config_file"):
        cmd += ["--config", config_file]
        if group := cfg.get("group"):
            cmd += ["--group", group]

    # Inline params
    if inline := cfg.get("inline"):
        cmd += [
            "--protocols", ",".join(inline["protocols"]),
            "--rates", ",".join(str(r) for r in inline["rates"]),
            "--faults", ",".join(str(f) for f in inline["faults"]),
            "--delays", ",".join(str(d) for d in inline["delays"]),
            "--runs", str(inline["runs"]),
            "--duration", str(inline["duration"]),
            "--nodes", str(inline["nodes"]),
            "--max-header-delay", str(inline["max_header_delay"]),
        ]

    # Outlier
    if outlier := cfg.get("outlier"):
        cmd += ["--outlier", outlier["method"]]
        if "middle_n" in outlier:
            cmd += ["--middle-n", str(outlier["middle_n"])]
        if "std_dev_threshold" in outlier:
            cmd += ["--std-dev", str(outlier["std_dev_threshold"])]

    # Sudo password for local
    if mode == "local":
        sudo_pw = os.environ.get("SWEEP_SUDO_PASSWORD", "")
        if sudo_pw:
            cmd += ["--sudo-password", sudo_pw]

    # Action
    if action == "full":
        cmd.append("full")
    elif action == "run":
        cmd.append("run")
        cmd.append("--fresh")
    elif action == "collect":
        cmd += ["collect", "--batch-id", "default"]
    elif action == "parse":
        cmd += ["parse", "--logs-dir", "logs"]
    elif action == "plot":
        cmd += ["plot", "--csv", "csv_plots/bench_runs.csv"]

    # Show final command
    print("\n  ── 执行命令 ──")
    print(f"  {' '.join(cmd)}\n")
    print("═" * 62)

    # Run
    try:
        result = subprocess.run(cmd, cwd=str(BENCH_DIR))
    except KeyboardInterrupt:
        print("\n  已中断。")
        return

    print("\n" + "═" * 62)
    if result.returncode == 0:
        print("  执行成功！")

        # Show output locations
        csv_dir = BENCH_DIR / "csv_plots"
        csv_files = sorted(csv_dir.glob("*.csv")) if csv_dir.exists() else []
        plot_dir = BENCH_DIR / "plots"
        png_files = sorted(plot_dir.glob("*.png")) if plot_dir.exists() else []

        if csv_files:
            latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
            print(f"\n  CSV 数据: {latest_csv}")
            # Print last few lines for quick preview
            try:
                lines = latest_csv.read_text().strip().splitlines()
                if len(lines) > 1:
                    print(f"  ({len(lines)-1} 条结果)")
                    print(f"  表头: {lines[0]}")
                    if len(lines) > 1:
                        print(f"  最新: {lines[-1]}")
            except Exception:
                pass
        else:
            output_cfg_path = BENCH_DIR / "csv_plots"
            print(f"\n  CSV 输出目录: {output_cfg_path}")

        if png_files:
            latest_pngs = sorted(png_files, key=lambda p: p.stat().st_mtime, reverse=True)[:3]
            print(f"\n  图表 ({len(png_files)} 张):")
            for p in latest_pngs:
                print(f"    {p}")

        if not csv_files and not png_files:
            logs_dir = BENCH_DIR / "logs"
            if logs_dir.exists():
                log_subdirs = [d for d in logs_dir.iterdir() if d.is_dir()]
                print(f"\n  日志目录: {logs_dir} ({len(log_subdirs)} 个子目录)")

        print("\n  提示: 你可以继续操作，比如选择 parse 或 plot 来处理结果。")
    else:
        print(f"  执行失败 (exit code: {result.returncode})")


if __name__ == "__main__":
    main()
