#!/usr/bin/env python3
"""Compare MiniOB TPC-C results between head and base commits.

- Fails (exit 1) if head consistency checks fail or deterministic state digest mismatches base.
- Warns (::warning:: + markdown alert) if performance regresses beyond threshold, without
  blocking merge unless --fail-on-regression is set.
"""

import argparse
import json
import os
import sys
from pathlib import Path


def pct_delta(head_val: float, base_val: float) -> float:
  if base_val == 0:
    return 0.0
  return ((head_val - base_val) / base_val) * 100.0


def format_delta(delta: float, higher_is_better: bool) -> str:
  sign = "+" if delta >= 0 else ""
  if abs(delta) < 1.0:
    badge = "➖"
  elif (delta > 0 and higher_is_better) or (delta < 0 and not higher_is_better):
    badge = "🚀"
  else:
    badge = "⚠️"
  return f"{badge} {sign}{delta:.2f}%"


def main() -> int:
  parser = argparse.ArgumentParser(description="Compare MiniOB TPC-C reports")
  parser.add_argument("--head", required=True, help="Path to head TPC-C JSON report")
  parser.add_argument("--base", default="", help="Optional path to base TPC-C JSON report")
  parser.add_argument("--label", default="TPC-C", help="Section title label")
  parser.add_argument(
      "--check-digest",
      action="store_true",
      help="Require deterministic_digest to match base (for single-thread seeded runs)",
  )
  parser.add_argument("--regress-threshold-pct", type=float, default=5.0)
  parser.add_argument("--latency-regress-pct", type=float, default=10.0)
  parser.add_argument("--fail-on-regression", action="store_true")
  parser.add_argument("--markdown-out", default="")
  args = parser.parse_args()

  head = json.loads(Path(args.head).read_text())
  base = json.loads(Path(args.base).read_text()) if args.base and Path(args.base).exists() else None

  correctness_ok = bool(head["consistency"]["passed"])
  correctness_errors = list(head["consistency"].get("failures", []))

  if base and args.check_digest and base["consistency"]["passed"]:
    if head["deterministic_digest"] != base["deterministic_digest"]:
      correctness_ok = False
      correctness_errors.append(
          f"Deterministic state digest mismatch: head={head['deterministic_digest']} "
          f"vs base={base['deterministic_digest']}"
      )

  lines = [f"### 📊 {args.label} 测试报告", ""]
  if correctness_ok:
    lines.append(f"- **正确性校验**: ✅ PASS (`digest={head['deterministic_digest']}`)")
  else:
    lines.append("- **正确性校验**: ❌ FAIL（禁止合入）")
    for err in correctness_errors:
      lines.append(f"  - `{err}`")
      print(f"::error::{args.label} consistency failed: {err}")

  regression_warnings = []
  if base:
    tps_delta = pct_delta(head["tps"], base["tps"])
    tpmc_delta = pct_delta(head["tpmC"], base["tpmC"])
    p95_delta = pct_delta(head["latency_ms"]["p95"], base["latency_ms"]["p95"])

    if tps_delta < -args.regress_threshold_pct:
      regression_warnings.append(
          f"TPS 下降 {abs(tps_delta):.2f}% ({base['tps']} -> {head['tps']})"
      )
    if tpmc_delta < -args.regress_threshold_pct:
      regression_warnings.append(
          f"tpmC 下降 {abs(tpmc_delta):.2f}% ({base['tpmC']} -> {head['tpmC']})"
      )
    if p95_delta > args.latency_regress_pct:
      regression_warnings.append(
          f"P95 延迟上升 {p95_delta:.2f}% ({base['latency_ms']['p95']}ms -> {head['latency_ms']['p95']}ms)"
      )

    lines.extend([
        "",
        "| 指标 | Base | Head | 变化 |",
        "| --- | ---: | ---: | --- |",
        f"| TPS | {base['tps']} | {head['tps']} | {format_delta(tps_delta, True)} |",
        f"| tpmC | {base['tpmC']} | {head['tpmC']} | {format_delta(tpmc_delta, True)} |",
        f"| Avg Latency (ms) | {base['latency_ms']['avg']} | {head['latency_ms']['avg']} | {format_delta(pct_delta(head['latency_ms']['avg'], base['latency_ms']['avg']), False)} |",
        f"| P95 Latency (ms) | {base['latency_ms']['p95']} | {head['latency_ms']['p95']} | {format_delta(p95_delta, False)} |",
        f"| Committed / Aborted | {base['total_committed']} / {base['total_aborted']} | {head['total_committed']} / {head['total_aborted']} | - |",
    ])
  else:
    lines.extend([
        "",
        f"- **TPS**: `{head['tps']}` | **tpmC**: `{head['tpmC']}` | "
        f"**Avg**: `{head['latency_ms']['avg']}ms` | **P95**: `{head['latency_ms']['p95']}ms` | "
        f"**Committed/Aborted**: `{head['total_committed']}/{head['total_aborted']}`",
    ])

  if regression_warnings:
    lines.append("")
    lines.append("> [!WARNING]")
    lines.append("> **性能回退提醒**：检测到以下指标出现性能下降：")
    for w in regression_warnings:
      lines.append(f"> - {w}")
      print(f"::warning::{args.label} performance regression: {w}")

  md = "\n".join(lines) + "\n"
  if args.markdown_out:
    out = Path(args.markdown_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md)

  summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
  if summary_path:
    with open(summary_path, "a") as f:
      f.write(md + "\n")

  print(md)

  if not correctness_ok:
    return 1
  if regression_warnings and args.fail_on_regression:
    return 1
  return 0


if __name__ == "__main__":
  sys.exit(main())
