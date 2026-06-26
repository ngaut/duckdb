#!/usr/bin/env python3
#
# Build a shape inventory from DuckDB JIT benchmark artifacts.

import argparse
import collections
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jit"))
from benchmark_common import read_csv, row_bool, row_float, row_int, write_csv

INVENTORY_FIELDS = (
    "workload",
    "query",
    "policy",
    "shape_signature",
    "classification",
    "next_action",
    "status",
    "execution_mode",
    "selected_runner",
    "blocker",
    "count",
    "run_count",
    "off_median_s",
    "policy_median_s",
    "speedup_vs_off_median",
    "correctness_diff",
    "compiled_regions",
    "runtime_regions",
    "unsupported_decisions",
    "skipped_decisions",
    "unavailable_decisions",
    "disabled_decisions",
    "compile_errors",
    "decision_time_us",
    "compile_time_us",
    "runtime_time_us",
    "source_contract_runtime_time_us",
    "sink_next_batch_runtime_time_us",
    "generated_body_runtime_time_us",
    "runtime_dominant_component",
    "input_rows",
    "output_rows",
    "invocation_count",
    "source_contract_invocation_count",
    "sink_next_batch_invocation_count",
    "hash_join_probe_layout",
    "jit_runtime_path_counts",
    "jit_materialization_boundary_counts",
    "runner_cost_profile",
    "runner_cost_rows",
    "runner_cost_batches",
    "runner_cost_expression_cost",
    "runner_cost_generated_stage_count",
    "runner_cost_materialization_elision_count",
    "runner_cost_materialization_source_append_count",
    "runner_cost_native_join_stage_count",
    "runner_cost_native_aggregate_stage_count",
    "runner_cost_native_grouped_aggregate_stage_count",
    "runner_cost_native_sort_stage_count",
    "runner_cost_full_pipeline",
    "runner_cost_funded_protocol_rule",
    "runner_cost_startup_rules",
    "runner_cost_selection_reason",
    "runner_cost_saved_work_per_batch",
    "runner_cost_accelerated_runner_benefit",
    "runner_cost_required_benefit",
    "runner_cost_net_benefit",
    "runner_cost_selected_accelerated_runner_count",
    "runner_cost_selected_compiled_vectorized_runner_count",
    "runner_cost_selected_gpu_runner_count",
    "runner_cost_compiled_vectorized_net_benefit",
    "runner_cost_gpu_net_benefit",
    "source_stage_runtime_breakdown",
    "generated_stage_runtime_breakdown",
    "profile_candidate_shapes",
    "profile_generated_work_classes",
    "profile_native_protocol_classes",
    "profile_funded_protocol_rules",
    "profile_startup_rules",
    "profile_selection_reasons",
    "profile_jit_runtime_paths",
    "profile_jit_materialization_boundaries",
    "profile_scan_filter_modes",
)

SUMMARY_BY_POLICY_FIELDS = (
    "run_count",
    "median_s",
    "speedup_vs_off_median",
    "correctness_diff",
    "compiled_regions",
    "runtime_regions",
    "unsupported_decisions",
    "skipped_decisions",
    "unavailable_decisions",
    "disabled_decisions",
    "compile_errors",
    "decision_time_us",
    "compile_time_us",
)

INT_SUM_FIELDS = (
    "count",
    "decision_time_us",
    "compile_time_us",
    "runtime_time_us",
    "source_contract_runtime_time_us",
    "sink_next_batch_runtime_time_us",
    "generated_body_runtime_time_us",
    "input_rows",
    "output_rows",
    "invocation_count",
    "source_contract_invocation_count",
    "sink_next_batch_invocation_count",
    "runner_cost_rows",
    "runner_cost_batches",
    "runner_cost_expression_cost",
    "runner_cost_generated_stage_count",
    "runner_cost_materialization_elision_count",
    "runner_cost_materialization_source_append_count",
    "runner_cost_native_join_stage_count",
    "runner_cost_native_aggregate_stage_count",
    "runner_cost_native_grouped_aggregate_stage_count",
    "runner_cost_native_sort_stage_count",
    "runner_cost_saved_work_per_batch",
    "runner_cost_accelerated_runner_benefit",
    "runner_cost_required_benefit",
    "runner_cost_net_benefit",
    "runner_cost_selected_accelerated_runner_count",
    "runner_cost_selected_compiled_vectorized_runner_count",
    "runner_cost_selected_gpu_runner_count",
    "runner_cost_compiled_vectorized_net_benefit",
    "runner_cost_gpu_net_benefit",
)


def normalized(value: str | None) -> str:
    return "" if value is None else str(value)


def bool_token(value: str | bool | None) -> str:
    return "1" if row_bool({"value": value}, "value") else "0"


def top_counter(counter: collections.Counter) -> str:
    if not counter:
        return ""
    value, _ = counter.most_common(1)[0]
    return value


def join_counter_keys(counter: collections.Counter, limit: int = 6) -> str:
    if not counter:
        return ""
    parts = []
    for value, count in counter.most_common(limit):
        if value:
            parts.append(f"{value}:{count}")
    return "|".join(parts)


def runner_cost_shape(row: dict) -> str:
    if not row_bool(row, "runner_cost_profile"):
        return "cost=none"
    parts = [
        f"gen={row_int(row, 'runner_cost_generated_stage_count')}",
        f"mat={row_int(row, 'runner_cost_materialization_elision_count')}",
        f"src_append={row_int(row, 'runner_cost_materialization_source_append_count')}",
        f"join={row_int(row, 'runner_cost_native_join_stage_count')}",
        f"agg={row_int(row, 'runner_cost_native_aggregate_stage_count')}",
        f"grouped={row_int(row, 'runner_cost_native_grouped_aggregate_stage_count')}",
        f"sort={row_int(row, 'runner_cost_native_sort_stage_count')}",
        f"full={bool_token(row.get('runner_cost_full_pipeline'))}",
        f"rule={row.get('runner_cost_funded_protocol_rule', '') or 'none'}",
        f"startup={row.get('runner_cost_startup_rules', '') or 'none'}",
        f"reason={row.get('runner_cost_selection_reason', '') or 'none'}",
    ]
    return ",".join(parts)


def shape_signature(row: dict) -> str:
    parts = [
        f"status={row.get('status', '')}",
        f"mode={row.get('execution_mode', '')}",
        f"runner={row.get('selected_runner', '')}",
        f"hash={row.get('hash_join_probe_layout', '') or 'none'}",
        f"path={row.get('jit_runtime_path_counts', '') or 'none'}",
        f"boundary={row.get('jit_materialization_boundary_counts', '') or 'none'}",
        runner_cost_shape(row),
    ]
    return ";".join(parts)


def no_decision_shape_signature(policy: str) -> str:
    runner = "vectorized" if policy == "off" else "none"
    mode = "vectorized" if policy == "off" else "none"
    return f"status=no_decision;mode={mode};runner={runner};hash=none;path=none;boundary=none;cost=none"


def runtime_dominant_component(row: dict) -> str:
    components = {
        "source": row_int(row, "source_contract_runtime_time_us"),
        "sink": row_int(row, "sink_next_batch_runtime_time_us"),
        "generated": row_int(row, "generated_body_runtime_time_us"),
    }
    attributed = sum(components.values())
    unattributed = max(0, row_int(row, "runtime_time_us") - attributed)
    components["unattributed"] = unattributed
    name, value = max(components.items(), key=lambda item: item[1])
    return name if value > 0 else ""


def profile_sets(trace_dir: Path, run_rows: list[dict]) -> dict[tuple[str, str], dict[str, collections.Counter]]:
    result: dict[tuple[str, str], dict[str, collections.Counter]] = collections.defaultdict(
        lambda: collections.defaultdict(collections.Counter)
    )
    for row in run_rows:
        profile_name = row.get("profile_json", "")
        if not profile_name:
            continue
        profile_path = trace_dir / profile_name
        if not profile_path.exists():
            continue
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        events = ((profile.get("execution_regions") or {}).get("events")) or []
        key = (row["query"], row["policy"])
        for event in events:
            shape = normalized(event.get("shape") or event.get("pipeline_shape"))
            if shape:
                result[key]["candidate_shapes"][shape] += 1
            work_class = normalized(event.get("runner_cost_generated_work_class"))
            if work_class:
                result[key]["generated_work_classes"][work_class] += 1
            protocol_class = normalized(event.get("runner_cost_native_protocol_class"))
            if protocol_class:
                result[key]["native_protocol_classes"][protocol_class] += 1
            funded_rule = normalized(event.get("runner_cost_funded_protocol_rule"))
            if funded_rule:
                result[key]["funded_protocol_rules"][funded_rule] += 1
            startup_rules = normalized(event.get("runner_cost_startup_rules"))
            if startup_rules:
                result[key]["startup_rules"][startup_rules] += 1
            selection_reason = normalized(event.get("runner_cost_selection_reason"))
            if selection_reason:
                result[key]["selection_reasons"][selection_reason] += 1
            runtime_path = normalized(event.get("jit_runtime_path_counts"))
            if runtime_path:
                result[key]["jit_runtime_paths"][runtime_path] += 1
            materialization_boundary = normalized(event.get("jit_materialization_boundary_counts"))
            if materialization_boundary:
                result[key]["jit_materialization_boundaries"][materialization_boundary] += 1
            scan_mode = (
                f"selected={bool_token(event.get('selected_uses_scan_filters'))},"
                f"candidate={bool_token(event.get('candidate_uses_scan_filters'))}"
            )
            result[key]["scan_filter_modes"][scan_mode] += 1
    return result


def summarize_rows(summary_rows: list[dict]) -> dict[tuple[str, str], dict]:
    return {(row["query"], row["policy"]): row for row in summary_rows}


def off_medians(summary_rows: list[dict]) -> dict[str, str]:
    return {row["query"]: row.get("median_s", "") for row in summary_rows if row["policy"] == "off"}


def counter_groups(counter_rows: list[dict]) -> dict[tuple[str, str, str], list[dict]]:
    grouped: dict[tuple[str, str, str], list[dict]] = collections.defaultdict(list)
    for row in counter_rows:
        grouped[(row["query"], row["policy"], shape_signature(row))].append(row)
    return grouped


def aggregate_group(rows: list[dict]) -> dict:
    result = {field: 0 for field in INT_SUM_FIELDS}
    result["runner_cost_profile"] = False
    result["runner_cost_full_pipeline"] = False
    statuses = collections.Counter()
    execution_modes = collections.Counter()
    selected_runners = collections.Counter()
    blockers = collections.Counter()
    hash_layouts = collections.Counter()
    runtime_paths = collections.Counter()
    materialization_boundaries = collections.Counter()
    funded_rules = collections.Counter()
    startup_rules = collections.Counter()
    selection_reasons = collections.Counter()
    source_stages = collections.Counter()
    generated_stages = collections.Counter()
    for row in rows:
        weight = max(1, row_int(row, "count"))
        for field in INT_SUM_FIELDS:
            result[field] += row_int(row, field)
        result["runner_cost_profile"] = result["runner_cost_profile"] or row_bool(row, "runner_cost_profile")
        result["runner_cost_full_pipeline"] = result["runner_cost_full_pipeline"] or row_bool(
            row, "runner_cost_full_pipeline"
        )
        statuses[normalized(row.get("status"))] += weight
        execution_modes[normalized(row.get("execution_mode"))] += weight
        selected_runners[normalized(row.get("selected_runner"))] += weight
        blockers[normalized(row.get("blocker"))] += weight
        hash_layouts[normalized(row.get("hash_join_probe_layout"))] += weight
        runtime_paths[normalized(row.get("jit_runtime_path_counts"))] += weight
        materialization_boundaries[normalized(row.get("jit_materialization_boundary_counts"))] += weight
        funded_rules[normalized(row.get("runner_cost_funded_protocol_rule"))] += weight
        startup_rules[normalized(row.get("runner_cost_startup_rules"))] += weight
        selection_reasons[normalized(row.get("runner_cost_selection_reason"))] += weight
        source_stages[normalized(row.get("source_stage_runtime_breakdown"))] += weight
        generated_stages[normalized(row.get("generated_stage_runtime_breakdown"))] += weight
    result["status"] = top_counter(statuses)
    result["execution_mode"] = top_counter(execution_modes)
    result["selected_runner"] = top_counter(selected_runners)
    result["blocker"] = top_counter(blockers)
    result["hash_join_probe_layout"] = top_counter(hash_layouts)
    result["jit_runtime_path_counts"] = top_counter(runtime_paths)
    result["jit_materialization_boundary_counts"] = top_counter(materialization_boundaries)
    result["runner_cost_funded_protocol_rule"] = top_counter(funded_rules)
    result["runner_cost_startup_rules"] = top_counter(startup_rules)
    result["runner_cost_selection_reason"] = top_counter(selection_reasons)
    result["source_stage_runtime_breakdown"] = top_counter(source_stages)
    result["generated_stage_runtime_breakdown"] = top_counter(generated_stages)
    result["runtime_dominant_component"] = runtime_dominant_component(result)
    return result


def classify(row: dict, material_speedup: float) -> tuple[str, str]:
    status = row["status"]
    policy = row["policy"]
    speedup = row_float(row, "speedup_vs_off_median")
    blocker = row.get("blocker", "")
    if policy == "off" or status == "disabled":
        return "vectorized_baseline", "none"
    if status == "no_decision":
        return "no_jit_decision", "inspect only if repeated planning noise appears"
    if row_int(row, "correctness_diff") != 0:
        return "correctness_failed", "fix correctness before performance work"
    if status in ("unsupported", "unavailable", "error") or "unsupported" in blocker or "backend" in blocker:
        return "backend_limited", "add backend capability or keep skipped"
    if status == "compiled":
        if speedup >= material_speedup:
            return "proven_win", "protect with benchmark and CBO rule"
        if speedup >= 0.98:
            return "runtime_limited", "profile runtime boundary before changing CBO"
        return "compiled_regression", "locate runtime root cause before admitting"
    if status == "skipped":
        net_benefit = row_int(row, "runner_cost_net_benefit")
        if row_bool(row, "runner_cost_profile") and net_benefit <= 0:
            return "correctly_skipped", "leave vectorized until shape changes"
        return "cbo_limited", "inspect forced/profile run before policy change"
    if status in ("executed", "source_contract"):
        return "runtime_observation", "use stage timings to find runtime bottleneck"
    return "unknown", "inspect counters and profile events"


def build_inventory_rows(
    trace_dir: Path,
    workload: str,
    material_speedup: float,
    summary_rows: list[dict],
    run_rows: list[dict],
    counter_rows: list[dict],
) -> list[dict]:
    summary_by_policy = summarize_rows(summary_rows)
    off_by_query = off_medians(summary_rows)
    profiles = profile_sets(trace_dir, run_rows)
    rows = []
    seen_policy_rows = set()
    for (query, policy, signature), grouped_rows in sorted(counter_groups(counter_rows).items()):
        seen_policy_rows.add((query, policy))
        aggregate = aggregate_group(grouped_rows)
        summary = summary_by_policy.get((query, policy), {})
        profile_key = profiles.get((query, policy), {})
        row = {
            "workload": workload,
            "query": query,
            "policy": policy,
            "shape_signature": signature,
            "run_count": summary.get("run_count", ""),
            "off_median_s": off_by_query.get(query, ""),
            "policy_median_s": summary.get("median_s", ""),
            "speedup_vs_off_median": summary.get("speedup_vs_off_median", ""),
            "correctness_diff": summary.get("correctness_diff", ""),
            "compiled_regions": summary.get("compiled_regions", ""),
            "runtime_regions": summary.get("runtime_regions", ""),
            "unsupported_decisions": summary.get("unsupported_decisions", ""),
            "skipped_decisions": summary.get("skipped_decisions", ""),
            "unavailable_decisions": summary.get("unavailable_decisions", ""),
            "disabled_decisions": summary.get("disabled_decisions", ""),
            "compile_errors": summary.get("compile_errors", ""),
            "profile_candidate_shapes": join_counter_keys(profile_key.get("candidate_shapes", collections.Counter())),
            "profile_generated_work_classes": join_counter_keys(
                profile_key.get("generated_work_classes", collections.Counter())
            ),
            "profile_native_protocol_classes": join_counter_keys(
                profile_key.get("native_protocol_classes", collections.Counter())
            ),
            "profile_funded_protocol_rules": join_counter_keys(
                profile_key.get("funded_protocol_rules", collections.Counter())
            ),
            "profile_startup_rules": join_counter_keys(profile_key.get("startup_rules", collections.Counter())),
            "profile_selection_reasons": join_counter_keys(
                profile_key.get("selection_reasons", collections.Counter())
            ),
            "profile_jit_runtime_paths": join_counter_keys(
                profile_key.get("jit_runtime_paths", collections.Counter())
            ),
            "profile_jit_materialization_boundaries": join_counter_keys(
                profile_key.get("jit_materialization_boundaries", collections.Counter())
            ),
            "profile_scan_filter_modes": join_counter_keys(profile_key.get("scan_filter_modes", collections.Counter())),
        }
        row.update(aggregate)
        row["runner_cost_profile"] = str(bool(row["runner_cost_profile"])).lower()
        row["runner_cost_full_pipeline"] = str(bool(row["runner_cost_full_pipeline"])).lower()
        classification, next_action = classify(row, material_speedup)
        row["classification"] = classification
        row["next_action"] = next_action
        rows.append({field: row.get(field, "") for field in INVENTORY_FIELDS})
    for query, policy in sorted(set(summary_by_policy) - seen_policy_rows):
        summary = summary_by_policy[(query, policy)]
        row = {
            "workload": workload,
            "query": query,
            "policy": policy,
            "shape_signature": no_decision_shape_signature(policy),
            "status": "no_decision",
            "execution_mode": "vectorized" if policy == "off" else "none",
            "selected_runner": "vectorized" if policy == "off" else "none",
            "blocker": "",
            "count": 0,
            "run_count": summary.get("run_count", ""),
            "off_median_s": off_by_query.get(query, ""),
            "policy_median_s": summary.get("median_s", ""),
            "speedup_vs_off_median": summary.get("speedup_vs_off_median", ""),
            "correctness_diff": summary.get("correctness_diff", ""),
            "compiled_regions": summary.get("compiled_regions", ""),
            "runtime_regions": summary.get("runtime_regions", ""),
            "unsupported_decisions": summary.get("unsupported_decisions", ""),
            "skipped_decisions": summary.get("skipped_decisions", ""),
            "unavailable_decisions": summary.get("unavailable_decisions", ""),
            "disabled_decisions": summary.get("disabled_decisions", ""),
            "compile_errors": summary.get("compile_errors", ""),
            "runner_cost_profile": "false",
            "runner_cost_full_pipeline": "false",
        }
        classification, next_action = classify(row, material_speedup)
        row["classification"] = classification
        row["next_action"] = next_action
        rows.append({field: row.get(field, "") for field in INVENTORY_FIELDS})
    rows.sort(key=lambda row: (row["query"], row["policy"], row["shape_signature"]))
    return rows


def write_markdown(path: Path, rows: list[dict], material_speedup: float) -> None:
    classes = collections.Counter(row["classification"] for row in rows)
    lines = [
        "# JIT Shape Inventory",
        "",
        f"Material speedup threshold: `{material_speedup:.3f}x`",
        "",
        "## Classification Counts",
        "",
        "| Classification | Shape Rows |",
        "| --- | ---: |",
    ]
    for classification, count in sorted(classes.items()):
        lines.append(f"| {classification} | {count} |")
    lines.extend(
        [
            "",
            "## Shapes",
            "",
            "| Query | Policy | Class | Status | Speedup | Count | Blocker | Shape |",
            "| --- | --- | --- | --- | ---: | ---: | --- | --- |",
        ]
    )
    for row in rows:
        blocker = row["blocker"].replace("|", "\\|")
        shape = row["shape_signature"].replace("|", "\\|")
        lines.append(
            f"| {row['query']} | {row['policy']} | {row['classification']} | {row['status']} | "
            f"{row['speedup_vs_off_median']} | {row['count']} | {blocker} | `{shape}` |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_shape_inventory(
    trace_dir: Path, *, workload: str = "tpch", material_speedup: float = 1.08
) -> list[dict]:
    trace_dir = trace_dir.resolve()
    summary_rows = read_csv(trace_dir / "summary.csv")
    run_rows = read_csv(trace_dir / "runs.csv")
    counter_rows = read_csv(trace_dir / "counters.csv")
    rows = build_inventory_rows(trace_dir, workload, material_speedup, summary_rows, run_rows, counter_rows)
    write_csv(trace_dir / "shape_inventory.csv", INVENTORY_FIELDS, rows)
    write_markdown(trace_dir / "shape_inventory.md", rows, material_speedup)
    return rows


def verify_shape_inventory(trace_dir: Path, queries: list[str], policies: list[str]) -> None:
    rows = read_csv(trace_dir / "shape_inventory.csv")
    if not rows:
        raise AssertionError("shape_inventory.csv: expected rows")
    missing = [field for field in INVENTORY_FIELDS if field not in rows[0]]
    if missing:
        raise AssertionError(f"shape_inventory.csv: missing required columns {missing}")
    expected = {(query, policy) for query in queries for policy in policies}
    actual = {(row["query"], row["policy"]) for row in rows}
    missing_pairs = expected - actual
    if missing_pairs:
        raise AssertionError(f"shape_inventory.csv: missing query/policy rows {sorted(missing_pairs)}")
    known_classes = {
        "backend_limited",
        "cbo_limited",
        "compiled_regression",
        "correctly_skipped",
        "correctness_failed",
        "no_jit_decision",
        "proven_win",
        "runtime_limited",
        "runtime_observation",
        "unknown",
        "vectorized_baseline",
    }
    for row in rows:
        if row["classification"] not in known_classes:
            raise AssertionError(f"shape_inventory.csv: unknown classification: {row}")
        if not row["shape_signature"]:
            raise AssertionError(f"shape_inventory.csv: missing shape signature: {row}")
    md_path = trace_dir / "shape_inventory.md"
    if not md_path.exists():
        raise AssertionError(f"missing shape inventory markdown: {md_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a JIT shape inventory from benchmark artifacts")
    parser.add_argument("trace_dir", type=Path)
    parser.add_argument("--workload", default="tpch")
    parser.add_argument("--material-speedup", type=float, default=1.08)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = write_shape_inventory(args.trace_dir, workload=args.workload, material_speedup=args.material_speedup)
    print(f"shape inventory rows: {len(rows)}")
    print(f"shape inventory: {args.trace_dir.resolve() / 'shape_inventory.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
