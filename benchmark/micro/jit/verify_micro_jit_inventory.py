#!/usr/bin/env python3
#
# Verify that JIT benchmark proofs match backend admission rules.

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from micro_jit_manifest import ADMITTED_FAMILIES, DIAGNOSTIC_FAMILIES, LARGE_ROW_COUNT


BENCHMARK_FILE_RE = re.compile(r"^(?P<family>.+?)(?P<threshold>_threshold)?_(?P<policy>off|auto|force)\.benchmark$")
SHAPE_CONSTANT_RE = re.compile(r"static constexpr const char \*(\w+)\s*=\s*\"([^\"]+)\";", re.MULTILINE)
ADMISSION_RULE_RE = re.compile(r'\{(?P<constant>\w+),\s*(?P<cardinality>\d+),\s*"(?P<proof>[^"]+)"\}')


def fail(message: str) -> None:
    raise AssertionError(message)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def read_text(root: Path, rel_path: str) -> str:
    path = root / rel_path
    if not path.exists():
        fail(f"missing required file: {rel_path}")
    return path.read_text(encoding="utf-8")


def parse_assignment(text: str, key: str) -> str:
    pattern = re.compile(rf"^{re.escape(key)}=(.+)$", re.MULTILINE)
    match = pattern.search(text)
    if not match:
        fail(f"missing {key} assignment")
    return match.group(1).strip()


def parse_template(text: str) -> str:
    match = re.search(r"^template\s+(.+)$", text, re.MULTILINE)
    if not match:
        fail("missing template assignment")
    return match.group(1).strip()


def verify_backend_rules(root: Path) -> None:
    backend_text = read_text(root, "extension/jit_sljit/sljit_backend.cpp")
    shape_header_text = read_text(root, "extension/jit_sljit/include/sljit_region_plan.hpp")
    shape_constants = dict(SHAPE_CONSTANT_RE.findall(shape_header_text))
    backend_rules = {
        match.group("constant"): {
            "min_cardinality": int(match.group("cardinality")),
            "proof": match.group("proof"),
        }
        for match in ADMISSION_RULE_RE.finditer(backend_text)
    }
    backend_proofs = {entry["proof"] for entry in backend_rules.values()}
    expected_proofs = {entry["proof"] for entry in ADMITTED_FAMILIES.values()}
    if backend_proofs != expected_proofs:
        fail(f"backend admission proofs mismatch: expected={sorted(expected_proofs)} actual={sorted(backend_proofs)}")
    for family, entry in ADMITTED_FAMILIES.items():
        backend_rule = backend_rules.get(entry["shape_constant"])
        if backend_rule is None:
            fail(f"backend admission rule for {family} does not reference shape constant {entry['shape_constant']}")
        if shape_constants.get(entry["shape_constant"]) != entry["shape_key"]:
            fail(f"shape constant for {family} does not define {entry['shape_key']}")
        if backend_rule["min_cardinality"] != entry["min_cardinality"]:
            fail(f"backend admission rule for {family} must keep min_cardinality={entry['min_cardinality']}")
        if backend_rule["proof"] != entry["proof"]:
            fail(f"backend admission rule for {family} must keep proof={entry['proof']}")


def verify_admitted_template(root: Path, family: str, entry: dict) -> None:
    text = read_text(root, entry["template"])
    required = [
        "result_query I",
        "duckdb_jit_events()",
        f"admission_shape_key='{entry['shape_key']}'",
        f"admission_proof='{entry['proof']}'",
        "policy_decision='auto'",
        "status='unsupported'",
        "region_execution_form='none'",
        "source-fusion-gap:requires-native-source",
        "code_size=0",
        "policy_decision='force'",
        "code_size > 0",
    ]
    for needle in required:
        if needle not in text:
            fail(f"{entry['template']}: admitted template {family} missing {needle}")


def verify_diagnostic_template(root: Path, family: str, template: str) -> None:
    text = read_text(root, template)
    if "result I" not in text and "result_query I" not in text:
        fail(f"{template}: diagnostic template {family} must validate SQL result")
    if "policy_decision='auto'" in text or "admission_proof=" in text:
        fail(f"{template}: diagnostic template {family} must not claim auto admission proof")


def verify_benchmark_file(root: Path, rel_path: str) -> tuple:
    text = read_text(root, rel_path)
    file_name = Path(rel_path).name
    match = BENCHMARK_FILE_RE.match(file_name)
    if not match:
        fail(f"{rel_path}: benchmark file name does not match expected family/policy format")
    family = match.group("family")
    policy = match.group("policy")
    is_threshold = bool(match.group("threshold"))
    template = parse_template(text)
    declared_policy = parse_assignment(text, "JIT_POLICY")
    row_count = int(parse_assignment(text, "ROW_COUNT"))
    if declared_policy != policy:
        fail(f"{rel_path}: policy suffix {policy} does not match JIT_POLICY={declared_policy}")
    if "EXPECTED_RESULT=" not in text:
        fail(f"{rel_path}: missing EXPECTED_RESULT")

    known_templates = {
        **{family_name: entry["template"] for family_name, entry in ADMITTED_FAMILIES.items()},
        **{family_name: entry["template"] for family_name, entry in DIAGNOSTIC_FAMILIES.items()},
    }
    if family not in known_templates:
        fail(f"{rel_path}: unknown microbenchmark family {family}")
    if template != known_templates[family]:
        fail(f"{rel_path}: template {template} does not match family {family}")
    if family in ADMITTED_FAMILIES:
        expected_rows = ADMITTED_FAMILIES[family]["threshold_row_count"] if is_threshold else LARGE_ROW_COUNT
    else:
        expected_rows = 1000000 if is_threshold else LARGE_ROW_COUNT
    if row_count != expected_rows:
        fail(f"{rel_path}: expected ROW_COUNT={expected_rows}, found {row_count}")
    if family not in ADMITTED_FAMILIES and policy == "auto":
        fail(f"{rel_path}: diagnostic family {family} must not define auto policy benchmark")
    return family, policy, is_threshold


def verify_benchmark_scripts(root: Path) -> None:
    manifest_backed_scripts = (
        "benchmark/micro/jit/micro_jit_benchmark.py",
        "benchmark/micro/jit/verify_micro_jit_benchmark.py",
        "benchmark/micro/jit/micro_jit_trace.py",
        "benchmark/micro/jit/verify_micro_jit_trace.py",
        "benchmark/micro/jit/micro_jit_diagnostic_benchmark.py",
        "benchmark/micro/jit/verify_micro_jit_diagnostic_benchmark.py",
        "benchmark/micro/jit/micro_jit_diagnostic_sweep.py",
        "benchmark/micro/jit/verify_micro_jit_diagnostic_sweep.py",
    )
    for script in manifest_backed_scripts:
        text = read_text(root, script)
        if "micro_jit_manifest" not in text:
            fail(f"{script}: must consume benchmark families from micro_jit_manifest.py")


def verify_inventory(root: Path) -> None:
    verify_backend_rules(root)
    for family, entry in ADMITTED_FAMILIES.items():
        verify_admitted_template(root, family, entry)
    for family, entry in DIAGNOSTIC_FAMILIES.items():
        verify_diagnostic_template(root, family, entry["template"])

    seen = set()
    benchmark_dir = root / "benchmark" / "micro" / "jit"
    for path in sorted(benchmark_dir.glob("*.benchmark")):
        family, policy, is_threshold = verify_benchmark_file(root, path.relative_to(root).as_posix())
        seen.add((family, policy, is_threshold))

    for family in ADMITTED_FAMILIES:
        for is_threshold in (False, True):
            for policy in ("off", "auto", "force"):
                if (family, policy, is_threshold) not in seen:
                    suffix = "threshold" if is_threshold else "large"
                    fail(f"missing admitted {suffix} benchmark for family={family} policy={policy}")
    for family in DIAGNOSTIC_FAMILIES:
        for is_threshold in (False, True):
            for policy in ("off", "force"):
                if (family, policy, is_threshold) not in seen:
                    suffix = "threshold" if is_threshold else "large"
                    fail(f"missing diagnostic {suffix} benchmark for family={family} policy={policy}")
            if (family, "auto", is_threshold) in seen:
                fail(f"diagnostic family {family} has an auto benchmark without admission proof")
    verify_benchmark_scripts(root)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify JIT microbenchmark inventory")
    parser.add_argument("--root", type=Path, default=repo_root())
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    verify_inventory(root)
    print(
        "ok micro_inventory admitted={admitted} diagnostic={diagnostic}".format(
            admitted=len(ADMITTED_FAMILIES),
            diagnostic=len(DIAGNOSTIC_FAMILIES),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
