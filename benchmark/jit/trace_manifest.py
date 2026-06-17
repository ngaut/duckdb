#!/usr/bin/env python3
#
# Shared manifest helpers for DuckDB compiled-region trace artifacts.

import csv
import datetime
import hashlib
import json
import tempfile
from pathlib import Path


TRACE_CONTRACT_VERSION = 1
TRACE_MANIFEST_NAME = "trace_manifest.json"


def default_trace_output_directory(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=f"duckdb_jit_{prefix}_"))


def prepare_trace_output_directory(out_dir: Path) -> Path:
    out_dir = out_dir.resolve()
    if out_dir.exists():
        if not out_dir.is_dir():
            raise RuntimeError(f"trace output path exists and is not a directory: {out_dir}")
        entries = sorted(child.name for child in out_dir.iterdir())
        if entries:
            example = ", ".join(entries[:5])
            if len(entries) > 5:
                example += ", ..."
            raise RuntimeError(
                f"trace output directory is not empty: {out_dir}; "
                f"choose a fresh --out-dir or empty it before tracing. Existing entries: {example}"
            )
    else:
        out_dir.mkdir(parents=True)
    return out_dir


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _csv_metadata(path: Path) -> dict:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            columns = next(reader)
        except StopIteration:
            columns = []
            rows = 0
        else:
            rows = sum(1 for _ in reader)
    return {
        "format": "csv",
        "columns": columns,
        "rows": rows,
    }


def artifact_metadata(out_dir: Path, name: str) -> dict:
    path = out_dir / name
    if not path.exists():
        raise RuntimeError(f"manifest artifact does not exist: {path}")
    metadata = {
        "path": name,
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }
    suffix = path.suffix.lower()
    if suffix == ".csv":
        metadata.update(_csv_metadata(path))
    elif suffix == ".json":
        metadata["format"] = "json"
    elif suffix == ".md":
        metadata["format"] = "markdown"
    else:
        metadata["format"] = "binary"
    return metadata


def write_trace_manifest(
    out_dir: Path,
    *,
    kind: str,
    generator: str,
    configuration: dict,
    artifact_names: list,
) -> None:
    unique_artifacts = sorted(dict.fromkeys(name for name in artifact_names if name != TRACE_MANIFEST_NAME))
    manifest = {
        "trace_contract_version": TRACE_CONTRACT_VERSION,
        "kind": kind,
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "generator": generator,
        "configuration": configuration,
        "artifacts": {name: artifact_metadata(out_dir, name) for name in unique_artifacts},
    }
    manifest_path = out_dir / TRACE_MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_trace_manifest(out_dir: Path) -> dict:
    manifest_path = out_dir / TRACE_MANIFEST_NAME
    if not manifest_path.exists():
        raise AssertionError(f"missing required trace artifact: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def verify_trace_manifest(out_dir: Path, *, kind: str, required_artifacts: list) -> dict:
    manifest = read_trace_manifest(out_dir)
    if manifest.get("trace_contract_version") != TRACE_CONTRACT_VERSION:
        raise AssertionError(
            f"{TRACE_MANIFEST_NAME}: expected contract version {TRACE_CONTRACT_VERSION}, "
            f"found {manifest.get('trace_contract_version')}"
        )
    if manifest.get("kind") != kind:
        raise AssertionError(f"{TRACE_MANIFEST_NAME}: expected kind {kind}, found {manifest.get('kind')}")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or not artifacts:
        raise AssertionError(f"{TRACE_MANIFEST_NAME}: missing artifacts")

    expected_entries = set(artifacts)
    expected_entries.add(TRACE_MANIFEST_NAME)
    actual_entries = {child.name for child in out_dir.iterdir()}
    extra_entries = sorted(actual_entries - expected_entries)
    missing_entries = sorted(expected_entries - actual_entries)
    if extra_entries:
        raise AssertionError(f"{TRACE_MANIFEST_NAME}: unexpected files in trace directory: {extra_entries}")
    if missing_entries:
        raise AssertionError(f"{TRACE_MANIFEST_NAME}: manifest files missing from trace directory: {missing_entries}")

    for name in required_artifacts:
        if name not in artifacts:
            raise AssertionError(f"{TRACE_MANIFEST_NAME}: missing required artifact entry {name}")
    for name, recorded in artifacts.items():
        actual = artifact_metadata(out_dir, name)
        for field in ("bytes", "sha256", "format"):
            if recorded.get(field) != actual.get(field):
                raise AssertionError(f"{TRACE_MANIFEST_NAME}: {name} {field} mismatch")
        if actual["format"] == "csv":
            for field in ("columns", "rows"):
                if recorded.get(field) != actual.get(field):
                    raise AssertionError(f"{TRACE_MANIFEST_NAME}: {name} {field} mismatch")
    return manifest
