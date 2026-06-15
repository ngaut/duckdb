#!/usr/bin/env python3
#
# Verify the DuckDB-native JIT architecture. This script is deliberately
# structural: it fails on deleted runtime vocabulary and on missing
# full-pipeline/native-source contracts.

from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def read_text(root: Path, path: str) -> str:
    full_path = root / path
    if not full_path.exists():
        raise AssertionError(f"missing required file: {path}")
    return full_path.read_text(encoding="utf-8")


def source_files(root: Path) -> list[Path]:
    roots = [
        "src",
        "extension/jit_sljit",
        "test/api/test_jit.cpp",
        "test/sql/jit/test_jit_framework.test",
        "benchmark/jit",
        "benchmark/micro/jit",
        "benchmark/tpch/jit",
        "JIT_ARCHITECTURE.md",
    ]
    result: list[Path] = []
    for name in roots:
        path = root / name
        if path.is_file():
            result.append(path)
            continue
        if not path.exists():
            continue
        for child in path.rglob("*"):
            if child.is_file() and child.suffix in {".cpp", ".hpp", ".h", ".py", ".md", ".test", ".in"}:
                result.append(child)
    return sorted(result)


def require_text(root: Path, path: str, snippets: list[str]) -> None:
    text = read_text(root, path)
    for snippet in snippets:
        if snippet not in text:
            raise AssertionError(f"{path}: missing required text: {snippet}")


def forbid_deleted_architecture(root: Path) -> None:
    deleted_tokens = [
        "SOURCE" + "_PREFIX",
        "JitRegionABIIs" + "SourcePrefix",
        "CanExecute" + "SourcePrefix",
        "TryExecute" + "SourcePrefix",
        "TryExecutePrepared" + "SourceReference",
        "source" + "_prefix",
        "source" + "-prefix",
        "generated source" + "-prefix table scan filters",
        "compiled source" + "-prefix without source" + "-prefix executable ABI",
        "native source" + "-prefix",
        "source" + "-pushed filters require source" + "-prefix",
    ]
    for path in source_files(root):
        rel = path.relative_to(root)
        text = path.read_text(encoding="utf-8")
        for token in deleted_tokens:
            if token in text:
                raise AssertionError(f"{rel}: deleted JIT architecture token remains: {token}")


def verify_core_contract(root: Path) -> None:
    require_text(
        root,
        "src/include/duckdb/execution/jit/common.hpp",
        [
            "enum class JitRegionABI : uint8_t { NONE, FULL_PIPELINE, STATE_SCAN };",
            "enum class JitRegionCandidateScope : uint8_t { FULL_PIPELINE };",
            "enum class JitRegionExecutionForm : uint8_t { NONE, FUSED };",
        ],
    )
    require_text(
        root,
        "src/execution/jit_types.cpp",
        [
            'return "full_pipeline";',
            "return abi == JitRegionABI::FULL_PIPELINE || abi == JitRegionABI::STATE_SCAN;",
            "return abi == JitRegionABI::FULL_PIPELINE;",
        ],
    )
    require_text(
        root,
        "src/include/duckdb/execution/jit/runtime.hpp",
        [
            "virtual bool CanExecuteFullPipeline() const;",
            "virtual bool TryExecuteFullPipeline(JitFullPipelineRuntime &runtime, JitFullPipelineResult &result);",
            "source_native_output_rows",
            "generated_body_runtime_time_us",
        ],
    )
    require_text(
        root,
        "src/include/duckdb/execution/jit/region_executor.hpp",
        ["static bool TryExecuteFullPipeline(PipelineExecutor &executor, idx_t max_chunks, PipelineExecuteResult &result);"],
    )
    require_text(
        root,
        "src/execution/jit.cpp",
        [
            "compiled full pipeline without full-pipeline executable ABI",
            "JitRegionABIIsFullPipeline(candidate.contract.abi) && !result.kernel->CanExecuteFullPipeline()",
        ],
    )


def verify_pipeline_dispatch(root: Path) -> None:
    require_text(
        root,
        "src/parallel/pipeline_executor.cpp",
        [
            "HasCompiledJitPreparedSourceOwnerKernel",
            "JitRegionExecutor::TryExecuteFullPipeline(*this, max_chunks, jit_result)",
            "use_prepared_source_input",
            "FetchFromNativeSource",
        ],
    )
    require_text(
        root,
        "src/execution/jit_region_executor.cpp",
        [
            "PipelineJitFullPipelineRuntime runtime(executor, max_chunks, trace_runtime)",
            "source-to-sink boundary",
            "JIT full pipeline kernel returned false at runtime",
            "FetchNativeSource(DataChunk *&result",
            "TryFlushCachingOperators",
            "full pipeline kernel executed",
        ],
    )


def verify_native_source_contract(root: Path) -> None:
    required_fields = [
        "native_source_input_column_count",
        "native_source_input_types",
        "native_source_output_projection_map",
        "native_source_filter_column_map",
        "native_source_requires_unfiltered_input",
        "native_source_filter_prune_required",
        "native_source_filter_takeover_supported",
    ]
    require_text(root, "src/include/duckdb/execution/jit/region.hpp", required_fields)
    require_text(root, "src/execution/jit_operator_descriptor.cpp", required_fields)
    require_text(root, "src/execution/jit_region_ir.cpp", required_fields)
    require_text(
        root,
        "extension/jit_sljit/sljit_region_plan.cpp",
        [
            "generated native table scan filters",
            "source-strategy=prepared-unfiltered-native-source",
            "source-pushed filters require native source-filter takeover",
            "PlanSljitFullPipelineSinkNode",
            "native operator protocol requires full-pipeline region ABI",
            "native_source_input_column_count",
            "native_source_input_types",
            "native_source_output_projection_map",
            "native_source_requires_unfiltered_input",
            "native_source_filter_prune_required",
            "native_source_filter_takeover_supported",
        ]
    )


def verify_observability_and_admission(root: Path) -> None:
    require_text(
        root,
        "extension/jit_sljit/sljit_backend.cpp",
        [
            "SLJIT full-pipeline region",
            "requires measured operator-aware admission proof",
            "jit_policy=auto skips region before backend analysis",
        ],
    )
    require_text(
        root,
        "test/api/test_jit.cpp",
        [
            "JIT full pipeline kernels must advertise the full-pipeline executable ABI explicitly",
            "JIT manager rejects full pipeline kernels without full-pipeline ABI",
            "JIT full pipeline ABI rejects runtime false return",
            "JIT auto rejects source-boundary full pipeline as proof-gap only",
            "JIT auto rejects native-source ungrouped aggregate without production proof",
            "generated native table scan filters",
            "native_source_input_columns=4",
            "candidate_scope == \"full_pipeline\"",
            "JIT full pipeline appends native result collector output",
            "JIT full pipeline executes grouped hash aggregate native lookup protocol",
            "JIT full pipeline executes native hash join build append protocol",
            "JIT full pipeline executes native hash join probe for regular native probe table",
        ],
    )
    require_text(
        root,
        "test/sql/jit/test_jit_framework.test",
        [
            "candidate_scope='full_pipeline'",
            "candidate_scope NOT IN ('full_pipeline', 'state_scan')",
            "jit_policy=auto skips pipeline before typed IR lowering",
            "no SLJIT auto admission family can match pipeline inventory",
            "full-pipeline-native-sink-update",
        ],
    )


def verify_trace_tooling(root: Path) -> None:
    require_text(
        root,
        "benchmark/jit/verify_jit_sql_trace.py",
        [
            '"full_pipeline"',
            '"state_scan"',
            "state-scan executable contains sink boundary",
        ],
    )
    require_text(
        root,
        "benchmark/tpch/jit/tpch_schema.py",
        [
            '"native_source_input_columns"',
            '"native_source_filter_takeover_supported"',
            '"full_pipeline"',
        ],
    )
    require_text(
        root,
        "benchmark/tpch/jit/tpch_trace.py",
        [
            "generated native table scan filters",
            "native_source_input_columns",
            "native_source_filter_takeover_supported",
        ],
    )
    require_text(
        root,
        "benchmark/micro/jit/micro_jit_manifest.py",
        [
            "SLJIT_FULL_PIPELINE_FILTER_PROJECTION_SHAPE",
            "SLJIT_FULL_PIPELINE_PROJECTION_CHAIN_SHAPE",
            "sljit:full-pipeline:filter-projection",
        ],
    )


def verify_architecture_doc(root: Path) -> None:
    require_text(
        root,
        "JIT_ARCHITECTURE.md",
        [
            "DuckDB Native JIT Architecture",
            "backend-neutral region IR",
            "The region planner emits only first-class execution units",
            "`full_pipeline`: owns source entry, every fused operator stage, and the sink.",
            "`state_scan`: owns a state scan source produced by a prior native protocol.",
            "Partial source execution that resumes the DuckDB executor mid-pipeline is not a",
            "valid JIT ABI.",
            "`native_source_input_columns`",
            "`JitRegionKernel::CanExecuteFullPipeline()`",
            "`sljit:full-pipeline:<shape>`",
        ],
    )


def main() -> None:
    root = repo_root()
    forbid_deleted_architecture(root)
    verify_core_contract(root)
    verify_pipeline_dispatch(root)
    verify_native_source_contract(root)
    verify_observability_and_admission(root)
    verify_trace_tooling(root)
    verify_architecture_doc(root)
    print("JIT architecture verification passed")


if __name__ == "__main__":
    main()
