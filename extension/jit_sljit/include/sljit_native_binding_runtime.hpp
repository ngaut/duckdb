//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_binding_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_runtime.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

//! Binds a native operator the kernel delegates to. The runner found every one ready before the first source fetch
//! and a running pipeline never changes runners, so an operator that defers is an invariant violation, not a fallback.
static ExecutionOperatorBinding &
SljitBindNativeOperator(ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
                        SljitExecutableRegionOp &op, DataChunk &input, const ExecutionRegionOperatorInfo &operator_info,
                        const char *error_prefix, optional_ptr<bool> bound = nullptr,
                        optional_ptr<const SljitNativeHashJoinProbePlan> probe_plan = nullptr) {
	auto &binding = scratch.OperatorBinding(op_idx);
	if (bound) {
		*bound = false;
	}
	if (scratch.HasOperatorBinding(op_idx)) {
		return binding;
	}
	if (op.operator_index == DConstants::INVALID_INDEX) {
		throw InternalException("%s is missing an operator index", error_prefix);
	}
	auto bind_result = native_runtime.BindOperator(op.operator_index, input, operator_info, binding);
	if (bound) {
		*bound = true;
	}
	if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
		throw InternalException("%s delegates to a native operator that is not ready: %s", error_prefix,
		                        binding.blocker.empty() ? string("no reason given") : binding.blocker);
	}
	if (bind_result != ExecutionOperatorBindResult::READY) {
		auto blocker = binding.blocker.empty() ? string("unknown") : binding.blocker;
		throw InternalException("%s operator binding failed: %s", error_prefix, blocker);
	}
	if (op.Kind() == SljitNativeRegionOpKind::HASH_JOIN_PROBE && binding.hash_join_probe.bloom_filter_requested) {
		// Keys that passed this build side's exact membership filter in the source all match, unless the scan dropped
		// the filter for keeping most rows: a bloom filter would reject nothing worth building it for
		auto &probe_key_filter = scratch.ExactSourceFilterIdentity(op.HashJoinProbe().plan.exact_source_filter_binding);
		if (!probe_key_filter || probe_key_filter != binding.hash_join_probe.table_layout.runtime_filter_identity) {
			ExecutionUseHashJoinProbeBloomFilter(binding.hash_join_probe);
		}
	}
	if (op.Kind() == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
		auto &plan = probe_plan ? *probe_plan : op.HashJoinProbe().plan;
		auto &probe = binding.hash_join_probe;
		if (!binding.ready || !probe.ready || plan.output_mode == ExecutionHashJoinProbeOutputMode::NONE ||
		    probe.output_mode != plan.output_mode || probe.probe_key_input_indices.size() != plan.keys.size()) {
			throw InternalException("SLJIT hash join binding does not implement its selected probe plan");
		}
		if (!probe.empty_build_side) {
			switch (probe.layout_kind) {
			case ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE:
				SljitValidateRegularHashJoinProbeExecutionLayout(plan, probe);
				break;
			case ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE:
				SljitValidatePerfectHashJoinProbeExecutionLayout(plan, probe, input);
				break;
			default:
				throw InternalException("SLJIT hash join binding has no executable layout");
			}
		}
	}
	scratch.MarkOperatorBindingReady(op_idx);
	return binding;
}

template <class SINK_INFO>
static ExecutionSinkBinding &SljitBindNativeSink(ExecutionOperatorRuntime &native_runtime,
                                                 SljitRegionExecutionScratch &scratch, idx_t op_idx, DataChunk &input,
                                                 const SINK_INFO &sink_info, const char *blocker_prefix,
                                                 const char *error_prefix, optional_ptr<bool> bound = nullptr) {
	auto &binding = scratch.SinkBinding(op_idx);
	if (bound) {
		*bound = false;
	}
	if (scratch.HasSinkBinding(op_idx)) {
		return binding;
	}
	if (!native_runtime.BindSink(input, sink_info, binding)) {
		auto blocker = binding.blocker.empty() ? string(blocker_prefix) : binding.blocker;
		throw InternalException("%s binding failed: %s", error_prefix, blocker);
	}
	scratch.MarkSinkBindingReady(op_idx);
	if (bound) {
		*bound = true;
	}
	return binding;
}

template <class SINK_INFO>
static ExecutionSinkBinding &
SljitBindRecordedNativeSink(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
                            SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitNativeRegionOpKind op_kind,
                            DataChunk &input, const SINK_INFO &sink_info, const char *blocker_prefix,
                            const char *error_prefix) {
	auto bind_stage_start = SljitRegionStageStart(runtime);
	bool bound = false;
	auto &binding =
	    SljitBindNativeSink(native_runtime, scratch, op_idx, input, sink_info, blocker_prefix, error_prefix, bound);
	if (bound) {
		RecordSljitRegionStageRuntime(runtime, op_idx, op_kind, "bind_sink_contract", bind_stage_start);
	}
	return binding;
}

static ExecutionOperatorBinding &SljitBindRecordedNativeOperator(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input, const ExecutionRegionOperatorInfo &operator_info,
    const char *error_prefix, optional_ptr<const SljitNativeHashJoinProbePlan> probe_plan = nullptr) {
	auto bind_stage_start = SljitRegionStageStart(runtime);
	bool bound = false;
	auto &binding = SljitBindNativeOperator(native_runtime, scratch, op_idx, op, input, operator_info, error_prefix,
	                                        bound, probe_plan);
	if (bound) {
		RecordSljitRegionStageRuntime(runtime, op_idx, op.Kind(), "bind_operator_contract", bind_stage_start);
		if (binding.hash_join_probe.bloom_filter_requested) {
			runtime.RecordJitRuntimePath(binding.hash_join_probe.use_bloom_filter
			                                 ? "hash_join_probe.bloom_filter.built"
			                                 : "hash_join_probe.bloom_filter.skipped_exact_source_filter");
		}
	}
	return binding;
}

} // namespace duckdb
