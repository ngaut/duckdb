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

static ExecutionOperatorBindResult
SljitBindNativeOperator(ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
                        SljitExecutableRegionOp &op, DataChunk &input, const ExecutionRegionOperatorInfo &operator_info,
                        const char *blocker_prefix, const char *error_prefix, ExecutionOperatorBinding *&binding_out,
                        string &deferred_reason, optional_ptr<bool> bound = nullptr) {
	auto &binding = scratch.OperatorBinding(op_idx);
	binding_out = &binding;
	if (bound) {
		*bound = false;
	}
	if (scratch.HasOperatorBinding(op_idx)) {
		return ExecutionOperatorBindResult::READY;
	}
	if (op.operator_index == DConstants::INVALID_INDEX) {
		throw InternalException("%s is missing an operator index", error_prefix);
	}
	auto bind_result = native_runtime.BindOperator(op.operator_index, input, operator_info, binding);
	if (bound) {
		*bound = true;
	}
	if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
		deferred_reason = binding.blocker.empty() ? string(blocker_prefix) : binding.blocker;
		return bind_result;
	}
	if (bind_result != ExecutionOperatorBindResult::READY) {
		auto blocker = binding.blocker.empty() ? string("unknown") : binding.blocker;
		throw InternalException("%s operator binding failed: %s", error_prefix, blocker);
	}
	if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE && binding.hash_join_probe.ready &&
	    binding.hash_join_probe.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE &&
	    !binding.hash_join_probe.empty_build_side) {
		SljitValidateRegularHashJoinProbeExecutionLayout(op.hash_join_probe.plan, binding.hash_join_probe);
	}
	scratch.MarkOperatorBindingReady(op_idx);
	return bind_result;
}

static ExecutionSinkBinding &SljitBindNativeSink(ExecutionOperatorRuntime &native_runtime,
                                                 SljitRegionExecutionScratch &scratch, idx_t op_idx, DataChunk &input,
                                                 const ExecutionRegionSinkInfo &sink_info, const char *blocker_prefix,
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

static ExecutionSinkBinding &SljitBindRecordedNativeSink(ExecutionRegionRuntime &runtime,
                                                         ExecutionOperatorRuntime &native_runtime,
                                                         SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                         SljitNativeRegionOpKind op_kind, DataChunk &input,
                                                         const ExecutionRegionSinkInfo &sink_info,
                                                         const char *blocker_prefix, const char *error_prefix) {
	auto bind_stage_start = SljitRegionStageStart(runtime);
	bool bound = false;
	auto &binding =
	    SljitBindNativeSink(native_runtime, scratch, op_idx, input, sink_info, blocker_prefix, error_prefix, bound);
	if (bound) {
		RecordSljitRegionStageRuntime(runtime, op_idx, op_kind, "bind_sink_contract", bind_stage_start);
	}
	return binding;
}

static ExecutionOperatorBindResult
SljitBindRecordedNativeOperator(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
                                SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
                                DataChunk &input, const ExecutionRegionOperatorInfo &operator_info,
                                const char *blocker_prefix, const char *error_prefix,
                                ExecutionOperatorBinding *&binding_out, string &deferred_reason) {
	auto bind_stage_start = SljitRegionStageStart(runtime);
	bool bound = false;
	auto bind_result = SljitBindNativeOperator(native_runtime, scratch, op_idx, op, input, operator_info,
	                                           blocker_prefix, error_prefix, binding_out, deferred_reason, bound);
	if (bound) {
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "bind_operator_contract", bind_stage_start);
	}
	return bind_result;
}

} // namespace duckdb
