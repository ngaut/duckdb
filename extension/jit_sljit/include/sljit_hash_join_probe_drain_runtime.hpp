//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_drain_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_executor_runtime.hpp"

namespace duckdb {

template <class KERNEL>
static ExecutionOperatorBindResult SljitExecuteNativeHashJoinProbeWithScratch(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
    SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input,
    DataChunk &output, SljitHashJoinProbeDrainState &state, string &deferred_reason,
    bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT,
    optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
	return SljitExecuteNativeHashJoinProbe(
	    kernel, runtime, native_runtime, scratch, hash_join_idx, hash_join_op, input, output,
	    scratch.FilterSelection(hash_join_idx), scratch.HashJoinBuildSelection(hash_join_idx),
	    scratch.HashJoinRowPointers(hash_join_idx), scratch.HashJoinSources(hash_join_idx),
	    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(hash_join_idx) : nullptr,
	    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(hash_join_idx)
	                                                         : nullptr,
	    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualMatchSelection(hash_join_idx)
	                                                         : nullptr,
	    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualRowPointers(hash_join_idx)
	                                                         : nullptr,
	    state, deferred_reason, source_key0_int64_to_int32_unchecked, output_contract, input_remap);
}

template <class KERNEL>
static ExecutionOperatorBindResult SljitExecuteRecordedNativeHashJoinProbeWithScratch(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
    SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input,
    DataChunk &output, SljitHashJoinProbeDrainState &state, string &deferred_reason,
    bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT,
    optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
	auto stage_start = SljitRegionStageStart(runtime);
	auto bind_result = SljitExecuteNativeHashJoinProbeWithScratch(
	    kernel, runtime, native_runtime, scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
	    source_key0_int64_to_int32_unchecked, output_contract, input_remap);
	RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, stage_start);
	return bind_result;
}

template <class OWNER>
struct SljitRecordedHashJoinProbeCallback {
	OWNER &owner;
	ExecutionRegionRuntime &runtime;
	ExecutionOperatorRuntime &native_runtime;
	optional_ptr<SljitRegionExecutionScratch> fixed_scratch;

	const shared_ptr<ExecutionRuntimeFilterIdentity> &ExactSourceFilterIdentity(idx_t hash_join_idx) const {
		return owner.ExactSourceFilterIdentity(hash_join_idx);
	}

	ExecutionOperatorBindResult Execute(SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
	                                    SljitExecutableRegionOp &hash_join_op, DataChunk &input, DataChunk &output,
	                                    SljitHashJoinProbeDrainState &state, string &deferred_reason,
	                                    bool source_key0_int64_to_int32_unchecked,
	                                    SljitHashJoinProbeOutputContract output_contract,
	                                    optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
		return SljitExecuteRecordedNativeHashJoinProbeWithScratch(
		    owner, runtime, native_runtime, scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
		    source_key0_int64_to_int32_unchecked, output_contract, input_remap);
	}

	ExecutionOperatorBindResult operator()(SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
	                                       SljitExecutableRegionOp &hash_join_op, DataChunk &input, DataChunk &output,
	                                       SljitHashJoinProbeDrainState &state, string &deferred_reason,
	                                       bool source_key0_int64_to_int32_unchecked,
	                                       SljitHashJoinProbeOutputContract output_contract,
	                                       optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
		return Execute(scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
		               source_key0_int64_to_int32_unchecked, output_contract, input_remap);
	}

	ExecutionOperatorBindResult operator()(idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input,
	                                       DataChunk &output, SljitHashJoinProbeDrainState &state,
	                                       string &deferred_reason, bool source_key0_int64_to_int32_unchecked,
	                                       SljitHashJoinProbeOutputContract output_contract,
	                                       optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
		if (!fixed_scratch) {
			throw InternalException("SLJIT recorded hash join probe callback requires fixed scratch");
		}
		return Execute(*fixed_scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
		               source_key0_int64_to_int32_unchecked, output_contract, input_remap);
	}
};

template <class OWNER>
static SljitRecordedHashJoinProbeCallback<OWNER>
SljitMakeRecordedHashJoinProbeCallback(OWNER &owner, ExecutionRegionRuntime &runtime,
                                       ExecutionOperatorRuntime &native_runtime) {
	return {owner, runtime, native_runtime, nullptr};
}

template <class OWNER>
static SljitRecordedHashJoinProbeCallback<OWNER>
SljitMakeFixedScratchRecordedHashJoinProbeCallback(OWNER &owner, ExecutionRegionRuntime &runtime,
                                                   ExecutionOperatorRuntime &native_runtime,
                                                   SljitRegionExecutionScratch &scratch) {
	return {owner, runtime, native_runtime, optional_ptr<SljitRegionExecutionScratch>(&scratch)};
}

template <class SCRATCH, class EXECUTE_HASH_JOIN_PROBE, class HANDLE_OUTPUT, class HANDLE_DEFER>
static bool SljitDrainHashJoinProbeOutputsWithState(
    SCRATCH &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input, DataChunk &output,
    EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe, HANDLE_OUTPUT &&handle_output, HANDLE_DEFER &&handle_defer,
    bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT,
    optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
	SljitHashJoinProbeDrainState state;
	do {
		output.Reset();
		string deferred_reason;
		auto bind_result =
		    execute_hash_join_probe(scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
		                            source_key0_int64_to_int32_unchecked, output_contract, input_remap);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return handle_defer(deferred_reason);
		}
		if (output.size() != 0 && handle_output(output, state)) {
			return true;
		}
	} while (!SljitHashJoinProbeDrainFinished(hash_join_op.hash_join_probe.plan.output_mode, state));
	return false;
}

template <class SCRATCH, class EXECUTE_HASH_JOIN_PROBE, class HANDLE_OUTPUT, class HANDLE_DEFER>
static bool SljitDrainHashJoinProbeOutputs(
    SCRATCH &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input, DataChunk &output,
    EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe, HANDLE_OUTPUT &&handle_output, HANDLE_DEFER &&handle_defer,
    bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT,
    optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
	auto handle_output_without_state = [&](DataChunk &output, SljitHashJoinProbeDrainState &) {
		return handle_output(output);
	};
	return SljitDrainHashJoinProbeOutputsWithState(
	    scratch, hash_join_idx, hash_join_op, input, output,
	    std::forward<EXECUTE_HASH_JOIN_PROBE>(execute_hash_join_probe), handle_output_without_state,
	    std::forward<HANDLE_DEFER>(handle_defer), source_key0_int64_to_int32_unchecked, output_contract, input_remap);
}

} // namespace duckdb
