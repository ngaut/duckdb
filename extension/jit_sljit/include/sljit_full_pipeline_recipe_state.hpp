//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_primitive_sequence.hpp"

#include <utility>

namespace duckdb {

struct SljitHashJoinDirectAggregateConsumerContract {
	idx_t probe_step_idx = DConstants::INVALID_INDEX;
	idx_t terminal_step_idx = DConstants::INVALID_INDEX;
	idx_t probe_input_filter_idx = DConstants::INVALID_INDEX;
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t aggregate_idx = DConstants::INVALID_INDEX;

	bool IsBound() const {
		return probe_step_idx != DConstants::INVALID_INDEX && terminal_step_idx != DConstants::INVALID_INDEX &&
		       hash_join_idx != DConstants::INVALID_INDEX && aggregate_idx != DConstants::INVALID_INDEX;
	}

	bool HasAnyBinding() const {
		return probe_step_idx != DConstants::INVALID_INDEX || terminal_step_idx != DConstants::INVALID_INDEX ||
		       probe_input_filter_idx != DConstants::INVALID_INDEX || hash_join_idx != DConstants::INVALID_INDEX ||
		       aggregate_idx != DConstants::INVALID_INDEX;
	}
};

static SljitHashJoinDirectAggregateConsumerContract
SljitMakeHashJoinDirectAggregateConsumerContract(idx_t probe_step_idx, idx_t terminal_step_idx, idx_t hash_join_idx,
                                                 idx_t aggregate_idx,
                                                 idx_t probe_input_filter_idx = DConstants::INVALID_INDEX) {
	SljitHashJoinDirectAggregateConsumerContract contract;
	contract.probe_step_idx = probe_step_idx;
	contract.terminal_step_idx = terminal_step_idx;
	contract.probe_input_filter_idx = probe_input_filter_idx;
	contract.hash_join_idx = hash_join_idx;
	contract.aggregate_idx = aggregate_idx;
	return contract;
}

static void
SljitValidateHashJoinDirectAggregateConsumerContract(const SljitFullPipelinePrimitiveSequence &primitive_sequence,
                                                     const SljitHashJoinDirectAggregateConsumerContract &contract) {
	if (!contract.IsBound()) {
		if (contract.HasAnyBinding()) {
			throw InternalException("SLJIT direct aggregate consumer contract is only partially bound");
		}
		return;
	}
	if (contract.probe_step_idx >= primitive_sequence.Count() ||
	    contract.terminal_step_idx + 1 != primitive_sequence.Count()) {
		throw InternalException("SLJIT direct aggregate consumer contract has invalid step ownership");
	}
	const auto &probe_step = primitive_sequence.Step(contract.probe_step_idx);
	const auto &terminal_step = primitive_sequence.Step(contract.terminal_step_idx);
	if (probe_step.kind != SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION ||
	    probe_step.hash_join_probe_selection.hash_join_idx != contract.hash_join_idx ||
	    terminal_step.kind != SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE ||
	    terminal_step.post_join_projection_aggregate.post_join_projection.hash_join_idx != contract.hash_join_idx ||
	    terminal_step.post_join_projection_aggregate.aggregate_idx != contract.aggregate_idx) {
		throw InternalException("SLJIT direct aggregate consumer contract has inconsistent operator ownership");
	}
	const auto expected_terminal_step_idx =
	    contract.probe_step_idx + (contract.probe_input_filter_idx == DConstants::INVALID_INDEX ? 1 : 2);
	if (expected_terminal_step_idx != contract.terminal_step_idx) {
		throw InternalException("SLJIT direct aggregate consumer contract has an unsupported primitive boundary");
	}
	if (contract.probe_input_filter_idx != DConstants::INVALID_INDEX) {
		const auto &filter_step = primitive_sequence.Step(contract.probe_step_idx + 1);
		if (filter_step.kind != SljitFullPipelinePrimitiveKind::GENERATED_FILTER ||
		    filter_step.generated_filter.filter_idx != contract.probe_input_filter_idx) {
			throw InternalException("SLJIT direct aggregate consumer contract has inconsistent filter ownership");
		}
	}
}

enum class SljitFullPipelineRuntimeKind : uint8_t { PRIMITIVE_SEQUENCE, SELECTED_HASH_JOIN_SINK };

struct SljitFullPipelineRecipe {
	SljitFullPipelinePrimitiveSequence primitive_sequence;
	SljitHashJoinDirectAggregateConsumerContract direct_aggregate_consumer;
	SljitFullPipelineRuntimeKind runtime_kind = SljitFullPipelineRuntimeKind::PRIMITIVE_SEQUENCE;
	bool uses_extended_source_fetch_budget = false;
	bool preserves_partitioned_source_chunks = false;
	bool has_scan_filter_executable_body = false;
	vector<idx_t> fused_filter_owners;

	bool UsesSelectedHashJoinSinkRuntime() const {
		return runtime_kind == SljitFullPipelineRuntimeKind::SELECTED_HASH_JOIN_SINK;
	}

	bool OwnsFusedFilter(idx_t filter_idx) const {
		for (auto owned_filter_idx : fused_filter_owners) {
			if (owned_filter_idx == filter_idx) {
				return true;
			}
		}
		return false;
	}
};

enum class SljitFullPipelineRecipePlanKind : uint8_t { INVALID, PRIMITIVE_RECIPE, NATIVE_ONLY };

class SljitFullPipelineRecipePlan {
public:
	SljitFullPipelineRecipePlan() = default;

	bool HasRecipe() const {
		return kind == SljitFullPipelineRecipePlanKind::PRIMITIVE_RECIPE;
	}

	SljitFullPipelineRecipePlanKind Kind() const {
		return kind;
	}

	const SljitFullPipelineRecipe &Recipe() const {
		D_ASSERT(HasRecipe());
		return recipe;
	}

	const string &NativeOnlyRuntimePath() const {
		D_ASSERT(kind == SljitFullPipelineRecipePlanKind::NATIVE_ONLY);
		return native_only_runtime_path;
	}

private:
	friend SljitFullPipelineRecipePlan SljitMakeFullPipelinePrimitiveRecipePlan(SljitFullPipelineRecipe recipe);
	friend SljitFullPipelineRecipePlan SljitMakeFullPipelineNativeOnlyPlan(string runtime_path);

	SljitFullPipelineRecipePlanKind kind = SljitFullPipelineRecipePlanKind::INVALID;
	SljitFullPipelineRecipe recipe;
	string native_only_runtime_path;
};

inline SljitFullPipelineRecipePlan SljitMakeFullPipelinePrimitiveRecipePlan(SljitFullPipelineRecipe recipe) {
	SljitFullPipelineRecipePlan plan;
	plan.kind = SljitFullPipelineRecipePlanKind::PRIMITIVE_RECIPE;
	plan.recipe = std::move(recipe);
	return plan;
}

inline SljitFullPipelineRecipePlan SljitMakeFullPipelineNativeOnlyPlan(string runtime_path) {
	if (runtime_path.empty()) {
		throw InternalException("SLJIT native-only recipe plan requires a runtime path");
	}
	SljitFullPipelineRecipePlan plan;
	plan.kind = SljitFullPipelineRecipePlanKind::NATIVE_ONLY;
	plan.native_only_runtime_path = std::move(runtime_path);
	return plan;
}

} // namespace duckdb
