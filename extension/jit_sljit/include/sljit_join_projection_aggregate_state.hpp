//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_projection_aggregate_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_source_indices.hpp"
#include "sljit_batch_layout.hpp"
#include "sljit_region_adapter_scratch.hpp"
#include "sljit_runtime_batch_state.hpp"

#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

struct SljitRegionExecutionScratch;

enum class SljitJoinProjectionAggregateInputKind : uint8_t {
	UNUSED,
	PROJECTION_OUTPUT, // Unbound; never published to execution.
	HASH_JOIN_LHS_INPUT,
	HASH_JOIN_RHS_INPUT,
	HASH_JOIN_LHS_EXPRESSION,
	HASH_JOIN_RHS_EXPRESSION,
	HASH_JOIN_MIXED_EXPRESSION
};

struct SljitJoinProjectionAggregateInputSource {
	SljitJoinProjectionAggregateInputKind kind = SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT;
	idx_t projection_idx = DConstants::INVALID_INDEX;
	idx_t input_idx = DConstants::INVALID_INDEX;
	LogicalType type;
	unique_ptr<SljitExecutableRegionExpression> expression;
	//! Query-local evaluation scratch; allocated only for a bound computed input.
	unique_ptr<SljitExpressionAdapterScratch> adapter;
	vector<SljitJoinProjectionAggregateInputSource> expression_sources;

	bool IsBuildInput() const {
		return kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_RHS_INPUT ||
		       kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_RHS_EXPRESSION;
	}
};

//! Immutable semantic terminal descriptor. It identifies a build-side payload;
//! execution resolves the current row or dictionary storage from the hash table.
struct SljitHashJoinDirectUngroupedAggregateDescriptor {
	AggregatePrimitiveUpdateKind primitive_kind = AggregatePrimitiveUpdateKind::NONE;
	idx_t rhs_output_idx = DConstants::INVALID_INDEX;
	LogicalType rhs_type;
	PhysicalType rhs_physical_type = PhysicalType::INVALID;

	bool Ready() const {
		return primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
		       (rhs_output_idx != DConstants::INVALID_INDEX && rhs_physical_type != PhysicalType::INVALID);
	}
};

struct SljitJoinProjectionAggregateDescriptor {
	SljitDeferredBuildState build_state;
	idx_t projection_idx = DConstants::INVALID_INDEX;
	optional_ptr<SljitExecutableRegionOp> projection_ref;
	vector<SljitJoinProjectionAggregateInputSource> input_sources;
	vector<ExecutionRowPointerGroupKeySource> group_sources;
	vector<idx_t> payload_source_indices;
	vector<bool> payload_source_not_null;
	SljitAggregatePayloadSourceLayout payload_source_layout = SljitAggregatePayloadSourceLayout::DIRECT_PER_LANE;
	vector<SljitExecutableRegionExpression> remapped_payloads;
	SljitHashJoinDirectUngroupedAggregateDescriptor direct_ungrouped_aggregate;
	//! Borrowed from the immutable recipe, whose lifetime covers this local state.
	optional_ptr<const SljitBatchLayout> producer_layout;
	SljitDataChunkBatch input;

	bool Built() const {
		return build_state.Built();
	}

	bool Ready() const {
		return build_state.Ready();
	}

	const string &Blocker() const {
		return build_state.blocker;
	}

	bool ProducerLayoutMatches(optional_ptr<const SljitBatchLayout> layout) const {
		return producer_layout.get() == layout.get();
	}

	void ClearProjectionState() {
		projection_idx = DConstants::INVALID_INDEX;
		projection_ref = nullptr;
		input_sources.clear();
		group_sources.clear();
		payload_source_indices.clear();
		payload_source_not_null.clear();
		payload_source_layout = SljitAggregatePayloadSourceLayout::DIRECT_PER_LANE;
		remapped_payloads.clear();
		direct_ungrouped_aggregate = SljitHashJoinDirectUngroupedAggregateDescriptor();
	}

	void ClearBuiltState() {
		ClearProjectionState();
		producer_layout = nullptr;
	}

	bool Block(const char *blocker_p) {
		ClearProjectionState();
		return build_state.Block(blocker_p);
	}

	void MarkReady() {
		build_state.MarkReady();
	}

	void BorrowProjection(SljitExecutableRegionOp &projection_p) {
		projection_ref = &projection_p;
	}

	void BorrowProjection(idx_t projection_idx_p, SljitExecutableRegionOp &projection_p) {
		projection_idx = projection_idx_p;
		projection_ref = &projection_p;
	}

	SljitExecutableRegionOp &Projection() {
		D_ASSERT(projection_ref);
		return *projection_ref;
	}

	vector<LogicalType> InputTypes() const {
		vector<LogicalType> types;
		types.reserve(input_sources.size());
		for (const auto &source : input_sources) {
			types.push_back(source.type);
		}
		return types;
	}

	void EnsureInput(Allocator &allocator) {
		if (!input.Initialized()) {
			input.Ensure(allocator, InputTypes());
		}
	}
};

} // namespace duckdb
