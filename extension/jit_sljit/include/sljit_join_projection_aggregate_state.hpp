//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_projection_aggregate_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_runtime_batch_state.hpp"
#include "sljit_region_executable.hpp"

#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

struct SljitRegionExecutionScratch;

enum class SljitJoinProjectionAggregateInputKind : uint8_t { UNUSED, PROJECTION_OUTPUT, HASH_JOIN_LHS_INPUT };

struct SljitJoinProjectionAggregateInputSource {
	SljitJoinProjectionAggregateInputKind kind = SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT;
	idx_t projection_idx = DConstants::INVALID_INDEX;
	idx_t input_idx = DConstants::INVALID_INDEX;
	LogicalType type;
};

struct SljitJoinProjectionAggregateDescriptor {
	SljitDeferredBuildState build_state;
	idx_t projection_idx = DConstants::INVALID_INDEX;
	SljitExecutableRegionOp projection;
	optional_ptr<SljitExecutableRegionOp> projection_ref;
	vector<idx_t> output_to_projection;
	vector<SljitJoinProjectionAggregateInputSource> input_sources;
	vector<LogicalType> input_types;
	vector<ExecutionRowPointerGroupKeySource> group_sources;
	vector<idx_t> payload_source_indices;
	vector<SljitExecutableRegionExpression> remapped_payloads;
	vector<idx_t> producer_output_column_map;
	bool has_producer_output_column_map = false;
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

	bool ProducerOutputColumnMapMatches(optional_ptr<const vector<idx_t>> output_column_map) const {
		if (!output_column_map) {
			return !has_producer_output_column_map;
		}
		return has_producer_output_column_map && producer_output_column_map == *output_column_map;
	}

	void SetProducerOutputColumnMap(optional_ptr<const vector<idx_t>> output_column_map) {
		producer_output_column_map.clear();
		has_producer_output_column_map = false;
		if (!output_column_map) {
			return;
		}
		producer_output_column_map = *output_column_map;
		has_producer_output_column_map = true;
	}

	void ClearProjectionState() {
		projection_idx = DConstants::INVALID_INDEX;
		projection = SljitExecutableRegionOp();
		projection_ref = nullptr;
		output_to_projection.clear();
		input_sources.clear();
		input_types.clear();
		group_sources.clear();
		payload_source_indices.clear();
		remapped_payloads.clear();
	}

	void ClearBuiltState() {
		ClearProjectionState();
		producer_output_column_map.clear();
		has_producer_output_column_map = false;
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

	void OwnProjection(idx_t projection_idx_p, SljitExecutableRegionOp &&projection_p) {
		projection_idx = projection_idx_p;
		projection = std::move(projection_p);
		projection_ref = nullptr;
	}

	SljitExecutableRegionOp &Projection() {
		return projection_ref ? *projection_ref : projection;
	}

	void EnsureInput(Allocator &allocator) {
		input.Ensure(allocator, input_types);
	}
};

} // namespace duckdb
