//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_aggregate_route_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_aggregate_route_common.hpp"
#include "sljit_projection_runtime.hpp"
#include "sljit_region_executable.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

struct SljitRegionExecutionScratch;

struct SljitJoinProjectionAggregateDescriptor {
	SljitDeferredBuildState build_state;
	idx_t projection_idx = DConstants::INVALID_INDEX;
	SljitExecutableRegionOp projection;
	optional_ptr<SljitExecutableRegionOp> projection_ref;
	vector<idx_t> output_to_projection;
	vector<LogicalType> input_types;
	vector<ExecutionRowPointerGroupKeySource> group_sources;
	vector<idx_t> payload_source_indices;
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

	void ClearBuiltState() {
		projection_idx = DConstants::INVALID_INDEX;
		projection = SljitExecutableRegionOp();
		projection_ref = nullptr;
		output_to_projection.clear();
		input_types.clear();
		group_sources.clear();
		payload_source_indices.clear();
	}

	bool Block(const char *blocker_p) {
		ClearBuiltState();
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
		return projection_ref ? *projection_ref : projection;
	}

	void EnsureInput(Allocator &allocator) {
		input.Ensure(allocator, input_types);
	}
};

struct SljitFinalProjectionAggregateBridge {
	SljitFinalProjectionAggregateBridge() : group_key_hashes(LogicalType::HASH) {
	}

	SljitDataChunkBatch group_key_batch;
	Vector group_key_hashes;
	SljitDeferredBuildState split_payload_descriptor;
	bool split_payload_uses_fused_update = false;
	SljitJoinProjectionAggregateDescriptor row_pointer_aggregate;
	vector<LogicalType> group_key_types;
	vector<idx_t> group_projection_indices;
	vector<idx_t> payload_source_indices;
};

struct SljitRuntimeDecimal64Source {
	const int64_t *data = nullptr;
	const sel_t *source_sel = nullptr;
	const SelectionVector *match_selection = nullptr;
	const data_ptr_t *row_pointers = nullptr;
	idx_t layout_offset = DConstants::INVALID_INDEX;
	bool row_pointer_source = false;
};

struct SljitRuntimeDecimal64DiscountedAmountProgram {
	bool ready = false;
	idx_t gross_source_idx = DConstants::INVALID_INDEX;
	idx_t discount_source_idx = DConstants::INVALID_INDEX;
	idx_t cost_source_idx = DConstants::INVALID_INDEX;
	idx_t quantity_source_idx = DConstants::INVALID_INDEX;
	int64_t discount_base = 0;
};

struct SljitBetweenJoinCompressedPassthrough {
	idx_t between_projection_idx = DConstants::INVALID_INDEX;
	idx_t first_join_output_source_idx = DConstants::INVALID_INDEX;
	idx_t sidecar_idx = DConstants::INVALID_INDEX;
};

struct SljitBetweenJoinPrecomputedPayload {
	idx_t second_projection_idx = DConstants::INVALID_INDEX;
	idx_t sidecar_idx = DConstants::INVALID_INDEX;
	SljitExecutableRegionExpression first_join_expr;
	vector<idx_t> between_projection_indices;
	SljitRuntimeDecimal64DiscountedAmountProgram decimal64_discounted_amount_program;
};

struct SljitBetweenJoinCompressedGroupKeyPlan {
	vector<uint8_t> between_skip;
	vector<uint8_t> second_skip;
};

struct SljitBetweenJoinPrecomputedPayloadPlan {
	vector<uint8_t> between_skip;
	vector<uint8_t> second_skip;
	vector<SljitBetweenJoinPrecomputedPayload> payloads;
	vector<LogicalType> payload_types;
};

struct SljitFinalGroupCompressedPassthroughSource {
	idx_t final_group_output_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	idx_t second_join_projection_idx = DConstants::INVALID_INDEX;
	idx_t second_join_input_col = DConstants::INVALID_INDEX;
	idx_t sidecar_idx = DConstants::INVALID_INDEX;
};

struct SljitSecondJoinProjectionOmissions {
	bool Differs(bool compressed_group_key_p, bool precomputed_payload_p) const {
		return compressed_group_key != compressed_group_key_p || precomputed_payload != precomputed_payload_p;
	}

	void Set(bool compressed_group_key_p, bool precomputed_payload_p) {
		compressed_group_key = compressed_group_key_p;
		precomputed_payload = precomputed_payload_p;
	}

	void ResetCompressedGroupKey() {
		compressed_group_key = false;
	}

	void ResetPrecomputedPayload() {
		precomputed_payload = false;
	}

	bool Any() const {
		return compressed_group_key || precomputed_payload;
	}

	bool compressed_group_key = false;
	bool precomputed_payload = false;
};

struct SljitBetweenJoinSidecars {
	bool CompressedPassthroughsPlanned(idx_t between_projection_count) const {
		return compressed_passthrough_by_between_projection.size() == between_projection_count;
	}

	void ResetCompressedPassthroughs() {
		if (compressed_passthrough_batch.ColumnCount() > 0) {
			compressed_passthrough_batch.Reset();
		}
	}

	void ResetPrecomputedPayloads() {
		if (precomputed_payload_batch.ColumnCount() > 0) {
			precomputed_payload_batch.Reset();
		}
	}

	SljitDataChunkBatch compressed_passthrough_batch;
	vector<SljitBetweenJoinCompressedPassthrough> compressed_passthroughs;
	vector<idx_t> compressed_passthrough_by_between_projection;
	SljitDataChunkBatch precomputed_payload_batch;
	vector<SljitBetweenJoinPrecomputedPayload> precomputed_payloads;
};

struct SljitBetweenJoinProjectionSkips {
	bool SecondJoinProjectionColumnIsOmitted(const SljitSecondJoinProjectionOmissions &omissions,
	                                         idx_t projection_idx) const {
		if (omissions.compressed_group_key && projection_idx < second_compressed_group_key.size() &&
		    second_compressed_group_key[projection_idx]) {
			return true;
		}
		return omissions.precomputed_payload && projection_idx < second_precomputed_payload.size() &&
		       second_precomputed_payload[projection_idx];
	}

	bool BuildSecondProjectionSkip(const SljitSecondJoinProjectionOmissions &omissions, idx_t projection_count,
	                               vector<uint8_t> &skip_projection,
	                               optional_ptr<const vector<uint8_t>> &skip_projection_ptr) const {
		optional_ptr<const vector<uint8_t>> compressed_skip;
		if (omissions.compressed_group_key) {
			compressed_skip = &second_compressed_group_key;
		}
		optional_ptr<const vector<uint8_t>> precomputed_payload_skip;
		if (omissions.precomputed_payload) {
			precomputed_payload_skip = &second_precomputed_payload;
		}
		return SljitOrProjectionSkips(projection_count, compressed_skip, precomputed_payload_skip, skip_projection,
		                              skip_projection_ptr);
	}

	bool BuildBetweenProjectionSkip(idx_t projection_count, bool use_compressed_group_key, bool use_precomputed_payload,
	                                vector<uint8_t> &skip_projection,
	                                optional_ptr<const vector<uint8_t>> &skip_projection_ptr) const {
		optional_ptr<const vector<uint8_t>> compressed_skip;
		if (use_compressed_group_key) {
			compressed_skip = &between_compressed_group_key;
		}
		optional_ptr<const vector<uint8_t>> precomputed_payload_skip;
		if (use_precomputed_payload) {
			precomputed_payload_skip = &between_precomputed_payload;
		}
		return SljitOrProjectionSkips(projection_count, compressed_skip, precomputed_payload_skip, skip_projection,
		                              skip_projection_ptr);
	}

	void ApplySecondCompressedGroupKeySkips(vector<uint8_t> &target) const {
		if (!compressed_group_key.Ready() || second_compressed_group_key.size() != target.size()) {
			return;
		}
		for (idx_t projection_idx = 0; projection_idx < target.size(); projection_idx++) {
			if (second_compressed_group_key[projection_idx]) {
				target[projection_idx] = 1;
			}
		}
	}

	SljitDeferredBuildState compressed_group_key;
	vector<uint8_t> between_compressed_group_key;
	vector<uint8_t> second_compressed_group_key;
	SljitDeferredBuildState precomputed_payload;
	vector<uint8_t> between_precomputed_payload;
	vector<uint8_t> second_precomputed_payload;
};

struct SljitTwoJoinGroupedAggregateRouteState {
	void Initialize(Allocator &allocator, const vector<LogicalType> &second_join_types) {
		second_join_batch.Initialize(allocator, second_join_types);
	}

	void ResetCompressedPassthroughs() {
		between_join_sidecars.ResetCompressedPassthroughs();
		second_join_omissions.ResetCompressedGroupKey();
	}

	void ResetPrecomputedPayloads() {
		between_join_sidecars.ResetPrecomputedPayloads();
		second_join_omissions.ResetPrecomputedPayload();
	}

	DataChunk second_join_batch;
	SljitBetweenJoinSidecars between_join_sidecars;
	SljitFinalProjectionAggregateBridge final_aggregate;
	SljitBetweenJoinProjectionSkips projection_skips;
	SljitSecondJoinProjectionOmissions second_join_omissions;
};

struct SljitDirectFinalProjectionAggregateUpdateResult {
	bool stop_pipeline = false;
	bool handled = false;
	bool processed_batch = false;
};

struct SljitDirectBetweenJoinProjectionAppendResult {
	bool stop_pipeline = false;
	bool handled = false;
};

struct SljitDirectSecondJoinProjectionResult {
	bool stop_pipeline = false;
	bool handled = false;
	bool processed_batch = false;
	DataChunk *projected = nullptr;
};

struct SljitTwoJoinGroupedAggregateRouteLayout {
	explicit SljitTwoJoinGroupedAggregateRouteLayout(idx_t op_count)
	    : aggregate_idx(op_count - 1), final_projection_idx(aggregate_idx - 1) {
	}

	void AssertOperatorBounds(idx_t op_count) const {
		D_ASSERT(first_hash_join_idx < op_count);
		D_ASSERT(between_projection_idx < op_count);
		D_ASSERT(second_hash_join_idx < op_count);
		D_ASSERT(second_projection_idx < op_count);
		D_ASSERT(post_second_projection_idx <= final_projection_idx);
		D_ASSERT(final_projection_idx < op_count);
		D_ASSERT(aggregate_idx < op_count);
	}

	idx_t first_hash_join_idx = 0;
	idx_t between_projection_idx = 1;
	idx_t second_hash_join_idx = 2;
	idx_t second_projection_idx = 3;
	idx_t post_second_projection_idx = 4;
	idx_t aggregate_idx;
	idx_t final_projection_idx;
};

struct SljitPendingRowPointerAggregateBatch {
	SljitPendingRowPointerAggregateBatch() : row_pointers(LogicalType::POINTER) {
	}

	idx_t Count() const {
		return initialized ? input.size() : 0;
	}

	void Ensure(Allocator &allocator, const vector<LogicalType> &input_types) {
		if (initialized) {
			return;
		}
		input.Initialize(allocator, input_types);
		row_pointers.Initialize(VectorDataInitialization::UNINITIALIZED, STANDARD_VECTOR_SIZE);
		FlatVector::SetSize(row_pointers, 0);
		initialized = true;
	}

	void Reset() {
		if (!initialized) {
			return;
		}
		input.Reset();
		FlatVector::SetSize(row_pointers, 0);
	}

	DataChunk input;
	Vector row_pointers;
	bool initialized = false;
	optional_ptr<SljitRegionExecutionScratch> scratch;
	optional_ptr<bool> deferred_grouped_finish;
};

enum class SljitDirectJoinOutputAggregateMode : uint8_t { SINGLE_PROJECTION, PROJECTION_CHAIN };

struct SljitDirectJoinOutputAggregateStrategy {
	SljitDirectJoinOutputAggregateStrategy(SljitDirectJoinOutputAggregateMode mode_p, idx_t aggregate_idx_p)
	    : mode(mode_p), aggregate_idx(aggregate_idx_p) {
	}

	bool UsesPendingBatch() const {
		return mode == SljitDirectJoinOutputAggregateMode::SINGLE_PROJECTION;
	}

	SljitDirectJoinOutputAggregateMode mode;
	idx_t aggregate_idx;
	bool disabled = false;
	SljitJoinProjectionAggregateDescriptor descriptor;
	SljitPendingRowPointerAggregateBatch pending_batch;
};

struct SljitDirectJoinOutputAggregatePolicy {
	SljitDirectJoinOutputAggregatePolicy() {
	}

	explicit SljitDirectJoinOutputAggregatePolicy(SljitDirectJoinOutputAggregateStrategy &strategy_p)
	    : strategy(&strategy_p) {
	}

	bool Enabled() const {
		return strategy != nullptr && !strategy->disabled;
	}

	bool UsesPendingBatch() const {
		return strategy != nullptr && strategy->UsesPendingBatch();
	}

	SljitDirectJoinOutputAggregateStrategy &Strategy() {
		return *strategy;
	}

	optional_ptr<SljitDirectJoinOutputAggregateStrategy> strategy;
};

} // namespace duckdb
