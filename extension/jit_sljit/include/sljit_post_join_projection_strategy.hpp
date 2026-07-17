//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_post_join_projection_strategy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_post_join_projection_fast_path.hpp"
#include "sljit_runtime_batch_state.hpp"
#include "sljit_region_executable.hpp"

#include <utility>

namespace duckdb {

struct SljitPostJoinProjectionDescriptor {
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
		projection_ref = nullptr;
		composed_projection = SljitExecutableRegionOp();
		output_to_projection.clear();
	}

	bool Block(const char *blocker_p) {
		ClearBuiltState();
		return build_state.Block(blocker_p);
	}

	void MarkReady() {
		build_state.MarkReady();
	}

	void BorrowProjection(idx_t projection_idx_p, SljitExecutableRegionOp &projection_p) {
		projection_idx = projection_idx_p;
		projection_ref = &projection_p;
	}

	void OwnProjection(idx_t projection_idx_p, SljitExecutableRegionOp &&projection_p) {
		projection_idx = projection_idx_p;
		composed_projection = std::move(projection_p);
		projection_ref = optional_ptr<SljitExecutableRegionOp>(&composed_projection);
	}

	SljitExecutableRegionOp &Projection() {
		return *projection_ref;
	}

	optional_ptr<const vector<idx_t>> OutputMap() const {
		if (output_to_projection.empty()) {
			return nullptr;
		}
		return optional_ptr<const vector<idx_t>>(&output_to_projection);
	}

	SljitDeferredBuildState build_state;
	idx_t projection_idx = DConstants::INVALID_INDEX;
	optional_ptr<SljitExecutableRegionOp> projection_ref;
	SljitExecutableRegionOp composed_projection;
	vector<idx_t> output_to_projection;
};

struct SljitPostJoinProjectionStrategy {
	void Initialize(idx_t hash_join_idx_p, idx_t first_projection_idx_p, idx_t final_projection_idx_p) {
		hash_join_idx = hash_join_idx_p;
		first_projection_idx = first_projection_idx_p;
		final_projection_idx = final_projection_idx_p;
		trace_projection_idx = first_projection_idx_p;
	}

	void InitializeSelectedJoinInput(idx_t hash_join_idx_p) {
		hash_join_idx = hash_join_idx_p;
		first_projection_idx = DConstants::INVALID_INDEX;
		final_projection_idx = DConstants::INVALID_INDEX;
		trace_projection_idx = hash_join_idx_p;
	}

	void EnableStringSetCaseGroupedPayload(const SljitStringSetCaseGroupedPayloadProjection &descriptor) {
		fast_path = SljitPostJoinProjectionFastPath::STRING_SET_CASE_GROUPED_PAYLOAD;
		string_set_case_projection = descriptor;
		direct_projection_disabled_reason = "string_set_case_fast_path";
	}

	bool HasFastPath() const {
		return fast_path != SljitPostJoinProjectionFastPath::NONE;
	}

	bool HasProjectionChain() const {
		return first_projection_idx != DConstants::INVALID_INDEX && final_projection_idx != DConstants::INVALID_INDEX;
	}

	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	idx_t trace_projection_idx = DConstants::INVALID_INDEX;
	const char *direct_projection_disabled_reason = nullptr;
	SljitPostJoinProjectionFastPath fast_path = SljitPostJoinProjectionFastPath::NONE;
	SljitStringSetCaseGroupedPayloadProjection string_set_case_projection;
	SljitPostJoinProjectionDescriptor descriptor;
};

struct SljitPostJoinProjectionPrimitive {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	idx_t trace_projection_idx = DConstants::INVALID_INDEX;
	SljitPostJoinProjectionFastPath fast_path = SljitPostJoinProjectionFastPath::NONE;
	SljitStringSetCaseGroupedPayloadProjection string_set_case_projection;
	const char *direct_projection_disabled_reason = nullptr;

	bool HasProjectionChain() const {
		return first_projection_idx != DConstants::INVALID_INDEX && final_projection_idx != DConstants::INVALID_INDEX;
	}

	void EnableStringSetCaseGroupedPayload(const SljitStringSetCaseGroupedPayloadProjection &descriptor) {
		fast_path = SljitPostJoinProjectionFastPath::STRING_SET_CASE_GROUPED_PAYLOAD;
		string_set_case_projection = descriptor;
		direct_projection_disabled_reason = "string_set_case_fast_path";
	}

	SljitPostJoinProjectionStrategy MakeStrategy() const {
		SljitPostJoinProjectionStrategy strategy;
		if (HasProjectionChain()) {
			strategy.Initialize(hash_join_idx, first_projection_idx, final_projection_idx);
		} else {
			strategy.InitializeSelectedJoinInput(hash_join_idx);
		}
		strategy.trace_projection_idx = trace_projection_idx;
		strategy.direct_projection_disabled_reason = direct_projection_disabled_reason;
		if (fast_path == SljitPostJoinProjectionFastPath::STRING_SET_CASE_GROUPED_PAYLOAD) {
			strategy.EnableStringSetCaseGroupedPayload(string_set_case_projection);
		}
		return strategy;
	}
};

static bool SljitCanBindPostJoinProjectionPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                                    idx_t first_projection_idx, idx_t final_projection_idx) {
	if (hash_join_idx >= ops.size() || ops[hash_join_idx].kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
		return false;
	}
	if (first_projection_idx == DConstants::INVALID_INDEX && final_projection_idx == DConstants::INVALID_INDEX) {
		return true;
	}
	if (first_projection_idx >= ops.size() || final_projection_idx >= ops.size() ||
	    first_projection_idx > final_projection_idx) {
		return false;
	}
	for (idx_t projection_idx = first_projection_idx; projection_idx <= final_projection_idx; projection_idx++) {
		if (ops[projection_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
			return false;
		}
	}
	return true;
}

static bool SljitTryBindPostJoinProjectionPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                                    idx_t first_projection_idx, idx_t final_projection_idx,
                                                    SljitPostJoinProjectionPrimitive &primitive) {
	if (!SljitCanBindPostJoinProjectionPrimitive(ops, hash_join_idx, first_projection_idx, final_projection_idx)) {
		return false;
	}
	SljitPostJoinProjectionPrimitive candidate;
	candidate.hash_join_idx = hash_join_idx;
	candidate.first_projection_idx = first_projection_idx;
	candidate.final_projection_idx = final_projection_idx;
	candidate.trace_projection_idx =
	    first_projection_idx == DConstants::INVALID_INDEX ? hash_join_idx : first_projection_idx;
	primitive = candidate;
	return true;
}

static SljitPostJoinProjectionPrimitive SljitBindPostJoinProjectionPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                             idx_t hash_join_idx,
                                                                             idx_t first_projection_idx,
                                                                             idx_t final_projection_idx) {
	SljitPostJoinProjectionPrimitive primitive;
	if (!SljitTryBindPostJoinProjectionPrimitive(ops, hash_join_idx, first_projection_idx, final_projection_idx,
	                                             primitive)) {
		throw InternalException("SLJIT post-join projection primitive cannot bind requested operators");
	}
	return primitive;
}

} // namespace duckdb
