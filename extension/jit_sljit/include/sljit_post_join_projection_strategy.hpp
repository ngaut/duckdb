//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_post_join_projection_strategy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

namespace duckdb {

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

	bool HasProjectionChain() const {
		return first_projection_idx != DConstants::INVALID_INDEX && final_projection_idx != DConstants::INVALID_INDEX;
	}

	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	idx_t trace_projection_idx = DConstants::INVALID_INDEX;
};

struct SljitPostJoinProjectionPrimitive {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	idx_t trace_projection_idx = DConstants::INVALID_INDEX;

	bool HasProjectionChain() const {
		return first_projection_idx != DConstants::INVALID_INDEX && final_projection_idx != DConstants::INVALID_INDEX;
	}

	SljitPostJoinProjectionStrategy MakeStrategy() const {
		SljitPostJoinProjectionStrategy strategy;
		if (HasProjectionChain()) {
			strategy.Initialize(hash_join_idx, first_projection_idx, final_projection_idx);
		} else {
			strategy.InitializeSelectedJoinInput(hash_join_idx);
		}
		strategy.trace_projection_idx = trace_projection_idx;
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

} // namespace duckdb
