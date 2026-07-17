//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_append_sink_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_primitive.hpp"

namespace duckdb {

struct SljitAppendSinkPrimitive {
	idx_t sink_idx = DConstants::INVALID_INDEX;
	idx_t selected_hash_join_idx = DConstants::INVALID_INDEX;
};

static bool SljitTryBindAppendSinkPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                            idx_t sink_idx, SljitAppendSinkPrimitive &primitive) {
	if (sink_idx >= ops.size() || sink_idx == 0 || sink_idx != ops.size() - 1 || hash_join_idx != sink_idx - 1 ||
	    ops[sink_idx].kind != SljitNativeRegionOpKind::APPEND_SINK ||
	    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
		return false;
	}
	if (ops[hash_join_idx].output_types != ops[sink_idx].append_sink.plan.input_types) {
		return false;
	}
	SljitAppendSinkPrimitive candidate;
	candidate.sink_idx = sink_idx;
	candidate.selected_hash_join_idx = hash_join_idx;
	primitive = candidate;
	return true;
}

} // namespace duckdb
