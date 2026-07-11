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

static bool SljitCanBindSelectedHashJoinAppendSinkPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                            idx_t hash_join_idx, idx_t sink_idx) {
	if (sink_idx >= ops.size() || sink_idx != ops.size() - 1 || hash_join_idx + 1 != sink_idx ||
	    ops[sink_idx].kind != SljitNativeRegionOpKind::APPEND_SINK ||
	    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
		return false;
	}
	return ops[hash_join_idx].output_types == ops[sink_idx].append_sink.plan.input_types;
}

static SljitAppendSinkPrimitive SljitBindSelectedHashJoinAppendSinkPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                             idx_t hash_join_idx, idx_t sink_idx) {
	if (!SljitCanBindSelectedHashJoinAppendSinkPrimitive(ops, hash_join_idx, sink_idx)) {
		throw InternalException("SLJIT append sink primitive cannot bind selected hash-join input");
	}
	SljitAppendSinkPrimitive primitive;
	primitive.sink_idx = sink_idx;
	primitive.selected_hash_join_idx = hash_join_idx;
	return primitive;
}

static bool SljitCanBindAppendSinkPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                            const SljitAppendSinkPrimitive &primitive) {
	return SljitCanBindSelectedHashJoinAppendSinkPrimitive(ops, primitive.selected_hash_join_idx, primitive.sink_idx);
}

} // namespace duckdb
