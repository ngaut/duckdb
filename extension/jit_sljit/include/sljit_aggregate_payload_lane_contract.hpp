//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_payload_lane_contract.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_descriptor.hpp"

#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

static inline bool SljitAggregatePayloadDescriptorMatchesLane(const SljitAggregatePayloadDescriptor &descriptor,
                                                              const ExecutionPrimitiveAggregateUpdateLane &lane) {
	return lane.ready && descriptor.state_size != 0 && descriptor.primitive_kind == lane.kind &&
	       descriptor.aggregate_index == lane.aggregate_index && descriptor.state_size == lane.state_size &&
	       descriptor.state_value_offset == lane.state_value_offset &&
	       descriptor.state_is_set_offset == lane.state_is_set_offset &&
	       (!descriptor.has_payload || descriptor.input_type == lane.payload_type);
}

} // namespace duckdb
