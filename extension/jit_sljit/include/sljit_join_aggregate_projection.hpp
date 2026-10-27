//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_aggregate_projection.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

namespace duckdb {

class SljitBatchLayout;

//! Prepare artifact-owned code once during recipe construction, never while binding a query-local descriptor.
bool SljitTryPrepareJoinAggregateInputs(const vector<idx_t> &lhs_output_columns,
                                        const vector<LogicalType> &output_types, const vector<LogicalType> &input_types,
                                        const SljitExecutableRegionOp &projection,
                                        optional_ptr<const SljitBatchLayout> producer_layout,
                                        optional_ptr<const SljitExecutableRegionOp> producer,
                                        vector<SljitExecutableRegionExpression> &prepared, idx_t &owned_code_size);

//! Artifact-owned expressions in semantic join-output and physical probe-plus-build coordinates.
struct SljitJoinAggregateProjection {
	SljitExecutableRegionOp semantic;
	vector<SljitExecutableRegionExpression> inputs;
	//! Only newly emitted code; borrowed expression handles are accounted for by their original operator.
	idx_t owned_code_size = 0;
};

} // namespace duckdb
