#pragma once

#include "duckdb/execution/execution_region_lowering.hpp"

namespace duckdb {

string DescribeExecutionRegionPipelineShape(const ExecutionRegionIR &region_ir);
string DescribeExecutionRegionPipelineShape(const ExecutionRegionIR &region_ir, idx_t first_node, idx_t node_count);
string DescribeExecutionRegionCandidateShape(const ExecutionRegionIR &region_ir,
                                             const ExecutionRegionCandidate &candidate);
ExecutionRegionSignature BuildExecutionRegionSignature(const ExecutionRegionIR &region_ir,
                                                       const ExecutionRegionCandidate &candidate);

} // namespace duckdb
