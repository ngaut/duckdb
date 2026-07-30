#pragma once

#include "duckdb/execution/execution_region_lowering.hpp"

namespace duckdb {

string DescribeExecutionRegionPipelineShape(const ExecutionRegionIR &region_ir);
string DescribeExecutionRegionCandidateShape(const ExecutionRegionIR &region_ir);
ExecutionRegionSignature BuildExecutionRegionSignature(const ExecutionRegionIR &region_ir);

} // namespace duckdb
