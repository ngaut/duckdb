//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/lowering.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/jit/pipeline_descriptor.hpp"
#include "duckdb/execution/jit/region.hpp"

namespace duckdb {

class Expression;

enum class JitRegionPipelineInventoryMode : uint8_t { ADMISSION, DIAGNOSTIC };

DUCKDB_API unique_ptr<JitExpressionFragment> TryLowerJitExpression(const Expression &expression,
                                                                   idx_t expression_index = 0);
DUCKDB_API string DescribeJitExpressionLoweringFailure(const Expression &expression);
DUCKDB_API unique_ptr<JitRegionPipelineInventory> TryInspectJitRegionPipeline(const JitPipelineDescriptor &descriptor,
                                                                              JitRegionPipelineInventoryMode mode);
DUCKDB_API unique_ptr<JitRegionIR> TryLowerJitRegion(const JitPipelineDescriptor &descriptor);

} // namespace duckdb
