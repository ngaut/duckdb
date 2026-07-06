#pragma once

#include "sljit_region_plan.hpp"

#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

struct SljitExecutableInt128Range {
	hugeint_t min;
	hugeint_t max;
	LogicalType type;
};

bool SljitExecutableValueToHugeint(const Value &value, const LogicalType &expected_type, hugeint_t &result);
bool SljitExecutableExpressionTreeRange(const ExecutionExpressionIR &node, const vector<Value> &input_min_values,
                                        const vector<Value> &input_max_values, SljitExecutableInt128Range &result);
bool SljitExecutableRangeValue(const LogicalType &type, const hugeint_t &input, Value &result);

} // namespace duckdb
