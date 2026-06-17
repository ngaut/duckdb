//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution_region_duckdb_type_adapter.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/execution/execution_region_common.hpp"

namespace duckdb {

inline ExecutionRegionJoinType ExecutionRegionJoinTypeFromDuckDB(JoinType join_type) {
	switch (join_type) {
	case JoinType::LEFT:
		return ExecutionRegionJoinType::LEFT;
	case JoinType::RIGHT:
		return ExecutionRegionJoinType::RIGHT;
	case JoinType::INNER:
		return ExecutionRegionJoinType::INNER;
	case JoinType::OUTER:
		return ExecutionRegionJoinType::OUTER;
	case JoinType::SEMI:
		return ExecutionRegionJoinType::SEMI;
	case JoinType::ANTI:
		return ExecutionRegionJoinType::ANTI;
	case JoinType::MARK:
		return ExecutionRegionJoinType::MARK;
	case JoinType::RIGHT_SEMI:
		return ExecutionRegionJoinType::RIGHT_SEMI;
	case JoinType::RIGHT_ANTI:
		return ExecutionRegionJoinType::RIGHT_ANTI;
	default:
		return ExecutionRegionJoinType::INVALID;
	}
}

inline ExecutionRegionComparisonType ExecutionRegionComparisonTypeFromDuckDB(ExpressionType comparison_type) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return ExecutionRegionComparisonType::EQUAL;
	case ExpressionType::COMPARE_NOTEQUAL:
		return ExecutionRegionComparisonType::NOT_EQUAL;
	case ExpressionType::COMPARE_LESSTHAN:
		return ExecutionRegionComparisonType::LESS_THAN;
	case ExpressionType::COMPARE_GREATERTHAN:
		return ExecutionRegionComparisonType::GREATER_THAN;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return ExecutionRegionComparisonType::DISTINCT_FROM;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return ExecutionRegionComparisonType::NOT_DISTINCT_FROM;
	default:
		return ExecutionRegionComparisonType::INVALID;
	}
}

} // namespace duckdb
