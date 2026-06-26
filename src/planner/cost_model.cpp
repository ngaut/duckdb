#include "duckdb/planner/cost_model.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/common/vector_size.hpp"

#include <limits>

namespace duckdb {

static idx_t DuckDBExpressionCost(const Expression &expr);
static constexpr int64_t BASIS_POINT_SCALE = 10000;
static constexpr int64_t MATERIALIZATION_SOURCE_APPEND_PENALTY = 80;
static constexpr int64_t BLOCKED_HASH_AGGREGATE_LOOKUP_PENALTY = 64;
static constexpr int64_t DISCOUNTED_JOIN_BLOCKED_HASH_AGGREGATE_LOOKUP_PENALTY = 64;
static constexpr idx_t JOIN_UNGROUPED_AGGREGATE_MIN_EXPRESSION_COST = 512;
static constexpr idx_t SCAN_FILTERED_JOIN_GROUPED_AGGREGATE_MIN_EXPRESSION_COST = 384;
static constexpr idx_t JOIN_BUILD_CHAIN_MIN_EXPRESSION_COST = 64;
static constexpr int64_t LONG_BLOCKED_JOIN_GROUPED_AGGREGATE_MIN_BATCHES = 512;
static constexpr int64_t STATEFUL_STANDALONE_PROJECTION_MIN_BATCHES = 128;
static constexpr int64_t JOIN_BUILD_CHAIN_MIN_BATCHES = 128;
static constexpr int64_t LONG_NATIVE_JOIN_CHAIN_MIN_BATCHES = 512;
static constexpr idx_t LONG_NATIVE_JOIN_CHAIN_MIN_EXPRESSION_COST = 64;
static constexpr idx_t HIGH_EXPRESSION_BLOCKED_JOIN_GROUPED_AGGREGATE_MIN_COST = 256;
static constexpr idx_t NARROW_TWO_JOIN_GROUPED_AGGREGATE_MIN_EXPRESSION_COST = 96;
static constexpr idx_t WIDE_TWO_JOIN_GROUPED_AGGREGATE_MIN_EXPRESSION_COST = 1024;
static constexpr int64_t SCAN_FILTERED_NARROW_TWO_JOIN_GROUPED_AGGREGATE_MIN_BATCHES = 512;
static constexpr idx_t JOIN_BUILD_CHAIN_MAX_PAYLOAD_COLUMNS = 2;
static constexpr idx_t JOIN_BUILD_CHAIN_MIN_SOURCE_PROJECTED_COLUMNS = 4;

static int64_t SaturatingCostCast(idx_t value) {
	auto max_value = static_cast<idx_t>(std::numeric_limits<int64_t>::max());
	return value > max_value ? std::numeric_limits<int64_t>::max() : NumericCast<int64_t>(value);
}

static int64_t AddCost(int64_t left, int64_t right) {
	if (right > 0 && left > std::numeric_limits<int64_t>::max() - right) {
		return std::numeric_limits<int64_t>::max();
	}
	return left + right;
}

static int64_t SubtractCost(int64_t left, int64_t right) {
	if (right > 0 && left < std::numeric_limits<int64_t>::min() + right) {
		return std::numeric_limits<int64_t>::min();
	}
	return left - right;
}

static int64_t MultiplyCost(int64_t left, int64_t right) {
	if (left <= 0 || right <= 0) {
		return 0;
	}
	if (left > std::numeric_limits<int64_t>::max() / right) {
		return std::numeric_limits<int64_t>::max();
	}
	return left * right;
}

static int64_t FractionalCost(int64_t value, int64_t numerator, int64_t denominator) {
	if (value <= 0 || numerator <= 0) {
		return 0;
	}
	auto multiplied = MultiplyCost(value, numerator);
	return multiplied / denominator;
}

static idx_t DuckDBPhysicalTypeCost(PhysicalType return_type, idx_t multiplier) {
	switch (return_type) {
	case PhysicalType::VARCHAR:
		return 5 * multiplier;
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return 2 * multiplier;
	default:
		return multiplier;
	}
}

static idx_t DuckDBBetweenExpressionCost(const BoundFunctionExpression &expr) {
	auto &input = BoundBetweenExpression::Input(expr);
	auto &lower_bound = BoundBetweenExpression::LowerBound(expr);
	auto &upper_bound = BoundBetweenExpression::UpperBound(expr);
	return DuckDBExpressionCost(input) + DuckDBExpressionCost(lower_bound) + DuckDBExpressionCost(upper_bound) + 10;
}

static idx_t DuckDBCaseExpressionCost(const BoundCaseExpression &expr) {
	idx_t case_cost = 0;
	for (auto &case_check : expr.CaseChecks()) {
		case_cost += DuckDBExpressionCost(*case_check.then_expr);
		case_cost += DuckDBExpressionCost(*case_check.when_expr);
	}
	case_cost += DuckDBExpressionCost(expr.Else());
	return case_cost;
}

static idx_t DuckDBCastExpressionCost(const BoundCastExpression &expr) {
	idx_t cast_cost = 0;
	if (expr.GetReturnType() != expr.source_type()) {
		if (expr.GetReturnType().id() == LogicalTypeId::VARCHAR || expr.source_type().id() == LogicalTypeId::VARCHAR ||
		    expr.GetReturnType().id() == LogicalTypeId::BLOB || expr.source_type().id() == LogicalTypeId::BLOB) {
			cast_cost = 200;
		} else {
			cast_cost = 5;
		}
	}
	return DuckDBExpressionCost(expr.Child()) + cast_cost;
}

static idx_t DuckDBComparisonExpressionCost(const BoundFunctionExpression &expr) {
	auto &left = BoundComparisonExpression::Left(expr);
	auto &right = BoundComparisonExpression::Right(expr);
	return DuckDBExpressionCost(left) + 5 + DuckDBExpressionCost(right);
}

static idx_t DuckDBConjunctionExpressionCost(const BoundConjunctionExpression &expr) {
	idx_t cost = 5;
	for (auto &child : expr.GetChildren()) {
		cost += DuckDBExpressionCost(*child);
	}
	return cost;
}

static bool DuckDBKnownFunctionCost(const string &name, idx_t &cost) {
	switch (name.size()) {
	case 1:
		if (name == "+" || name == "-" || name == "&" || name == "#") {
			cost = 5;
			return true;
		}
		if (name == "*" || name == "%") {
			cost = 10;
			return true;
		}
		if (name == "/") {
			cost = 15;
			return true;
		}
		return false;
	case 2:
		if (name == ">>" || name == "<<" || name == "~~" || name == "||") {
			cost = name == "~~" || name == "||" ? 200 : 5;
			return true;
		}
		return false;
	case 3:
		if (StringUtil::CIEquals(name, "abs")) {
			cost = 5;
			return true;
		}
		if (name == "!~~") {
			cost = 200;
			return true;
		}
		return false;
	case 4:
		if (StringUtil::CIEquals(name, "year")) {
			cost = 20;
			return true;
		}
		return false;
	case 5:
		if (StringUtil::CIEquals(name, "round")) {
			cost = 100;
			return true;
		}
		return false;
	case 9:
		if (StringUtil::CIEquals(name, "date_part")) {
			cost = 20;
			return true;
		}
		return false;
	case 14:
		if (StringUtil::CIEquals(name, "regexp_matches")) {
			cost = 200;
			return true;
		}
		return false;
	default:
		if (StringUtil::StartsWith(name, "__internal_compress_string_") || name == "__internal_decompress_string") {
			cost = 0;
			return true;
		}
		return false;
	}
}

static idx_t DuckDBFunctionExpressionCost(const BoundFunctionExpression &expr) {
	if (expr.GetExpressionType() == ExpressionType::COMPARE_BETWEEN) {
		return DuckDBBetweenExpressionCost(expr);
	}
	if (BoundComparisonExpression::IsComparison(expr)) {
		return DuckDBComparisonExpressionCost(expr);
	}

	idx_t cost_children = 0;
	for (auto &child : expr.GetChildren()) {
		cost_children += DuckDBExpressionCost(*child);
	}

	idx_t function_cost = 0;
	if (DuckDBKnownFunctionCost(expr.Function().GetName().GetIdentifierName(), function_cost)) {
		return cost_children + function_cost;
	}
	return cost_children + 1000;
}

static idx_t DuckDBOperatorExpressionCost(const BoundOperatorExpression &expr, ExpressionType expr_type) {
	idx_t sum = 0;
	for (auto &child : expr.GetChildren()) {
		sum += DuckDBExpressionCost(*child);
	}

	if (expr_type == ExpressionType::OPERATOR_IS_NULL || expr_type == ExpressionType::OPERATOR_IS_NOT_NULL) {
		return sum + 5;
	}
	if (expr_type == ExpressionType::COMPARE_IN || expr_type == ExpressionType::COMPARE_NOT_IN) {
		return sum + (expr.GetChildren().size() - 1) * 100;
	}
	if (expr_type == ExpressionType::OPERATOR_NOT) {
		return sum + 10;
	}
	return sum + 1000;
}

static idx_t DuckDBExpressionCost(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CASE:
		return DuckDBCaseExpressionCost(expr.Cast<BoundCaseExpression>());
	case ExpressionClass::BOUND_CAST:
		return DuckDBCastExpressionCost(expr.Cast<BoundCastExpression>());
	case ExpressionClass::BOUND_CONJUNCTION:
		return DuckDBConjunctionExpressionCost(expr.Cast<BoundConjunctionExpression>());
	case ExpressionClass::BOUND_FUNCTION:
		return DuckDBFunctionExpressionCost(expr.Cast<BoundFunctionExpression>());
	case ExpressionClass::BOUND_OPERATOR:
		return DuckDBOperatorExpressionCost(expr.Cast<BoundOperatorExpression>(), expr.GetExpressionType());
	case ExpressionClass::BOUND_COLUMN_REF:
		return DuckDBPhysicalTypeCost(expr.Cast<BoundColumnRefExpression>().GetReturnType().InternalType(), 8);
	case ExpressionClass::BOUND_CONSTANT:
		return DuckDBPhysicalTypeCost(expr.Cast<BoundConstantExpression>().GetReturnType().InternalType(), 1);
	case ExpressionClass::BOUND_PARAMETER:
		return DuckDBPhysicalTypeCost(expr.Cast<BoundParameterExpression>().GetReturnType().InternalType(), 1);
	case ExpressionClass::BOUND_REF:
		return DuckDBPhysicalTypeCost(expr.Cast<BoundReferenceExpression>().GetReturnType().InternalType(), 8);
	default:
		return 1000;
	}
}

idx_t DuckDBCostModel::ExpressionCost(const Expression &expr) {
	return DuckDBExpressionCost(expr);
}

idx_t DuckDBCostModel::FilterCost(const TableFilter &filter) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "DuckDBCostModel::FilterCost");
	auto &expr = *expr_filter.expr;
	if (ExpressionFilter::ContainsInternalFunction(expr, DynamicFilterScalarFun::NAME) ||
	    ExpressionFilter::IsOptionalExpression(expr)) {
		return 0;
	}
	return ExpressionCost(expr);
}

vector<idx_t> DuckDBCostModel::InitialFilterOrder(const TableFilterSet &table_filters) {
	struct FilterCost {
		idx_t index;
		idx_t cost;

		bool operator==(const FilterCost &p) const {
			return cost == p.cost;
		}
		bool operator<(const FilterCost &p) const {
			return cost < p.cost;
		}
	};

	vector<FilterCost> filter_costs;
	idx_t filter_index = 0;
	for (auto &entry : table_filters) {
		FilterCost cost;
		cost.index = filter_index;
		cost.cost = DuckDBCostModel::FilterCost(entry.Filter());
		filter_costs.push_back(cost);
		filter_index++;
	}
	sort(filter_costs.begin(), filter_costs.end());

	vector<idx_t> initial_permutation;
	for (idx_t i = 0; i < filter_costs.size(); i++) {
		initial_permutation.push_back(filter_costs[i].index);
	}
	return initial_permutation;
}

static int64_t PhysicalRunnerRows(const PhysicalRunnerCostInput &input) {
	auto rows = input.estimated_cardinality;
	if (rows == 0) {
		rows = 1;
	}
	if (input.uses_scan_filters) {
		// DuckDB-owned scan filters run before the accelerated body, so cost the
		// generated/native work on a conservative post-filter row estimate.
		const auto filter_count = MaxValue<idx_t>(input.source_filter_count, 1);
		for (idx_t filter_idx = 0; filter_idx < filter_count; filter_idx++) {
			rows = MaxValue<idx_t>((rows + 9) / 10, 1);
		}
	}
	return SaturatingCostCast(rows);
}

static int64_t PhysicalRunnerBatches(int64_t rows) {
	return AddCost(rows, STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
}

static bool PhysicalRunnerHasWideStringGroupedAggregate(const PhysicalRunnerCostInput &input) {
	if (input.native_grouped_aggregate_stage_count == 0) {
		return false;
	}
	if (input.grouped_aggregate_varchar_group_count >= 2) {
		return true;
	}
	if (input.grouped_aggregate_group_count >= 5 && input.reference_varchar_projection_count > 0) {
		return true;
	}
	return input.native_join_stage_count >= 2 && input.grouped_aggregate_group_count >= 3 &&
	       input.reference_varchar_projection_count >= 2;
}

struct PhysicalRunnerShapeFacts {
	int64_t rows = 1;
	int64_t batches = 1;
	idx_t native_operator_stage_count = 0;
	bool has_generated_compute_work = false;
	bool no_native_join = false;
	bool no_native_sort = false;
	bool has_native_grouped_aggregate = false;
	bool grouped_aggregate_only = false;
	bool has_wide_string_grouped_aggregate = false;
};

static PhysicalRunnerShapeFacts PhysicalRunnerBuildShapeFacts(const PhysicalRunnerCostInput &input) {
	PhysicalRunnerShapeFacts facts;
	facts.rows = PhysicalRunnerRows(input);
	facts.batches = PhysicalRunnerBatches(facts.rows);
	facts.native_operator_stage_count =
	    input.native_join_stage_count + input.native_aggregate_stage_count + input.native_sort_stage_count;
	facts.has_generated_compute_work = input.generated_work_class != PhysicalRunnerGeneratedWorkClass::NONE &&
	                                   input.generated_work_class != PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE;
	facts.no_native_join = input.native_join_stage_count == 0;
	facts.no_native_sort = input.native_sort_stage_count == 0;
	facts.has_native_grouped_aggregate = input.native_grouped_aggregate_stage_count > 0;
	facts.grouped_aggregate_only = facts.has_native_grouped_aggregate &&
	                               input.native_aggregate_stage_count == input.native_grouped_aggregate_stage_count;
	facts.has_wide_string_grouped_aggregate = PhysicalRunnerHasWideStringGroupedAggregate(input);
	return facts;
}

static bool PhysicalRunnerIsNativeContractProjectionGlue(const PhysicalRunnerCostInput &input,
                                                         const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline &&
	       input.native_protocol_class == PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL &&
	       input.generated_work_class == PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE &&
	       facts.native_operator_stage_count == 0 && input.materialization_elision_count == 0;
}

static bool PhysicalRunnerIsSmallStatefulStandaloneProjection(const PhysicalRunnerCostInput &input,
                                                              const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline &&
	       input.native_protocol_class == PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL &&
	       input.sort_sink && input.generated_stage_count > 0 && facts.has_generated_compute_work &&
	       facts.native_operator_stage_count == 0 && input.materialization_elision_count == 0 &&
	       facts.batches < STATEFUL_STANDALONE_PROJECTION_MIN_BATCHES;
}

static bool PhysicalRunnerGeneratedWorkPaysStandaloneUngroupedAggregateProtocol(const PhysicalRunnerCostInput &input,
                                                                                const PhysicalRunnerShapeFacts &facts) {
	return input.generated_stage_count > 0 && facts.has_generated_compute_work && facts.no_native_join &&
	       input.native_aggregate_stage_count > 0 && input.native_grouped_aggregate_stage_count == 0 &&
	       facts.no_native_sort;
}

static bool PhysicalRunnerGeneratedWorkPaysStandaloneGroupedAggregateProtocol(const PhysicalRunnerCostInput &input,
                                                                              const PhysicalRunnerShapeFacts &facts) {
	return input.generated_stage_count >= 2 && facts.has_generated_compute_work && facts.no_native_join &&
	       facts.grouped_aggregate_only && facts.no_native_sort;
}

static bool PhysicalRunnerGeneratedWorkPaysJoinGroupedAggregateProtocol(const PhysicalRunnerCostInput &input,
                                                                        const PhysicalRunnerShapeFacts &facts) {
	if (!input.full_pipeline || input.uses_scan_filters || input.generated_stage_count < 2 ||
	    !facts.has_generated_compute_work || !facts.grouped_aggregate_only || !facts.no_native_sort) {
		return false;
	}
	if (facts.has_wide_string_grouped_aggregate) {
		return false;
	}
	if (input.native_join_stage_count == 1) {
		if (input.generated_stage_count < 3 && input.blocked_hash_aggregate_lookup_count > 0) {
			return false;
		}
		return true;
	}
	if (input.generated_stage_count < 3) {
		return false;
	}
	return input.native_join_stage_count == 2 && input.source_filter_count == 0;
}

static bool
PhysicalRunnerGeneratedWorkPaysLongBlockedJoinGroupedAggregateProtocol(const PhysicalRunnerCostInput &input,
                                                                       const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline && !input.uses_scan_filters && input.generated_stage_count == 2 &&
	       input.materialization_elision_count > 0 && facts.has_generated_compute_work &&
	       input.native_join_stage_count == 1 && facts.grouped_aggregate_only &&
	       input.blocked_hash_aggregate_lookup_count > 0 && facts.no_native_sort &&
	       !facts.has_wide_string_grouped_aggregate;
}

static bool PhysicalRunnerGeneratedWorkPaysHighExpressionBlockedJoinGroupedAggregateProtocol(
    const PhysicalRunnerCostInput &input, const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline && !input.uses_scan_filters && input.generated_stage_count >= 2 &&
	       input.materialization_elision_count == 0 &&
	       input.expression_cost >= HIGH_EXPRESSION_BLOCKED_JOIN_GROUPED_AGGREGATE_MIN_COST &&
	       facts.has_generated_compute_work && input.native_join_stage_count == 1 && facts.grouped_aggregate_only &&
	       input.blocked_hash_aggregate_lookup_count > 0 && facts.no_native_sort &&
	       !facts.has_wide_string_grouped_aggregate;
}

static bool
PhysicalRunnerGeneratedWorkPaysNarrowTwoJoinGroupedAggregateProtocol(const PhysicalRunnerCostInput &input,
                                                                     const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline && !input.uses_scan_filters && input.generated_stage_count == 2 &&
	       input.materialization_elision_count == 0 &&
	       input.expression_cost >= NARROW_TWO_JOIN_GROUPED_AGGREGATE_MIN_EXPRESSION_COST &&
	       facts.has_generated_compute_work && input.native_join_stage_count == 2 &&
	       input.native_grouped_aggregate_stage_count == 1 && facts.grouped_aggregate_only &&
	       input.grouped_aggregate_group_count <= 2 && input.grouped_aggregate_varchar_group_count <= 1 &&
	       input.reference_varchar_projection_count == 0 && input.blocked_hash_aggregate_lookup_count > 0 &&
	       facts.no_native_sort && !facts.has_wide_string_grouped_aggregate;
}

static bool PhysicalRunnerGeneratedWorkPaysScanFilteredNarrowTwoJoinGroupedAggregateProtocol(
    const PhysicalRunnerCostInput &input, const PhysicalRunnerShapeFacts &facts) {
	const bool supported_reference_varchar_projection =
	    input.reference_varchar_projection_count == 0 ||
	    (input.generated_stage_count >= 4 &&
	     input.expression_cost >= WIDE_TWO_JOIN_GROUPED_AGGREGATE_MIN_EXPRESSION_COST);
	return input.full_pipeline && input.uses_scan_filters && input.source_filter_count == 0 &&
	       input.generated_stage_count >= 2 && input.materialization_elision_count == 0 &&
	       input.expression_cost >= NARROW_TWO_JOIN_GROUPED_AGGREGATE_MIN_EXPRESSION_COST &&
	       facts.has_generated_compute_work && input.native_join_stage_count == 2 &&
	       input.native_grouped_aggregate_stage_count == 1 && facts.grouped_aggregate_only &&
	       input.grouped_aggregate_group_count <= 2 && input.grouped_aggregate_varchar_group_count <= 1 &&
	       supported_reference_varchar_projection && input.blocked_hash_aggregate_lookup_count > 0 &&
	       facts.no_native_sort && !facts.has_wide_string_grouped_aggregate &&
	       facts.batches >= SCAN_FILTERED_NARROW_TWO_JOIN_GROUPED_AGGREGATE_MIN_BATCHES;
}

static bool PhysicalRunnerGeneratedWorkPaysWideTwoJoinGroupedAggregateProtocol(const PhysicalRunnerCostInput &input,
                                                                               const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline && !input.uses_scan_filters && input.generated_stage_count >= 4 &&
	       input.materialization_elision_count == 0 &&
	       input.expression_cost >= WIDE_TWO_JOIN_GROUPED_AGGREGATE_MIN_EXPRESSION_COST &&
	       facts.has_generated_compute_work && input.native_join_stage_count == 2 &&
	       input.native_grouped_aggregate_stage_count == 1 && facts.grouped_aggregate_only &&
	       input.grouped_aggregate_group_count >= 3 && input.reference_varchar_projection_count >= 2 &&
	       input.blocked_hash_aggregate_lookup_count > 0 && facts.no_native_sort &&
	       facts.has_wide_string_grouped_aggregate;
}

static bool
PhysicalRunnerGeneratedWorkPaysScanFilteredWideTwoJoinGroupedAggregateProtocol(const PhysicalRunnerCostInput &input,
                                                                               const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline && input.uses_scan_filters && input.source_filter_count == 0 &&
	       input.generated_stage_count >= 4 && input.materialization_elision_count == 0 &&
	       input.expression_cost >= WIDE_TWO_JOIN_GROUPED_AGGREGATE_MIN_EXPRESSION_COST &&
	       facts.has_generated_compute_work && input.native_join_stage_count == 2 &&
	       input.native_grouped_aggregate_stage_count == 1 && facts.grouped_aggregate_only &&
	       input.grouped_aggregate_group_count >= 3 && input.reference_varchar_projection_count >= 2 &&
	       input.blocked_hash_aggregate_lookup_count > 0 && facts.no_native_sort &&
	       facts.has_wide_string_grouped_aggregate;
}

static bool
PhysicalRunnerGeneratedWorkPaysScanFilteredJoinGroupedAggregateProtocol(const PhysicalRunnerCostInput &input,
                                                                        const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline && input.uses_scan_filters && input.source_filter_count == 0 &&
	       input.generated_stage_count >= 4 && input.materialization_elision_count == 0 &&
	       input.expression_cost >= SCAN_FILTERED_JOIN_GROUPED_AGGREGATE_MIN_EXPRESSION_COST &&
	       facts.has_generated_compute_work && input.native_join_stage_count == 1 &&
	       input.native_grouped_aggregate_stage_count == 1 && facts.grouped_aggregate_only && facts.no_native_sort &&
	       !facts.has_wide_string_grouped_aggregate;
}

static bool PhysicalRunnerGeneratedWorkPaysJoinBuildChainProtocol(const PhysicalRunnerCostInput &input,
                                                                  const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline && input.generated_stage_count == 1 && input.materialization_elision_count == 0 &&
	       input.expression_cost >= JOIN_BUILD_CHAIN_MIN_EXPRESSION_COST && input.source_filter_count > 0 &&
	       input.perfect_hash_join_probe_count > 0 &&
	       input.source_projected_column_count >= JOIN_BUILD_CHAIN_MIN_SOURCE_PROJECTED_COLUMNS &&
	       input.hash_join_build_payload_column_count > 0 &&
	       input.hash_join_build_payload_column_count <= JOIN_BUILD_CHAIN_MAX_PAYLOAD_COLUMNS &&
	       facts.has_generated_compute_work && input.native_join_stage_count == 2 &&
	       input.native_aggregate_stage_count == 0 && facts.no_native_sort;
}

static bool
PhysicalRunnerGeneratedWorkPaysScanFilteredJoinUngroupedAggregateProtocol(const PhysicalRunnerCostInput &input,
                                                                          const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline && input.uses_scan_filters && input.generated_stage_count > 0 &&
	       input.source_filter_count > 0 && facts.has_generated_compute_work && input.native_join_stage_count > 0 &&
	       input.native_join_stage_count <= 2 && input.native_aggregate_stage_count > 0 &&
	       input.native_grouped_aggregate_stage_count == 0 && facts.no_native_sort;
}

static bool PhysicalRunnerGeneratedWorkPaysJoinUngroupedAggregateProtocol(const PhysicalRunnerCostInput &input,
                                                                          const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline && input.generated_stage_count >= 2 && input.materialization_elision_count > 0 &&
	       input.expression_cost >= JOIN_UNGROUPED_AGGREGATE_MIN_EXPRESSION_COST && facts.has_generated_compute_work &&
	       input.native_join_stage_count == 1 && input.native_aggregate_stage_count == 1 &&
	       input.native_grouped_aggregate_stage_count == 0 && facts.no_native_sort;
}

static bool PhysicalRunnerGeneratedWorkPaysGeneratedSourceFilterJoinUngroupedAggregateProtocol(
    const PhysicalRunnerCostInput &input, const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline && !input.uses_scan_filters && input.source_filter_count > 0 &&
	       input.generated_stage_count >= 2 && input.materialization_elision_count == 0 &&
	       input.expression_cost >= JOIN_UNGROUPED_AGGREGATE_MIN_EXPRESSION_COST && facts.has_generated_compute_work &&
	       input.native_join_stage_count == 1 && input.native_aggregate_stage_count == 1 &&
	       input.native_grouped_aggregate_stage_count == 0 && facts.no_native_sort;
}

static bool PhysicalRunnerLongNativeJoinChainProtocol(const PhysicalRunnerCostInput &input,
                                                      const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline && !input.uses_scan_filters && input.generated_stage_count == 0 &&
	       input.materialization_elision_count == 0 && input.source_filter_count == 0 &&
	       input.expression_cost >= LONG_NATIVE_JOIN_CHAIN_MIN_EXPRESSION_COST && input.native_join_stage_count == 2 &&
	       input.perfect_hash_join_probe_count > 0 && input.native_aggregate_stage_count == 0 &&
	       input.native_grouped_aggregate_stage_count == 0 && facts.no_native_sort;
}

static bool PhysicalRunnerUsesDiscountedJoinGroupedAggregateStartup(const PhysicalRunnerCostInput &input,
                                                                    const PhysicalRunnerShapeFacts &facts) {
	return input.generated_stage_count >= 3 && input.materialization_elision_count > 0 &&
	       input.native_join_stage_count == 1 &&
	       PhysicalRunnerGeneratedWorkPaysJoinGroupedAggregateProtocol(input, facts);
}

static bool PhysicalRunnerUsesScanFilteredJoinUngroupedAggregateStartupWaiver(const PhysicalRunnerCostInput &input,
                                                                              const PhysicalRunnerShapeFacts &facts) {
	return input.full_pipeline && input.uses_scan_filters && input.source_filter_count >= 3 &&
	       facts.rows <= SaturatingCostCast(STANDARD_VECTOR_SIZE * 32ULL) &&
	       PhysicalRunnerGeneratedWorkPaysScanFilteredJoinUngroupedAggregateProtocol(input, facts) &&
	       input.native_join_stage_count <= 2 && input.native_aggregate_stage_count == 1 &&
	       input.native_grouped_aggregate_stage_count == 0 && facts.no_native_sort;
}

static bool PhysicalRunnerUsesLongBlockedJoinGroupedAggregateStartupWaiver(const PhysicalRunnerCostInput &input,
                                                                           const PhysicalRunnerShapeFacts &facts) {
	if (!PhysicalRunnerGeneratedWorkPaysLongBlockedJoinGroupedAggregateProtocol(input, facts)) {
		return false;
	}
	return facts.batches >= LONG_BLOCKED_JOIN_GROUPED_AGGREGATE_MIN_BATCHES;
}

static bool PhysicalRunnerUsesJoinBuildChainStartupWaiver(const PhysicalRunnerCostInput &input,
                                                          const PhysicalRunnerShapeFacts &facts) {
	if (!PhysicalRunnerGeneratedWorkPaysJoinBuildChainProtocol(input, facts)) {
		return false;
	}
	return facts.batches >= JOIN_BUILD_CHAIN_MIN_BATCHES;
}

static bool PhysicalRunnerUsesLongNativeJoinChainStartupWaiver(const PhysicalRunnerCostInput &input,
                                                               const PhysicalRunnerShapeFacts &facts) {
	if (!PhysicalRunnerLongNativeJoinChainProtocol(input, facts)) {
		return false;
	}
	return facts.batches >= LONG_NATIVE_JOIN_CHAIN_MIN_BATCHES;
}

struct PhysicalRunnerRule {
	const char *name;
	bool (*matches)(const PhysicalRunnerCostInput &input, const PhysicalRunnerShapeFacts &facts);
};

static const PhysicalRunnerRule PHYSICAL_RUNNER_FUNDED_PROTOCOL_RULES[] = {
    {"standalone_ungrouped_aggregate", PhysicalRunnerGeneratedWorkPaysStandaloneUngroupedAggregateProtocol},
    {"standalone_grouped_aggregate", PhysicalRunnerGeneratedWorkPaysStandaloneGroupedAggregateProtocol},
    {"join_grouped_aggregate", PhysicalRunnerGeneratedWorkPaysJoinGroupedAggregateProtocol},
    {"long_blocked_join_grouped_aggregate", PhysicalRunnerGeneratedWorkPaysLongBlockedJoinGroupedAggregateProtocol},
    {"high_expression_blocked_join_grouped_aggregate",
     PhysicalRunnerGeneratedWorkPaysHighExpressionBlockedJoinGroupedAggregateProtocol},
    {"narrow_two_join_grouped_aggregate", PhysicalRunnerGeneratedWorkPaysNarrowTwoJoinGroupedAggregateProtocol},
    {"scan_filtered_narrow_two_join_grouped_aggregate",
     PhysicalRunnerGeneratedWorkPaysScanFilteredNarrowTwoJoinGroupedAggregateProtocol},
    {"wide_two_join_grouped_aggregate", PhysicalRunnerGeneratedWorkPaysWideTwoJoinGroupedAggregateProtocol},
    {"scan_filtered_wide_two_join_grouped_aggregate",
     PhysicalRunnerGeneratedWorkPaysScanFilteredWideTwoJoinGroupedAggregateProtocol},
    {"scan_filtered_join_grouped_aggregate", PhysicalRunnerGeneratedWorkPaysScanFilteredJoinGroupedAggregateProtocol},
    {"join_build_chain", PhysicalRunnerGeneratedWorkPaysJoinBuildChainProtocol},
    {"scan_filtered_join_ungrouped_aggregate",
     PhysicalRunnerGeneratedWorkPaysScanFilteredJoinUngroupedAggregateProtocol},
    {"join_ungrouped_aggregate", PhysicalRunnerGeneratedWorkPaysJoinUngroupedAggregateProtocol},
    {"generated_source_filter_join_ungrouped_aggregate",
     PhysicalRunnerGeneratedWorkPaysGeneratedSourceFilterJoinUngroupedAggregateProtocol},
    {"long_native_join_chain", PhysicalRunnerUsesLongNativeJoinChainStartupWaiver},
};

enum class PhysicalRunnerStartupRuleAction : uint8_t { DISCOUNT_HALF, WAIVE };

struct PhysicalRunnerStartupRule {
	const char *name;
	PhysicalRunnerStartupRuleAction action;
	bool (*matches)(const PhysicalRunnerCostInput &input, const PhysicalRunnerShapeFacts &facts);
};

static const PhysicalRunnerStartupRule PHYSICAL_RUNNER_STARTUP_RULES[] = {
    {"discount_join_grouped_aggregate_startup", PhysicalRunnerStartupRuleAction::DISCOUNT_HALF,
     PhysicalRunnerUsesDiscountedJoinGroupedAggregateStartup},
    {"scan_filtered_join_ungrouped_aggregate_startup_waiver", PhysicalRunnerStartupRuleAction::WAIVE,
     PhysicalRunnerUsesScanFilteredJoinUngroupedAggregateStartupWaiver},
    {"long_blocked_join_grouped_aggregate_startup_waiver", PhysicalRunnerStartupRuleAction::WAIVE,
     PhysicalRunnerUsesLongBlockedJoinGroupedAggregateStartupWaiver},
    {"join_build_chain_startup_waiver", PhysicalRunnerStartupRuleAction::WAIVE,
     PhysicalRunnerUsesJoinBuildChainStartupWaiver},
    {"long_native_join_chain_startup_waiver", PhysicalRunnerStartupRuleAction::WAIVE,
     PhysicalRunnerUsesLongNativeJoinChainStartupWaiver},
};

static const PhysicalRunnerRule *PhysicalRunnerFindMatchingRule(const PhysicalRunnerCostInput &input,
                                                                const PhysicalRunnerShapeFacts &facts,
                                                                const PhysicalRunnerRule *rules, idx_t rule_count) {
	for (idx_t rule_idx = 0; rule_idx < rule_count; rule_idx++) {
		if (rules[rule_idx].matches(input, facts)) {
			return &rules[rule_idx];
		}
	}
	return nullptr;
}

static const PhysicalRunnerRule *PhysicalRunnerFindFundedProtocolRule(const PhysicalRunnerCostInput &input,
                                                                      const PhysicalRunnerShapeFacts &facts) {
	return PhysicalRunnerFindMatchingRule(input, facts, PHYSICAL_RUNNER_FUNDED_PROTOCOL_RULES,
	                                      sizeof(PHYSICAL_RUNNER_FUNDED_PROTOCOL_RULES) /
	                                          sizeof(PHYSICAL_RUNNER_FUNDED_PROTOCOL_RULES[0]));
}

static string PhysicalRunnerMatchedStartupRules(const PhysicalRunnerCostInput &input,
                                                const PhysicalRunnerShapeFacts &facts) {
	string result;
	for (const auto &rule : PHYSICAL_RUNNER_STARTUP_RULES) {
		if (!rule.matches(input, facts)) {
			continue;
		}
		if (!result.empty()) {
			result += "|";
		}
		result += rule.name;
	}
	return result;
}

static void PhysicalRunnerAppendReasonToken(string &result, const string &token) {
	if (token.empty()) {
		return;
	}
	if (!result.empty()) {
		result += "|";
	}
	result += token;
}

static string PhysicalRunnerAccelerationBasis(const PhysicalRunnerCostInput &input,
                                              const PhysicalRunnerShapeFacts &facts,
                                              const PhysicalRunnerCostParameters &parameters) {
	string result;
	auto funded_protocol_rule = PhysicalRunnerFindFundedProtocolRule(input, facts);
	if (funded_protocol_rule) {
		PhysicalRunnerAppendReasonToken(result, string("funded_protocol_rule:") + funded_protocol_rule->name);
	}
	if (input.generated_stage_count > 0 && parameters.generated_stage_benefit > 0) {
		PhysicalRunnerAppendReasonToken(result, "generated_stage_benefit");
	}
	if (facts.native_operator_stage_count > 0 && parameters.native_operator_stage_benefit > 0) {
		PhysicalRunnerAppendReasonToken(result, "native_operator_stage_benefit");
	}
	if (input.materialization_elision_count > 0 && parameters.materialization_elision_benefit > 0) {
		PhysicalRunnerAppendReasonToken(result, "materialization_elision_benefit");
	}
	if (input.full_pipeline && parameters.full_pipeline_benefit > 0) {
		PhysicalRunnerAppendReasonToken(result, "full_pipeline_benefit");
	}
	if (PhysicalRunnerUsesLongNativeJoinChainStartupWaiver(input, facts)) {
		PhysicalRunnerAppendReasonToken(result, "long_native_join_chain_startup_waiver");
	}
	return result.empty() ? "none" : result;
}

static string PhysicalRunnerRejectedAcceleratedWorkReason(const PhysicalRunnerCostInput &input,
                                                          const PhysicalRunnerShapeFacts &facts,
                                                          const PhysicalRunnerCostParameters &parameters,
                                                          const PhysicalRunnerCostProfile &profile) {
	if (PhysicalRunnerIsNativeContractProjectionGlue(input, facts)) {
		return "rejected_native_contract_projection_glue";
	}
	if (PhysicalRunnerIsSmallStatefulStandaloneProjection(input, facts)) {
		return "rejected_small_stateful_standalone_projection";
	}
	if (!input.has_accelerated_work) {
		return "rejected_no_accelerated_work";
	}
	const bool funded_protocol_rule_matches = PhysicalRunnerFindFundedProtocolRule(input, facts) != nullptr;
	const bool native_operator_work_is_costed =
	    facts.native_operator_stage_count == 0 || parameters.native_operator_stage_benefit > 0 ||
	    (input.full_pipeline && parameters.full_pipeline_benefit > 0) || funded_protocol_rule_matches;
	if (!native_operator_work_is_costed) {
		return "rejected_native_operator_work_uncosted";
	}
	if (PhysicalRunnerAccelerationBasis(input, facts, parameters) == "none") {
		return "rejected_no_costed_acceleration";
	}
	if (profile.saved_work_per_batch <= 0) {
		return "rejected_saved_work_non_positive";
	}
	return string();
}

static void PhysicalRunnerComputeWorkComponents(const PhysicalRunnerCostInput &input,
                                                const PhysicalRunnerShapeFacts &facts,
                                                const PhysicalRunnerCostParameters &parameters,
                                                PhysicalRunnerCostProfile &profile) {
	if (PhysicalRunnerIsNativeContractProjectionGlue(input, facts)) {
		return;
	}
	const auto expression_cost = SaturatingCostCast(input.expression_cost);
	profile.generated_expression_work = expression_cost;
	auto generated_stage_work = MultiplyCost(SaturatingCostCast(input.generated_stage_count),
	                                         SaturatingCostCast(parameters.generated_stage_benefit));
	if (expression_cost > 0 && generated_stage_work > expression_cost) {
		generated_stage_work = expression_cost;
	}
	profile.generated_stage_work = generated_stage_work;
	const auto native_join_stage_count = SaturatingCostCast(input.native_join_stage_count);
	const auto native_aggregate_stage_count = SaturatingCostCast(input.native_aggregate_stage_count);
	const auto native_grouped_aggregate_stage_count = SaturatingCostCast(input.native_grouped_aggregate_stage_count);
	const auto native_sort_stage_count = SaturatingCostCast(input.native_sort_stage_count);
	D_ASSERT(native_aggregate_stage_count >= native_grouped_aggregate_stage_count);
	const auto native_operator_stage_count =
	    AddCost(AddCost(native_join_stage_count, native_aggregate_stage_count), native_sort_stage_count);
	const auto native_operator_stage_benefit = SaturatingCostCast(parameters.native_operator_stage_benefit);
	profile.native_operator_work = MultiplyCost(native_operator_stage_count, native_operator_stage_benefit);
	if (input.materialization_elision_count > 0) {
		profile.materialization_elision_work =
		    MultiplyCost(SaturatingCostCast(input.materialization_elision_count),
		                 SaturatingCostCast(parameters.materialization_elision_benefit));
	}
	if (input.materialization_source_append_count > 0) {
		profile.materialization_source_append_penalty = MultiplyCost(
		    SaturatingCostCast(input.materialization_source_append_count), MATERIALIZATION_SOURCE_APPEND_PENALTY);
	}
	if (input.full_pipeline) {
		const auto full_pipeline_benefit = SaturatingCostCast(parameters.full_pipeline_benefit);
		const auto stateful_protocol_stage_count =
		    AddCost(native_join_stage_count, native_grouped_aggregate_stage_count);
		if (stateful_protocol_stage_count > 0) {
			profile.stateful_protocol_penalty = MultiplyCost(
			    AddCost(stateful_protocol_stage_count, stateful_protocol_stage_count), native_operator_stage_benefit);
			profile.stateful_protocol_penalty = AddCost(profile.stateful_protocol_penalty, full_pipeline_benefit);
		} else {
			profile.full_pipeline_work = full_pipeline_benefit;
		}
	}
	if (input.blocked_hash_aggregate_lookup_count > 0) {
		profile.stateful_protocol_penalty =
		    AddCost(profile.stateful_protocol_penalty,
		            MultiplyCost(SaturatingCostCast(input.blocked_hash_aggregate_lookup_count),
		                         BLOCKED_HASH_AGGREGATE_LOOKUP_PENALTY));
		if (PhysicalRunnerUsesDiscountedJoinGroupedAggregateStartup(input, facts)) {
			profile.stateful_protocol_penalty =
			    AddCost(profile.stateful_protocol_penalty,
			            MultiplyCost(SaturatingCostCast(input.blocked_hash_aggregate_lookup_count),
			                         DISCOUNTED_JOIN_BLOCKED_HASH_AGGREGATE_LOOKUP_PENALTY));
		}
	}
	auto work = profile.generated_expression_work;
	work = AddCost(work, profile.generated_stage_work);
	work = AddCost(work, profile.native_operator_work);
	work = AddCost(work, profile.materialization_elision_work);
	work = AddCost(work, profile.full_pipeline_work);
	work = SubtractCost(work, profile.materialization_source_append_penalty);
	work = SubtractCost(work, profile.stateful_protocol_penalty);
	profile.saved_work_per_batch = work;
}

static int64_t PhysicalRunnerStartupCost(const PhysicalRunnerCostInput &input, const PhysicalRunnerShapeFacts &facts,
                                         const PhysicalRunnerCostParameters &parameters) {
	auto startup_cost = SaturatingCostCast(parameters.startup_base_cost);
	for (const auto &rule : PHYSICAL_RUNNER_STARTUP_RULES) {
		if (!rule.matches(input, facts)) {
			continue;
		}
		switch (rule.action) {
		case PhysicalRunnerStartupRuleAction::DISCOUNT_HALF:
			startup_cost /= 2;
			break;
		case PhysicalRunnerStartupRuleAction::WAIVE:
			startup_cost = 0;
			break;
		default:
			throw InternalException("Unknown physical runner startup rule action");
		}
	}
	return startup_cost;
}

static PhysicalRunnerCostParameters PhysicalRunnerGpuCostParameters(const PhysicalRunnerCostParameters &parameters) {
	PhysicalRunnerCostParameters result;
	result.compiled_vectorized_runner_available = false;
	result.generated_stage_benefit = parameters.gpu_generated_stage_benefit;
	result.native_operator_stage_benefit = parameters.gpu_native_operator_stage_benefit;
	result.materialization_elision_benefit = parameters.gpu_materialization_elision_benefit;
	result.full_pipeline_benefit = parameters.gpu_full_pipeline_benefit;
	result.startup_base_cost = parameters.gpu_startup_base_cost;
	result.startup_margin_basis_points = parameters.gpu_startup_margin_basis_points;
	return result;
}

static int64_t PhysicalRunnerRequiredBenefit(const PhysicalRunnerCostProfile &profile,
                                             const PhysicalRunnerCostParameters &parameters) {
	auto margin = FractionalCost(profile.startup_cost, SaturatingCostCast(parameters.startup_margin_basis_points),
	                             BASIS_POINT_SCALE);
	return AddCost(profile.startup_cost, margin);
}

static void PhysicalRunnerInitializeProfile(const PhysicalRunnerCostInput &input, const PhysicalRunnerShapeFacts &facts,
                                            PhysicalRunnerCostProfile &profile) {
	profile.present = true;
	profile.rows = facts.rows;
	profile.batches = facts.batches;
	profile.expression_cost = SaturatingCostCast(input.expression_cost);
	profile.generated_stage_count = SaturatingCostCast(input.generated_stage_count);
	profile.materialization_elision_count = SaturatingCostCast(input.materialization_elision_count);
	profile.materialization_source_append_count = SaturatingCostCast(input.materialization_source_append_count);
	profile.native_join_stage_count = SaturatingCostCast(input.native_join_stage_count);
	profile.native_aggregate_stage_count = SaturatingCostCast(input.native_aggregate_stage_count);
	profile.native_grouped_aggregate_stage_count = SaturatingCostCast(input.native_grouped_aggregate_stage_count);
	profile.native_sort_stage_count = SaturatingCostCast(input.native_sort_stage_count);
	profile.source_filter_count = SaturatingCostCast(input.source_filter_count);
	profile.full_pipeline = input.full_pipeline;
	profile.generated_work_class = input.generated_work_class;
	profile.native_protocol_class = input.native_protocol_class;
	auto funded_protocol_rule = PhysicalRunnerFindFundedProtocolRule(input, facts);
	profile.funded_protocol_rule = funded_protocol_rule ? funded_protocol_rule->name : string();
	profile.startup_rules = PhysicalRunnerMatchedStartupRules(input, facts);
}

struct PhysicalRunnerSelectionAnalysis {
	bool selected = false;
	string selection_reason;
};

static PhysicalRunnerSelectionAnalysis PhysicalRunnerAnalyzeSelection(const PhysicalRunnerCostInput &input,
                                                                      const PhysicalRunnerShapeFacts &facts,
                                                                      const PhysicalRunnerCostParameters &parameters,
                                                                      const PhysicalRunnerCostProfile &profile,
                                                                      bool runner_available) {
	PhysicalRunnerSelectionAnalysis result;
	if (!runner_available) {
		result.selection_reason = "rejected_runner_unavailable";
		return result;
	}
	result.selection_reason = PhysicalRunnerRejectedAcceleratedWorkReason(input, facts, parameters, profile);
	if (!result.selection_reason.empty()) {
		return result;
	}
	if (profile.accelerated_runner_benefit <= profile.required_benefit) {
		result.selection_reason = "rejected_insufficient_benefit";
		return result;
	}
	result.selected = true;
	result.selection_reason = "admitted_" + PhysicalRunnerAccelerationBasis(input, facts, parameters);
	return result;
}

PhysicalRunnerCostProfile DuckDBCostModel::SelectPhysicalRunner(const PhysicalRunnerCostInput &input,
                                                                const PhysicalRunnerCostParameters &parameters) {
	auto facts = PhysicalRunnerBuildShapeFacts(input);
	PhysicalRunnerCostProfile compiled_profile;
	PhysicalRunnerInitializeProfile(input, facts, compiled_profile);
	PhysicalRunnerComputeWorkComponents(input, facts, parameters, compiled_profile);
	compiled_profile.accelerated_runner_benefit =
	    MultiplyCost(compiled_profile.batches, compiled_profile.saved_work_per_batch);
	compiled_profile.startup_cost = PhysicalRunnerStartupCost(input, facts, parameters);
	compiled_profile.required_benefit = PhysicalRunnerRequiredBenefit(compiled_profile, parameters);
	compiled_profile.net_benefit =
	    SubtractCost(compiled_profile.accelerated_runner_benefit, compiled_profile.startup_cost);

	auto gpu_parameters = PhysicalRunnerGpuCostParameters(parameters);
	PhysicalRunnerCostProfile gpu_profile;
	PhysicalRunnerInitializeProfile(input, facts, gpu_profile);
	PhysicalRunnerComputeWorkComponents(input, facts, gpu_parameters, gpu_profile);
	gpu_profile.accelerated_runner_benefit = MultiplyCost(gpu_profile.batches, gpu_profile.saved_work_per_batch);
	gpu_profile.gpu_transfer_cost =
	    MultiplyCost(gpu_profile.batches, SaturatingCostCast(parameters.gpu_transfer_cost_per_batch));
	gpu_profile.startup_cost = PhysicalRunnerStartupCost(input, facts, gpu_parameters);
	gpu_profile.required_benefit =
	    AddCost(PhysicalRunnerRequiredBenefit(gpu_profile, gpu_parameters), gpu_profile.gpu_transfer_cost);
	gpu_profile.net_benefit = SubtractCost(
	    SubtractCost(gpu_profile.accelerated_runner_benefit, gpu_profile.startup_cost), gpu_profile.gpu_transfer_cost);

	auto compiled_selection = PhysicalRunnerAnalyzeSelection(input, facts, parameters, compiled_profile,
	                                                         parameters.compiled_vectorized_runner_available);
	auto gpu_selection =
	    PhysicalRunnerAnalyzeSelection(input, facts, gpu_parameters, gpu_profile, parameters.gpu_runner_available);
	const bool compiled_selected = compiled_selection.selected;
	const bool gpu_selected = gpu_selection.selected;

	const bool gpu_profile_preferred =
	    parameters.gpu_runner_available &&
	    (!parameters.compiled_vectorized_runner_available || gpu_profile.net_benefit > compiled_profile.net_benefit);
	PhysicalRunnerCostProfile profile = gpu_profile_preferred ? gpu_profile : compiled_profile;
	if (gpu_selected && (!compiled_selected || gpu_profile_preferred)) {
		profile = gpu_profile;
		profile.selected_runner = ExecutionRunnerKind::COMPILED_GPU;
		profile.selected_gpu_runner = true;
		profile.selection_reason = gpu_selection.selection_reason;
	} else if (compiled_selected) {
		profile = compiled_profile;
		profile.selected_runner = ExecutionRunnerKind::COMPILED_VECTORIZED;
		profile.selected_compiled_vectorized_runner = true;
		profile.selection_reason = compiled_selection.selection_reason;
	} else {
		const auto &primary_rejection =
		    gpu_profile_preferred ? gpu_selection.selection_reason : compiled_selection.selection_reason;
		const auto &fallback_rejection =
		    gpu_profile_preferred ? compiled_selection.selection_reason : gpu_selection.selection_reason;
		profile.selection_reason = primary_rejection.empty() ? fallback_rejection : primary_rejection;
	}
	profile.compiled_vectorized_runner_benefit = compiled_profile.accelerated_runner_benefit;
	profile.compiled_vectorized_startup_cost = compiled_profile.startup_cost;
	profile.compiled_vectorized_required_benefit = compiled_profile.required_benefit;
	profile.compiled_vectorized_net_benefit = compiled_profile.net_benefit;
	profile.gpu_runner_benefit = gpu_profile.accelerated_runner_benefit;
	profile.gpu_transfer_cost = gpu_profile.gpu_transfer_cost;
	profile.gpu_startup_cost = gpu_profile.startup_cost;
	profile.gpu_required_benefit = gpu_profile.required_benefit;
	profile.gpu_net_benefit = gpu_profile.net_benefit;
	profile.selected_accelerated_runner = profile.selected_runner != ExecutionRunnerKind::VECTORIZED;
	return profile;
}

const char *PhysicalRunnerGeneratedWorkClassToString(PhysicalRunnerGeneratedWorkClass work_class) {
	switch (work_class) {
	case PhysicalRunnerGeneratedWorkClass::NONE:
		return "none";
	case PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE:
		return "projection_glue";
	case PhysicalRunnerGeneratedWorkClass::HIGH_COST_PROJECTION:
		return "high_cost_projection";
	case PhysicalRunnerGeneratedWorkClass::COMPUTE:
		return "compute";
	default:
		return "unknown";
	}
}

const char *PhysicalRunnerNativeProtocolClassToString(PhysicalRunnerNativeProtocolClass protocol_class) {
	switch (protocol_class) {
	case PhysicalRunnerNativeProtocolClass::NONE:
		return "none";
	case PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL:
		return "stateful_source_sink_protocol";
	default:
		return "unknown";
	}
}

} // namespace duckdb
