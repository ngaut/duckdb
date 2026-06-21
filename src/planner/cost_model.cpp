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
	return SaturatingCostCast(rows);
}

static int64_t PhysicalRunnerBatches(int64_t rows) {
	return AddCost(rows, STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
}

static int64_t PhysicalRunnerSavedWorkPerBatch(const PhysicalRunnerCostInput &input,
                                               const PhysicalRunnerCostParameters &parameters) {
	auto work = SaturatingCostCast(input.expression_cost);
	work = AddCost(work, MultiplyCost(SaturatingCostCast(input.generated_stage_count),
	                                  SaturatingCostCast(parameters.generated_stage_benefit)));
	const auto native_operator_stage_count = AddCost(AddCost(SaturatingCostCast(input.native_join_stage_count),
	                                                         SaturatingCostCast(input.native_aggregate_stage_count)),
	                                                 SaturatingCostCast(input.native_sort_stage_count));
	work = AddCost(
	    work, MultiplyCost(native_operator_stage_count, SaturatingCostCast(parameters.native_operator_stage_benefit)));
	if (input.materialization_elision_count > 0) {
		auto elision_work = MultiplyCost(SaturatingCostCast(input.materialization_elision_count),
		                                 SaturatingCostCast(parameters.materialization_elision_benefit));
		work = AddCost(work, elision_work);
	}
	if (input.full_pipeline) {
		work = AddCost(work, SaturatingCostCast(parameters.full_pipeline_benefit));
	}
	return work;
}

static int64_t PhysicalRunnerStartupCost(const PhysicalRunnerCostParameters &parameters) {
	return SaturatingCostCast(parameters.startup_base_cost);
}

static int64_t PhysicalRunnerRequiredBenefit(const PhysicalRunnerCostProfile &profile,
                                             const PhysicalRunnerCostParameters &parameters) {
	auto margin = FractionalCost(profile.startup_cost, SaturatingCostCast(parameters.startup_margin_basis_points),
	                             BASIS_POINT_SCALE);
	return AddCost(profile.startup_cost, margin);
}

static bool PhysicalRunnerHasAcceleratedWork(const PhysicalRunnerCostInput &input,
                                             const PhysicalRunnerCostParameters &parameters,
                                             const PhysicalRunnerCostProfile &profile) {
	const auto native_operator_stage_count =
	    input.native_join_stage_count + input.native_aggregate_stage_count + input.native_sort_stage_count;
	const bool native_operator_work_is_costed = native_operator_stage_count == 0 ||
	                                            parameters.native_operator_stage_benefit > 0 ||
	                                            (input.full_pipeline && parameters.full_pipeline_benefit > 0);
	const bool has_costed_acceleration =
	    (input.generated_stage_count > 0 && parameters.generated_stage_benefit > 0) ||
	    (native_operator_stage_count > 0 && parameters.native_operator_stage_benefit > 0) ||
	    (input.materialization_elision_count > 0 && parameters.materialization_elision_benefit > 0) ||
	    (input.full_pipeline && parameters.full_pipeline_benefit > 0);
	return input.has_accelerated_work && native_operator_work_is_costed && has_costed_acceleration &&
	       profile.saved_work_per_batch > 0;
}

static bool PhysicalRunnerShouldSelectAccelerated(const PhysicalRunnerCostInput &input,
                                                  const PhysicalRunnerCostParameters &parameters,
                                                  const PhysicalRunnerCostProfile &profile) {
	if (!PhysicalRunnerHasAcceleratedWork(input, parameters, profile)) {
		return false;
	}
	return profile.accelerated_runner_benefit > profile.required_benefit;
}

PhysicalRunnerCostProfile DuckDBCostModel::SelectPhysicalRunner(const PhysicalRunnerCostInput &input,
                                                                const PhysicalRunnerCostParameters &parameters) {
	PhysicalRunnerCostProfile profile;
	profile.present = true;
	profile.rows = PhysicalRunnerRows(input);
	profile.batches = PhysicalRunnerBatches(profile.rows);
	profile.expression_cost = SaturatingCostCast(input.expression_cost);
	profile.generated_stage_count = SaturatingCostCast(input.generated_stage_count);
	profile.materialization_elision_count = SaturatingCostCast(input.materialization_elision_count);
	profile.native_join_stage_count = SaturatingCostCast(input.native_join_stage_count);
	profile.native_aggregate_stage_count = SaturatingCostCast(input.native_aggregate_stage_count);
	profile.native_sort_stage_count = SaturatingCostCast(input.native_sort_stage_count);
	profile.full_pipeline = input.full_pipeline;
	profile.saved_work_per_batch = PhysicalRunnerSavedWorkPerBatch(input, parameters);
	profile.accelerated_runner_benefit = MultiplyCost(profile.batches, profile.saved_work_per_batch);
	profile.startup_cost = PhysicalRunnerStartupCost(parameters);
	profile.required_benefit = PhysicalRunnerRequiredBenefit(profile, parameters);
	profile.net_benefit = profile.accelerated_runner_benefit - profile.startup_cost;
	profile.selected_accelerated_runner = PhysicalRunnerShouldSelectAccelerated(input, parameters, profile);
	return profile;
}

} // namespace duckdb
