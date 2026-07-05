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
static constexpr int64_t NATIVE_JOIN_PROTOCOL_PENALTY = 720;
static constexpr int64_t NATIVE_HASH_JOIN_BUILD_SINK_PROTOCOL_PENALTY = 720;
static constexpr int64_t NATIVE_GROUPED_AGGREGATE_PARALLELISM_PENALTY = 160;

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

static int64_t DoubleCost(int64_t value) {
	return AddCost(value, value);
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
	return SaturatingCostCast(rows);
}

static int64_t PhysicalRunnerBatches(int64_t rows) {
	return AddCost(rows, STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
}

struct PhysicalRunnerShapeFacts {
	int64_t rows = 1;
	int64_t batches = 1;
	idx_t native_operator_stage_count = 0;
	bool has_generated_compute_work = false;
	bool no_native_sort = false;
};

static PhysicalRunnerShapeFacts PhysicalRunnerBuildShapeFacts(const PhysicalRunnerCostInput &input) {
	PhysicalRunnerShapeFacts facts;
	facts.rows = PhysicalRunnerRows(input);
	facts.batches = PhysicalRunnerBatches(facts.rows);
	facts.native_operator_stage_count =
	    input.native_join_stage_count + input.native_aggregate_stage_count + input.native_sort_stage_count;
	facts.has_generated_compute_work = input.generated_work_class != PhysicalRunnerGeneratedWorkClass::NONE &&
	                                   input.generated_work_class != PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE;
	facts.no_native_sort = input.native_sort_stage_count == 0;
	return facts;
}

static bool PhysicalRunnerFullPipelineBenefitPays(const PhysicalRunnerCostInput &input,
                                                  const PhysicalRunnerShapeFacts &facts) {
	return input.input_scope == PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE && input.full_pipeline &&
	       facts.native_operator_stage_count == 0 && input.generated_stage_count == 0 &&
	       input.materialization_elision_count == 0 && input.native_hash_join_build_sink_count == 0;
}

static bool PhysicalRunnerNativeStageBenefitCanPay(const PhysicalRunnerCostInput &input,
                                                   const PhysicalRunnerCostParameters &parameters) {
	if (parameters.native_operator_stage_benefit == 0) {
		return false;
	}
	if (input.native_sort_stage_count > 0) {
		return false;
	}
	if (input.native_protocol_class == PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL &&
	    input.generated_backend_stage_count == 0) {
		return false;
	}
	return input.native_join_stage_count > 0 || input.native_aggregate_stage_count > 0;
}

static bool PhysicalRunnerMaterializationElisionBenefitCanPay(const PhysicalRunnerCostInput &input,
                                                              const PhysicalRunnerCostParameters &parameters) {
	if (parameters.materialization_elision_benefit == 0 || input.materialization_elision_count == 0) {
		return false;
	}
	return true;
}

static bool PhysicalRunnerGeneratedBackendStageBenefitCanPay(const PhysicalRunnerCostInput &input) {
	if (input.generated_backend_stage_count == 0 || input.generated_stage_count == 0) {
		return false;
	}
	if (input.native_hash_join_build_sink_count > 0 && input.native_join_stage_count == 0 &&
	    input.native_aggregate_stage_count == 0 && input.native_sort_stage_count == 0) {
		return false;
	}
	return true;
}

static bool PhysicalRunnerHasGeneratedComputePrefix(const PhysicalRunnerCostInput &input) {
	return input.generated_work_class != PhysicalRunnerGeneratedWorkClass::NONE &&
	       input.generated_work_class != PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE &&
	       input.generated_stage_count > 0;
}

static bool PhysicalRunnerHashJoinBuildSinkProtocolPenaltyApplies(const PhysicalRunnerCostInput &input) {
	if (input.native_hash_join_build_sink_count == 0) {
		return false;
	}
	return !PhysicalRunnerHasGeneratedComputePrefix(input) || input.native_join_stage_count > 0 ||
	       input.native_aggregate_stage_count > 0 || input.native_sort_stage_count > 0;
}

static idx_t PhysicalRunnerCostedGeneratedBackendStageCount(const PhysicalRunnerCostInput &input) {
	if (!PhysicalRunnerGeneratedBackendStageBenefitCanPay(input)) {
		return 0;
	}
	return MinValue(input.generated_backend_stage_count, input.generated_stage_count);
}

struct PhysicalRunnerAdmission {
	string admission_class;
	bool full_pipeline_benefit_pays = false;
	bool native_operator_work_is_costed = false;
	string acceleration_basis;
};

static string PhysicalRunnerAdmissionClass(const PhysicalRunnerCostInput &input, const PhysicalRunnerShapeFacts &facts,
                                           const PhysicalRunnerCostParameters &parameters,
                                           bool full_pipeline_benefit_pays) {
	if (!input.has_accelerated_work) {
		return "none";
	}
	if (PhysicalRunnerMaterializationElisionBenefitCanPay(input, parameters)) {
		return "materialization_elision";
	}
	if (full_pipeline_benefit_pays) {
		return "full_pipeline";
	}
	if (facts.native_operator_stage_count == 0) {
		return facts.has_generated_compute_work && input.generated_stage_count > 0 &&
		               parameters.generated_stage_benefit > 0
		           ? "generated"
		           : "none";
	}
	if (facts.has_generated_compute_work && input.generated_stage_count > 0 && facts.no_native_sort) {
		return "generated_native_fusion";
	}
	return "none";
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

static bool PhysicalRunnerNativeOperatorBenefitPays(const PhysicalRunnerCostInput &input,
                                                    const PhysicalRunnerCostParameters &parameters,
                                                    const PhysicalRunnerAdmission &admission) {
	return admission.admission_class != "none" && PhysicalRunnerNativeStageBenefitCanPay(input, parameters);
}

static void PhysicalRunnerBuildAccelerationBasis(const PhysicalRunnerCostInput &input,
                                                 const PhysicalRunnerShapeFacts &facts,
                                                 const PhysicalRunnerCostParameters &parameters,
                                                 PhysicalRunnerAdmission &admission) {
	if (admission.admission_class != "none") {
		PhysicalRunnerAppendReasonToken(admission.acceleration_basis,
		                                string("admission_class:") + admission.admission_class);
	}
	if (facts.has_generated_compute_work && input.generated_stage_count > 0 && parameters.generated_stage_benefit > 0) {
		PhysicalRunnerAppendReasonToken(admission.acceleration_basis, "generated_stage_benefit");
	}
	if (facts.native_operator_stage_count > 0 &&
	    PhysicalRunnerNativeOperatorBenefitPays(input, parameters, admission)) {
		PhysicalRunnerAppendReasonToken(admission.acceleration_basis, "native_operator_stage_benefit");
	}
	if (PhysicalRunnerMaterializationElisionBenefitCanPay(input, parameters)) {
		PhysicalRunnerAppendReasonToken(admission.acceleration_basis, "materialization_elision_benefit");
	}
	if (admission.full_pipeline_benefit_pays) {
		PhysicalRunnerAppendReasonToken(admission.acceleration_basis, "full_pipeline_benefit");
	}
	if (admission.acceleration_basis.empty()) {
		admission.acceleration_basis = "none";
	}
}

static PhysicalRunnerAdmission PhysicalRunnerEvaluateAdmission(const PhysicalRunnerCostInput &input,
                                                               const PhysicalRunnerShapeFacts &facts,
                                                               const PhysicalRunnerCostParameters &parameters) {
	PhysicalRunnerAdmission admission;
	admission.full_pipeline_benefit_pays =
	    parameters.full_pipeline_benefit > 0 && PhysicalRunnerFullPipelineBenefitPays(input, facts);
	admission.admission_class =
	    PhysicalRunnerAdmissionClass(input, facts, parameters, admission.full_pipeline_benefit_pays);
	admission.native_operator_work_is_costed =
	    facts.native_operator_stage_count == 0 || admission.admission_class != "none";
	PhysicalRunnerBuildAccelerationBasis(input, facts, parameters, admission);
	return admission;
}

static string PhysicalRunnerRejectedAcceleratedWorkReason(const PhysicalRunnerCostInput &input,
                                                          const PhysicalRunnerAdmission &admission,
                                                          const PhysicalRunnerCostProfile &profile) {
	if (!input.has_accelerated_work) {
		return "rejected_no_accelerated_work";
	}
	if (!admission.native_operator_work_is_costed) {
		return "rejected_native_operator_work_uncosted";
	}
	if (admission.acceleration_basis == "none") {
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
                                                const PhysicalRunnerAdmission &admission,
                                                PhysicalRunnerCostProfile &profile) {
	const auto expression_cost = SaturatingCostCast(input.expression_cost);
	if (facts.has_generated_compute_work) {
		profile.generated_expression_work = expression_cost;
		const auto generated_stage_benefit = SaturatingCostCast(parameters.generated_stage_benefit);
		const auto generated_backend_stage_count = PhysicalRunnerCostedGeneratedBackendStageCount(input);
		const auto generated_expression_stage_count = input.generated_stage_count - generated_backend_stage_count;
		int64_t generated_expression_stage_work = 0;
		if (expression_cost > 0) {
			generated_expression_stage_work =
			    MultiplyCost(SaturatingCostCast(generated_expression_stage_count), generated_stage_benefit);
			if (generated_expression_stage_work > expression_cost) {
				generated_expression_stage_work = expression_cost;
			}
		}
		profile.generated_backend_stage_work =
		    MultiplyCost(SaturatingCostCast(generated_backend_stage_count), generated_stage_benefit);
		profile.generated_stage_work = AddCost(generated_expression_stage_work, profile.generated_backend_stage_work);
	}
	D_ASSERT(input.native_aggregate_stage_count >= input.native_grouped_aggregate_stage_count);
	if (PhysicalRunnerNativeOperatorBenefitPays(input, parameters, admission)) {
		const auto native_operator_stage_benefit = SaturatingCostCast(parameters.native_operator_stage_benefit);
		profile.native_operator_work =
		    MultiplyCost(SaturatingCostCast(facts.native_operator_stage_count), native_operator_stage_benefit);
	}
	if (PhysicalRunnerMaterializationElisionBenefitCanPay(input, parameters)) {
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
		if (admission.full_pipeline_benefit_pays) {
			profile.full_pipeline_work = full_pipeline_benefit;
		}
	}
	if (input.native_join_stage_count > 0) {
		const auto native_join_stage_count = SaturatingCostCast(input.native_join_stage_count);
		profile.stateful_protocol_penalty = AddCost(
		    profile.stateful_protocol_penalty, MultiplyCost(native_join_stage_count, NATIVE_JOIN_PROTOCOL_PENALTY));
	}
	if (PhysicalRunnerHashJoinBuildSinkProtocolPenaltyApplies(input)) {
		const auto native_hash_join_build_sink_count = SaturatingCostCast(input.native_hash_join_build_sink_count);
		profile.stateful_protocol_penalty =
		    AddCost(profile.stateful_protocol_penalty,
		            MultiplyCost(native_hash_join_build_sink_count, NATIVE_HASH_JOIN_BUILD_SINK_PROTOCOL_PENALTY));
	}
	if (input.unfused_mark_filter_aggregate_count > 0) {
		profile.unfused_mark_filter_aggregate_penalty =
		    DoubleCost(MultiplyCost(SaturatingCostCast(input.unfused_mark_filter_aggregate_count),
		                            SaturatingCostCast(parameters.generated_stage_benefit)));
		profile.stateful_protocol_penalty =
		    AddCost(profile.stateful_protocol_penalty, profile.unfused_mark_filter_aggregate_penalty);
	}
	if (parameters.vectorized_parallelism > 1 && input.native_grouped_aggregate_stage_count > 0) {
		const auto grouped_aggregate_parallel_penalty =
		    MultiplyCost(SaturatingCostCast(input.native_grouped_aggregate_stage_count),
		                 SaturatingCostCast(parameters.vectorized_parallelism - 1));
		profile.stateful_protocol_penalty =
		    AddCost(profile.stateful_protocol_penalty,
		            MultiplyCost(grouped_aggregate_parallel_penalty, NATIVE_GROUPED_AGGREGATE_PARALLELISM_PENALTY));
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

static int64_t PhysicalRunnerStartupCost(const PhysicalRunnerCostParameters &parameters) {
	return MultiplyCost(SaturatingCostCast(parameters.startup_base_cost),
	                    SaturatingCostCast(MaxValue<idx_t>(parameters.vectorized_parallelism, 1)));
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
	result.vectorized_parallelism = parameters.vectorized_parallelism;
	return result;
}

static int64_t PhysicalRunnerRequiredBenefit(const PhysicalRunnerCostProfile &profile,
                                             const PhysicalRunnerCostParameters &parameters) {
	auto margin = FractionalCost(profile.startup_cost, SaturatingCostCast(parameters.startup_margin_basis_points),
	                             BASIS_POINT_SCALE);
	auto required_benefit = AddCost(profile.startup_cost, margin);
	return required_benefit;
}

static int64_t PhysicalRunnerParallelPerBatchWorkFloor(const PhysicalRunnerCostParameters &parameters) {
	if (parameters.vectorized_parallelism <= 1) {
		return 0;
	}
	return MultiplyCost(SaturatingCostCast(parameters.vectorized_parallelism),
	                    SaturatingCostCast(parameters.native_operator_stage_benefit));
}

static bool PhysicalRunnerParallelPerBatchWorkFloorApplies(const PhysicalRunnerCostInput &input) {
	return input.native_join_stage_count > 0 || input.native_aggregate_stage_count > 0 ||
	       input.native_sort_stage_count > 0;
}

static void PhysicalRunnerInitializeProfile(const PhysicalRunnerCostInput &input, const PhysicalRunnerShapeFacts &facts,
                                            const PhysicalRunnerAdmission &admission,
                                            PhysicalRunnerCostProfile &profile) {
	profile.present = true;
	profile.input_scope = input.input_scope;
	profile.rows = facts.rows;
	profile.batches = facts.batches;
	profile.expression_cost = SaturatingCostCast(input.expression_cost);
	profile.generated_stage_count = SaturatingCostCast(input.generated_stage_count);
	profile.generated_backend_stage_count = SaturatingCostCast(input.generated_backend_stage_count);
	profile.materialization_elision_count = SaturatingCostCast(input.materialization_elision_count);
	profile.materialization_source_append_count = SaturatingCostCast(input.materialization_source_append_count);
	profile.unfused_mark_filter_aggregate_count = SaturatingCostCast(input.unfused_mark_filter_aggregate_count);
	profile.native_join_stage_count = SaturatingCostCast(input.native_join_stage_count);
	profile.native_hash_join_build_sink_count = SaturatingCostCast(input.native_hash_join_build_sink_count);
	profile.native_aggregate_stage_count = SaturatingCostCast(input.native_aggregate_stage_count);
	profile.native_grouped_aggregate_stage_count = SaturatingCostCast(input.native_grouped_aggregate_stage_count);
	profile.native_sort_stage_count = SaturatingCostCast(input.native_sort_stage_count);
	profile.source_filter_count = SaturatingCostCast(input.source_filter_count);
	profile.full_pipeline = input.full_pipeline;
	profile.generated_work_class = input.generated_work_class;
	profile.native_protocol_class = input.native_protocol_class;
	profile.admission_class = admission.admission_class;
}

struct PhysicalRunnerSelectionAnalysis {
	bool selected = false;
	string selection_reason;
};

static PhysicalRunnerSelectionAnalysis PhysicalRunnerAnalyzeSelection(const PhysicalRunnerCostInput &input,
                                                                      const PhysicalRunnerAdmission &admission,
                                                                      const PhysicalRunnerCostProfile &profile,
                                                                      bool runner_available,
                                                                      int64_t parallel_per_batch_work_floor) {
	PhysicalRunnerSelectionAnalysis result;
	if (!runner_available) {
		result.selection_reason = "rejected_runner_unavailable";
		return result;
	}
	result.selection_reason = PhysicalRunnerRejectedAcceleratedWorkReason(input, admission, profile);
	if (!result.selection_reason.empty()) {
		return result;
	}
	if (PhysicalRunnerParallelPerBatchWorkFloorApplies(input) && parallel_per_batch_work_floor > 0 &&
	    profile.saved_work_per_batch < parallel_per_batch_work_floor) {
		result.selection_reason = "rejected_parallel_per_batch_work_floor";
		return result;
	}
	if (profile.accelerated_runner_benefit <= profile.required_benefit) {
		result.selection_reason = "rejected_insufficient_benefit";
		return result;
	}
	result.selected = true;
	result.selection_reason = "admitted_" + admission.acceleration_basis;
	return result;
}

PhysicalRunnerCostProfile DuckDBCostModel::SelectPhysicalRunner(const PhysicalRunnerCostInput &input,
                                                                const PhysicalRunnerCostParameters &parameters) {
	auto facts = PhysicalRunnerBuildShapeFacts(input);
	auto compiled_admission = PhysicalRunnerEvaluateAdmission(input, facts, parameters);
	PhysicalRunnerCostProfile compiled_profile;
	PhysicalRunnerInitializeProfile(input, facts, compiled_admission, compiled_profile);
	PhysicalRunnerComputeWorkComponents(input, facts, parameters, compiled_admission, compiled_profile);
	compiled_profile.accelerated_runner_benefit =
	    MultiplyCost(compiled_profile.batches, compiled_profile.saved_work_per_batch);
	compiled_profile.startup_cost = PhysicalRunnerStartupCost(parameters);
	compiled_profile.required_benefit = PhysicalRunnerRequiredBenefit(compiled_profile, parameters);
	compiled_profile.net_benefit =
	    SubtractCost(compiled_profile.accelerated_runner_benefit, compiled_profile.startup_cost);

	auto gpu_parameters = PhysicalRunnerGpuCostParameters(parameters);
	auto gpu_admission = PhysicalRunnerEvaluateAdmission(input, facts, gpu_parameters);
	PhysicalRunnerCostProfile gpu_profile;
	PhysicalRunnerInitializeProfile(input, facts, gpu_admission, gpu_profile);
	PhysicalRunnerComputeWorkComponents(input, facts, gpu_parameters, gpu_admission, gpu_profile);
	gpu_profile.accelerated_runner_benefit = MultiplyCost(gpu_profile.batches, gpu_profile.saved_work_per_batch);
	gpu_profile.gpu_transfer_cost =
	    MultiplyCost(gpu_profile.batches, SaturatingCostCast(parameters.gpu_transfer_cost_per_batch));
	gpu_profile.startup_cost = PhysicalRunnerStartupCost(gpu_parameters);
	gpu_profile.required_benefit =
	    AddCost(PhysicalRunnerRequiredBenefit(gpu_profile, gpu_parameters), gpu_profile.gpu_transfer_cost);
	gpu_profile.net_benefit = SubtractCost(
	    SubtractCost(gpu_profile.accelerated_runner_benefit, gpu_profile.startup_cost), gpu_profile.gpu_transfer_cost);

	const auto compiled_parallel_per_batch_work_floor = PhysicalRunnerParallelPerBatchWorkFloor(parameters);
	auto compiled_selection = PhysicalRunnerAnalyzeSelection(input, compiled_admission, compiled_profile,
	                                                         parameters.compiled_vectorized_runner_available,
	                                                         compiled_parallel_per_batch_work_floor);
	auto gpu_selection =
	    PhysicalRunnerAnalyzeSelection(input, gpu_admission, gpu_profile, parameters.gpu_runner_available, 0);
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

const char *PhysicalRunnerCostInputScopeToString(PhysicalRunnerCostInputScope input_scope) {
	switch (input_scope) {
	case PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE:
		return "execution_region_candidate";
	case PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE:
		return "physical_pipeline";
	default:
		return "unknown";
	}
}

} // namespace duckdb
