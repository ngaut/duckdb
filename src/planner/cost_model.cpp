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
static constexpr int64_t NATIVE_JOIN_PROTOCOL_PENALTY = 720;
static constexpr int64_t NATIVE_HASH_JOIN_BUILD_SINK_PROTOCOL_PENALTY = 720;
static constexpr int64_t NATIVE_GROUPED_AGGREGATE_PARALLELISM_PENALTY = 160;
static constexpr int64_t MATERIALIZING_NATIVE_PROTOCOL_STAGE_MULTIPLIER = 2;
static constexpr int64_t UNKNOWN_OUTPUT_NATIVE_FUSION_MIN_COSTED_BATCHES = 1024;
static constexpr idx_t UNKNOWN_OUTPUT_HASH_BUILD_HIGH_EXPANSION_FACTOR = 2;
static constexpr idx_t LARGE_NATIVE_GROUPED_LOOKUP_INPUT_ROWS = 8 * 1024 * 1024;
static constexpr idx_t MIN_NATIVE_GROUPED_LOOKUP_EXPRESSION_COST = 64;
static constexpr idx_t MIN_GENERATED_GROUPED_UPDATE_EXPRESSION_COST = 40;
static constexpr idx_t LARGE_UNKNOWN_OUTPUT_LOOKUP_SOURCE_ROWS = 4 * 1024 * 1024;
static constexpr int64_t MIN_STATEFUL_BACKEND_COSTED_BATCHES = 32;

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

//! The axis-independent evaluation inputs. Shape facts and the amortization predicates
//! may depend only on these, which is what makes one set of shape facts valid for every
//! runner axis: the type system rules out accidental per-runner shape divergence.
struct PhysicalRunnerSharedCostInputs {
	idx_t source_contract_scan_filter_penalty = 4096;
	idx_t vectorized_parallelism = 1;
};

//! Everything the model may consult while pricing one runner axis.
struct PhysicalRunnerAxisEvaluation {
	const RunnerCostAxis &axis;
	const PhysicalRunnerSharedCostInputs &shared;
};

static int64_t MaterializingNativeProtocolPenalty(const RunnerCostAxis &axis) {
	return MultiplyCost(SaturatingCostCast(MATERIALIZING_NATIVE_PROTOCOL_STAGE_MULTIPLIER),
	                    SaturatingCostCast(axis.generated_stage_benefit));
}

static bool PhysicalRunnerHasGeneratedGroupedLookupReplacementProof(const PhysicalRunnerCostInput &input) {
	return input.materialization_elision_count > 0 && input.native_grouped_state_address_lookup_count > 0 &&
	       input.generated_grouped_aggregate_stage_count >= input.native_grouped_state_address_lookup_count &&
	       input.generated_backend_stage_count >= input.native_grouped_state_address_lookup_count &&
	       input.generated_stage_count >= input.native_grouped_state_address_lookup_count * 2;
}

static bool PhysicalRunnerHasEstimatedGroupedReduction(const PhysicalRunnerCostInput &input) {
	return PhysicalRunnerHasGeneratedGroupedLookupReplacementProof(input) &&
	       input.source_contract_input_cardinality > 0 && input.grouped_aggregate_estimated_cardinality > 0 &&
	       input.grouped_aggregate_estimated_cardinality <= input.source_contract_input_cardinality / 2;
}

static bool PhysicalRunnerHasGeneratedJoinGroupedFusionProof(const PhysicalRunnerCostInput &input) {
	return input.native_hash_join_build_sink_count == 0 && input.native_join_stage_count > 0 &&
	       input.native_grouped_state_address_lookup_count > 0 && input.generated_backend_stage_count >= 2 &&
	       input.generated_grouped_aggregate_stage_count >= input.native_grouped_state_address_lookup_count &&
	       input.generated_stage_count > input.native_grouped_state_address_lookup_count * 2;
}

static bool PhysicalRunnerLargeLowWorkGroupedUpdate(const PhysicalRunnerCostInput &input,
                                                    const PhysicalRunnerSharedCostInputs &shared) {
	const auto has_amortization_proof =
	    (shared.vectorized_parallelism > 1 ? PhysicalRunnerHasEstimatedGroupedReduction(input)
	                                       : PhysicalRunnerHasGeneratedGroupedLookupReplacementProof(input)) ||
	    PhysicalRunnerHasGeneratedJoinGroupedFusionProof(input);
	const auto has_native_grouped_lookup = input.native_grouped_state_address_lookup_count > 0;
	const auto has_grouped_update = has_native_grouped_lookup || input.generated_grouped_aggregate_stage_count > 0;
	const auto minimum_expression_cost = has_native_grouped_lookup ? MIN_NATIVE_GROUPED_LOOKUP_EXPRESSION_COST
	                                                               : MIN_GENERATED_GROUPED_UPDATE_EXPRESSION_COST;
	return has_grouped_update && input.source_contract_input_cardinality >= LARGE_NATIVE_GROUPED_LOOKUP_INPUT_ROWS &&
	       input.expression_cost < minimum_expression_cost && !has_amortization_proof;
}

static bool PhysicalRunnerCanAmortizeNativeGroupedStateAddressLookup(const PhysicalRunnerCostInput &input,
                                                                     const PhysicalRunnerSharedCostInputs &shared) {
	if (input.native_grouped_state_address_lookup_count == 0 || input.source_contract_input_cardinality == 0 ||
	    input.generated_backend_stage_count == 0 || PhysicalRunnerLargeLowWorkGroupedUpdate(input, shared)) {
		return false;
	}
	if (PhysicalRunnerHasEstimatedGroupedReduction(input)) {
		return true;
	}
	if (input.source_contract_output_cardinality_unknown) {
		if (shared.vectorized_parallelism > 1) {
			return PhysicalRunnerHasGeneratedJoinGroupedFusionProof(input);
		}
		if (input.native_hash_join_build_sink_count > 0) {
			return false;
		}
		if (input.finalized_dynamic_filter_cardinality_estimate) {
			return input.generated_stage_count > input.native_grouped_state_address_lookup_count * 2;
		}
		if (input.source_contract_input_cardinality >= LARGE_UNKNOWN_OUTPUT_LOOKUP_SOURCE_ROWS &&
		    input.estimated_cardinality <= input.source_contract_input_cardinality / 2) {
			return PhysicalRunnerHasGeneratedGroupedLookupReplacementProof(input) ||
			       PhysicalRunnerHasGeneratedJoinGroupedFusionProof(input) || input.native_join_stage_count >= 2;
		}
		return true;
	}
	// A complete grouped-lookup replacement is thread-local by contract. The planner cannot assume that
	// runtime local ranges will be disjoint, so only cost it as a full replacement in serial execution.
	return (shared.vectorized_parallelism == 1 && PhysicalRunnerHasGeneratedGroupedLookupReplacementProof(input)) ||
	       input.generated_stage_count > input.native_grouped_state_address_lookup_count * 2;
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

static bool PhysicalRunnerSmallStatefulBackendCandidate(const PhysicalRunnerCostInput &input) {
	return input.native_protocol_class == PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL &&
	       input.source_contract_input_cardinality > 0 &&
	       PhysicalRunnerBatches(SaturatingCostCast(input.source_contract_input_cardinality)) <
	           MIN_STATEFUL_BACKEND_COSTED_BATCHES;
}

struct PhysicalRunnerShapeFacts {
	int64_t rows = 1;
	int64_t batches = 1;
	int64_t costed_batches = 1;
	int64_t source_contract_input_rows = 0;
	int64_t source_contract_input_batches = 0;
	idx_t native_operator_stage_count = 0;
	bool source_contract_output_cardinality_unknown = false;
	bool has_generated_compute_work = false;
	bool no_native_sort = false;
};

static bool PhysicalRunnerHasSourceFilterSensitiveDownstreamWork(const PhysicalRunnerCostInput &input) {
	return input.generated_backend_stage_count > 0 || input.materialization_elision_count > 0 ||
	       input.native_join_stage_count > 0 || input.native_hash_join_build_sink_count > 0 ||
	       input.native_aggregate_stage_count > 0 || input.native_sort_stage_count > 0;
}

static bool PhysicalRunnerCanPriceUnknownSourceOutputGeneratedJoinFusion(const PhysicalRunnerCostInput &input,
                                                                         const PhysicalRunnerSharedCostInputs &shared) {
	if (input.source_contract_input_cardinality == 0 || !input.source_contract_output_cardinality_unknown ||
	    input.generated_backend_stage_count == 0 || input.native_join_stage_count == 0) {
		return false;
	}
	if (input.native_hash_join_build_sink_count > 0) {
		return false;
	}
	if (input.native_join_stage_count >= 3) {
		return true;
	}
	if (input.native_grouped_state_address_lookup_count == 0 && shared.vectorized_parallelism == 1) {
		return true;
	}
	return PhysicalRunnerCanAmortizeNativeGroupedStateAddressLookup(input, shared);
}

static bool
PhysicalRunnerCanCreditUnknownSourceOutputGeneratedJoinBackendStage(const PhysicalRunnerCostInput &input,
                                                                    const PhysicalRunnerSharedCostInputs &shared) {
	if (!PhysicalRunnerCanPriceUnknownSourceOutputGeneratedJoinFusion(input, shared) ||
	    input.native_hash_join_build_sink_count > 0) {
		return false;
	}
	if (input.native_join_stage_count >= 2) {
		return true;
	}
	return PhysicalRunnerHasGeneratedGroupedLookupReplacementProof(input) ||
	       PhysicalRunnerHasGeneratedJoinGroupedFusionProof(input);
}

static bool PhysicalRunnerUnknownOutputHashBuildSinkHasHighEstimatedExpansion(const PhysicalRunnerCostInput &input) {
	return input.native_hash_join_build_sink_count > 0 && input.source_contract_input_cardinality > 0 &&
	       input.estimated_cardinality / UNKNOWN_OUTPUT_HASH_BUILD_HIGH_EXPANSION_FACTOR >
	           input.source_contract_input_cardinality;
}

static bool PhysicalRunnerHasDirectGeneratedJoinProbeHashBuildPath(const PhysicalRunnerCostInput &input) {
	return input.native_join_stage_count == 1 && input.native_hash_join_build_sink_count == 1 &&
	       input.generated_backend_stage_count == 1 && input.native_aggregate_stage_count == 0 &&
	       input.native_sort_stage_count == 0;
}

static bool PhysicalRunnerUnknownFilteredSourceOutputCapsCostedBatches(const PhysicalRunnerCostInput &input,
                                                                       const PhysicalRunnerSharedCostInputs &shared,
                                                                       int64_t batches) {
	if (!input.uses_scan_filters || !input.source_contract_output_cardinality_unknown ||
	    !PhysicalRunnerHasSourceFilterSensitiveDownstreamWork(input)) {
		return false;
	}
	if (shared.source_contract_scan_filter_penalty == 0) {
		return false;
	}
	if (input.finalized_dynamic_filter_cardinality_estimate) {
		return false;
	}
	if (PhysicalRunnerCanPriceUnknownSourceOutputGeneratedJoinFusion(input, shared)) {
		return false;
	}
	if (input.materialization_elision_count > 0) {
		return false;
	}
	if (input.source_contract_input_cardinality == 0) {
		return true;
	}
	if (PhysicalRunnerUnknownOutputHashBuildSinkHasHighEstimatedExpansion(input)) {
		return true;
	}
	// This shape has one bounded probe and one terminal hash-build sink. There is
	// no intermediate operator that can multiply the unknown filtered output, so
	// the candidate estimate is a safe amortization bound. Keep the high-expansion
	// guard above for genuinely unstable build inputs.
	if (PhysicalRunnerHasDirectGeneratedJoinProbeHashBuildPath(input)) {
		return false;
	}
	if (PhysicalRunnerBatches(SaturatingCostCast(input.source_contract_input_cardinality)) <= batches) {
		return false;
	}
	return batches < UNKNOWN_OUTPUT_NATIVE_FUSION_MIN_COSTED_BATCHES;
}

static PhysicalRunnerShapeFacts PhysicalRunnerBuildShapeFacts(const PhysicalRunnerCostInput &input,
                                                              const PhysicalRunnerSharedCostInputs &shared) {
	PhysicalRunnerShapeFacts facts;
	facts.rows = PhysicalRunnerRows(input);
	facts.batches = PhysicalRunnerBatches(facts.rows);
	facts.costed_batches = facts.batches;
	if (PhysicalRunnerUnknownFilteredSourceOutputCapsCostedBatches(input, shared, facts.batches)) {
		facts.costed_batches = 1;
	}
	facts.source_contract_input_rows = SaturatingCostCast(input.source_contract_input_cardinality);
	facts.source_contract_input_batches = PhysicalRunnerBatches(facts.source_contract_input_rows);
	facts.source_contract_output_cardinality_unknown = input.source_contract_output_cardinality_unknown;
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

static bool PhysicalRunnerNativeStageBenefitCanPay(const PhysicalRunnerCostInput &input, const RunnerCostAxis &axis) {
	if (axis.native_operator_stage_benefit == 0) {
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
                                                              const PhysicalRunnerAxisEvaluation &evaluation) {
	if (evaluation.axis.materialization_elision_benefit == 0 || input.materialization_elision_count == 0) {
		return false;
	}
	if (PhysicalRunnerSmallStatefulBackendCandidate(input)) {
		return false;
	}
	if (input.source_contract_output_cardinality_unknown) {
		return false;
	}
	if (PhysicalRunnerLargeLowWorkGroupedUpdate(input, evaluation.shared)) {
		return false;
	}
	return true;
}

static bool PhysicalRunnerSmallFinalizedDynamicNativeAggregateTail(const PhysicalRunnerCostInput &input) {
	return input.finalized_dynamic_filter_cardinality_estimate && input.native_aggregate_stage_count > 0 &&
	       input.generated_grouped_aggregate_stage_count == 0 &&
	       input.estimated_cardinality <= STANDARD_VECTOR_SIZE * 2;
}

static bool PhysicalRunnerSmallFinalizedDynamicJoinOnlyTail(const PhysicalRunnerCostInput &input,
                                                            const RunnerCostAxis &axis) {
	return axis.startup_base_cost > 0 && input.finalized_dynamic_filter_cardinality_estimate &&
	       input.expression_cost == 0 && input.generated_backend_stage_count > 0 && input.native_join_stage_count > 0 &&
	       input.native_hash_join_build_sink_count == 0 && input.native_aggregate_stage_count == 0 &&
	       input.native_sort_stage_count == 0 && input.estimated_cardinality <= STANDARD_VECTOR_SIZE * 4;
}

static bool PhysicalRunnerGeneratedBackendStageBenefitCanPay(const PhysicalRunnerCostInput &input,
                                                             const PhysicalRunnerAxisEvaluation &evaluation) {
	if (input.generated_backend_stage_count == 0 || input.generated_stage_count == 0) {
		return false;
	}
	if (PhysicalRunnerSmallStatefulBackendCandidate(input)) {
		return false;
	}
	if (PhysicalRunnerSmallFinalizedDynamicNativeAggregateTail(input)) {
		return false;
	}
	if (PhysicalRunnerSmallFinalizedDynamicJoinOnlyTail(input, evaluation.axis)) {
		return false;
	}
	if (input.source_contract_output_cardinality_unknown &&
	    !PhysicalRunnerCanCreditUnknownSourceOutputGeneratedJoinBackendStage(input, evaluation.shared)) {
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

static bool PhysicalRunnerHasGeneratedExpressionOnlyPrefix(const PhysicalRunnerCostInput &input) {
	return PhysicalRunnerHasGeneratedComputePrefix(input) && input.generated_backend_stage_count == 0 &&
	       input.materialization_elision_count == 0 && input.native_join_stage_count == 0 &&
	       input.native_aggregate_stage_count == 0 && input.native_sort_stage_count == 0;
}

static bool PhysicalRunnerHashJoinBuildSinkProtocolPenaltyApplies(const PhysicalRunnerCostInput &input) {
	if (input.native_hash_join_build_sink_count == 0) {
		return false;
	}
	return PhysicalRunnerHasGeneratedExpressionOnlyPrefix(input) || !PhysicalRunnerHasGeneratedComputePrefix(input) ||
	       input.native_join_stage_count > 0 || input.native_aggregate_stage_count > 0 ||
	       input.native_sort_stage_count > 0;
}

static bool PhysicalRunnerHashJoinBuildSinkExpansionPenaltyApplies(const PhysicalRunnerCostInput &input,
                                                                   const PhysicalRunnerShapeFacts &facts) {
	if (input.native_hash_join_build_sink_count == 0 || facts.source_contract_input_rows <= 0) {
		return false;
	}
	return facts.rows > facts.source_contract_input_rows;
}

static int64_t PhysicalRunnerHashJoinBuildSinkProtocolPenalty(const PhysicalRunnerCostInput &input,
                                                              const PhysicalRunnerShapeFacts &facts,
                                                              const RunnerCostAxis &axis) {
	auto result = NATIVE_HASH_JOIN_BUILD_SINK_PROTOCOL_PENALTY;
	if (PhysicalRunnerHasGeneratedExpressionOnlyPrefix(input)) {
		result = AddCost(result, MaterializingNativeProtocolPenalty(axis));
	}
	if (PhysicalRunnerHashJoinBuildSinkExpansionPenaltyApplies(input, facts)) {
		result = AddCost(result, FractionalCost(SaturatingCostCast(axis.generated_stage_benefit), 1, 4));
	}
	return result;
}

static idx_t PhysicalRunnerCostedGeneratedBackendStageCount(const PhysicalRunnerCostInput &input,
                                                            const PhysicalRunnerAxisEvaluation &evaluation) {
	if (!PhysicalRunnerGeneratedBackendStageBenefitCanPay(input, evaluation)) {
		return 0;
	}
	return MinValue(input.generated_backend_stage_count, input.generated_stage_count);
}

static int64_t PhysicalRunnerSourceContractScanPenalty(const PhysicalRunnerCostInput &input,
                                                       const PhysicalRunnerShapeFacts &facts,
                                                       const PhysicalRunnerSharedCostInputs &shared) {
	if (!input.uses_scan_filters || input.source_contract_input_cardinality == 0) {
		return 0;
	}
	if (input.finalized_dynamic_filter_cardinality_estimate) {
		return 0;
	}
	// A generated filtered reduction consumes DuckDB's filtered source batches
	// directly and updates its terminal aggregate without publishing the
	// intermediate projection. Raw scan work is shared with the vectorized path;
	// charging it again would erase the materialization-elision benefit.
	if (input.full_pipeline && input.source_filter_count > 0 && input.generated_backend_stage_count > 0 &&
	    input.generated_grouped_aggregate_stage_count == 0 && input.materialization_elision_count > 0 &&
	    input.selected_hash_join_filter_materialization_count == 0 && input.native_join_stage_count == 0 &&
	    input.native_hash_join_build_sink_count == 0 && input.native_grouped_state_address_lookup_count == 0 &&
	    input.native_aggregate_stage_count == 0 && input.native_sort_stage_count == 0) {
		return 0;
	}
	// A single generated probe feeding a hash-build sink has no intermediate
	// operator chain to amplify scan-filter materialization. The native region
	// consumes each selected row directly, so charging the generic filtered-scan
	// penalty rejects a profitable full-pipeline fusion. Multi-join and aggregate
	// tails retain the guard because they have additional stateful protocol work.
	if (PhysicalRunnerHasDirectGeneratedJoinProbeHashBuildPath(input)) {
		return 0;
	}
	if (input.source_contract_output_cardinality_unknown && input.native_grouped_state_address_lookup_count == 0 &&
	    input.native_hash_join_build_sink_count == 0) {
		return 0;
	}
	if (PhysicalRunnerCanPriceUnknownSourceOutputGeneratedJoinFusion(input, shared)) {
		return 0;
	}
	if (facts.source_contract_input_batches <= facts.batches) {
		return 0;
	}
	if (!PhysicalRunnerHasSourceFilterSensitiveDownstreamWork(input)) {
		return 0;
	}
	const auto penalty_unit = SaturatingCostCast(shared.source_contract_scan_filter_penalty);
	if (penalty_unit <= 0) {
		return 0;
	}
	const auto extra_source_batches = facts.source_contract_input_batches - facts.batches;
	const auto total_penalty = MultiplyCost(extra_source_batches, penalty_unit);
	return total_penalty / facts.batches;
}

enum class PhysicalRunnerAdmissionClass : uint8_t {
	NONE,
	MATERIALIZATION_ELISION,
	FULL_PIPELINE,
	GENERATED,
	GENERATED_NATIVE_FUSION
};

//! The telemetry spelling of an admission class; profiles and reasons carry this string.
static const char *PhysicalRunnerAdmissionClassToString(PhysicalRunnerAdmissionClass admission_class) {
	switch (admission_class) {
	case PhysicalRunnerAdmissionClass::MATERIALIZATION_ELISION:
		return "materialization_elision";
	case PhysicalRunnerAdmissionClass::FULL_PIPELINE:
		return "full_pipeline";
	case PhysicalRunnerAdmissionClass::GENERATED:
		return "generated";
	case PhysicalRunnerAdmissionClass::GENERATED_NATIVE_FUSION:
		return "generated_native_fusion";
	default:
		return "none";
	}
}

struct PhysicalRunnerAdmission {
	PhysicalRunnerAdmissionClass admission_class = PhysicalRunnerAdmissionClass::NONE;
	bool full_pipeline_benefit_pays = false;
	bool native_operator_work_is_costed = false;
	string acceleration_basis;

	bool Admitted() const {
		return admission_class != PhysicalRunnerAdmissionClass::NONE;
	}
};

static PhysicalRunnerAdmissionClass PhysicalRunnerEvaluateAdmissionClass(const PhysicalRunnerCostInput &input,
                                                                         const PhysicalRunnerShapeFacts &facts,
                                                                         const PhysicalRunnerAxisEvaluation &evaluation,
                                                                         bool full_pipeline_benefit_pays) {
	if (!input.has_accelerated_work) {
		return PhysicalRunnerAdmissionClass::NONE;
	}
	if (PhysicalRunnerMaterializationElisionBenefitCanPay(input, evaluation)) {
		return PhysicalRunnerAdmissionClass::MATERIALIZATION_ELISION;
	}
	if (full_pipeline_benefit_pays) {
		return PhysicalRunnerAdmissionClass::FULL_PIPELINE;
	}
	if (facts.native_operator_stage_count == 0) {
		return facts.has_generated_compute_work && input.generated_stage_count > 0 &&
		               evaluation.axis.generated_stage_benefit > 0 &&
		               !PhysicalRunnerLargeLowWorkGroupedUpdate(input, evaluation.shared)
		           ? PhysicalRunnerAdmissionClass::GENERATED
		           : PhysicalRunnerAdmissionClass::NONE;
	}
	if (facts.has_generated_compute_work && input.generated_stage_count > 0 && facts.no_native_sort) {
		return PhysicalRunnerAdmissionClass::GENERATED_NATIVE_FUSION;
	}
	return PhysicalRunnerAdmissionClass::NONE;
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
                                                    const PhysicalRunnerAxisEvaluation &evaluation,
                                                    const PhysicalRunnerAdmission &admission) {
	if (!admission.Admitted() || !PhysicalRunnerNativeStageBenefitCanPay(input, evaluation.axis)) {
		return false;
	}
	if (PhysicalRunnerSmallFinalizedDynamicJoinOnlyTail(input, evaluation.axis)) {
		return false;
	}
	if (input.finalized_dynamic_filter_cardinality_estimate && input.expression_cost == 0 &&
	    input.native_hash_join_build_sink_count == 0 &&
	    !PhysicalRunnerGeneratedBackendStageBenefitCanPay(input, evaluation)) {
		return input.native_delim_join_sink || evaluation.axis.startup_base_cost == 0;
	}
	return true;
}

static void PhysicalRunnerBuildAccelerationBasis(const PhysicalRunnerCostInput &input,
                                                 const PhysicalRunnerShapeFacts &facts,
                                                 const PhysicalRunnerAxisEvaluation &evaluation,
                                                 PhysicalRunnerAdmission &admission) {
	if (admission.Admitted()) {
		PhysicalRunnerAppendReasonToken(admission.acceleration_basis,
		                                string("admission_class:") +
		                                    PhysicalRunnerAdmissionClassToString(admission.admission_class));
	}
	if (facts.has_generated_compute_work && input.generated_stage_count > 0 &&
	    evaluation.axis.generated_stage_benefit > 0 &&
	    !PhysicalRunnerLargeLowWorkGroupedUpdate(input, evaluation.shared)) {
		PhysicalRunnerAppendReasonToken(admission.acceleration_basis, "generated_stage_benefit");
	}
	if (facts.native_operator_stage_count > 0 &&
	    PhysicalRunnerNativeOperatorBenefitPays(input, evaluation, admission)) {
		PhysicalRunnerAppendReasonToken(admission.acceleration_basis, "native_operator_stage_benefit");
	}
	if (PhysicalRunnerMaterializationElisionBenefitCanPay(input, evaluation)) {
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
                                                               const PhysicalRunnerAxisEvaluation &evaluation) {
	PhysicalRunnerAdmission admission;
	admission.full_pipeline_benefit_pays =
	    evaluation.axis.full_pipeline_benefit > 0 && PhysicalRunnerFullPipelineBenefitPays(input, facts);
	admission.admission_class =
	    PhysicalRunnerEvaluateAdmissionClass(input, facts, evaluation, admission.full_pipeline_benefit_pays);
	admission.native_operator_work_is_costed = facts.native_operator_stage_count == 0 || admission.Admitted();
	PhysicalRunnerBuildAccelerationBasis(input, facts, evaluation, admission);
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
                                                const PhysicalRunnerAxisEvaluation &evaluation,
                                                const PhysicalRunnerAdmission &admission,
                                                PhysicalRunnerCostProfile &profile) {
	const auto expression_cost = SaturatingCostCast(input.expression_cost);
	if (facts.has_generated_compute_work && !PhysicalRunnerLargeLowWorkGroupedUpdate(input, evaluation.shared)) {
		profile.generated_expression_work = expression_cost;
		const auto generated_stage_benefit = SaturatingCostCast(evaluation.axis.generated_stage_benefit);
		const auto generated_backend_stage_count = PhysicalRunnerCostedGeneratedBackendStageCount(input, evaluation);
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
	D_ASSERT(input.generated_backend_stage_count >= input.generated_grouped_aggregate_stage_count);
	if (PhysicalRunnerNativeOperatorBenefitPays(input, evaluation, admission)) {
		const auto native_operator_stage_benefit = SaturatingCostCast(evaluation.axis.native_operator_stage_benefit);
		auto native_operator_stage_count = facts.native_operator_stage_count;
		if (PhysicalRunnerSmallFinalizedDynamicNativeAggregateTail(input)) {
			native_operator_stage_count -= input.native_aggregate_stage_count;
		}
		profile.native_operator_work =
		    MultiplyCost(SaturatingCostCast(native_operator_stage_count), native_operator_stage_benefit);
	}
	if (PhysicalRunnerMaterializationElisionBenefitCanPay(input, evaluation)) {
		profile.materialization_elision_work =
		    MultiplyCost(SaturatingCostCast(input.materialization_elision_count),
		                 SaturatingCostCast(evaluation.axis.materialization_elision_benefit));
	}
	if (input.selected_hash_join_filter_materialization_count > 0) {
		profile.selected_hash_join_filter_materialization_penalty =
		    MultiplyCost(SaturatingCostCast(input.selected_hash_join_filter_materialization_count),
		                 SaturatingCostCast(evaluation.axis.generated_stage_benefit));
	}
	if (input.full_pipeline) {
		const auto full_pipeline_benefit = SaturatingCostCast(evaluation.axis.full_pipeline_benefit);
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
		const auto hash_build_sink_penalty =
		    PhysicalRunnerHashJoinBuildSinkProtocolPenalty(input, facts, evaluation.axis);
		profile.stateful_protocol_penalty =
		    AddCost(profile.stateful_protocol_penalty,
		            MultiplyCost(native_hash_join_build_sink_count, hash_build_sink_penalty));
	}
	if (input.native_grouped_state_address_lookup_count > 0 &&
	    !PhysicalRunnerCanAmortizeNativeGroupedStateAddressLookup(input, evaluation.shared)) {
		const auto native_grouped_lookup_penalty = MaterializingNativeProtocolPenalty(evaluation.axis);
		profile.stateful_protocol_penalty =
		    AddCost(profile.stateful_protocol_penalty,
		            MultiplyCost(SaturatingCostCast(input.native_grouped_state_address_lookup_count),
		                         native_grouped_lookup_penalty));
	}
	profile.source_contract_scan_penalty = PhysicalRunnerSourceContractScanPenalty(input, facts, evaluation.shared);
	if (evaluation.shared.vectorized_parallelism > 1 && input.native_grouped_aggregate_stage_count > 0) {
		const auto grouped_aggregate_parallel_penalty =
		    MultiplyCost(SaturatingCostCast(input.native_grouped_aggregate_stage_count),
		                 SaturatingCostCast(evaluation.shared.vectorized_parallelism - 1));
		profile.stateful_protocol_penalty =
		    AddCost(profile.stateful_protocol_penalty,
		            MultiplyCost(grouped_aggregate_parallel_penalty, NATIVE_GROUPED_AGGREGATE_PARALLELISM_PENALTY));
	}
	if (evaluation.shared.vectorized_parallelism > 1 && input.generated_grouped_aggregate_stage_count > 0) {
		const auto grouped_aggregate_parallel_penalty =
		    MultiplyCost(SaturatingCostCast(input.generated_grouped_aggregate_stage_count),
		                 SaturatingCostCast(evaluation.shared.vectorized_parallelism - 1));
		profile.stateful_protocol_penalty =
		    AddCost(profile.stateful_protocol_penalty,
		            MultiplyCost(grouped_aggregate_parallel_penalty, NATIVE_GROUPED_AGGREGATE_PARALLELISM_PENALTY));
	}
	auto work = profile.generated_expression_work;
	work = AddCost(work, profile.generated_stage_work);
	work = AddCost(work, profile.native_operator_work);
	work = AddCost(work, profile.materialization_elision_work);
	work = AddCost(work, profile.full_pipeline_work);
	work = SubtractCost(work, profile.selected_hash_join_filter_materialization_penalty);
	work = SubtractCost(work, profile.stateful_protocol_penalty);
	work = SubtractCost(work, profile.source_contract_scan_penalty);
	profile.saved_work_per_batch = work;
}

static void PhysicalRunnerBuildRuntimeProofRequirements(PhysicalRunnerCostProfile &profile) {
	if (profile.generated_stage_work > 0) {
		profile.required_runtime_proofs |=
		    ExecutionRegionJitRuntimeProofBit(ExecutionRegionJitRuntimeProof::GENERATED_STAGE_WORK);
	}
	if (profile.generated_backend_stage_work > 0 || profile.native_operator_work > 0) {
		profile.required_runtime_proofs |=
		    ExecutionRegionJitRuntimeProofBit(ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK);
	}
	if (profile.materialization_elision_work > 0) {
		profile.required_runtime_proofs |=
		    ExecutionRegionJitRuntimeProofBit(ExecutionRegionJitRuntimeProof::MATERIALIZATION_ELISION);
	}
	if (profile.full_pipeline_work > 0) {
		profile.required_runtime_proofs |=
		    ExecutionRegionJitRuntimeProofBit(ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP);
	}
}

static int64_t PhysicalRunnerStartupCost(const PhysicalRunnerAxisEvaluation &evaluation) {
	return MultiplyCost(SaturatingCostCast(evaluation.axis.startup_base_cost),
	                    SaturatingCostCast(MaxValue<idx_t>(evaluation.shared.vectorized_parallelism, 1)));
}

static int64_t PhysicalRunnerRequiredBenefit(const PhysicalRunnerCostProfile &profile, const RunnerCostAxis &axis) {
	auto margin =
	    FractionalCost(profile.startup_cost, SaturatingCostCast(axis.startup_margin_basis_points), BASIS_POINT_SCALE);
	if (profile.costed_batches <= 1) {
		margin = AddCost(margin, profile.startup_cost);
	}
	auto required_benefit = AddCost(profile.startup_cost, margin);
	return required_benefit;
}

static int64_t PhysicalRunnerParallelPerBatchWorkFloor(const PhysicalRunnerAxisEvaluation &evaluation) {
	if (evaluation.shared.vectorized_parallelism <= 1) {
		return 0;
	}
	return MultiplyCost(SaturatingCostCast(evaluation.shared.vectorized_parallelism),
	                    SaturatingCostCast(evaluation.axis.native_operator_stage_benefit));
}

static bool PhysicalRunnerParallelPerBatchWorkFloorApplies(const PhysicalRunnerCostInput &input) {
	if (input.generated_backend_stage_count > 0) {
		return false;
	}
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
	profile.costed_batches = facts.costed_batches;
	profile.expression_cost = SaturatingCostCast(input.expression_cost);
	profile.source_contract_input_rows = facts.source_contract_input_rows;
	profile.source_contract_input_batches = facts.source_contract_input_batches;
	profile.source_contract_output_cardinality_unknown = facts.source_contract_output_cardinality_unknown;
	profile.generated_stage_count = SaturatingCostCast(input.generated_stage_count);
	profile.generated_backend_stage_count = SaturatingCostCast(input.generated_backend_stage_count);
	profile.generated_grouped_aggregate_stage_count = SaturatingCostCast(input.generated_grouped_aggregate_stage_count);
	profile.native_grouped_state_address_lookup_count =
	    SaturatingCostCast(input.native_grouped_state_address_lookup_count);
	profile.grouped_aggregate_estimated_cardinality = SaturatingCostCast(input.grouped_aggregate_estimated_cardinality);
	profile.materialization_elision_count = SaturatingCostCast(input.materialization_elision_count);
	profile.selected_hash_join_filter_materialization_count =
	    SaturatingCostCast(input.selected_hash_join_filter_materialization_count);
	profile.native_join_stage_count = SaturatingCostCast(input.native_join_stage_count);
	profile.native_hash_join_build_sink_count = SaturatingCostCast(input.native_hash_join_build_sink_count);
	profile.native_aggregate_stage_count = SaturatingCostCast(input.native_aggregate_stage_count);
	profile.native_grouped_aggregate_stage_count = SaturatingCostCast(input.native_grouped_aggregate_stage_count);
	profile.native_sort_stage_count = SaturatingCostCast(input.native_sort_stage_count);
	profile.source_filter_count = SaturatingCostCast(input.source_filter_count);
	profile.full_pipeline = input.full_pipeline;
	profile.generated_work_class = input.generated_work_class;
	profile.native_protocol_class = input.native_protocol_class;
	profile.admission_class = PhysicalRunnerAdmissionClassToString(admission.admission_class);
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
	if (input.vectorized_execution_preferred) {
		result.selection_reason = "rejected_vectorized_execution_preferred";
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
	constexpr auto AXIS_COUNT = PhysicalRunnerCostParameters::AXIS_COUNT;
	PhysicalRunnerSharedCostInputs shared;
	shared.source_contract_scan_filter_penalty = parameters.source_contract_scan_filter_penalty;
	shared.vectorized_parallelism = parameters.vectorized_parallelism;
	auto facts = PhysicalRunnerBuildShapeFacts(input, shared);

	PhysicalRunnerCostProfile axis_profiles[AXIS_COUNT];
	PhysicalRunnerSelectionAnalysis axis_selections[AXIS_COUNT];
	int64_t axis_transfer_costs[AXIS_COUNT] = {};
	for (idx_t axis_idx = 0; axis_idx < AXIS_COUNT; axis_idx++) {
		auto &axis = parameters.AxisAt(axis_idx);
		// The reference axis is costed even when unavailable so telemetry always reports
		// its hypothetical economics; any other unavailable axis is never evaluated and
		// its profile stays empty instead of carrying costs a backend cannot realize.
		if (axis_idx != 0 && !axis.available) {
			axis_selections[axis_idx].selection_reason = "rejected_runner_unavailable";
			continue;
		}
		PhysicalRunnerAxisEvaluation evaluation {axis, shared};
		auto &profile = axis_profiles[axis_idx];
		auto admission = PhysicalRunnerEvaluateAdmission(input, facts, evaluation);
		PhysicalRunnerInitializeProfile(input, facts, admission, profile);
		PhysicalRunnerComputeWorkComponents(input, facts, evaluation, admission, profile);
		PhysicalRunnerBuildRuntimeProofRequirements(profile);
		profile.accelerated_runner_benefit = MultiplyCost(profile.costed_batches, profile.saved_work_per_batch);
		auto &transfer_cost = axis_transfer_costs[axis_idx];
		transfer_cost = MultiplyCost(profile.batches, SaturatingCostCast(axis.transfer_cost_per_batch));
		profile.startup_cost = PhysicalRunnerStartupCost(evaluation);
		profile.required_benefit = AddCost(PhysicalRunnerRequiredBenefit(profile, axis), transfer_cost);
		profile.net_benefit =
		    SubtractCost(SubtractCost(profile.accelerated_runner_benefit, profile.startup_cost), transfer_cost);
		// The parallel per-batch work floor models the parallel vectorized baseline that
		// shares the host memory domain; it gates only the compiled-vectorized axis.
		const auto parallel_per_batch_work_floor =
		    axis_idx == 0 ? PhysicalRunnerParallelPerBatchWorkFloor(evaluation) : 0;
		axis_selections[axis_idx] =
		    PhysicalRunnerAnalyzeSelection(input, admission, profile, axis.available, parallel_per_batch_work_floor);
	}

	// The preferred axis provides the profile the caller sees when no runner is selected;
	// among selected axes the highest net benefit wins and ties keep the earlier axis.
	idx_t preferred_axis = 0;
	bool has_winner = false;
	idx_t winner_axis = 0;
	for (idx_t axis_idx = 0; axis_idx < AXIS_COUNT; axis_idx++) {
		auto &axis = parameters.AxisAt(axis_idx);
		if (axis_idx > 0 && axis.available &&
		    (!parameters.AxisAt(preferred_axis).available ||
		     axis_profiles[axis_idx].net_benefit > axis_profiles[preferred_axis].net_benefit)) {
			preferred_axis = axis_idx;
		}
		if (axis_selections[axis_idx].selected &&
		    (!has_winner || axis_profiles[axis_idx].net_benefit > axis_profiles[winner_axis].net_benefit)) {
			has_winner = true;
			winner_axis = axis_idx;
		}
	}

	PhysicalRunnerCostProfile profile = axis_profiles[has_winner ? winner_axis : preferred_axis];
	if (has_winner) {
		profile.selected_runner = PhysicalRunnerCostParameters::AxisRunner(winner_axis);
		profile.selection_reason = axis_selections[winner_axis].selection_reason;
	} else {
		profile.selection_reason = axis_selections[preferred_axis].selection_reason;
		for (idx_t axis_idx = 0; profile.selection_reason.empty() && axis_idx < AXIS_COUNT; axis_idx++) {
			profile.selection_reason = axis_selections[axis_idx].selection_reason;
		}
	}
	for (idx_t axis_idx = 0; axis_idx < AXIS_COUNT; axis_idx++) {
		auto &breakdown = profile.AxisAt(axis_idx);
		breakdown.available = parameters.AxisAt(axis_idx).available;
		breakdown.selected = axis_selections[axis_idx].selected;
		breakdown.selection_reason = axis_selections[axis_idx].selection_reason;
		breakdown.runner_benefit = axis_profiles[axis_idx].accelerated_runner_benefit;
		breakdown.transfer_cost = axis_transfer_costs[axis_idx];
		breakdown.startup_cost = axis_profiles[axis_idx].startup_cost;
		breakdown.required_benefit = axis_profiles[axis_idx].required_benefit;
		breakdown.net_benefit = axis_profiles[axis_idx].net_benefit;
	}
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
