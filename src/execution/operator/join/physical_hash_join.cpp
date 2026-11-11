#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/once_flag.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include <thread>
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"
#include "duckdb/planner/expression_iterator.hpp"

#include <chrono>
#include <cstring>

namespace duckdb {

static vector<bool> UnknownPhysicalOutputNotNull(const PhysicalOperator &op) {
	return vector<bool>(op.GetTypes().size(), false);
}

static vector<bool> BuildPhysicalOperatorOutputNotNull(const PhysicalOperator &op);

static vector<bool> BuildPhysicalTableScanOutputNotNull(const PhysicalTableScan &scan) {
	vector<bool> input_not_null(scan.column_ids.size(), false);
	if (StringUtil::Lower(scan.function.GetName().GetIdentifierName()) == "seq_scan" && scan.bind_data) {
		auto &bind_data = scan.bind_data->Cast<TableScanBindData>();
		for (auto &constraint : bind_data.table.GetConstraints()) {
			if (constraint->type != ConstraintType::NOT_NULL) {
				continue;
			}
			auto &not_null = constraint->Cast<NotNullConstraint>();
			for (idx_t column_idx = 0; column_idx < scan.column_ids.size(); column_idx++) {
				auto &column_id = scan.column_ids[column_idx];
				if (column_id.HasPrimaryIndex() && column_id.GetPrimaryIndex() == not_null.index.index) {
					input_not_null[column_idx] = true;
				}
			}
		}
	}

	vector<bool> result;
	result.reserve(scan.GetTypes().size());
	if (!scan.projection_ids.empty()) {
		for (auto projection_id : scan.projection_ids) {
			result.push_back(projection_id < input_not_null.size() ? input_not_null[projection_id] : false);
		}
		return result;
	}
	for (idx_t output_idx = 0; output_idx < scan.GetTypes().size(); output_idx++) {
		result.push_back(output_idx < input_not_null.size() ? input_not_null[output_idx] : false);
	}
	return result;
}

static bool PhysicalExpressionResultNotNull(const Expression &expr, const vector<bool> &input_not_null) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONSTANT:
		return !expr.Cast<BoundConstantExpression>().GetValue().IsNull();
	case ExpressionClass::BOUND_REF: {
		auto source_index = expr.Cast<BoundReferenceExpression>().Index();
		return source_index < input_not_null.size() && input_not_null[source_index];
	}
	default:
		return false;
	}
}

static vector<bool> BuildPhysicalProjectionOutputNotNull(const PhysicalProjection &projection) {
	if (projection.children.size() != 1) {
		return UnknownPhysicalOutputNotNull(projection);
	}
	auto input_not_null = BuildPhysicalOperatorOutputNotNull(projection.children[0].get());
	vector<bool> result;
	result.reserve(projection.select_list.size());
	for (auto &expr : projection.select_list) {
		result.push_back(PhysicalExpressionResultNotNull(*expr, input_not_null));
	}
	return result;
}

static vector<bool> BuildPhysicalOperatorOutputNotNull(const PhysicalOperator &op) {
	switch (op.type) {
	case PhysicalOperatorType::TABLE_SCAN:
		return BuildPhysicalTableScanOutputNotNull(op.Cast<PhysicalTableScan>());
	case PhysicalOperatorType::FILTER:
		if (op.children.size() == 1) {
			return BuildPhysicalOperatorOutputNotNull(op.children[0].get());
		}
		return UnknownPhysicalOutputNotNull(op);
	case PhysicalOperatorType::PROJECTION:
		return BuildPhysicalProjectionOutputNotNull(op.Cast<PhysicalProjection>());
	default:
		return UnknownPhysicalOutputNotNull(op);
	}
}

PhysicalHashJoin::PhysicalHashJoin(PhysicalPlan &physical_plan, LogicalOperator &op, PhysicalOperator &left,
                                   PhysicalOperator &right, vector<JoinCondition> conds, JoinType join_type,
                                   const vector<ProjectionIndex> &left_projection_map,
                                   const vector<ProjectionIndex> &right_projection_map, vector<LogicalType> delim_types,
                                   idx_t estimated_cardinality, unique_ptr<JoinFilterPushdownInfo> pushdown_info_p)
    : PhysicalComparisonJoin(physical_plan, op, PhysicalOperatorType::HASH_JOIN, std::move(conds), join_type,
                             estimated_cardinality),
      delim_types(std::move(delim_types)) {
	D_ASSERT(join_type != JoinType::MARK || !predicate);
	filter_pushdown = std::move(pushdown_info_p);

	children.push_back(left);
	children.push_back(right);

	auto &lhs_input_types = children[0].get().GetTypes();
	auto &rhs_input_types = children[1].get().GetTypes();
	auto lhs_input_not_null = BuildPhysicalOperatorOutputNotNull(children[0].get());
	auto rhs_input_not_null = BuildPhysicalOperatorOutputNotNull(children[1].get());

	for (auto &condition : conditions) {
		D_ASSERT(condition.IsComparison());
		condition_types.push_back(condition.GetLHS().GetReturnType());
	}

	vector<idx_t> probe_cols;
	vector<idx_t> build_cols;

	if (predicate) {
		ExtractResidualPredicateColumns(predicate, lhs_input_types.size(), probe_cols, build_cols);
		residual_info = make_uniq<ResidualPredicateInfo>();
	}

	// build lhs_output_columns
	lhs_output_columns.col_idxs = FillProjectionMap(left, left_projection_map);
	for (auto &lhs_col : lhs_output_columns.col_idxs) {
		lhs_output_columns.col_types.push_back(lhs_input_types[lhs_col]);
	}

	// initialize residual predicate structures if present
	if (residual_info) {
		InitializeResidualPredicate(lhs_input_types, lhs_input_not_null, probe_cols);
	} else {
		// lhs_probe_columns = lhs_output_columns
		lhs_probe_columns = lhs_output_columns;
		lhs_output_in_probe.reserve(lhs_output_columns.col_idxs.size());
		for (idx_t i = 0; i < lhs_output_columns.col_idxs.size(); i++) {
			lhs_output_in_probe.push_back(i);
		}
	}

	// store probe types
	if (residual_info) {
		residual_info->probe_types = lhs_probe_columns.col_types;
	}

	// handle build side (RHS)
	InitializeBuildSide(lhs_input_types, rhs_input_types, rhs_input_not_null, right_projection_map, build_cols);
}

PhysicalHashJoin::PhysicalHashJoin(PhysicalPlan &physical_plan, LogicalOperator &op, PhysicalOperator &left,
                                   PhysicalOperator &right, vector<JoinCondition> cond, JoinType join_type,
                                   idx_t estimated_cardinality)
    : PhysicalHashJoin(physical_plan, op, left, right, std::move(cond), join_type, {}, {}, {}, estimated_cardinality,
                       nullptr) {
}

void PhysicalHashJoin::ExtractResidualPredicateColumns(unique_ptr<Expression> &predicate, idx_t probe_column_count,
                                                       vector<idx_t> &probe_column_ids,
                                                       vector<idx_t> &build_column_ids) {
	unordered_set<idx_t> probe_cols;
	unordered_set<idx_t> build_cols;

	ExpressionIterator::EnumerateExpression(predicate, [&](unique_ptr<Expression> &expr) {
		if (expr->GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto &ref = expr->Cast<BoundReferenceExpression>();
			idx_t col_idx = ref.Index();

			if (col_idx < probe_column_count) {
				// Probe (LHS) column
				probe_cols.insert(col_idx);
			} else {
				// Build (RHS) column
				build_cols.insert(col_idx);
			}
		}
	});

	probe_column_ids.assign(probe_cols.begin(), probe_cols.end());
	build_column_ids.assign(build_cols.begin(), build_cols.end());
}

void PhysicalHashJoin::InitializeResidualPredicate(const vector<LogicalType> &lhs_input_types,
                                                   const vector<bool> &lhs_input_not_null,
                                                   const vector<idx_t> &probe_cols) {
	D_ASSERT(residual_info);
	// build lhs_probe_columns (output + predicate columns)
	unordered_set<idx_t> required_probe_cols;
	for (auto col : lhs_output_columns.col_idxs) {
		required_probe_cols.insert(col);
	}
	for (auto col : probe_cols) {
		required_probe_cols.insert(col);
	}

	lhs_probe_columns.col_idxs.assign(required_probe_cols.begin(), required_probe_cols.end());
	std::sort(lhs_probe_columns.col_idxs.begin(), lhs_probe_columns.col_idxs.end());

	for (auto col_idx : lhs_probe_columns.col_idxs) {
		lhs_probe_columns.col_types.push_back(lhs_input_types[col_idx]);
	}

	// build mapping for predicate probe columns
	for (auto predicate_col_idx : probe_cols) {
		for (idx_t i = 0; i < lhs_probe_columns.col_idxs.size(); i++) {
			if (lhs_probe_columns.col_idxs[i] == predicate_col_idx) {
				residual_info->probe_input_to_probe_map[predicate_col_idx] = i;
				residual_info->probe_input_not_null_map[predicate_col_idx] =
				    predicate_col_idx < lhs_input_not_null.size() && lhs_input_not_null[predicate_col_idx];
				break;
			}
		}
	}

	// build lhs_output_in_probe mapping
	lhs_output_in_probe.reserve(lhs_output_columns.col_idxs.size());
	for (auto output_col_idx : lhs_output_columns.col_idxs) {
		for (idx_t i = 0; i < lhs_probe_columns.col_idxs.size(); i++) {
			if (lhs_probe_columns.col_idxs[i] == output_col_idx) {
				lhs_output_in_probe.push_back(i);
				break;
			}
		}
	}
}

void PhysicalHashJoin::InitializeBuildSide(const vector<LogicalType> &lhs_input_types,
                                           const vector<LogicalType> &rhs_input_types,
                                           const vector<bool> &rhs_input_not_null,
                                           const vector<ProjectionIndex> &right_projection_map,
                                           const vector<idx_t> &build_cols) {
	unordered_map<idx_t, idx_t> build_columns_in_conditions;
	unordered_map<idx_t, idx_t> build_input_to_layout;

	// Only consider comparison conditions for the hash join conditions
	idx_t cond_idx = 0;
	for (auto &condition : conditions) {
		if (condition.GetRHS().GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto build_input_idx = condition.GetRHS().Cast<BoundReferenceExpression>().Index();
			build_columns_in_conditions.emplace(build_input_idx, cond_idx);
		}
		cond_idx++;
	}

	// handle SEMI/ANTI/MARK joins
	if (join_type == JoinType::ANTI || join_type == JoinType::SEMI || join_type == JoinType::MARK) {
		MapResidualBuildColumns(lhs_input_types, rhs_input_types, rhs_input_not_null, build_cols,
		                        build_columns_in_conditions, build_input_to_layout);

		if (residual_info) {
			residual_info->build_input_to_layout_map = std::move(build_input_to_layout);
		}
		return;
	}

	// for other join types
	auto right_projection_map_copy = FillProjectionMap(children[1].get(), right_projection_map);

	// map ALL predicate columns (both in conditions and not)
	MapResidualBuildColumns(lhs_input_types, rhs_input_types, rhs_input_not_null, build_cols,
	                        build_columns_in_conditions, build_input_to_layout);

	// build rhs_output_columns
	for (auto &rhs_col : right_projection_map_copy) {
		auto &rhs_col_type = rhs_input_types[rhs_col];
		idx_t rhs_col_with_offset = lhs_input_types.size() + rhs_col;

		auto it = build_columns_in_conditions.find(rhs_col);
		if (it != build_columns_in_conditions.end()) {
			// it's in join conditions
			rhs_output_columns.col_idxs.push_back(it->second);
		} else {
			// check if already in payload (from predicate)
			auto layout_it = build_input_to_layout.find(rhs_col_with_offset);
			if (layout_it != build_input_to_layout.end()) {
				rhs_output_columns.col_idxs.push_back(layout_it->second);
			} else {
				// new column - add to payload
				idx_t layout_pos = condition_types.size() + payload_columns.col_idxs.size();
				payload_columns.col_idxs.push_back(rhs_col);
				payload_columns.col_types.push_back(rhs_col_type);
				rhs_output_columns.col_idxs.push_back(layout_pos);
			}
		}
		rhs_output_columns.col_types.push_back(rhs_col_type);
	}

	if (residual_info) {
		residual_info->build_input_to_layout_map = std::move(build_input_to_layout);
	}
}

void PhysicalHashJoin::MapResidualBuildColumns(const vector<LogicalType> &lhs_input_types,
                                               const vector<LogicalType> &rhs_input_types,
                                               const vector<bool> &rhs_input_not_null, const vector<idx_t> &build_cols,
                                               const unordered_map<idx_t, idx_t> &build_columns_in_conditions,
                                               unordered_map<idx_t, idx_t> &build_input_to_layout) {
	if (!residual_info) {
		return;
	}

	for (auto rhs_idx_with_offset : build_cols) {
		idx_t rhs_idx = rhs_idx_with_offset - lhs_input_types.size();
		residual_info->build_input_not_null_map[rhs_idx_with_offset] =
		    rhs_idx < rhs_input_not_null.size() && rhs_input_not_null[rhs_idx];
		auto it = build_columns_in_conditions.find(rhs_idx);

		if (it != build_columns_in_conditions.end()) {
			// column IS in conditions
			build_input_to_layout[rhs_idx_with_offset] = it->second;
		} else {
			// column NOT in conditions - add to payload
			idx_t layout_pos = condition_types.size() + payload_columns.col_idxs.size();
			build_input_to_layout[rhs_idx_with_offset] = layout_pos;
			payload_columns.col_idxs.push_back(rhs_idx);
			payload_columns.col_types.push_back(rhs_input_types[rhs_idx]);
		}
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
JoinFilterGlobalState::~JoinFilterGlobalState() {
}

JoinFilterLocalState::~JoinFilterLocalState() {
}

static bool CanUsePerfectHashJoin(const PhysicalHashJoin &op, PerfectHashJoinExecutor &perfect_join_executor) {
	if (op.conditions.size() != 1 || !op.conditions[0].GetRightStats()) {
		return false;
	}
	const auto &right_stats = *op.conditions[0].GetRightStats();
	if (!TypeIsIntegral(right_stats.GetType().InternalType()) || !NumericStats::HasMinMax(right_stats)) {
		return false;
	}
	return perfect_join_executor.CanDoPerfectHashJoin(op, NumericStats::Min(right_stats),
	                                                  NumericStats::Max(right_stats));
}

struct PerfectHashJoinBuildBounds {
	LogicalType key_type;
	PhysicalType key_physical_type = PhysicalType::INVALID;
	bool initialized = false;
	bool has_value = false;
	int64_t min = 0;
	int64_t max = 0;
	hugeint_t min_128;
	hugeint_t max_128;
	uhugeint_t min_u128;
	uhugeint_t max_u128;

	void Initialize(const LogicalType &type) {
		key_type = type;
		key_physical_type = type.InternalType();
		initialized = true;
		has_value = false;
		min = 0;
		max = 0;
		min_128 = hugeint_t();
		max_128 = hugeint_t();
		min_u128 = uhugeint_t();
		max_u128 = uhugeint_t();
	}

	void Update(int64_t value) {
		if (!has_value) {
			min = value;
			max = value;
			has_value = true;
			return;
		}
		min = MinValue(min, value);
		max = MaxValue(max, value);
	}

	void Update(hugeint_t value) {
		if (!has_value) {
			min_128 = value;
			max_128 = value;
			has_value = true;
			return;
		}
		if (Hugeint::LessThan(value, min_128)) {
			min_128 = value;
		}
		if (Hugeint::GreaterThan(value, max_128)) {
			max_128 = value;
		}
	}

	void Update(uhugeint_t value) {
		if (!has_value) {
			min_u128 = value;
			max_u128 = value;
			has_value = true;
			return;
		}
		if (Uhugeint::LessThan(value, min_u128)) {
			min_u128 = value;
		}
		if (Uhugeint::GreaterThan(value, max_u128)) {
			max_u128 = value;
		}
	}

	void Combine(const PerfectHashJoinBuildBounds &other) {
		if (!other.has_value) {
			return;
		}
		D_ASSERT(initialized && other.initialized);
		switch (key_physical_type) {
		case PhysicalType::INT128:
			Update(other.min_128);
			Update(other.max_128);
			break;
		case PhysicalType::UINT128:
			Update(other.min_u128);
			Update(other.max_u128);
			break;
		default:
			Update(other.min);
			Update(other.max);
			break;
		}
	}

	Value Min() const {
		D_ASSERT(initialized && has_value);
		if (key_physical_type == PhysicalType::INT128) {
			return Value::HUGEINT(min_128);
		}
		if (key_physical_type == PhysicalType::UINT128) {
			return Value::UHUGEINT(min_u128);
		}
		return Value::Numeric(key_type, min);
	}

	Value Max() const {
		D_ASSERT(initialized && has_value);
		if (key_physical_type == PhysicalType::INT128) {
			return Value::HUGEINT(max_128);
		}
		if (key_physical_type == PhysicalType::UINT128) {
			return Value::UHUGEINT(max_u128);
		}
		return Value::Numeric(key_type, max);
	}
};

static bool CanCollectPerfectHashJoinBuildBounds(const PhysicalHashJoin &op) {
	if (op.predicate || op.join_type != JoinType::INNER || op.conditions.size() != 1 ||
	    op.conditions[0].GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	switch (op.condition_types[0].InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
		break;
	default:
		return false;
	}
	for (auto &type : op.children[1].get().GetTypes()) {
		switch (type.InternalType()) {
		case PhysicalType::STRUCT:
		case PhysicalType::LIST:
		case PhysicalType::ARRAY:
			return false;
		default:
			break;
		}
	}
	return true;
}

template <class T>
static void UpdatePerfectHashJoinBuildBounds(UnifiedVectorFormat &format, idx_t count,
                                             PerfectHashJoinBuildBounds &bounds) {
	auto data = UnifiedVectorFormat::GetData<T>(format);
	auto all_valid = format.validity.CannotHaveNull();
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto source_idx = format.sel->get_index(row_idx);
		if (!all_valid && !format.validity.RowIsValid(source_idx)) {
			continue;
		}
		bounds.Update(data[source_idx]);
	}
}

static void UpdatePerfectHashJoinBuildBounds(Vector &keys, idx_t count, PerfectHashJoinBuildBounds &bounds) {
	if (count == 0) {
		return;
	}
	UnifiedVectorFormat format;
	keys.ToUnifiedFormat(format);
	switch (keys.GetType().InternalType()) {
	case PhysicalType::INT8:
		UpdatePerfectHashJoinBuildBounds<int8_t>(format, count, bounds);
		break;
	case PhysicalType::INT16:
		UpdatePerfectHashJoinBuildBounds<int16_t>(format, count, bounds);
		break;
	case PhysicalType::INT32:
		UpdatePerfectHashJoinBuildBounds<int32_t>(format, count, bounds);
		break;
	case PhysicalType::INT64:
		UpdatePerfectHashJoinBuildBounds<int64_t>(format, count, bounds);
		break;
	case PhysicalType::INT128:
		UpdatePerfectHashJoinBuildBounds<hugeint_t>(format, count, bounds);
		break;
	case PhysicalType::UINT128:
		UpdatePerfectHashJoinBuildBounds<uhugeint_t>(format, count, bounds);
		break;
	default:
		throw InternalException("perfect hash join build bounds received unsupported key type");
	}
}

unique_ptr<JoinFilterGlobalState> JoinFilterPushdownInfo::GetGlobalState(ClientContext &context,
                                                                         const PhysicalOperator &op) const {
	// clear any previously set filters
	// we can have previous filters for this operator in case of e.g. recursive CTEs
	for (auto &info : probe_info) {
		info.dynamic_filters->ClearFilters(op);
	}
	auto result = make_uniq<JoinFilterGlobalState>();
	result->global_aggregate_state =
	    make_uniq<GlobalUngroupedAggregateState>(BufferAllocator::Get(context), min_max_aggregates);
	return result;
}

//! Synchronisation state for the first-chunk publication of the TupleDataLayout: the canonical layout
//! shared by all per-thread JHTs, plus the per-column decision on whether to narrow the row-store slot.
struct LayoutGate {
	//! Recreated by Reset(): the flag must be re-armed for the next recursive CTE iteration
	unique_ptr<OnceFlag> publish_once = make_uniq<OnceFlag>();
	//! Set once layout_ptr and dict_info are published, for readers outside the publishing CallOnce (the execution
	//! contract plans against the published layout)
	atomic<bool> published {false};
	shared_ptr<TupleDataLayout> layout_ptr;
	//! Per narrowed build column: slot width, owned dictionary (+ NULL slot) and upstream dictionary id; shared by all
	//! threads, nullptr when no column is narrowed
	shared_ptr<const DictSurvivingInfo> dict_info;

	void Reset() {
		publish_once = make_uniq<OnceFlag>();
		published.store(false, std::memory_order_release);
		layout_ptr.reset();
		dict_info.reset();
	}
};

static idx_t GetBuildSpaceRequirement(ClientContext &context, const JoinHashTable &ht, idx_t num_threads);

//! Building the bloom filter is costly on the build side to make probing faster,
//! so only use it if there are less build tuples than probing tuples
static constexpr double BUILD_TO_PROBE_RATIO_THRESHOLD = 1.0;
//! If we have a build side without a filter, only build the bloom filter if the ratio/size is small enough
static constexpr double NON_FILTERING_RATIO_THRESHOLD = 0.1;
static constexpr idx_t NON_FILTERING_BUILD_SIDE_THRESHOLD = 4194304;
//! A join is expected to stay in memory while its estimated build side is below this share of the memory budget
static constexpr idx_t LIKELY_IN_MEMORY_BUILD_MEMORY_FRACTION = 4;

//! Whether the build side is expected to exceed the memory budget and spill
static bool JoinIsLikelyExternal(ClientContext &context, const PhysicalHashJoin &op) {
	if (Settings::Get<DebugForceExternalSetting>(context)) {
		return true;
	}
	idx_t row_width = sizeof(data_ptr_t) + sizeof(hash_t); // per-row pointer table and hash overhead
	for (auto &type : op.children[1].get().GetTypes()) {
		row_width += GetTypeIdSize(type.InternalType());
	}
	const auto build_size = op.children[1].get().estimated_cardinality * row_width;
	const auto memory_budget = BufferManager::GetBufferManager(context).GetMaxMemory();
	return build_size > memory_budget / LIKELY_IN_MEMORY_BUILD_MEMORY_FRACTION;
}

//! Whether to accumulate a bloom filter during Sink. Only an external join needs that: its finalize only ever
//! materializes some partitions, so a filter built there would be incomplete, while every row passes through
//! Sink. An in-memory join builds its filter during finalize instead, where the actual build count and the
//! competing filter choices are known. The estimate-based checks mirror CanUseBloomFilter, which decides
//! publication once the actual build count is known.
static bool ShouldBuildBloomFilterInSink(ClientContext &context, const PhysicalHashJoin &op) {
	if (!op.filter_pushdown || op.filter_pushdown->probe_info.empty()) {
		return false;
	}
	idx_t equality_count = 0;
	for (auto &cond : op.conditions) {
		const auto cmp = cond.GetComparisonType();
		if (cmp == ExpressionType::COMPARE_EQUAL || cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			equality_count++;
		}
	}
	if (equality_count != 1) {
		return false;
	}
	const auto build_estimate = static_cast<double>(op.children[1].get().estimated_cardinality);
	const auto probe_estimate = static_cast<double>(op.children[0].get().estimated_cardinality);
	if (build_estimate > BUILD_TO_PROBE_RATIO_THRESHOLD * probe_estimate) {
		return false;
	}
	if (!op.filter_pushdown->build_side_has_filter && build_estimate > NON_FILTERING_RATIO_THRESHOLD * probe_estimate &&
	    build_estimate > NON_FILTERING_BUILD_SIDE_THRESHOLD) {
		return false;
	}
	return JoinIsLikelyExternal(context, op);
}

class HashJoinGlobalSinkState : public GlobalSinkState {
public:
	HashJoinGlobalSinkState(const PhysicalHashJoin &op_p, ClientContext &context_p)
	    : context(context_p), op(op_p), num_threads(TaskScheduler::GetScheduler(context).NumberOfThreads()),
	      temporary_memory_state(TemporaryMemoryManager::Get(context.GetContext()).Register(context.GetContext())),
	      finalized(false), active_local_states(0), total_size(0), max_partition_size(0), max_partition_count(0),
	      probe_side_requirement(0), scanned_data(false) {
		// Start with sixteen partitions; a repartition derives its fanout from the local tables' radix bits
		hash_table = op.InitializeHashTable(context, 4);
		// Reserve the initial partition buffers before upstream hash tables are finalized.
		auto minimum_reservation = MinValue(temporary_memory_state->GetMinimumReservation(),
		                                    GetBuildSpaceRequirement(context, *hash_table, num_threads));
		temporary_memory_state->SetMinimumReservation(minimum_reservation);
		temporary_memory_state->SetRemainingSize(minimum_reservation);
		temporary_memory_state->UpdateReservation(context.GetContext());

		// Set probe types
		probe_types = op.children[0].get().GetTypes();
		probe_types.emplace_back(LogicalType::HASH);

		InitializePerExecutionState(context);
	}

	//! Establishes the per-execution decisions, both at construction and at a recursive iteration reset:
	//! the perfect-hash candidate, filter-pushdown participation, and the sink bloom filter.
	void InitializePerExecutionState(ClientContext &context, shared_ptr<BloomFilter> recycled_filter = nullptr) {
		perfect_join_executor = make_uniq<PerfectHashJoinExecutor>(op, *hash_table);
		can_use_perfect_hash = CanUsePerfectHashJoin(op, *perfect_join_executor);
		collect_perfect_join_bounds = CanCollectPerfectHashJoinBuildBounds(op);
		perfect_join_bounds = PerfectHashJoinBuildBounds();
		if (collect_perfect_join_bounds) {
			perfect_join_bounds.Initialize(op.condition_types[0]);
		}
		external = Settings::Get<DebugForceExternalSetting>(context);
		skip_filter_pushdown = false;
		global_filter_state.reset();
		if (op.filter_pushdown) {
			if (op.filter_pushdown->probe_info.empty() && can_use_perfect_hash) {
				// Only computing min/max to check for perfect HJ, but we already can
				skip_filter_pushdown = true;
			}
			global_filter_state = op.filter_pushdown->GetGlobalState(context, op);
		}
		InitializeBloomFilter(std::move(recycled_filter));
	}

	//! Creates the bloom filter that every thread-local hash table inserts into during Sink.
	//! A filter recycled from the previous recursive iteration is reused unless it was published.
	void InitializeBloomFilter(shared_ptr<BloomFilter> recycled_filter = nullptr) {
		// a published filter is shared with probe-side scans and immutable, so it cannot be reused
		if (recycled_filter && recycled_filter->IsPublished()) {
			recycled_filter.reset();
		}
		// a perfect hash join publishes its own exact filter, so a bloom filter would never leave the sink
		if (skip_filter_pushdown || can_use_perfect_hash || !ShouldBuildBloomFilterInSink(context, op)) {
			hash_table->SetBloomFilter(nullptr);
			// an in-memory join builds its filter during finalize, which can still reuse the allocation;
			// an iteration without a filter must not clear a slot that finalize has not consumed yet
			if (recycled_filter) {
				hash_table->SetRecycledBloomFilter(std::move(recycled_filter));
			}
			return;
		}
		if (!recycled_filter) {
			recycled_filter = make_shared_ptr<BloomFilter>();
		}
		recycled_filter->Initialize(context, MaxValue<idx_t>(op.children[1].get().estimated_cardinality, 1));
		hash_table->SetBloomFilter(std::move(recycled_filter));
	}

	~HashJoinGlobalSinkState() override {
		DUCKDB_LOG(context, PhysicalOperatorLogType, op, "PhysicalHashJoin", "GetData",
		           {{"total_probe_matches", to_string(hash_table->total_probe_matches)}});
	}

	void ScheduleFinalize(Pipeline &pipeline, Event &event);
	void InitializeProbeSpill();
	void UpdateProbeSideRequirement();
	//! First-chunk election: build the canonical layout (with per-column slot narrowing) and publish it.
	//! Idempotent and safe to call concurrently; only the first thread runs the slow path.
	void PublishLayoutIfFirst(class HashJoinLocalSinkState &lstate, DataChunk &payload_chunk);
	//! True iff at least one column on the global JHT carries a pinned upstream dictionary entry.
	bool DictSurvivingActive() const;

	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ClientContext &context) override {
		// Keep the previous iteration's bloom filter so its allocation can be reused.
		auto recycled_bloom_filter = hash_table->GetBloomFilter();
		// Use single-partition mode on subsequent iterations to avoid 16-partition overhead for small joins.
		hash_table->ResetForNewIterationSinglePartition();
		finalized = false;
		active_local_states = 0;
		total_size = 0;
		max_partition_size = 0;
		max_partition_count = 0;
		probe_side_requirement = 0;
		local_hash_tables.clear();
		owned_local_hash_tables.clear();
		probe_spill.reset();
		scanned_data = false;
		preserve_build_for_reuse = false;
		temporary_memory_state->SetZero();
		keep_local_hash_tables = true;
		InitializePerExecutionState(context, std::move(recycled_bloom_filter));
		// The next iteration's first chunk publishes the layout afresh: the rerun build may deliver a different
		// dictionary (or none), so neither a narrowed slot nor the dictionary it indexes into can be carried over.
		// ResetForNewIterationSinglePartition returned the global JHT to the unpublished state accordingly.
		layout_gate.Reset();
		GlobalSinkState::Reset(context);
	}

public:
	ClientContext &context;
	const PhysicalHashJoin &op;

	const idx_t num_threads;
	//! Temporary memory state for managing this operator's memory usage
	unique_ptr<TemporaryMemoryState> temporary_memory_state;

	//! Global HT used by the join
	unique_ptr<JoinHashTable> hash_table;
	//! The perfect hash join executor (if any)
	unique_ptr<PerfectHashJoinExecutor> perfect_join_executor;
	//! Whether or not the hash table has been finalized
	bool finalized;
	//! The number of active local states
	atomic<idx_t> active_local_states;

	//! The memory available to the hash table: the reservation minus what partitioning the probe side requires.
	//! A capped reservation may be smaller than the spill-buffer estimate; zero streams one partition at a time.
	idx_t GetMaxExternalHTSize() const {
		const auto reservation = temporary_memory_state->GetReservation();
		return reservation > probe_side_requirement ? reservation - probe_side_requirement : 0;
	}

	//! Whether we are doing an external + some sizes
	bool external;
	idx_t total_size;
	idx_t max_partition_size;
	idx_t max_partition_count;
	idx_t probe_side_requirement;

	//! Hash tables built by each thread (owned by the corresponding local sink states)
	vector<reference<JoinHashTable>> local_hash_tables;
	//! Local hash tables whose ownership has been transferred during the normal one-shot finalize path
	vector<unique_ptr<JoinHashTable>> owned_local_hash_tables;

	//! Excess probe data gathered during Sink
	vector<LogicalType> probe_types;
	unique_ptr<JoinHashTable::ProbeSpill> probe_spill;

	//! Whether or not we have started scanning data using GetData
	atomic<bool> scanned_data;
	//! Preserve the finalized build-side state across recursive CTE iterations
	bool preserve_build_for_reuse = false;

	bool skip_filter_pushdown = false;
	unique_ptr<JoinFilterGlobalState> global_filter_state;
	bool keep_local_hash_tables = false;
	bool collect_perfect_join_bounds = false;
	PerfectHashJoinBuildBounds perfect_join_bounds;

	//! Coordinates first-chunk publication of the TupleDataLayout across parallel sinks.
	LayoutGate layout_gate;
	//! True iff this join may use perfect-hash-join at Finalize. PHJ's FillPerfectHashTable reads payload at native
	//! width, so it disables dict-surviving slot narrowing.
	bool can_use_perfect_hash = false;
};

//! Prove stability for this output lane, not for unrelated columns or already-materialized ancestors.
static bool OutputPreservesGlobalDictionary(const PhysicalOperator &op, idx_t column) {
	if (column >= op.GetTypes().size()) {
		return false;
	}
	switch (op.type) {
	case PhysicalOperatorType::PROJECTION: {
		const auto &expr = *op.Cast<PhysicalProjection>().select_list[column];
		return expr.GetExpressionClass() == ExpressionClass::BOUND_REF &&
		       OutputPreservesGlobalDictionary(op.children[0], expr.Cast<BoundReferenceExpression>().Index());
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::STREAMING_SAMPLE:
		return OutputPreservesGlobalDictionary(op.children[0], column);
	case PhysicalOperatorType::LEFT_DELIM_JOIN:
	case PhysicalOperatorType::RIGHT_DELIM_JOIN:
		return OutputPreservesGlobalDictionary(op.Cast<PhysicalDelimJoin>().join, column);
	case PhysicalOperatorType::HASH_JOIN: {
		const auto &join = op.Cast<PhysicalHashJoin>();
		if (!join.sink_state) {
			return false;
		}
		const auto &state = join.sink_state->Cast<HashJoinGlobalSinkState>();
		if (!state.finalized) {
			return false;
		}
		const auto left_count = join.lhs_output_columns.col_types.size();
		if (column < left_count) {
			// Only probe lanes can become NULL-filled or be replayed flat from spill storage.
			return !IsRightOuterJoin(join.join_type) && !state.external &&
			       OutputPreservesGlobalDictionary(join.children[0], join.lhs_output_columns.col_idxs[column]);
		}
		const auto rhs_column = column - left_count;
		// Finalized RHS producers own their dictionaries independently of their input encodings.
		return rhs_column < join.rhs_output_columns.col_types.size() &&
		       (state.perfect_join_executor || state.hash_table->RHSOutputIsGlobalDictionary(rhs_column));
	}
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
		const auto &join = op.Cast<PhysicalJoin>();
		return !IsRightOuterJoin(join.join_type) && column < op.children[0].get().GetTypes().size() &&
		       OutputPreservesGlobalDictionary(op.children[0], column);
	}
	default:
		// Materialization, multi-source streams and unknown transformations do not preserve this promise.
		return false;
	}
}

unique_ptr<JoinFilterLocalState> JoinFilterPushdownInfo::GetLocalState(JoinFilterGlobalState &gstate) const {
	auto result = make_uniq<JoinFilterLocalState>();
	result->local_aggregate_state = make_uniq<LocalUngroupedAggregateState>(*gstate.global_aggregate_state);
	return result;
}

class HashJoinLocalSinkState : public LocalSinkState {
public:
	HashJoinLocalSinkState(const PhysicalHashJoin &op, ClientContext &context, HashJoinGlobalSinkState &gstate)
	    : op(op), join_key_executor(context) {
		auto &allocator = BufferAllocator::Get(context);

		for (auto &cond : op.conditions) {
			join_key_executor.AddExpression(cond.GetRHS());
		}
		join_keys.Initialize(allocator, op.condition_types);

		if (!op.payload_columns.col_types.empty()) {
			payload_chunk.InitializeEmpty(op.payload_columns.col_types);
		}

		hash_table = op.InitializeHashTable(context, gstate.hash_table->GetRadixBits());
		// every thread inserts into this one filter - InsertOne is an atomic fetch_or, so it needs no lock
		hash_table->SetBloomFilter(gstate.hash_table->GetBloomFilter());
		// sink_collection exists only after the layout is published on the first build chunk, so
		// InitializeAppendState runs lazily inside Sink (or the execution-region build binding).
		keep_hash_table = gstate.keep_local_hash_tables;

		gstate.active_local_states++;

		if (op.filter_pushdown) {
			local_filter_state = op.filter_pushdown->GetLocalState(*gstate.global_filter_state);
		}
		if (gstate.collect_perfect_join_bounds) {
			perfect_join_bounds.Initialize(op.condition_types[0]);
		}
	}

public:
	const PhysicalHashJoin &op;
	PartitionedTupleDataAppendState append_state;
	//! True once InitializeAppendState has been called against the published sink_collection
	bool append_state_initialised = false;

	ExpressionExecutor join_key_executor;
	DataChunk join_keys;

	DataChunk payload_chunk;

	//! Thread-local HT
	unique_ptr<JoinHashTable> hash_table;
	bool keep_hash_table = false;

	unique_ptr<JoinFilterLocalState> local_filter_state;
	PerfectHashJoinBuildBounds perfect_join_bounds;

	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ExecutionContext &context, GlobalSinkState &gstate_p) override {
		auto &gstate = gstate_p.Cast<HashJoinGlobalSinkState>();
		join_keys.Reset();
		payload_chunk.Reset();
		if (hash_table) {
			// Drop the row data and the layout: the next iteration's first chunk republishes it, and Sink then
			// reruns the lazy per-thread setup (FinishInitWithLayout + InitializeAppendState) against it.
			hash_table->ResetForNewIterationSinglePartition();
		} else {
			// HT was moved into gstate during a Combine that did not keep local hash tables. Rebuild against the
			// global radix_bits so partition counts stay consistent in PrepareFinalize.
			hash_table = op.InitializeHashTable(context.client, gstate.hash_table->GetRadixBits());
		}
		append_state_initialised = false;
		// the global state resets before any local one, so the filter this picks up is always a fresh one
		D_ASSERT(!gstate.hash_table->GetBloomFilter() || !gstate.hash_table->GetBloomFilter()->IsSealed());
		hash_table->SetBloomFilter(gstate.hash_table->GetBloomFilter());
		keep_hash_table = gstate.keep_local_hash_tables;
		gstate.active_local_states++;
		if (op.filter_pushdown) {
			local_filter_state = op.filter_pushdown->GetLocalState(*gstate.global_filter_state);
		} else {
			local_filter_state.reset();
		}
		if (gstate.collect_perfect_join_bounds) {
			perfect_join_bounds.Initialize(op.condition_types[0]);
		} else {
			perfect_join_bounds = PerfectHashJoinBuildBounds();
		}
	}
};

//! Build the row layout [conditions, build payload, (found flag), hash], narrowing a payload column to its
//! dict-index slot when dict_info narrows it. Shared by publisher and empty-input fallback to avoid drift.
static shared_ptr<TupleDataLayout> BuildJoinLayout(const vector<LogicalType> &cond_types,
                                                   const vector<LogicalType> &build_types, JoinType join_type,
                                                   optional_ptr<const DictSurvivingInfo> dict_info) {
	return JoinHashTable::BuildRowLayout(cond_types, build_types, join_type,
	                                     DictSurvivingInfo::IndexWidths(dict_info, build_types.size()));
}

//! Join-level gather compatibility; publication proves dictionary stability separately for each payload column.
static bool CanUseDictSurvivingJoin(const PhysicalHashJoin &op, const JoinHashTable &ht, bool can_use_perfect_hash) {
	// external is safe here: the dictionary is an in-memory self-owned copy and the index is a plain row-store
	// column, so a spill/repartition preserves both (unlike the pointer-embedding dict-emission/compressed-probe paths)
	// SINGLE joins need FlatVector::SetNull on unmatched rows; dictionary vectors cannot supply it
	if (ht.join_type == JoinType::SINGLE) {
		return false;
	}
	// LEFT may dispatch into NextUniqueLeftJoin, which gathers payload via ScanStructure::GatherResult and
	// bypasses GatherRHS' dict branch; the narrowed slot would be read as native type and trip the gather type check.
	if (ht.join_type == JoinType::LEFT) {
		return false;
	}
	// OUTER fills unmatched-probe rows with CONSTANT_NULL (NextLeftJoin), mixing dict chunks with flat fill chunks;
	// admitting it would re-emit a falsely global dictionary a downstream consumer cannot trust.
	if (ht.join_type == JoinType::OUTER) {
		return false;
	}
	if (op.rhs_output_columns.col_types.empty()) {
		return false;
	}
	// PHJ's FillPerfectHashTable reads payload from the row store at native width; a narrowed slot would corrupt it.
	if (can_use_perfect_hash) {
		return false;
	}
	return true;
}

enum class JitHashJoinProbeFilterStrategy : uint8_t { DIRECT_HASH_TABLE, BLOOM_REJECT };

static bool JitProbeBloomCandidate(const PhysicalHashJoin &op) {
	// Rejecting probe rows that cannot match is sound wherever an unmatched probe row produces no output
	if ((op.join_type != JoinType::INNER && op.join_type != JoinType::SEMI) || op.residual_info) {
		return false;
	}
	if (op.conditions.empty() || op.conditions.size() > 2) {
		return false;
	}
	for (auto &cond : op.conditions) {
		if (cond.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		auto key_type = cond.GetLHS().GetReturnType().InternalType();
		switch (key_type) {
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
		case PhysicalType::UINT8:
		case PhysicalType::UINT16:
		case PhysicalType::UINT32:
		case PhysicalType::UINT64:
			break;
		default:
			return false;
		}
	}
	auto probe_estimated_cardinality = op.children[0].get().estimated_cardinality;
	auto build_estimated_cardinality = op.children[1].get().estimated_cardinality;
	if (probe_estimated_cardinality == 0 || build_estimated_cardinality == 0) {
		return false;
	}

	static constexpr idx_t MIN_PROBE_CARDINALITY = 262144;
	if (probe_estimated_cardinality < MIN_PROBE_CARDINALITY) {
		return false;
	}

	static constexpr double JIT_PROBE_BLOOM_BUILD_TO_PROBE_RATIO_THRESHOLD = 0.25;
	const double build_to_probe_ratio =
	    static_cast<double>(build_estimated_cardinality) / static_cast<double>(probe_estimated_cardinality);
	return build_to_probe_ratio <= JIT_PROBE_BLOOM_BUILD_TO_PROBE_RATIO_THRESHOLD;
}

static JitHashJoinProbeFilterStrategy SelectJitHashJoinProbeFilterStrategy(const PhysicalHashJoin &op,
                                                                           idx_t build_count) {
	if (!JitProbeBloomCandidate(op)) {
		return JitHashJoinProbeFilterStrategy::DIRECT_HASH_TABLE;
	}
	if (build_count <= JoinHashTable::USE_SALT_THRESHOLD) {
		return JitHashJoinProbeFilterStrategy::DIRECT_HASH_TABLE;
	}
	return JitHashJoinProbeFilterStrategy::BLOOM_REJECT;
}

vector<uint8_t> PhysicalHashJoin::GetPublishedBuildDictIndexWidths() const {
	if (!sink_state) {
		return {};
	}
	auto &gstate = sink_state->Cast<HashJoinGlobalSinkState>();
	if (!gstate.layout_gate.published.load(std::memory_order_acquire)) {
		return {};
	}
	return DictSurvivingInfo::IndexWidths(gstate.layout_gate.dict_info, gstate.hash_table->build_types.size());
}

bool HashJoinGlobalSinkState::DictSurvivingActive() const {
	return hash_table && hash_table->HasDictSurvivingColumns();
}

void HashJoinGlobalSinkState::PublishLayoutIfFirst(HashJoinLocalSinkState &lstate, DataChunk &payload_chunk) {
	CallOnce(*layout_gate.publish_once, [&] {
		const auto &cond_types = lstate.hash_table->condition_types;
		const auto &build_types = lstate.hash_table->build_types;

		shared_ptr<const DictSurvivingInfo> dict_info;
		if (CanUseDictSurvivingJoin(op, *lstate.hash_table, can_use_perfect_hash)) {
			// Per-column width decision lives on the JHT (GetDictSurvivingIndexWidth); feed it each arriving vector.
			// The dictionary a narrowed slot indexes into is created here, once, and handed to every thread with the
			// layout: a later chunk may carry no dictionary at all (NULL-only), so no thread can pin it lazily.
			// Each narrowed lane must also be proven to keep that dictionary on every chunk.
			DictSurvivingInfo info;
			info.columns.resize(build_types.size());
			for (idx_t col = 0; col < MinValue<idx_t>(build_types.size(), payload_chunk.ColumnCount()); col++) {
				auto &incoming = payload_chunk.data[col];
				const auto index_width = lstate.hash_table->GetDictSurvivingIndexWidth(col, incoming);
				if (index_width != 0 &&
				    OutputPreservesGlobalDictionary(op.children[1], op.payload_columns.col_idxs[col])) {
					info.columns[col] = JoinHashTable::CreateDictSurvivingColumn(incoming, index_width);
				}
			}
			if (info.Any()) {
				dict_info = make_shared_ptr<DictSurvivingInfo>(std::move(info));
			}
		}

		auto layout = BuildJoinLayout(cond_types, build_types, lstate.hash_table->join_type, dict_info.get());
		// global HT receives the same layout so Merge/Combine and finalize-time scans operate against it
		if (hash_table && !hash_table->IsLayoutFinalized()) {
			hash_table->FinishInitWithLayout(layout, dict_info);
		}
		layout_gate.layout_ptr = std::move(layout);
		layout_gate.dict_info = std::move(dict_info);
		layout_gate.published.store(true, std::memory_order_release);
	});
}

//! Lazy per-thread sink setup against the published layout. Shared by the interpreted Sink and the execution-region
//! build binding so both paths publish and attach to the same canonical layout.
static void InitializeHashJoinSinkAppendState(HashJoinGlobalSinkState &gstate, HashJoinLocalSinkState &lstate,
                                              DataChunk &payload_chunk) {
	// first-chunk: publish the canonical layout against the actually-arriving vector types
	gstate.PublishLayoutIfFirst(lstate, payload_chunk);
	if (lstate.append_state_initialised) {
		return;
	}
	lstate.hash_table->FinishInitWithLayout(gstate.layout_gate.layout_ptr, gstate.layout_gate.dict_info);
	lstate.hash_table->GetSinkCollection().InitializeAppendState(lstate.append_state);
	lstate.append_state_initialised = true;
}

unique_ptr<JoinHashTable> PhysicalHashJoin::InitializeHashTable(ClientContext &context,
                                                                const idx_t initial_radix_bits) const {
	auto result =
	    make_uniq<JoinHashTable>(context, *this, conditions, payload_columns.col_types, join_type, initial_radix_bits,
	                             rhs_output_columns.col_idxs, residual_info ? residual_info->Copy() : nullptr,
	                             predicate ? predicate.get() : nullptr, lhs_output_in_probe);

	if (!delim_types.empty() && join_type == JoinType::MARK) {
		// correlated MARK join
		if (delim_types.size() + 1 == conditions.size()) {
			// the correlated MARK join has one more condition than the amount of correlated columns
			// this is the case in a correlated ANY() expression
			// in this case we need to keep track of additional entries, namely:
			// - (1) the total amount of elements per group
			// - (2) the amount of non-null elements per group
			// we need these to correctly deal with the cases of either:
			// - (1) the group being empty [in which case the result is always false, even if the comparison is NULL]
			// - (2) the group containing a NULL value [in which case FALSE becomes NULL]
			auto &info = result->mark_join_info;

			vector<LogicalType> delim_payload_types;
			vector<AggregateObject> correlated_aggregates;
			unique_ptr<BoundAggregateExpression> aggr;

			// jury-rigging the GroupedAggregateHashTable
			// we need a count_star and a count to get counts with and without NULLs

			FunctionBinder function_binder(context);
			aggr = function_binder.BindAggregateFunction(GetBuiltinAggregateFunction(context, CountStarFun::Name, {}),
			                                             {}, nullptr, AggregateType::NON_DISTINCT);
			correlated_aggregates.emplace_back(*aggr);
			delim_payload_types.push_back(aggr->GetReturnType());
			info.correlated_aggregates.push_back(std::move(aggr));

			auto count_fun = GetBuiltinAggregateFunction(context, CountFun::Name, {LogicalType::BIGINT});
			vector<unique_ptr<Expression>> children;
			// this is a dummy but we need it to make the hash table understand whats going on
			children.push_back(make_uniq_base<Expression, BoundReferenceExpression>(count_fun->GetReturnType(), 0U));
			aggr = function_binder.BindAggregateFunction(count_fun, std::move(children), nullptr,
			                                             AggregateType::NON_DISTINCT);
			correlated_aggregates.emplace_back(*aggr);
			delim_payload_types.push_back(aggr->GetReturnType());
			info.correlated_aggregates.push_back(std::move(aggr));

			auto &allocator = BufferAllocator::Get(context);
			info.correlated_counts = make_uniq<GroupedAggregateHashTable>(
			    context, allocator, delim_types, delim_payload_types, std::move(correlated_aggregates));
			info.correlated_types = delim_types;
			info.group_chunk.InitializeEmpty(delim_types);
			info.result_chunk.Initialize(allocator, delim_payload_types);
		}
	}
	const bool has_row_equality =
	    conditions.size() > 1 ||
	    (conditions.size() == 1 && conditions[0].GetLHS().GetReturnType().id() == LogicalTypeId::TUPLE);
	if (delim_types.empty() && join_type == JoinType::MARK && has_row_equality) {
		bool all_equal = true;
		for (auto &condition : conditions) {
			all_equal = all_equal && condition.GetComparisonType() == ExpressionType::COMPARE_EQUAL;
		}
		if (all_equal) {
			result->InitializeUncorrelatedMarkJoin();
		}
	}
	return result;
}

unique_ptr<GlobalSinkState> PhysicalHashJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<HashJoinGlobalSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<HashJoinGlobalSinkState>();
	return make_uniq<HashJoinLocalSinkState>(*this, context.client, gstate);
}

void PhysicalHashJoin::SetPreserveBuildForRecursiveReuse(bool preserve) const {
	if (!sink_state) {
		return;
	}
	auto &state = sink_state->Cast<HashJoinGlobalSinkState>();
	state.preserve_build_for_reuse = preserve;
	if (preserve) {
		state.scanned_data = false;
	}
}

bool PhysicalHashJoin::CanPreserveBuildForRecursiveReuse() const {
	if (!sink_state) {
		return false;
	}
	auto &state = sink_state->Cast<HashJoinGlobalSinkState>();
	return state.preserve_build_for_reuse && !state.external;
}

void JoinFilterPushdownInfo::Sink(DataChunk &chunk, JoinFilterLocalState &lstate) const {
	// if we are pushing any filters into a probe-side, compute the min/max over the columns that we are pushing
	for (idx_t pushdown_idx = 0; pushdown_idx < join_condition.size(); pushdown_idx++) {
		auto join_condition_idx = join_condition[pushdown_idx];
		for (idx_t i = 0; i < 2; i++) {
			idx_t aggr_idx = pushdown_idx * 2 + i;
			lstate.local_aggregate_state->Sink(chunk, join_condition_idx, aggr_idx, chunk.size());
		}
	}
}

//! The key statistics every build chunk contributes, whether the native Sink or a compiled kernel appends it: the
//! join-filter min/max and the perfect-hash build bounds. Finalize sizes the perfect hash table from them, so a chunk
//! that skipped them would have its keys fall outside the table
static void RecordHashJoinBuildKeys(HashJoinGlobalSinkState &gstate, HashJoinLocalSinkState &lstate) {
	auto &filter_pushdown = lstate.op.filter_pushdown;
	if (filter_pushdown && !gstate.skip_filter_pushdown) {
		filter_pushdown->Sink(lstate.join_keys, *lstate.local_filter_state);
	}
	if (gstate.collect_perfect_join_bounds) {
		UpdatePerfectHashJoinBuildBounds(lstate.join_keys.data[0], lstate.join_keys.size(), lstate.perfect_join_bounds);
	}
}

SinkResultType PhysicalHashJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<HashJoinGlobalSinkState>();
	auto &lstate = input.local_state.Cast<HashJoinLocalSinkState>();

	// resolve the join keys for the right chunk
	lstate.join_keys.Reset();
	lstate.join_key_executor.Execute(chunk, lstate.join_keys);
	RecordHashJoinBuildKeys(gstate, lstate);

	if (payload_columns.col_types.empty()) { // there are only keys: place an empty chunk in the payload
		lstate.payload_chunk.SetChildCardinality(chunk.size());
	} else { // there are payload columns
		lstate.payload_chunk.ReferenceColumns(chunk, payload_columns.col_idxs);
	}

	InitializeHashJoinSinkAppendState(gstate, lstate, lstate.payload_chunk);

	// build the HT
	lstate.hash_table->Build(lstate.append_state, lstate.join_keys, lstate.payload_chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

static bool BindExecutionHashJoinBuildColumns(const ExecutionRegionHashJoinInfo &sink_info, DataChunk &chunk,
                                              ExecutionHashJoinBuildBinding &build, string &blocker) {
	auto &contract = sink_info.contract;
	if (!contract.present) {
		blocker = "hash-join-build-native-runtime-missing-contract";
		return false;
	}
	if (contract.native_build_contract.status != ExecutionRegionStateContractStatus::READY) {
		blocker = contract.native_build_contract.blocker.empty() ? "hash-join-build-native-runtime-contract-not-ready"
		                                                         : contract.native_build_contract.blocker;
		return false;
	}
	if (!contract.build_sink_shape_ready) {
		blocker = contract.build_sink_shape_blocker.empty() ? "hash-join-build-native-runtime-sink-shape-not-ready"
		                                                    : contract.build_sink_shape_blocker;
		return false;
	}
	if (sink_info.keys.size() != contract.condition_count) {
		blocker = "hash-join-build-native-runtime-key-count-mismatch";
		return false;
	}
	for (auto &key : sink_info.keys) {
		if (!key.supported_reference) {
			blocker = key.reason.empty() ? "hash-join-build-native-runtime-key-not-reference" : key.reason;
			return false;
		}
		if (key.input_index >= chunk.ColumnCount()) {
			blocker = "hash-join-build-native-runtime-key-input-out-of-range";
			return false;
		}
		build.key_input_indices.push_back(NumericCast<column_t>(key.input_index));
		build.key_types.push_back(key.type);
	}
	if (contract.payload_column_indices.size() != contract.payload_types.size()) {
		blocker = "hash-join-build-native-runtime-payload-contract-mismatch";
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < contract.payload_column_indices.size(); payload_idx++) {
		auto input_index = contract.payload_column_indices[payload_idx];
		if (input_index >= chunk.ColumnCount()) {
			blocker = "hash-join-build-native-runtime-payload-input-out-of-range";
			return false;
		}
		build.payload_input_indices.push_back(NumericCast<column_t>(input_index));
		build.payload_types.push_back(contract.payload_types[payload_idx]);
	}
	return true;
}

bool ExecutionBindHashJoinBuild(ExecutionContext &context, OperatorSinkInput &input, DataChunk &chunk,
                                const ExecutionRegionHashJoinInfo &sink_info, ExecutionSinkBinding &binding) {
	(void)context;
	binding = ExecutionSinkBinding();
	binding.kind = ExecutionRegionSinkKind::HASH_JOIN_BUILD;

	auto &gstate = input.global_state.Cast<HashJoinGlobalSinkState>();
	auto &lstate = input.local_state.Cast<HashJoinLocalSinkState>();
	auto &build = binding.hash_join_build;
	string blocker;
	if (!BindExecutionHashJoinBuildColumns(sink_info, chunk, build, blocker)) {
		binding.blocker = std::move(blocker);
		build.blocker = binding.blocker;
		return false;
	}
	if (!lstate.hash_table) {
		binding.blocker = "hash-join-build-native-runtime-missing-local-hash-table";
		build.blocker = binding.blocker;
		return false;
	}
	if (!lstate.join_keys.ColumnCount()) {
		binding.blocker = "hash-join-build-native-runtime-missing-key-chunk";
		build.blocker = binding.blocker;
		return false;
	}
	if (lstate.join_keys.ColumnCount() != build.key_input_indices.size()) {
		binding.blocker = "hash-join-build-native-runtime-key-chunk-count-mismatch";
		build.blocker = binding.blocker;
		return false;
	}
	if (lstate.payload_chunk.ColumnCount() != build.payload_input_indices.size()) {
		binding.blocker = "hash-join-build-native-runtime-payload-chunk-count-mismatch";
		build.blocker = binding.blocker;
		return false;
	}
	if (lstate.op.filter_pushdown && !gstate.skip_filter_pushdown && !lstate.local_filter_state) {
		binding.blocker = "hash-join-build-native-runtime-missing-local-filter-state";
		build.blocker = binding.blocker;
		return false;
	}
	// The row layout is published on the first build chunk. The compiled sink appends through the bound
	// append state, so attach to the published layout here, exactly as the interpreted Sink does.
	if (build.payload_input_indices.empty()) {
		lstate.payload_chunk.SetChildCardinality(chunk.size());
	} else {
		lstate.payload_chunk.ReferenceColumns(chunk, build.payload_input_indices);
	}
	InitializeHashJoinSinkAppendState(gstate, lstate, lstate.payload_chunk);

	binding.ready = true;
	binding.blocker.clear();
	build.ready = true;
	build.hash_table = lstate.hash_table.get();
	build.append_state = &lstate.append_state;
	build.join_keys = &lstate.join_keys;
	build.payload_chunk = &lstate.payload_chunk;
	build.global_state = &gstate;
	build.local_state = &lstate;
	build.blocker.clear();
	return true;
}

static std::chrono::steady_clock::time_point
HashJoinBuildTraceStart(optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	return recorder ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
}

static void RecordHashJoinBuildTraceStage(optional_ptr<ExecutionOperatorStageRecorder> recorder, const string &stage,
                                          std::chrono::steady_clock::time_point start) {
	if (!recorder) {
		return;
	}
	auto end = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
	recorder->RecordStageRuntime(stage, elapsed);
}

static void AssertExecutionHashJoinBuildReady(const ExecutionHashJoinBuildBinding &binding) {
	if (!binding.ready || !binding.hash_table || !binding.append_state || !binding.join_keys ||
	    !binding.payload_chunk || !binding.global_state || !binding.local_state) {
		throw InternalException("execution native hash join build sink requires a bound hash join build state");
	}
	if (binding.join_keys->ColumnCount() != binding.key_input_indices.size()) {
		throw InternalException("execution native hash join build key count mismatch");
	}
	if (binding.payload_chunk->ColumnCount() != binding.payload_input_indices.size()) {
		throw InternalException("execution native hash join build payload count mismatch");
	}
}

void ExecutionHashJoinBuildReferenceKeys(const ExecutionHashJoinBuildBinding &binding, DataChunk &input) {
	AssertExecutionHashJoinBuildReady(binding);
	binding.join_keys->ReferenceColumns(input, binding.key_input_indices);
}

void ExecutionHashJoinBuildRecordKeys(const ExecutionHashJoinBuildBinding &binding) {
	AssertExecutionHashJoinBuildReady(binding);
	auto &lstate = binding.local_state->Cast<HashJoinLocalSinkState>();
	D_ASSERT(binding.join_keys == &lstate.join_keys);
	RecordHashJoinBuildKeys(binding.global_state->Cast<HashJoinGlobalSinkState>(), lstate);
}

void ExecutionHashJoinBuildReferencePayload(const ExecutionHashJoinBuildBinding &binding, DataChunk &input) {
	AssertExecutionHashJoinBuildReady(binding);
	if (binding.payload_input_indices.empty()) {
		binding.payload_chunk->SetChildCardinality(input.size());
		return;
	}
	binding.payload_chunk->ReferenceColumns(input, binding.payload_input_indices);
}

idx_t ExecutionHashJoinBuildPrepare(const ExecutionHashJoinBuildBinding &binding, DataChunk &source_chunk,
                                    Vector &hash_values, SelectionVector &build_sel,
                                    optional_ptr<const SelectionVector> &build_selection) {
	AssertExecutionHashJoinBuildReady(binding);
	if (source_chunk.ColumnCount() == 0) {
		binding.hash_table->InitializeBuildChunk(source_chunk);
	} else {
		source_chunk.Reset();
	}
	return binding.hash_table->PrepareBuildChunk(*binding.append_state, *binding.join_keys, *binding.payload_chunk,
	                                             source_chunk, hash_values, build_sel, build_selection);
}

void ExecutionHashJoinBuildHash(const ExecutionHashJoinBuildBinding &binding, DataChunk &source_chunk,
                                Vector &hash_values, const SelectionVector &build_selection, idx_t build_count) {
	AssertExecutionHashJoinBuildReady(binding);
	binding.hash_table->HashBuildChunk(*binding.append_state, *binding.join_keys, source_chunk, hash_values,
	                                   build_selection, build_count);
}

void ExecutionHashJoinBuildAppend(const ExecutionHashJoinBuildBinding &binding, DataChunk &source_chunk,
                                  const SelectionVector &build_selection, idx_t build_count) {
	AssertExecutionHashJoinBuildReady(binding);
	binding.hash_table->AppendBuildChunk(*binding.append_state, source_chunk, build_selection, build_count);
}

SinkResultType ExecutionSinkHashJoinBuild(const ExecutionHashJoinBuildBinding &binding, DataChunk &input,
                                          optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	AssertExecutionHashJoinBuildReady(binding);

	auto stage_start = HashJoinBuildTraceStart(recorder);
	ExecutionHashJoinBuildReferenceKeys(binding, input);
	RecordHashJoinBuildTraceStage(recorder, "reference_keys", stage_start);

	stage_start = HashJoinBuildTraceStart(recorder);
	ExecutionHashJoinBuildRecordKeys(binding);
	RecordHashJoinBuildTraceStage(recorder, "filter_pushdown", stage_start);

	stage_start = HashJoinBuildTraceStart(recorder);
	ExecutionHashJoinBuildReferencePayload(binding, input);
	RecordHashJoinBuildTraceStage(recorder, "reference_payload", stage_start);

	DataChunk source_chunk;
	Vector hash_values(LogicalType::HASH);
	SelectionVector build_sel(STANDARD_VECTOR_SIZE);
	optional_ptr<const SelectionVector> build_selection;

	stage_start = HashJoinBuildTraceStart(recorder);
	auto build_count = ExecutionHashJoinBuildPrepare(binding, source_chunk, hash_values, build_sel, build_selection);
	RecordHashJoinBuildTraceStage(recorder, "hash_table_prepare", stage_start);
	if (build_count == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	stage_start = HashJoinBuildTraceStart(recorder);
	ExecutionHashJoinBuildHash(binding, source_chunk, hash_values, *build_selection, build_count);
	RecordHashJoinBuildTraceStage(recorder, "hash_table_hash", stage_start);

	stage_start = HashJoinBuildTraceStart(recorder);
	ExecutionHashJoinBuildAppend(binding, source_chunk, *build_selection, build_count);
	RecordHashJoinBuildTraceStage(recorder, "hash_table_append", stage_start);
	return SinkResultType::NEED_MORE_INPUT;
}

void JoinFilterPushdownInfo::Combine(JoinFilterGlobalState &gstate, JoinFilterLocalState &lstate) const {
	gstate.global_aggregate_state->Combine(*lstate.local_aggregate_state);
}

SinkCombineResultType PhysicalHashJoin::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<HashJoinGlobalSinkState>();
	auto &lstate = input.local_state.Cast<HashJoinLocalSinkState>();

	// Under deferred layout, a thread that never received a Sink chunk has no sink_collection to flush
	const bool has_layout = lstate.append_state_initialised;
	if (has_layout) {
		lstate.hash_table->GetSinkCollection().FlushAppendState(lstate.append_state);
	}
	annotated_lock_guard<annotated_mutex> guard(gstate.lock);
	if (!has_layout) {
		// nothing to merge — drop the empty thread-local hash table
		gstate.active_local_states--;
	} else if (lstate.keep_hash_table) {
		gstate.local_hash_tables.push_back(*lstate.hash_table);
	} else {
		gstate.owned_local_hash_tables.push_back(std::move(lstate.hash_table));
		gstate.local_hash_tables.push_back(*gstate.owned_local_hash_tables.back());
	}
	if (gstate.local_hash_tables.size() == gstate.active_local_states) {
		// Set to 0 until PrepareFinalize
		gstate.temporary_memory_state->SetZero();
	}

	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this);
	client_profiler.Flush(context.thread.profiler);
	if (filter_pushdown && !gstate.skip_filter_pushdown) {
		filter_pushdown->Combine(*gstate.global_filter_state, *lstate.local_filter_state);
	}
	if (gstate.collect_perfect_join_bounds) {
		gstate.perfect_join_bounds.Combine(lstate.perfect_join_bounds);
	}

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//

struct HashJoinBuildConfig {
	static constexpr idx_t PARALLEL_CONSTRUCT_THRESHOLD = 1048576;
	static constexpr idx_t SMALL_PARALLEL_CONSTRUCT_THRESHOLD = 65536;
	static constexpr idx_t SMALL_PARALLEL_MIN_THREADS = 4;
	static constexpr double SKEW_SINGLE_THREADED_THRESHOLD = 0.33;
	static constexpr double SMALL_BUILD_SKEW_THRESHOLD = 0.20;
};

//! If the data is very skewed (many of the exact same key), our finalize will become slow,
//! due to completely slamming the same atomic using compare-and-swaps.
//! We can detect this because we partition the data, and go for a single-threaded finalize instead.
static bool KeysAreSkewed(const HashJoinGlobalSinkState &sink,
                          double threshold = HashJoinBuildConfig::SKEW_SINGLE_THREADED_THRESHOLD) {
	const auto max_partition_ht_size =
	    sink.max_partition_size + sink.hash_table->PointerTableSize(sink.max_partition_count);
	const auto skew = static_cast<double>(max_partition_ht_size) / static_cast<double>(sink.total_size);
	return skew > threshold;
}

//! If we have only one thread, always finalize single-threaded. Otherwise, we finalize in parallel if we
//! have at least 1M rows, an unskewed medium input with enough threads, or want to verify parallelism.
static bool FinalizeSingleThreaded(const HashJoinGlobalSinkState &sink) {
	// if only one thread, finalize single-threaded
	const auto num_threads = NumericCast<idx_t>(sink.num_threads);
	if (num_threads == 1) {
		return true;
	}

	// if we want to verify parallelism, finalize parallel
	if (ClientConfig::GetConfig(sink.context).verify_parallelism) {
		return false;
	}

	const auto &ht = *sink.hash_table;
	if (ht.Count() >= HashJoinBuildConfig::PARALLEL_CONSTRUCT_THRESHOLD) {
		return false;
	}
	if (num_threads < HashJoinBuildConfig::SMALL_PARALLEL_MIN_THREADS ||
	    ht.Count() < HashJoinBuildConfig::SMALL_PARALLEL_CONSTRUCT_THRESHOLD) {
		return true;
	}
	return KeysAreSkewed(sink, HashJoinBuildConfig::SMALL_BUILD_SKEW_THRESHOLD);
}

static idx_t GetTupleWidth(const vector<LogicalType> &types, bool &all_constant) {
	TupleDataLayout layout;
	layout.Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	all_constant = layout.AllConstant();
	return layout.GetRowWidth();
}

static idx_t GetPartitioningSpaceRequirement(ClientContext &context, const vector<LogicalType> &types,
                                             const idx_t radix_bits, const idx_t num_threads) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	bool all_constant;
	idx_t tuple_width = GetTupleWidth(types, all_constant);

	if (tuple_width == 0) {
		throw InternalException("GetPartitioningSpaceRequirement: tuple width should not be 0");
	}

	auto tuples_per_block = MaxValue<idx_t>(buffer_manager.GetBlockSize() / tuple_width, 1);
	auto blocks_per_chunk = (STANDARD_VECTOR_SIZE + tuples_per_block) / tuples_per_block + 1;
	if (!all_constant) {
		blocks_per_chunk += 2;
	}
	auto size_per_partition = blocks_per_chunk * buffer_manager.GetBlockAllocSize();
	auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);

	return num_threads * num_partitions * size_per_partition;
}

void HashJoinGlobalSinkState::UpdateProbeSideRequirement() {
	// Estimate the same schema that ProbeSpill stores, including the hash column, before and after repartition.
	probe_side_requirement =
	    GetPartitioningSpaceRequirement(context, probe_types, hash_table->GetRadixBits(), num_threads);
}

static idx_t GetBuildSpaceRequirement(ClientContext &context, const JoinHashTable &ht, idx_t num_threads) {
	auto layout = BuildJoinLayout(ht.condition_types, ht.build_types, ht.join_type, {});
	return GetPartitioningSpaceRequirement(context, layout->GetTypes(), ht.GetRadixBits(), num_threads);
}

void PhysicalHashJoin::PrepareFinalize(ClientContext &context, GlobalSinkState &global_state) const {
	auto &gstate = global_state.Cast<HashJoinGlobalSinkState>();
	// If no Sink chunk ever arrived, the layout was never published. Fall back to a default layout
	// (all columns at their native width) so finalize-time scans can dereference data_collection.
	CallOnce(*gstate.layout_gate.publish_once, [&] {
		const auto &cond_types = gstate.hash_table->condition_types;
		const auto &build_types = gstate.hash_table->build_types;
		// no dict_info => BuildJoinLayout keeps every build column at its native width
		gstate.layout_gate.dict_info.reset();
		auto layout = BuildJoinLayout(cond_types, build_types, gstate.hash_table->join_type, nullptr);
		if (!gstate.hash_table->IsLayoutFinalized()) {
			gstate.hash_table->FinishInitWithLayout(layout);
		}
		gstate.layout_gate.layout_ptr = std::move(layout);
		gstate.layout_gate.published.store(true, std::memory_order_release);
	});
	const auto &ht = *gstate.hash_table;

	gstate.total_size =
	    ht.GetTotalSize(gstate.local_hash_tables, gstate.max_partition_size, gstate.max_partition_count);
	gstate.UpdateProbeSideRequirement();
	const auto max_partition_ht_size =
	    gstate.max_partition_size + gstate.hash_table->PointerTableSize(gstate.max_partition_count);
	gstate.temporary_memory_state->SetMinimumReservation(max_partition_ht_size + gstate.probe_side_requirement);

	bool all_constant;
	gstate.temporary_memory_state->SetMaterializationPenalty(GetTupleWidth(gstate.probe_types, all_constant));
	gstate.temporary_memory_state->SetRemainingSize(gstate.total_size);
}

static void ExecuteHashJoinTableInitTask(HashJoinGlobalSinkState &sink, idx_t entry_idx_from, idx_t entry_idx_to) {
	sink.hash_table->InitializePointerTable(entry_idx_from, entry_idx_to);
}

struct PrefixRangeBuildLane {
	explicit PrefixRangeBuildLane(unique_ptr<PrefixRangeFilter::BuildState> state_p) : state(std::move(state_p)) {
	}

	unique_ptr<PrefixRangeFilter::BuildState> state;
	idx_t task_count = 0;
};

//! Prefix-range build states replicated across the tasks of one finalize event, merged once every task is done.
class PrefixRangeBuildLanes {
public:
	void Initialize(JoinHashTable &ht, idx_t task_count) {
		if (!ht.ShouldBuildPrefixRangeFilter() || task_count == 0) {
			return;
		}
		// Bound replicated PRF state independently of the number of radix partitions.
		static constexpr idx_t MAX_PREFIX_RANGE_BUILD_MEMORY = 16ULL * 1024ULL * 1024ULL;
		const auto state_size = ht.GetPrefixRangeBuildStateSize();
		D_ASSERT(state_size > 0);
		const auto lanes_within_budget = MaxValue<idx_t>(MAX_PREFIX_RANGE_BUILD_MEMORY / state_size, 1);
		const auto lane_count = MinValue<idx_t>(task_count, lanes_within_budget);
		lanes.reserve(lane_count);
		for (idx_t lane_idx = 0; lane_idx < lane_count; lane_idx++) {
			lanes.push_back(make_uniq<PrefixRangeBuildLane>(ht.InitializePrefixRangeBuildState()));
		}
	}

	optional_ptr<PrefixRangeBuildLane> Assign(idx_t task_idx) {
		if (lanes.empty()) {
			return nullptr;
		}
		auto &lane = *lanes[task_idx % lanes.size()];
		lane.task_count++;
		return lane;
	}

	void Merge(JoinHashTable &ht) {
		for (auto &lane : lanes) {
			ht.MergePrefixRangeBuildState(*lane->state);
		}
	}

private:
	vector<unique_ptr<PrefixRangeBuildLane>> lanes;
};

struct JoinBuildRange {
	idx_t begin;
	idx_t end;
};

static void ExecuteHashJoinFinalizeTask(HashJoinGlobalSinkState &sink, const JoinBuildRange &chunk_range, bool parallel,
                                        optional_ptr<PrefixRangeBuildLane> prefix_range_lane) {
	optional_ptr<PrefixRangeFilter::BuildState> prefix_range_state;
	bool prefix_range_parallel = false;
	if (prefix_range_lane) {
		prefix_range_state = *prefix_range_lane->state;
		prefix_range_parallel = prefix_range_lane->task_count > 1;
	}
	sink.hash_table->Finalize(chunk_range.begin, chunk_range.end, parallel, prefix_range_state, prefix_range_parallel);
}

static void PublishDeferredRuntimeFilters(ClientContext &context, JoinHashTable &ht, JoinFilterGlobalState &gstate);

//! Event-owned ranges, consumed near their preferred CPU group before stealing.
class JoinBuildQueue {
public:
	explicit JoinBuildQueue(idx_t partitions) : partitions(partitions), group_count(GetGroupCount()) {
		const auto count = MinValue(partitions, group_count);
		for (idx_t i = 0; i < count; i++) {
			shards.push_back(make_uniq<Shard>());
		}
	}

	void Add(idx_t partition, idx_t begin, idx_t end) {
		D_ASSERT(begin < end);
		shards[partition * shards.size() / partitions]->ranges.push_back({begin, end});
		range_count++;
	}

	idx_t Count() const {
		return range_count;
	}

	idx_t StartShard() const {
		const auto group = (TaskScheduler::GetEstimatedCPUId() / CPUS_PER_GROUP) % group_count;
		return group * shards.size() / group_count;
	}

	bool Next(idx_t start, idx_t &pass, JoinBuildRange &range) {
		for (; pass < shards.size(); pass++) {
			auto &shard = *shards[(start + pass) % shards.size()];
			const auto next = shard.next.fetch_add(1, std::memory_order_relaxed);
			if (next < shard.ranges.size()) {
				range = shard.ranges[next];
				return true;
			}
		}
		return false;
	}

private:
	static constexpr idx_t CPUS_PER_GROUP = 8;

	static idx_t GetGroupCount() {
#ifndef DUCKDB_NO_THREADS
		const idx_t cpus = std::thread::hardware_concurrency();
		return MaxValue<idx_t>(1, (cpus + CPUS_PER_GROUP - 1) / CPUS_PER_GROUP);
#else
		return 1;
#endif
	}

	struct alignas(64) Shard {
		vector<JoinBuildRange> ranges;
		atomic<idx_t> next {0};
	};
	vector<unique_ptr<Shard>> shards;
	const idx_t partitions;
	const idx_t group_count;
	idx_t range_count = 0;
};

class HashJoinTableInitTask : public ExecutorTask {
public:
	HashJoinTableInitTask(shared_ptr<Event> event_p, ClientContext &context, HashJoinGlobalSinkState &sink_p,
	                      const PhysicalOperator &op_p, JoinBuildQueue &queue_p)
	    : ExecutorTask(context, std::move(event_p), op_p), sink(sink_p), queue(queue_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		const auto start = queue.StartShard();
		idx_t pass = 0;
		JoinBuildRange range;
		while (queue.Next(start, pass, range)) {
			sink.context.InterruptCheck();
			ExecuteHashJoinTableInitTask(sink, range.begin, range.end);
		}
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "HashJoinTableInitTask";
	}

private:
	HashJoinGlobalSinkState &sink;
	JoinBuildQueue &queue;
};

class HashJoinTableInitEvent : public BasePipelineEvent {
public:
	HashJoinTableInitEvent(Pipeline &pipeline_p, HashJoinGlobalSinkState &sink)
	    : BasePipelineEvent(pipeline_p), sink(sink) {
	}

	HashJoinGlobalSinkState &sink;

public:
	vector<shared_ptr<Task>> GetTasks() {
		auto &ht = *sink.hash_table;
		const auto entry_count = ht.capacity;
		auto &context = pipeline->GetClientContext();
		vector<shared_ptr<Task>> finalize_tasks;
		auto num_threads = NumericCast<idx_t>(sink.num_threads);
		// Aim for four ranges per thread, with a minimum range size.
		const idx_t entries_per_task = MaxValue(entry_count / num_threads / 4, MINIMUM_ENTRIES_PER_TASK);
		// With unshifted buckets, partitions share the entire pointer table.
		const auto partitions = ht.GetBucketShift() ? RadixPartitioning::NumberOfPartitions(ht.GetRadixBits()) : 1;
		queue = make_uniq<JoinBuildQueue>(partitions);
		const auto partition_size = entry_count / partitions;
		for (idx_t partition = 0; partition < partitions; partition++) {
			const auto end = (partition + 1) * partition_size;
			for (idx_t begin = partition * partition_size; begin < end; begin += entries_per_task) {
				queue->Add(partition, begin, MinValue(begin + entries_per_task, end));
			}
		}
		const auto tasks = MinValue<idx_t>(queue->Count(), sink.num_threads);
		for (idx_t i = 0; i < tasks; i++) {
			finalize_tasks.push_back(
			    make_uniq<HashJoinTableInitTask>(shared_from_this(), context, sink, sink.op, *queue));
		}
		return finalize_tasks;
	}

	void ExecuteDirectly() {
		ExecuteHashJoinTableInitTask(sink, 0U, sink.hash_table->capacity);
	}

	void Schedule() override {
		SetTasks(GetTasks());
	}
	static constexpr const idx_t MINIMUM_ENTRIES_PER_TASK = 131072;

private:
	unique_ptr<JoinBuildQueue> queue;
};

class HashJoinFinalizeTask : public ExecutorTask {
public:
	HashJoinFinalizeTask(HashJoinGlobalSinkState &sink_p, shared_ptr<Event> event, JoinBuildQueue &queue_p,
	                     bool parallel_p, optional_ptr<PrefixRangeBuildLane> prefix_range_lane_p = nullptr)
	    : ExecutorTask(sink_p.context, std::move(event), sink_p.op), sink(sink_p), parallel(parallel_p),
	      prefix_range_lane(prefix_range_lane_p), queue(queue_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		const auto start = queue.StartShard();
		idx_t pass = 0;
		JoinBuildRange range;
		while (queue.Next(start, pass, range)) {
			sink.context.InterruptCheck();
			ExecuteHashJoinFinalizeTask(sink, range, parallel, prefix_range_lane);
		}
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}
	string TaskType() const override {
		return "HashJoinFinalizeTask";
	}

private:
	HashJoinGlobalSinkState &sink;
	bool parallel;
	optional_ptr<PrefixRangeBuildLane> prefix_range_lane;
	JoinBuildQueue &queue;
};

class HashJoinFinalizeEvent : public BasePipelineEvent {
public:
	HashJoinFinalizeEvent(Pipeline &pipeline_p, HashJoinGlobalSinkState &sink)
	    : BasePipelineEvent(pipeline_p), sink(sink) {
	}

	HashJoinGlobalSinkState &sink;

public:
	vector<shared_ptr<Task>> GetTasks() {
		auto &ht = *sink.hash_table;
		auto &data_collection = ht.GetDataCollection();
		const bool parallel = !FinalizeSingleThreaded(sink) && ht.GetRadixBits() != 0;
		const auto partitions = RadixPartitioning::NumberOfPartitions(ht.GetRadixBits());
		queue = make_uniq<JoinBuildQueue>(partitions);
		if (!parallel) {
			queue->Add(0, 0, data_collection.ChunkCount());
		} else {
			const auto &current_partitions = ht.GetCurrentPartitions();
			for (idx_t partition = 0; partition < partitions; partition++) {
				if (sink.external && !current_partitions.RowIsValidUnsafe(partition)) {
					continue;
				}
				for (const auto &range : data_collection.GetChunkRangesForPartition(partition)) {
					for (auto begin = range.first; begin < range.second; begin += CHUNKS_PER_TASK) {
						queue->Add(partition, begin, MinValue(begin + CHUNKS_PER_TASK, range.second));
					}
				}
			}
		}

		const auto tasks = MinValue<idx_t>(queue->Count(), sink.num_threads);
		prefix_range_lanes.Initialize(ht, tasks);
		vector<shared_ptr<Task>> finalize_tasks;
		finalize_tasks.reserve(tasks);
		for (idx_t task_idx = 0; task_idx < tasks; task_idx++) {
			finalize_tasks.push_back(make_uniq<HashJoinFinalizeTask>(sink, shared_from_this(), *queue, parallel,
			                                                         prefix_range_lanes.Assign(task_idx)));
		}
		return finalize_tasks;
	}

	void ExecuteDirectly() {
		auto &ht = *sink.hash_table;
		prefix_range_lanes.Initialize(ht, 1);
		ExecuteHashJoinFinalizeTask(sink, {0, ht.GetDataCollection().ChunkCount()}, false,
		                            prefix_range_lanes.Assign(0));
		FinishTasks();
	}

	void Schedule() override {
		SetTasks(GetTasks());
	}

	void FinishEvent() override {
		FinishTasks();
	}

	static constexpr idx_t CHUNKS_PER_TASK = 64;

private:
	void FinishTasks() {
		prefix_range_lanes.Merge(*sink.hash_table);
		sink.hash_table->GetDataCollection().VerifyEverythingPinned();

		// Both finalize paths finish writing the chains before reaching here,
		// so dictionary emission is safe on either path
		if (sink.hash_table->CanUseDictionaryEmission(sink.op, sink.external,
		                                              sink.op.children[0].get().estimated_cardinality)) {
			sink.hash_table->BuildDictionaryArrays(sink.op);
		}

		if (sink.global_filter_state) {
			// a finalize-built bloom filter is complete once the tasks are done; a sink-built one is
			// already sealed and this is a no-op
			sink.hash_table->SealBloomFilter();
			PublishDeferredRuntimeFilters(sink.context, *sink.hash_table, *sink.global_filter_state);
		}
		sink.hash_table->MarkFinalized();
	}

	PrefixRangeBuildLanes prefix_range_lanes;
	unique_ptr<JoinBuildQueue> queue;
};

void HashJoinGlobalSinkState::ScheduleFinalize(Pipeline &pipeline, Event &event) {
	if (hash_table->Count() == 0) {
		hash_table->MarkFinalized();
		return;
	}
	hash_table->AllocatePointerTable(!external && !FinalizeSingleThreaded(*this));

	auto new_init_event = make_shared_ptr<HashJoinTableInitEvent>(pipeline, *this);
	if (FinalizeSingleThreaded(*this)) {
		new_init_event->ExecuteDirectly();
		auto new_finalize_event = make_shared_ptr<HashJoinFinalizeEvent>(pipeline, *this);
		new_finalize_event->ExecuteDirectly();
		return;
	}
	event.InsertEvent(new_init_event);

	auto new_finalize_event = make_shared_ptr<HashJoinFinalizeEvent>(pipeline, *this);
	new_init_event->InsertEvent(std::move(new_finalize_event));
}

//! Runs one finalize step over the build chunk ranges of a shared queue.
class HashJoinChunkRangeTask : public ExecutorTask {
public:
	using execute_range_t = std::function<void(const JoinBuildRange &range)>;

	HashJoinChunkRangeTask(HashJoinGlobalSinkState &sink_p, shared_ptr<Event> event, JoinBuildQueue &queue_p,
	                       string task_type_p, execute_range_t execute_range_p)
	    : ExecutorTask(sink_p.context, std::move(event), sink_p.op), sink(sink_p), queue(queue_p),
	      task_type(std::move(task_type_p)), execute_range(std::move(execute_range_p)) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		const auto start = queue.StartShard();
		idx_t pass = 0;
		JoinBuildRange range;
		while (queue.Next(start, pass, range)) {
			sink.context.InterruptCheck();
			execute_range(range);
		}
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return task_type;
	}

private:
	HashJoinGlobalSinkState &sink;
	JoinBuildQueue &queue;
	string task_type;
	execute_range_t execute_range;
};

static unique_ptr<JoinBuildQueue> CreateHashJoinChunkRangeQueue(const HashJoinGlobalSinkState &sink) {
	static constexpr idx_t CHUNKS_PER_TASK = 64;
	auto queue = make_uniq<JoinBuildQueue>(1);
	const auto chunk_count = sink.hash_table->GetDataCollection().ChunkCount();
	for (idx_t begin = 0; begin < chunk_count; begin += CHUNKS_PER_TASK) {
		queue->Add(0, begin, MinValue(begin + CHUNKS_PER_TASK, chunk_count));
	}
	return queue;
}

//! Builds the prefix-range filter of a finalized perfect hash table from the build chunks.
class HashJoinPrefixRangeEvent : public BasePipelineEvent {
public:
	HashJoinPrefixRangeEvent(Pipeline &pipeline_p, HashJoinGlobalSinkState &sink)
	    : BasePipelineEvent(pipeline_p), sink(sink) {
	}

	HashJoinGlobalSinkState &sink;

public:
	void Schedule() override {
		auto &ht = *sink.hash_table;
		queue = CreateHashJoinChunkRangeQueue(sink);
		const auto tasks = MinValue<idx_t>(queue->Count(), sink.num_threads);
		lanes.Initialize(ht, tasks);
		vector<shared_ptr<Task>> prefix_range_tasks;
		for (idx_t task_idx = 0; task_idx < tasks; task_idx++) {
			auto lane = lanes.Assign(task_idx);
			D_ASSERT(lane);
			prefix_range_tasks.push_back(make_uniq<HashJoinChunkRangeTask>(
			    sink, shared_from_this(), *queue, "HashJoinPrefixRangeTask", [this, lane](const JoinBuildRange &range) {
				    sink.hash_table->InsertPrefixRange(range.begin, range.end, *lane->state, lane->task_count > 1);
			    }));
		}
		SetTasks(std::move(prefix_range_tasks));
	}

	void FinishEvent() override {
		lanes.Merge(*sink.hash_table);
		PublishDeferredRuntimeFilters(sink.context, *sink.hash_table, *sink.global_filter_state);
	}

private:
	PrefixRangeBuildLanes lanes;
	unique_ptr<JoinBuildQueue> queue;
};

//! Completes the finalize of a filled perfect hash table: a duplicate build key falls back to the regular hash join,
//! otherwise the runtime filters are registered with the knowledge that the probe uses the perfect hash table.
static void CompletePerfectHashJoinFinalize(Pipeline &pipeline, Event &event, HashJoinGlobalSinkState &sink,
                                            unique_ptr<DataChunk> filter_min_max, bool parallel) {
	auto &op = sink.op;
	auto &ht = *sink.hash_table;
	const auto use_perfect_hash = sink.perfect_join_executor->FinalizePerfectHashTable();
	if (!use_perfect_hash) {
		sink.perfect_join_executor.reset();
	}
	if (filter_min_max) {
		op.filter_pushdown->FinalizeFilters(sink.context, op, std::move(filter_min_max), &ht, !use_perfect_hash, true,
		                                    sink.global_filter_state.get());
		if (use_perfect_hash) {
			if (parallel && ht.ShouldBuildPrefixRangeFilter()) {
				event.InsertEvent(make_shared_ptr<HashJoinPrefixRangeEvent>(pipeline, sink));
			} else {
				ht.BuildPrefixRangeFilter();
				PublishDeferredRuntimeFilters(sink.context, ht, *sink.global_filter_state);
			}
		}
	}
	if (!use_perfect_hash) {
		sink.ScheduleFinalize(pipeline, event);
	}
}

//! Fills the perfect hash table from disjoint build chunk ranges.
class PerfectHashJoinFillEvent : public BasePipelineEvent {
public:
	PerfectHashJoinFillEvent(Pipeline &pipeline_p, HashJoinGlobalSinkState &sink, unique_ptr<DataChunk> filter_min_max)
	    : BasePipelineEvent(pipeline_p), sink(sink), filter_min_max(std::move(filter_min_max)) {
	}

	HashJoinGlobalSinkState &sink;

public:
	void Schedule() override {
		queue = CreateHashJoinChunkRangeQueue(sink);
		const auto tasks = MinValue<idx_t>(queue->Count(), sink.num_threads);
		vector<shared_ptr<Task>> fill_tasks;
		for (idx_t task_idx = 0; task_idx < tasks; task_idx++) {
			fill_tasks.push_back(make_uniq<HashJoinChunkRangeTask>(
			    sink, shared_from_this(), *queue, "PerfectHashJoinFillTask", [this](const JoinBuildRange &range) {
				    sink.perfect_join_executor->FillPerfectHashTable(range.begin, range.end);
			    }));
		}
		SetTasks(std::move(fill_tasks));
	}

	void FinishEvent() override {
		CompletePerfectHashJoinFinalize(*pipeline, *this, sink, std::move(filter_min_max), true);
	}

private:
	unique_ptr<DataChunk> filter_min_max;
	unique_ptr<JoinBuildQueue> queue;
};

void HashJoinGlobalSinkState::InitializeProbeSpill() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (!probe_spill) {
		probe_spill = make_uniq<JoinHashTable::ProbeSpill>(*hash_table, context, probe_types);
	}
}

//! Computes and registers the runtime filters of an external join. Both external paths call this exactly once,
//! after the whole build side has been merged into the global hash table and before the finalize event is
//! scheduled. Every input is accumulated during Sink, so it does not matter which partitions are materialized.
static void FinalizeExternalRuntimeFilters(ClientContext &context, const PhysicalHashJoin &op,
                                           HashJoinGlobalSinkState &sink) {
	if (!op.filter_pushdown || sink.skip_filter_pushdown) {
		return;
	}
	auto &ht = *sink.hash_table;
	if (ht.GetSinkCollection().Count() == 0) {
		return;
	}
	auto filter_min_max = op.filter_pushdown->FinalizeMinMax(*sink.global_filter_state);
	// prefix-range filters are built from the materialized hash table, which only ever holds one round
	op.filter_pushdown->FinalizeFilters(context, op, std::move(filter_min_max), &ht, true, false,
	                                    sink.global_filter_state.get());
}

class HashJoinRepartitionTask : public ExecutorTask {
public:
	HashJoinRepartitionTask(shared_ptr<Event> event_p, ClientContext &context, JoinHashTable &global_ht,
	                        JoinHashTable &local_ht, const PhysicalOperator &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), global_ht(global_ht), local_ht(local_ht) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		local_ht.Repartition(global_ht);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "HashJoinRepartitionTask";
	}

private:
	JoinHashTable &global_ht;
	JoinHashTable &local_ht;
};

static idx_t RepartitionMemoryPerPartition(const vector<reference<JoinHashTable>> &local_hts, idx_t block_size) {
	idx_t total_size = 0;
	idx_t total_count = 0;
	for (auto &local_ht : local_hts) {
		auto &collection = local_ht.get().GetSinkCollection();
		total_size += collection.SizeInBytes();
		total_count += collection.Count();
	}
	const auto total_blocks = MaxValue<idx_t>((total_size + block_size - 1) / block_size, 1);
	const auto count_per_block = MaxValue<idx_t>(total_count / total_blocks, 1);
	const auto blocks_per_vector = MaxValue<idx_t>(STANDARD_VECTOR_SIZE / count_per_block, 2);
	// Input and output pin states coexist while splitting one old partition.
	return 2 * blocks_per_vector * block_size;
}

class HashJoinRepartitionEvent : public BasePipelineEvent {
public:
	HashJoinRepartitionEvent(Pipeline &pipeline_p, const PhysicalHashJoin &op_p, HashJoinGlobalSinkState &sink,
	                         vector<reference<JoinHashTable>> &local_hts)
	    : BasePipelineEvent(pipeline_p), op(op_p), sink(sink), local_hts(local_hts) {
	}

	const PhysicalHashJoin &op;
	HashJoinGlobalSinkState &sink;
	vector<reference<JoinHashTable>> &local_hts;

public:
	void Schedule() override {
		D_ASSERT(!local_hts.empty());
		const auto radix_bits_before = local_hts[0].get().GetRadixBits();
		D_ASSERT(sink.hash_table->GetRadixBits() > radix_bits_before);
		const auto block_size = sink.hash_table->buffer_manager.GetBlockAllocSize();
		auto partition_multiplier =
		    RadixPartitioning::NumberOfPartitions(sink.hash_table->GetRadixBits() - radix_bits_before);
		auto thread_memory = RepartitionMemoryPerPartition(local_hts, block_size) * partition_multiplier;
		auto repartition_threads = sink.temporary_memory_state->GetReservation() / thread_memory;
		D_ASSERT(repartition_threads > 0);

		if (repartition_threads < local_hts.size()) {
			// Limit the number of threads working on repartitioning based on our memory reservation
			for (idx_t thread_idx = repartition_threads; thread_idx < local_hts.size(); thread_idx++) {
				local_hts[thread_idx % repartition_threads].get().Merge(local_hts[thread_idx].get());
			}
			auto repartition_end =
			    local_hts.begin() + static_cast<vector<reference<JoinHashTable>>::difference_type>(repartition_threads);
			local_hts.erase(repartition_end, local_hts.end());
		}

		auto &context = pipeline->GetClientContext();

		vector<shared_ptr<Task>> partition_tasks;
		partition_tasks.reserve(local_hts.size());
		for (auto &local_ht : local_hts) {
			partition_tasks.push_back(
			    make_uniq<HashJoinRepartitionTask>(shared_from_this(), context, *sink.hash_table, local_ht.get(), op));
		}
		SetTasks(std::move(partition_tasks));
	}

	void FinishEvent() override {
		local_hts.clear();
		sink.owned_local_hash_tables.clear();

		// Minimum reservation is now the new smallest partition size
		const auto num_partitions = RadixPartitioning::NumberOfPartitions(sink.hash_table->GetRadixBits());
		vector<idx_t> partition_sizes(num_partitions, 0);
		vector<idx_t> partition_counts(num_partitions, 0);
		sink.total_size = sink.hash_table->GetTotalSize(partition_sizes, partition_counts, sink.max_partition_size,
		                                                sink.max_partition_count);
		sink.UpdateProbeSideRequirement();

		sink.temporary_memory_state->SetMinimumReservation(sink.max_partition_size +
		                                                   sink.hash_table->PointerTableSize(sink.max_partition_count) +
		                                                   sink.probe_side_requirement);
		sink.temporary_memory_state->UpdateReservation(executor.context.GetContext());

		FinalizeExternalRuntimeFilters(sink.context, op, sink);

		sink.hash_table->PrepareExternalFinalize(sink.GetMaxExternalHTSize());
		sink.ScheduleFinalize(*pipeline, *this);
	}
};

bool JoinFilterPushdownInfo::CanUseInFilter(const ClientContext &context, optional_ptr<JoinHashTable> ht,
                                            const ExpressionType &cmp) const {
	auto dynamic_or_filter_threshold = Settings::Get<DynamicOrFilterThresholdSetting>(context);
	return ht && ht->Count() > 1 && ht->Count() <= dynamic_or_filter_threshold &&
	       (cmp == ExpressionType::COMPARE_EQUAL || cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM);
}

bool JoinFilterPushdownInfo::CanUseBloomFilter(const JoinFilterPushdownFilter &info, const ClientContext &context,
                                               const PhysicalComparisonJoin &op, const ExpressionType &cmp,
                                               optional_ptr<JoinHashTable> ht) const {
	if (!ht) {
		return false;
	}

	// bf is only supported for equality conditions
	if (cmp != ExpressionType::COMPARE_EQUAL && cmp != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		return false;
	}

	// and only if there is exactly one equality condition
	// as the Filter API only allows single-column filters so far
	idx_t equality_column_count = 0;
	for (auto &cond : ht->conditions) {
		const auto cond_cmp = cond.GetComparisonType();
		if (cond_cmp == ExpressionType::COMPARE_EQUAL || cond_cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			equality_column_count++;
		}
	}
	if (equality_column_count != 1) {
		return false;
	}

	auto build_count = ht->Count();
	if (build_count == 0) {
		build_count = ht->GetSinkCollection().Count();
	}
	auto probe_cardinality = op.children[0].get().estimated_cardinality;
	if (build_side_has_filter) {
		probe_cardinality = MaxValue(info.estimated_scan_cardinality, probe_cardinality);
	}
	const double build_to_probe_ratio = static_cast<double>(build_count) / static_cast<double>(probe_cardinality);
	if (build_to_probe_ratio > BUILD_TO_PROBE_RATIO_THRESHOLD) {
		return false;
	}

	// if we have a build side without a filter, only build the bloom filter if the ratio/size is small enough
	if (!build_side_has_filter && build_to_probe_ratio > NON_FILTERING_RATIO_THRESHOLD &&
	    build_count > NON_FILTERING_BUILD_SIDE_THRESHOLD) {
		return false;
	}

	return true;
}

bool JoinFilterPushdownInfo::CanUsePrefixRangeFilter(const JoinFilterPushdownFilter &info, const ClientContext &context,
                                                     const PhysicalComparisonJoin &op, optional_ptr<JoinHashTable> ht,
                                                     const ExpressionType &cmp) const {
	if (!CanUseBloomFilter(info, context, op, cmp, ht)) {
		return false;
	}
	const auto &key_type = ht->conditions[0].GetLHS().GetReturnType();
	return PrefixRangeFilter::SupportedType(key_type);
}

static unique_ptr<ExpressionFilter> CreateSelectivityOptionalExpressionFilter(unique_ptr<Expression> child_expr,
                                                                              const LogicalType &column_type,
                                                                              SelectivityOptionalFilterType type);

struct RuntimeFilterInput {
	unique_ptr<Expression> expression;
	bool preserves_cast_errors;
};

static RuntimeFilterInput CreateRuntimeFilterInputExpression(ClientContext &context,
                                                             const JoinFilterPushdownColumn &column,
                                                             const LogicalType &runtime_type) {
	bool preserves_cast_errors = false;
	auto expression = RuntimeFilterCastUtil::CreateRuntimeFilterInputExpression(context, column, preserves_cast_errors);
	D_ASSERT(expression->GetReturnType() == RuntimeFilterCastUtil::GetRuntimeFilterInputType(column, runtime_type));
	return {std::move(expression), preserves_cast_errors};
}

static unique_ptr<Expression> RetainRuntimeFilterNulls(unique_ptr<Expression> filter_expr,
                                                       unique_ptr<Expression> null_check_input) {
	if (!null_check_input) {
		return filter_expr;
	}
	// Retain NULL join keys and inputs whose original cast can fail.
	auto null_check =
	    ExpressionFilter::CreateNullCheckExpression(std::move(null_check_input), ExpressionType::OPERATOR_IS_NULL);
	return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(null_check),
	                                             std::move(filter_expr));
}

void JoinFilterPushdownInfo::DeferRuntimeFilter(DeferredRuntimeFilterType type, const PhysicalOperator &op,
                                                const JoinFilterPushdownFilter &info,
                                                const JoinFilterPushdownColumn &column, ProjectionIndex filter_col_idx,
                                                JoinFilterGlobalState &gstate) const {
	gstate.deferred_runtime_filters.push_back({type, &op, info.dynamic_filters, column, filter_col_idx});
}

bool JoinFilterPushdownInfo::TryRegisterPrefixRangeFilter(const JoinFilterPushdownFilter &info, ClientContext &context,
                                                          JoinHashTable &ht, const PhysicalOperator &op,
                                                          const JoinFilterPushdownColumn &column,
                                                          ProjectionIndex filter_col_idx, const Value &min_val,
                                                          const Value &max_val, idx_t max_bits,
                                                          JoinFilterGlobalState &gstate) const {
	if (!ht.GetPrefixRangeFilter()) {
		const auto key_type = ht.conditions[0].GetLHS().GetReturnType();
		auto prefix_filter = PrefixRangeFilter::CreatePrefixRangeFilter(key_type);
		prefix_filter->Initialize(context, ht.Count(), min_val, max_val, max_bits);
		ht.SetPrefixRangeFilter(std::move(prefix_filter));
		ht.SetBuildPrefixRangeFilter();
	}

	DeferRuntimeFilter(DeferredRuntimeFilterType::PREFIX_RANGE, op, info, column, filter_col_idx, gstate);
	return true;
}

unique_ptr<DataChunk> JoinFilterPushdownInfo::FinalizeMinMax(JoinFilterGlobalState &gstate) const {
	// finalize the min/max aggregates
	vector<LogicalType> min_max_types;
	for (auto &aggr_expr : min_max_aggregates) {
		min_max_types.push_back(aggr_expr->GetReturnType());
	}
	auto final_min_max = make_uniq<DataChunk>();
	final_min_max->Initialize(Allocator::DefaultAllocator(), min_max_types);

	gstate.global_aggregate_state->Finalize(*final_min_max);
	return final_min_max;
}

static unique_ptr<ExpressionFilter> CreateSelectivityOptionalExpressionFilter(unique_ptr<Expression> child_expr,
                                                                              const LogicalType &column_type,
                                                                              SelectivityOptionalFilterType type) {
	float selectivity_threshold;
	idx_t n_vectors_to_check;
	GetThresholdAndVectorsToCheck(type, selectivity_threshold, n_vectors_to_check);
	return make_uniq<ExpressionFilter>(CreateSelectivityOptionalFilterExpression(
	    std::move(child_expr), column_type, selectivity_threshold, n_vectors_to_check));
}

static SelectivityOptionalFilterType GetSelectivityOptionalFilterType(DeferredRuntimeFilterType type) {
	switch (type) {
	case DeferredRuntimeFilterType::BLOOM_FILTER:
		return SelectivityOptionalFilterType::BF;
	case DeferredRuntimeFilterType::PREFIX_RANGE:
		return SelectivityOptionalFilterType::PRF;
	default:
		throw InternalException("Unknown deferred runtime filter type");
	}
}

static const char *GetRuntimeFilterTypeName(DeferredRuntimeFilterType type) {
	switch (type) {
	case DeferredRuntimeFilterType::BLOOM_FILTER:
		return "BLOOM_FILTER";
	case DeferredRuntimeFilterType::PREFIX_RANGE:
		return "PREFIX_RANGE";
	default:
		throw InternalException("Unknown deferred runtime filter type");
	}
}

static const char *GetRuntimeFilterReconstructionModeName(JoinFilterPushdownMode mode) {
	switch (mode) {
	case JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION:
		return "RECONSTRUCT_EXPRESSION";
	case JoinFilterPushdownMode::STORAGE_ONLY:
		return "STORAGE_ONLY";
	default:
		throw InternalException("Unknown join filter pushdown mode");
	}
}

static unique_ptr<Expression> CreateRuntimeFilterExpression(ClientContext &context, JoinHashTable &ht,
                                                            const DeferredRuntimeFilterPushdown &deferred) {
	const auto key_name = ht.conditions[0].GetRHS().ToString();
	const auto key_type = ht.conditions[0].GetLHS().GetReturnType();
	auto filter_input_type = RuntimeFilterCastUtil::GetRuntimeFilterInputType(deferred.column, key_type);
	auto input = CreateRuntimeFilterInputExpression(context, deferred.column, key_type);
	const auto filters_null_values = !input.preserves_cast_errors && !ht.NullValuesAreEqual(0);
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(input.expression));

	unique_ptr<Expression> filter_expr;
	switch (deferred.type) {
	case DeferredRuntimeFilterType::BLOOM_FILTER: {
		// a filter that is not sealed does not describe the complete build side - publishing it would drop
		// rows - and a filter that saturated is not worth the hashing it costs the scan
		if (!ht.HasPublishableBloomFilter()) {
			return nullptr;
		}
		auto bloom_filter = ht.GetBloomFilter();
		// the filter escapes to probe-side scans here, so a later recursive iteration must not reuse it
		bloom_filter->MarkPublished();
		filter_expr = make_uniq<BoundFunctionExpression>(
		    BoundScalarFunction(BloomFilterScalarFun::GetFunction(filter_input_type)), std::move(children),
		    make_uniq<BloomFilterFunctionData>(std::move(bloom_filter), filters_null_values, key_name, key_type));
		break;
	}
	case DeferredRuntimeFilterType::PREFIX_RANGE: {
		auto prefix_range_filter = ht.GetPrefixRangeFilter();
		D_ASSERT(prefix_range_filter && prefix_range_filter->IsInitialized());
		if (!prefix_range_filter || !prefix_range_filter->IsInitialized()) {
			return nullptr;
		}
		filter_expr = make_uniq<BoundFunctionExpression>(
		    BoundScalarFunction(PrefixRangeScalarFun::GetFunction(filter_input_type)), std::move(children),
		    make_uniq<PrefixRangeFunctionData>(prefix_range_filter, filters_null_values, key_name, key_type,
		                                       ht.GetRuntimeFilterIdentity()));
		break;
	}
	default:
		throw InternalException("Unknown deferred runtime filter type");
	}
	return filter_expr;
}

static void PublishDeferredRuntimeFilters(ClientContext &context, JoinHashTable &ht, JoinFilterGlobalState &gstate) {
	if (gstate.deferred_runtime_filters.empty()) {
		return;
	}
	for (auto &deferred : gstate.deferred_runtime_filters) {
		D_ASSERT(deferred.op);
		if (!deferred.op || !deferred.dynamic_filters) {
			continue;
		}
		const auto filter_type = GetSelectivityOptionalFilterType(deferred.type);
		auto filter_expr = CreateRuntimeFilterExpression(context, ht, deferred);
		if (!filter_expr) {
			continue;
		}
		deferred.dynamic_filters->PushFilter(*deferred.op, deferred.filter_col_idx,
		                                     CreateSelectivityOptionalExpressionFilter(
		                                         std::move(filter_expr), deferred.column.storage_type, filter_type));
		DUCKDB_LOG(context, PhysicalOperatorLogType, *deferred.op, "PhysicalHashJoin", "PublishRuntimeFilter",
		           {{"kind", GetRuntimeFilterTypeName(deferred.type)},
		            {"storage_type", deferred.column.storage_type.ToString()},
		            {"reconstruction_mode", GetRuntimeFilterReconstructionModeName(deferred.column.mode)},
		            {"uses_try_cast", to_string(RuntimeFilterCastUtil::RuntimeFilterUsesTryCast(deferred.column))}});
	}
	gstate.deferred_runtime_filters.clear();
}

static void CreateDynamicMinMaxFilter(const PhysicalComparisonJoin &op, const JoinFilterPushdownFilter &info,
                                      const ProjectionIndex &filter_col_idx, unique_ptr<Expression> filter_expr,
                                      const LogicalType &column_type, bool selectivity_optional) {
	if (!filter_expr) {
		return;
	}
	if (selectivity_optional) {
		info.dynamic_filters->PushFilter(
		    op, filter_col_idx,
		    CreateSelectivityOptionalExpressionFilter(std::move(filter_expr), column_type,
		                                              SelectivityOptionalFilterType::MIN_MAX));
	} else {
		info.dynamic_filters->PushFilter(
		    op, filter_col_idx,
		    make_uniq<ExpressionFilter>(CreateOptionalFilterExpression(std::move(filter_expr), column_type)));
	}
}

static unique_ptr<Expression> CreateComparisonExpressionFilter(ExpressionType comparison_type,
                                                               unique_ptr<Expression> input, const Value &constant,
                                                               const LogicalType &comparison_logical_type) {
	auto constant_value = constant.DefaultTryCastAs(comparison_logical_type);
	if (!constant_value) {
		return nullptr;
	}
	return BoundComparisonExpression::Create(comparison_type, std::move(input),
	                                         make_uniq<BoundConstantExpression>(std::move(*constant_value)));
}

static unique_ptr<Expression> CreateComparisonExpressionFilter(ExpressionType comparison_type, const Value &constant,
                                                               const LogicalType &column_type) {
	auto column = make_uniq<BoundReferenceExpression>(column_type, 0ULL);
	return CreateComparisonExpressionFilter(comparison_type, std::move(column), constant, column_type);
}

//! Cast the build-side bounds into the probe column's storage domain. Join-filter aggregates are evaluated in the
//! join-key domain while min/max filters are evaluated on raw storage values. When a bound is not representable in an
//! integral storage domain, intersect the two domains instead of dropping the filter: build keys outside the storage
//! domain cannot match any stored value, so they must not suppress the filter for the representable range.
static bool TryCastJoinFilterBoundsToStorage(const LogicalType &storage_type, const Value &min_val,
                                             const Value &max_val, Value &storage_min, Value &storage_max) {
	auto cast_min = min_val.DefaultTryCastAs(storage_type);
	auto cast_max = max_val.DefaultTryCastAs(storage_type);
	if (cast_min && cast_max) {
		storage_min = std::move(*cast_min);
		storage_max = std::move(*cast_max);
		return true;
	}
	if (min_val.IsNull() || max_val.IsNull() || !storage_type.IsIntegral() || !min_val.type().IsIntegral() ||
	    !max_val.type().IsIntegral()) {
		return false;
	}
	auto source_min = min_val.DefaultTryCastAs(LogicalType::HUGEINT);
	auto source_max = max_val.DefaultTryCastAs(LogicalType::HUGEINT);
	auto domain_min = Value::MinimumValue(storage_type).DefaultTryCastAs(LogicalType::HUGEINT);
	auto domain_max = Value::MaximumValue(storage_type).DefaultTryCastAs(LogicalType::HUGEINT);
	if (!source_min || !source_max || !domain_min || !domain_max) {
		return false;
	}
	if (*source_max < *domain_min || *source_min > *domain_max) {
		// no build key is representable in the storage domain
		return false;
	}
	const auto &clamped_min = *source_min < *domain_min ? *domain_min : *source_min;
	const auto &clamped_max = *source_max > *domain_max ? *domain_max : *source_max;
	cast_min = clamped_min.DefaultTryCastAs(storage_type);
	cast_max = clamped_max.DefaultTryCastAs(storage_type);
	if (!cast_min || !cast_max) {
		return false;
	}
	storage_min = std::move(*cast_min);
	storage_max = std::move(*cast_max);
	return true;
}

static unique_ptr<Expression> CreateJoinFilterComparisonExpression(
    ClientContext &context, const JoinFilterPushdownColumn &column, ExpressionType comparison_type,
    const Value &constant, const LogicalType &comparison_logical_type, bool reconstruct_expression, bool retain_nulls) {
	if (!reconstruct_expression) {
		D_ASSERT(!retain_nulls);
		return CreateComparisonExpressionFilter(comparison_type, constant, comparison_logical_type);
	}
	auto input = CreateRuntimeFilterInputExpression(context, column, comparison_logical_type);
	auto input_type = input.expression->GetReturnType();
	unique_ptr<Expression> null_check_input;
	if (input.preserves_cast_errors || retain_nulls) {
		null_check_input = input.expression->Copy();
	}
	auto filter_expr =
	    CreateComparisonExpressionFilter(comparison_type, std::move(input.expression), constant, input_type);
	if (!filter_expr) {
		return nullptr;
	}
	return RetainRuntimeFilterNulls(std::move(filter_expr), std::move(null_check_input));
}

bool JoinFilterPushdownInfo::PushInFilter(ClientContext &context, const JoinFilterPushdownFilter &info,
                                          const JoinFilterPushdownColumn &column, JoinHashTable &ht,
                                          const PhysicalOperator &op, idx_t filter_idx,
                                          ProjectionIndex filter_col_idx) const {
	// Generate an IN filter by scanning the build-side key column.
	auto build_idx = join_condition[filter_idx];
	const auto &runtime_type = ht.conditions[build_idx].GetLHS().GetReturnType();
	Vector tuples_addresses(LogicalType::POINTER, ht.Count());
	Vector build_vector(ht.layout_ptr->GetTypes()[build_idx], ht.Count());
	auto key_count = ht.ScanKeyColumn(tuples_addresses, build_vector, build_idx);
	if (key_count == 0) {
		return false;
	}

	const auto retain_nulls = ht.conditions[build_idx].GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	const auto reconstruct_expression =
	    retain_nulls || RuntimeFilterCastUtil::RequiresRuntimeFilterExpressionReconstruction(column, runtime_type);
	auto filter_input_type = column.storage_type;
	unique_ptr<Expression> filter_input;
	unique_ptr<Expression> null_check_input;
	if (reconstruct_expression) {
		auto input = CreateRuntimeFilterInputExpression(context, column, runtime_type);
		filter_input_type = input.expression->GetReturnType();
		if (input.preserves_cast_errors || retain_nulls) {
			null_check_input = input.expression->Copy();
		}
		filter_input = std::move(input.expression);
	} else {
		filter_input = make_uniq<BoundReferenceExpression>(column.storage_type, idx_t(0));
	}

	value_set_t unique_ht_values;
	for (idx_t k = 0; k < key_count; k++) {
		auto value = build_vector.GetValue(k).DefaultTryCastAs(filter_input_type);
		if (!value) {
			return false;
		}
		if (retain_nulls && value->IsNull()) {
			continue;
		}
		unique_ht_values.insert(std::move(*value));
	}
	vector<Value> in_list(unique_ht_values.begin(), unique_ht_values.end());

	// An IN filter is only useful when the values are neither null nor dense.
	if (in_list.empty() || FilterCombiner::ContainsNull(in_list) || FilterCombiner::IsDenseRange(in_list)) {
		return false;
	}

	auto in_expr = ExpressionFilter::CreateInExpression(std::move(filter_input), std::move(in_list));
	in_expr = RetainRuntimeFilterNulls(std::move(in_expr), std::move(null_check_input));
	auto filter = make_uniq<ExpressionFilter>(CreateOptionalFilterExpression(std::move(in_expr), column.storage_type));
	info.dynamic_filters->PushFilter(op, filter_col_idx, std::move(filter));
	return true;
}

static void CreateDynamicMinMaxFilters(const PhysicalComparisonJoin &op, const JoinFilterPushdownFilter &info,
                                       ClientContext &context, const JoinFilterPushdownColumn &column,
                                       ProjectionIndex filter_col_idx, ExpressionType cmp, const Value &min_val,
                                       const Value &max_val, const LogicalType &condition_type,
                                       bool reconstruct_expression, bool selectivity_optional) {
	auto filter_column_type = reconstruct_expression ? column.storage_type : condition_type;
	const auto retain_nulls = cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	switch (cmp) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		CreateDynamicMinMaxFilter(
		    op, info, filter_col_idx,
		    CreateJoinFilterComparisonExpression(context, column, ExpressionType::COMPARE_GREATERTHANOREQUALTO, min_val,
		                                         condition_type, reconstruct_expression, retain_nulls),
		    filter_column_type, selectivity_optional);
		break;
	}
	default:
		break;
	}
	switch (cmp) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		CreateDynamicMinMaxFilter(
		    op, info, filter_col_idx,
		    CreateJoinFilterComparisonExpression(context, column, ExpressionType::COMPARE_LESSTHANOREQUALTO, max_val,
		                                         condition_type, reconstruct_expression, retain_nulls),
		    filter_column_type, selectivity_optional);
		break;
	}
	default:
		break;
	}
}

static idx_t BloomFilterBitBudget(idx_t ht_count) {
	return BloomFilter::GetNumberOfSectors(ht_count) * 64;
}

unique_ptr<DataChunk> JoinFilterPushdownInfo::FinalizeFilters(ClientContext &context, const PhysicalComparisonJoin &op,
                                                              unique_ptr<DataChunk> final_min_max,
                                                              optional_ptr<JoinHashTable> ht, bool allow_bloom_filters,
                                                              bool allow_prefix_range_filters,
                                                              optional_ptr<JoinFilterGlobalState> gstate) const {
	if (probe_info.empty()) {
		return final_min_max; // There are no table sources in which we can push down filters
	}

	// create a filter for each column that reached a table scan
	for (auto &info : probe_info) {
		for (auto &pushdown_column : info.columns) {
			auto filter_idx = pushdown_column.join_filter_idx;
			D_ASSERT(filter_idx != DConstants::INVALID_INDEX);
			D_ASSERT(filter_idx < join_condition.size());
			const auto cmp = op.conditions[join_condition[filter_idx]].GetComparisonType();
			auto &filter_col_idx = pushdown_column.probe_column_index.column_index;
			auto min_idx = filter_idx * 2;
			auto max_idx = min_idx + 1;

			auto min_val_before_cast = final_min_max->data[min_idx].GetValue(0);
			auto max_val_before_cast = final_min_max->data[max_idx].GetValue(0);

			auto min_val = min_val_before_cast;
			auto max_val = max_val_before_cast;
			const bool retain_nulls = cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			const bool reconstruct_filter_expression =
			    retain_nulls || RuntimeFilterCastUtil::RequiresRuntimeFilterExpressionReconstruction(
			                        pushdown_column, min_val_before_cast.type());

			// Cast to storage type (intersecting integral domains), skip if no build key is representable
			if (pushdown_column.storage_type.IsValid() && !reconstruct_filter_expression) {
				if (!TryCastJoinFilterBoundsToStorage(pushdown_column.storage_type, min_val_before_cast,
				                                      max_val_before_cast, min_val, max_val)) {
					continue;
				}
			}

			if (min_val.IsNull() || max_val.IsNull()) {
				// min/max is NULL
				// this can happen in case all values in the RHS column are NULL, but they are still pushed into the
				// hash table e.g. because they are part of a RIGHT join
				continue;
			}

			auto condition_type = min_val.type();
			auto runtime_filter_input_type =
			    RuntimeFilterCastUtil::GetRuntimeFilterInputType(pushdown_column, condition_type);
			bool can_emit_runtime_filters = pushdown_column.mode == JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION;
			if (can_emit_runtime_filters && ht) {
				can_emit_runtime_filters = runtime_filter_input_type == ht->conditions[0].GetLHS().GetReturnType();
			}

			if (Value::NotDistinctFrom(min_val, max_val)) {
				// min = max - single value
				// generate a "one-sided" comparison filter for the LHS
				// Note that this also works for equalities.
				auto filter_expr =
				    CreateJoinFilterComparisonExpression(context, pushdown_column, cmp, min_val, condition_type,
				                                         reconstruct_filter_expression, retain_nulls);
				if (filter_expr) {
					info.dynamic_filters->PushFilter(op, filter_col_idx,
					                                 make_uniq<ExpressionFilter>(std::move(filter_expr)));
				}
			} else {
				if (cmp != ExpressionType::COMPARE_EQUAL && !retain_nulls) {
					// min != max - generate range filters for non-equality comparisons.
					// For non-equalities, the range must be half-open.
					CreateDynamicMinMaxFilters(op, info, context, pushdown_column, filter_col_idx, cmp, min_val,
					                           max_val, condition_type, reconstruct_filter_expression, true);
					continue;
				}

				uhugeint_t span;
				const auto can_compute_span =
				    PrefixRangeFilter::TryComputeSpan(min_val_before_cast, max_val_before_cast, span);
				const auto can_emit_prf = allow_prefix_range_filters && can_emit_runtime_filters && gstate &&
				                          CanUsePrefixRangeFilter(info, context, op, ht, cmp) && can_compute_span;

				bool pushed_in_filter = false;
				if (CanUseInFilter(context, ht, cmp)) {
					pushed_in_filter =
					    PushInFilter(context, info, pushdown_column, *ht, op, filter_idx, filter_col_idx);
				}

				static constexpr idx_t SMALL_EXACT_PRF_BITS = 1ULL << 26;
				if (can_emit_prf && span < SMALL_EXACT_PRF_BITS &&
				    TryRegisterPrefixRangeFilter(info, context, *ht, op, pushdown_column, filter_col_idx,
				                                 min_val_before_cast, max_val_before_cast, SMALL_EXACT_PRF_BITS,
				                                 *gstate)) {
					continue;
				}

				if (can_emit_prf) {
					auto build_count = ht->Count();
					if (build_count == 0) {
						build_count = ht->GetSinkCollection().Count();
					}
					const auto bloom_filter_bits = BloomFilterBitBudget(build_count);
					if (span <= bloom_filter_bits &&
					    TryRegisterPrefixRangeFilter(info, context, *ht, op, pushdown_column, filter_col_idx,
					                                 min_val_before_cast, max_val_before_cast, bloom_filter_bits,
					                                 *gstate)) {
						continue;
					}
				}

				if (!pushed_in_filter) {
					CreateDynamicMinMaxFilters(op, info, context, pushdown_column, filter_col_idx, cmp, min_val,
					                           max_val, condition_type, reconstruct_filter_expression, false);
				}
				if (allow_bloom_filters && can_emit_runtime_filters && ht && gstate &&
				    CanUseBloomFilter(info, context, op, cmp, ht)) {
					if (ht->HasPublishableBloomFilter()) {
						// a complete filter was accumulated during Sink (external joins build it there)
						DeferRuntimeFilter(DeferredRuntimeFilterType::BLOOM_FILTER, op, info, pushdown_column,
						                   filter_col_idx, *gstate);
					} else if (allow_prefix_range_filters) {
						// like a prefix-range filter, a bloom filter may be built during finalize when the
						// finalize materializes the whole build side - an external finalize does not
						ht->SetBuildBloomFilter(true);
						DeferRuntimeFilter(DeferredRuntimeFilterType::BLOOM_FILTER, op, info, pushdown_column,
						                   filter_col_idx, *gstate);
					}
				}
			}
		}
	}
	return final_min_max;
}

unique_ptr<DataChunk> JoinFilterPushdownInfo::Finalize(ClientContext &context, JoinFilterGlobalState &gstate,
                                                       const PhysicalComparisonJoin &op,
                                                       optional_ptr<JoinHashTable> ht) const {
	auto final_min_max = FinalizeMinMax(gstate);
	return FinalizeFilters(context, op, std::move(final_min_max), ht, true, true, &gstate);
}

SinkFinalizeType PhysicalHashJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	auto &sink = input.global_state.Cast<HashJoinGlobalSinkState>();
	auto &ht = *sink.hash_table;

	// the filter covers the whole build side once Sink is done
	ht.SealBloomFilter();

	sink.temporary_memory_state->UpdateReservation(context.GetContext());
	sink.external = sink.temporary_memory_state->GetReservation() < sink.total_size;
	if (sink.external) {
		// For external join we reduce the load factor, this may even prevent the external join altogether
		ht.load_factor = JoinHashTable::EXTERNAL_LOAD_FACTOR;

		idx_t temp_max_partition_size;
		idx_t temp_max_partition_count;
		idx_t temp_total_size =
		    ht.GetTotalSize(sink.local_hash_tables, temp_max_partition_size, temp_max_partition_count);

		if (temp_total_size < sink.temporary_memory_state->GetReservation()) {
			// We prevented the external join by reducing the load factor. Update the state accordingly
			sink.temporary_memory_state->SetMinimumReservation(temp_total_size);
			sink.temporary_memory_state->SetRemainingSizeAndUpdateReservation(context.GetContext(), temp_total_size);

			sink.total_size = temp_total_size;
			sink.max_partition_size = temp_max_partition_size;
			sink.max_partition_count = temp_max_partition_count;

			sink.external = false;
		}
	}
	DUCKDB_LOG(context, PhysicalOperatorLogType, *this, "PhysicalHashJoin", "Finalize",
	           {{"external", to_string(sink.external)}});
	if (sink.external) {
		// External Hash Join
		// Recursive preserved-build reuse only applies to the in-memory finalized HT. External hash join
		// runs through repeated build/probe rounds with partition/probe-spill state, so it cannot be
		// treated as a stable materialized build that later recursive iterations may skip entirely.
		sink.preserve_build_for_reuse = false;
		sink.perfect_join_executor.reset();

		const auto max_partition_ht_size = sink.max_partition_size + ht.PointerTableSize(sink.max_partition_count);
		const auto very_very_skewed = // No point in repartitioning if it's this skewed
		    static_cast<double>(max_partition_ht_size) >= 0.8 * static_cast<double>(sink.total_size);
		const auto radix_bits_before = ht.GetRadixBits();
		if (!very_very_skewed &&
		    (max_partition_ht_size + sink.probe_side_requirement) > sink.temporary_memory_state->GetReservation() &&
		    ht.SetRepartitionRadixBits(
		        sink.GetMaxExternalHTSize(), sink.max_partition_size, sink.max_partition_count,
		        RepartitionMemoryPerPartition(sink.local_hash_tables, ht.buffer_manager.GetBlockAllocSize()))) {
			// We have to repartition
			DUCKDB_LOG(context, PhysicalOperatorLogType, *this, "PhysicalHashJoin", "Repartition",
			           {{"partitions_before", to_string(RadixPartitioning::NumberOfPartitions(radix_bits_before))},
			            {"partitions_after", to_string(RadixPartitioning::NumberOfPartitions(ht.GetRadixBits()))}});
			auto new_event = make_shared_ptr<HashJoinRepartitionEvent>(pipeline, *this, sink, sink.local_hash_tables);
			event.InsertEvent(std::move(new_event));
		} else {
			// No repartitioning!
			for (auto &local_ht : sink.local_hash_tables) {
				ht.Merge(local_ht.get());
			}
			sink.local_hash_tables.clear();
			sink.owned_local_hash_tables.clear();
			FinalizeExternalRuntimeFilters(context, *this, sink);
			sink.hash_table->PrepareExternalFinalize(sink.GetMaxExternalHTSize());
			sink.ScheduleFinalize(pipeline, event);
		}
		sink.finalized = true;
		return SinkFinalizeType::READY;
	}

	// In-memory Hash Join
	for (auto &local_ht : sink.local_hash_tables) {
		ht.Merge(local_ht.get());
	}
	sink.local_hash_tables.clear();
	sink.owned_local_hash_tables.clear();
	ht.Unpartition();

	Value min;
	Value max;
	unique_ptr<DataChunk> filter_min_max = nullptr;

	if (filter_pushdown && !sink.skip_filter_pushdown && ht.Count() > 0) {
		filter_min_max = filter_pushdown->FinalizeMinMax(*sink.global_filter_state);
		min = filter_min_max->data[0].GetValue(0);
		max = filter_min_max->data[1].GetValue(0);
	} else if (sink.collect_perfect_join_bounds && sink.perfect_join_bounds.has_value) {
		min = sink.perfect_join_bounds.Min();
		max = sink.perfect_join_bounds.Max();
	} else if (TypeIsIntegral(conditions[0].GetRHS().GetReturnType().InternalType())) {
		min = Value::MinimumValue(conditions[0].GetRHS().GetReturnType());
		max = Value::MaximumValue(conditions[0].GetRHS().GetReturnType());
	}

	// check for possible perfect hash table
	auto use_perfect_hash = sink.perfect_join_executor->CanDoPerfectHashJoin(*this, min, max);
	// PHJ's FillPerfectHashTable reads payload at native width; if any slot was narrowed it would crash. Runtime
	// min/max from filter pushdown can re-enable PHJ here, so re-check after dict-surviving may have narrowed.
	if (use_perfect_hash && sink.DictSurvivingActive()) {
		use_perfect_hash = false;
	}
	if (use_perfect_hash) {
		// A duplicate build key, found while filling, still falls back to the regular hash join
		sink.perfect_join_executor->InitializePerfectHashTable();
		if (FinalizeSingleThreaded(sink)) {
			sink.perfect_join_executor->FillPerfectHashTable(0, ht.GetDataCollection().ChunkCount());
			CompletePerfectHashJoinFinalize(pipeline, event, sink, std::move(filter_min_max), false);
		} else {
			event.InsertEvent(make_shared_ptr<PerfectHashJoinFillEvent>(pipeline, sink, std::move(filter_min_max)));
		}
	} else {
		// A large build side uses the regular hash join
		sink.perfect_join_executor.reset();
		if (filter_min_max) {
			filter_pushdown->FinalizeFilters(context, *this, std::move(filter_min_max), &ht, true, true,
			                                 sink.global_filter_state.get());
		}
		sink.ScheduleFinalize(pipeline, event);
	}
	sink.finalized = true;
	if (ht.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
//! Structural gate for the compressed-vector probe paths; evaluated once per operator state.
//! Decides whether to allocate ProbeState::dict_state; Probe() dispatches into the fast paths
//! whenever that allocation is present.
static bool CanUseCompressedProbe(const HashJoinGlobalSinkState &sink, const vector<JoinCondition> &conditions) {
	if (sink.external) {
		// external joins re-finalize the HT mid-probe, invalidating the per-id pointer cache
		return false;
	}
	if (sink.perfect_join_executor) {
		return false;
	}
	if (conditions.size() != 1) {
		return false;
	}
	const auto cmp = conditions[0].GetComparisonType();
	if (cmp != ExpressionType::COMPARE_EQUAL && cmp != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		return false;
	}
	if (conditions[0].GetLHS().GetExpressionClass() != ExpressionClass::BOUND_REF) {
		return false;
	}
	if (conditions[0].GetLHS().GetReturnType().InternalType() == PhysicalType::STRUCT) {
		return false;
	}
	return true;
}

class HashJoinOperatorState : public CachingOperatorState {
public:
	HashJoinOperatorState(ClientContext &context, const PhysicalHashJoin &op_p, HashJoinGlobalSinkState &sink)
	    : op(op_p), probe_executor(context), scan_structure(*sink.hash_table, join_key_state) {
	}

	const PhysicalHashJoin &op;
	DataChunk lhs_join_keys;
	TupleDataChunkState join_key_state;
	DataChunk lhs_probe_data;
	DataChunk lhs_output_data;

	ExpressionExecutor probe_executor;
	JoinHashTable::ScanStructure scan_structure;
	unique_ptr<OperatorState> perfect_hash_join_state;

	JoinHashTable::ProbeSpillLocalAppendState spill_state;
	JoinHashTable::ProbeState probe_state;
	//! Chunk to sink data into for external join
	DataChunk spill_chunk;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		scan_structure.dictionary_gather_state.Reset();
		if (perfect_hash_join_state) {
			perfect_hash_join_state->Finalize(op, context);
		}
		context.thread.profiler.Flush(op);
	}

	bool SupportsReuse() const override {
		return true;
	}

	void Reset() override {
		ResetCachingState();
		lhs_join_keys.Reset();
		lhs_probe_data.Reset();
		lhs_output_data.Reset();
		scan_structure.Reset();
		perfect_hash_join_state.reset();
		spill_state = JoinHashTable::ProbeSpillLocalAppendState();
		TupleDataCollection::InitializeChunkState(join_key_state, op.condition_types);
		if (spill_chunk.ColumnCount() != 0) {
			spill_chunk.Reset();
		}
		// discard cached build-side pointers; the HT may have been reset between iterations (e.g. recursive CTE)
		if (probe_state.dict_state) {
			probe_state.dict_state = make_uniq<JoinHashTable::ProbeDictionaryState>();
		}
		// perfect_hash_join_state will be lazily initialized on first Execute when we have a real ExecutionContext
	}
};

unique_ptr<OperatorState> PhysicalHashJoin::GetOperatorState(ExecutionContext &context) const {
	auto &allocator = BufferAllocator::Get(context.client);
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	auto state = make_uniq<HashJoinOperatorState>(context.client, *this, sink);
	state->lhs_join_keys.Initialize(allocator, condition_types);

	// initialize probe data with ALL probe columns (output + predicate)
	if (!lhs_probe_columns.col_types.empty()) {
		state->lhs_probe_data.InitializeEmpty(lhs_probe_columns.col_types);
	}
	if (!lhs_output_columns.col_types.empty()) {
		state->lhs_output_data.InitializeEmpty(lhs_output_columns.col_types);
	}

	for (auto &cond : conditions) {
		state->probe_executor.AddExpression(cond.GetLHS());
	}
	TupleDataCollection::InitializeChunkState(state->join_key_state, condition_types);

	if (sink.perfect_join_executor) {
		state->perfect_hash_join_state = sink.perfect_join_executor->GetOperatorState(context);
	}

	if (sink.external) {
		state->spill_chunk.InitializeEmpty(sink.probe_types);
		sink.InitializeProbeSpill();
	}

	if (CanUseCompressedProbe(sink, conditions)) {
		// non-null dict_state is the enabling signal for the compressed-vector probe paths in Probe()
		state->probe_state.dict_state = make_uniq<JoinHashTable::ProbeDictionaryState>();
	}

	return std::move(state);
}

void PhysicalHashJoin::InitializeOutputChunk(Allocator &allocator, DataChunk &chunk) const {
	if (join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI) {
		PhysicalOperator::InitializeOutputChunk(allocator, chunk);
		return;
	}
	// the probe-side output columns are always referenced or sliced from the probe input
	vector<bool> initialize(types.size(), true);
	for (idx_t i = 0; i < lhs_output_columns.col_idxs.size(); i++) {
		initialize[i] = false;
	}
	chunk.Initialize(allocator, types, initialize);
}

static bool ExecutionHashJoinProbeEmptyBuildSideSupported(ExecutionHashJoinProbeOutputMode output_mode,
                                                          bool correlated_mark_counts_required) {
	switch (output_mode) {
	case ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD:
	case ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD:
	case ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY:
	case ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY:
		return true;
	case ExecutionHashJoinProbeOutputMode::MARK_PROBE:
		return !correlated_mark_counts_required;
	default:
		return false;
	}
}

static ExecutionOperatorReadiness MakeExecutionHashJoinProbeReadiness(ExecutionRegionOperatorContractKind kind,
                                                                      ExecutionOperatorReadinessStatus status,
                                                                      string blocker) {
	ExecutionOperatorReadiness readiness;
	readiness.kind = kind;
	readiness.status = status;
	readiness.blocker = std::move(blocker);
	return readiness;
}

static bool TryGetNativePerfectHashJoinProbeLayout(const HashJoinGlobalSinkState &sink,
                                                   const ExecutionRegionHashJoinContract &contract,
                                                   ExecutionPerfectHashJoinTableLayout &perfect_layout) {
	perfect_layout = ExecutionPerfectHashJoinTableLayout();
	if (!sink.finalized || !sink.perfect_join_executor || !contract.perfect_hash_probe_shape_ready) {
		return false;
	}
	if (!sink.perfect_join_executor->GetExecutionPerfectHashJoinTableLayout(perfect_layout)) {
		return false;
	}
	if (perfect_layout.rhs_output_column_count != contract.rhs_output_column_count) {
		perfect_layout.blocker = "perfect-hash-join-native-runtime-rhs-output-count-mismatch";
		return false;
	}
	return true;
}

enum class ExecutionHashJoinProbeLayoutResolutionStatus : uint8_t { READY, NOT_READY, INVALID };

struct ExecutionHashJoinProbeLayoutResolution {
	ExecutionHashJoinProbeLayoutResolutionStatus status = ExecutionHashJoinProbeLayoutResolutionStatus::INVALID;
	ExecutionHashJoinProbeLayoutKind kind = ExecutionHashJoinProbeLayoutKind::NONE;
	ExecutionHashJoinTableLayout table_layout;
	ExecutionPerfectHashJoinTableLayout perfect_layout;
	bool empty_build_side = false;
	string blocker;
};

static ExecutionHashJoinProbeLayoutResolution
ResolveExecutionHashJoinProbeLayout(const HashJoinGlobalSinkState &sink,
                                    const ExecutionRegionHashJoinContract &contract) {
	ExecutionHashJoinProbeLayoutResolution result;
	if (TryGetNativePerfectHashJoinProbeLayout(sink, contract, result.perfect_layout)) {
		result.status = ExecutionHashJoinProbeLayoutResolutionStatus::READY;
		result.kind = ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
		return result;
	}

	ExecutionGetHashJoinTableLayout(*sink.hash_table, result.table_layout);
	if (result.table_layout.Ready()) {
		result.status = ExecutionHashJoinProbeLayoutResolutionStatus::READY;
		result.kind = ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
		return result;
	}

	if (result.table_layout.status == ExecutionHashJoinTableLayoutStatus::NOT_FINALIZED && sink.finalized &&
	    sink.perfect_join_executor) {
		if (!contract.perfect_hash_probe_shape_ready) {
			result.blocker = contract.perfect_hash_probe_shape_blocker.empty()
			                     ? "perfect-hash-join-native-probe-shape-not-ready"
			                     : contract.perfect_hash_probe_shape_blocker;
			return result;
		}
		if (!sink.perfect_join_executor->GetExecutionPerfectHashJoinTableLayout(result.perfect_layout)) {
			result.blocker = result.perfect_layout.blocker.empty() ? "perfect-hash-join-native-layout-not-ready"
			                                                       : result.perfect_layout.blocker;
			return result;
		}
		if (result.perfect_layout.rhs_output_column_count != contract.rhs_output_column_count) {
			result.blocker = "perfect-hash-join-native-rhs-output-count-mismatch";
			return result;
		}
		result.status = ExecutionHashJoinProbeLayoutResolutionStatus::READY;
		result.kind = ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
		return result;
	}

	if (result.table_layout.Empty() &&
	    ExecutionHashJoinProbeEmptyBuildSideSupported(contract.native_probe_output_mode,
	                                                  contract.correlated_mark_counts_required)) {
		result.status = ExecutionHashJoinProbeLayoutResolutionStatus::READY;
		result.kind = ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
		result.empty_build_side = true;
		return result;
	}

	result.blocker = ExecutionHashJoinTableLayoutBlocker(result.table_layout.status);
	result.status = result.table_layout.CanDefer() ? ExecutionHashJoinProbeLayoutResolutionStatus::NOT_READY
	                                               : ExecutionHashJoinProbeLayoutResolutionStatus::INVALID;
	return result;
}

static string ValidateExecutionHashJoinProbeLayout(const HashJoinGlobalSinkState &sink,
                                                   const ExecutionRegionHashJoinContract &contract,
                                                   const ExecutionHashJoinProbeLayoutResolution &resolution) {
	if (contract.non_equality_condition_count != 0 &&
	    contract.native_probe_output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	    contract.native_probe_output_mode != ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD &&
	    contract.native_probe_output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
		return "hash-join-native-non-equality-output-mode-contract-invariant";
	}
	if (resolution.kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE &&
	    resolution.table_layout.dictionary_emission && resolution.table_layout.chains_longer_than_one &&
	    !resolution.table_layout.entries.aux_next_ptrs &&
	    contract.native_probe_output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY) {
		return "hash-join-native-dictionary-chain-layout-missing";
	}
	if (resolution.kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE &&
	    resolution.table_layout.condition_count != contract.condition_count) {
		return "hash-join-native-condition-count-mismatch";
	}
	if (resolution.kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE &&
	    (resolution.table_layout.layout_offsets != contract.layout_offsets ||
	     resolution.table_layout.tuple_size != contract.tuple_size ||
	     resolution.table_layout.pointer_offset != contract.pointer_offset)) {
		// The contract is planned from the layout the sink published (including dict-surviving narrowed slots);
		// compiled probes bind those offsets, so a table published with a different layout cannot be bound.
		return "hash-join-native-layout-differs-from-plan";
	}
	if (contract.correlated_mark_counts_required) {
		auto &mark_info = sink.hash_table->mark_join_info;
		if (!mark_info.correlated_counts) {
			return "hash-join-native-missing-correlated-mark-counts";
		}
		if (mark_info.correlated_types.size() != contract.delim_type_count) {
			return "hash-join-native-correlated-mark-count-shape-mismatch";
		}
	}
	if (sink.hash_table->HasUncorrelatedMarkJoin()) {
		// Row-valued uncorrelated MARK joins refine FALSE into UNKNOWN by rescanning the build condition rows
		// (JoinHashTable::ConstructMarkJoinResult); native probes do not implement that refinement.
		return "hash-join-native-uncorrelated-mark-refinement";
	}
	return string();
}

ExecutionOperatorReadiness
PhysicalHashJoin::GetExecutionOperatorReadiness(ClientContext &context,
                                                const ExecutionRegionOperatorInfo &operator_info) const {
	(void)context;
	auto invalid = [&](string blocker) {
		return MakeExecutionHashJoinProbeReadiness(operator_info.Kind(), ExecutionOperatorReadinessStatus::INVALID,
		                                           std::move(blocker));
	};
	auto not_ready = [&](string blocker) {
		return MakeExecutionHashJoinProbeReadiness(operator_info.Kind(), ExecutionOperatorReadinessStatus::NOT_READY,
		                                           std::move(blocker));
	};
	auto ready = [&]() {
		return MakeExecutionHashJoinProbeReadiness(operator_info.Kind(), ExecutionOperatorReadinessStatus::READY,
		                                           "none");
	};

	if (operator_info.Kind() != ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE) {
		return invalid("hash-join-native-readiness-wrong-operator-kind");
	}
	auto &contract = operator_info.HashJoin().contract;
	if (!contract.present) {
		return invalid("hash-join-native-readiness-missing-contract");
	}
	if (contract.native_probe_contract.status != ExecutionRegionStateContractStatus::READY) {
		return invalid(contract.native_probe_contract.blocker.empty()
		                   ? "hash-join-native-readiness-probe-contract-not-ready"
		                   : contract.native_probe_contract.blocker);
	}
	if (!contract.native_probe_shape_ready) {
		return invalid(contract.native_probe_shape_blocker.empty() ? "hash-join-native-readiness-probe-shape-not-ready"
		                                                           : contract.native_probe_shape_blocker);
	}
	if (operator_info.HashJoin().keys.size() != contract.condition_count) {
		return invalid("hash-join-native-readiness-probe-key-count-mismatch");
	}
	for (auto &key : operator_info.HashJoin().keys) {
		// Expression keys are computed by the backend ahead of the probe, which then binds them as references
		if (!key.supported_reference && !key.expression_ready) {
			return invalid(key.reason.empty() ? "hash-join-native-readiness-probe-key-not-reference" : key.reason);
		}
	}
	if (!sink_state) {
		return not_ready("hash-join-native-readiness-missing-sink-state");
	}
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	if (!sink.hash_table) {
		return not_ready("hash-join-native-readiness-missing-hash-table");
	}
	if (sink.external) {
		// An external join's table holds only its current partitions: probe rows of the other partitions must be
		// spilled and probed in later rounds, which only DuckDB's probe implements
		return invalid("hash-join-native-external-probe-spill");
	}

	auto resolution = ResolveExecutionHashJoinProbeLayout(sink, contract);
	if (resolution.status == ExecutionHashJoinProbeLayoutResolutionStatus::NOT_READY) {
		return not_ready(std::move(resolution.blocker));
	}
	if (resolution.status != ExecutionHashJoinProbeLayoutResolutionStatus::READY) {
		return invalid(resolution.blocker.empty() ? "hash-join-native-readiness-table-layout-invalid"
		                                          : std::move(resolution.blocker));
	}
	auto layout_blocker = ValidateExecutionHashJoinProbeLayout(sink, contract, resolution);
	if (!layout_blocker.empty()) {
		return invalid(std::move(layout_blocker));
	}
	return ready();
}

ExecutionOperatorBindResult PhysicalHashJoin::BindExecutionOperator(ExecutionContext &context, DataChunk &input,
                                                                    GlobalOperatorState &gstate, OperatorState &state_p,
                                                                    const ExecutionRegionOperatorInfo &operator_info,
                                                                    ExecutionOperatorBinding &binding) const {
	(void)context;
	(void)gstate;
	(void)state_p;
	binding = ExecutionOperatorBinding();
	binding.kind = operator_info.Kind();
	if (operator_info.Kind() != ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE) {
		binding.blocker = "hash-join-native-runtime-wrong-operator-kind";
		return ExecutionOperatorBindResult::INVALID;
	}
	auto &contract = operator_info.HashJoin().contract;
	if (!contract.present) {
		binding.blocker = "hash-join-native-runtime-missing-contract";
		return ExecutionOperatorBindResult::INVALID;
	}
	if (contract.native_probe_contract.status != ExecutionRegionStateContractStatus::READY) {
		binding.blocker = contract.native_probe_contract.blocker.empty()
		                      ? "hash-join-native-runtime-probe-contract-not-ready"
		                      : contract.native_probe_contract.blocker;
		return ExecutionOperatorBindResult::INVALID;
	}
	if (!contract.native_probe_shape_ready) {
		binding.blocker = contract.native_probe_shape_blocker.empty() ? "hash-join-native-runtime-probe-shape-not-ready"
		                                                              : contract.native_probe_shape_blocker;
		return ExecutionOperatorBindResult::INVALID;
	}
	if (operator_info.HashJoin().keys.size() != contract.condition_count) {
		binding.blocker = "hash-join-native-runtime-probe-key-count-mismatch";
		return ExecutionOperatorBindResult::INVALID;
	}
	vector<idx_t> probe_key_input_indices;
	probe_key_input_indices.reserve(operator_info.HashJoin().keys.size());
	for (auto &key : operator_info.HashJoin().keys) {
		if (!key.supported_reference) {
			binding.blocker = key.reason.empty() ? "hash-join-native-runtime-probe-key-not-reference" : key.reason;
			return ExecutionOperatorBindResult::INVALID;
		}
		if (key.input_index >= input.ColumnCount()) {
			binding.blocker = "hash-join-native-runtime-probe-key-input-out-of-range";
			return ExecutionOperatorBindResult::INVALID;
		}
		probe_key_input_indices.push_back(key.input_index);
	}
	if (!sink_state) {
		binding.blocker = "hash-join-native-runtime-missing-sink-state";
		return ExecutionOperatorBindResult::INVALID;
	}
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	if (!sink.hash_table) {
		binding.blocker = "hash-join-native-runtime-missing-hash-table";
		return ExecutionOperatorBindResult::INVALID;
	}
	if (sink.external) {
		binding.blocker = "hash-join-native-external-probe-spill";
		return ExecutionOperatorBindResult::INVALID;
	}

	auto resolution = ResolveExecutionHashJoinProbeLayout(sink, contract);
	if (resolution.status == ExecutionHashJoinProbeLayoutResolutionStatus::NOT_READY) {
		binding.blocker = std::move(resolution.blocker);
		return ExecutionOperatorBindResult::DEFERRED;
	}
	if (resolution.status != ExecutionHashJoinProbeLayoutResolutionStatus::READY) {
		binding.blocker = resolution.blocker.empty() ? "hash-join-native-runtime-table-layout-invalid"
		                                             : std::move(resolution.blocker);
		return ExecutionOperatorBindResult::INVALID;
	}
	auto layout_blocker = ValidateExecutionHashJoinProbeLayout(sink, contract, resolution);
	if (!layout_blocker.empty()) {
		binding.blocker = std::move(layout_blocker);
		return ExecutionOperatorBindResult::INVALID;
	}

	const bool perfect_hash_layout = resolution.kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;

	binding.ready = true;
	binding.blocker.clear();
	binding.hash_join_probe.ready = true;
	binding.hash_join_probe.hash_table = sink.hash_table.get();
	binding.hash_join_probe.layout_kind = resolution.kind;
	binding.hash_join_probe.bloom_filter_requested =
	    !perfect_hash_layout && !resolution.empty_build_side &&
	    SelectJitHashJoinProbeFilterStrategy(*this, sink.hash_table->Count()) ==
	        JitHashJoinProbeFilterStrategy::BLOOM_REJECT;
	binding.hash_join_probe.build_side_has_filtered_null = sink.hash_table->has_filtered_null;
	binding.hash_join_probe.condition_null_values_are_equal.resize(contract.condition_count);
	for (idx_t condition_idx = 0; condition_idx < contract.condition_count; condition_idx++) {
		binding.hash_join_probe.condition_null_values_are_equal[condition_idx] =
		    sink.hash_table->NullValuesAreEqual(condition_idx);
	}
	binding.hash_join_probe.table_layout = std::move(resolution.table_layout);
	binding.hash_join_probe.perfect_layout = std::move(resolution.perfect_layout);
	binding.hash_join_probe.empty_build_side = resolution.empty_build_side;
	binding.hash_join_probe.probe_key_input_indices = std::move(probe_key_input_indices);
	binding.hash_join_probe.rhs_condition_types = contract.rhs_condition_types;
	binding.hash_join_probe.lhs_output_column_indices = contract.lhs_output_column_indices;
	binding.hash_join_probe.rhs_output_column_count = contract.rhs_output_column_count;
	binding.hash_join_probe.rhs_dictionary_owners =
	    make_shared_ptr<DictionaryOwnershipCache>(contract.rhs_output_column_count);
	binding.hash_join_probe.rhs_fixed_column_sources.resize(contract.rhs_output_column_count);
	if (!perfect_hash_layout && !resolution.empty_build_side &&
	    contract.native_probe_output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		for (idx_t rhs_idx = 0; rhs_idx < contract.rhs_output_column_count; rhs_idx++) {
			sink.hash_table->TryGetRHSFixedColumnSource(rhs_idx,
			                                            binding.hash_join_probe.rhs_fixed_column_sources[rhs_idx]);
		}
	}
	// Perfect-hash RHS columns are always emitted as the table's global dictionaries.
	auto &requires_global_dictionary = binding.hash_join_probe.rhs_output_requires_global_dictionary;
	requires_global_dictionary.assign(contract.rhs_output_column_count, false);
	if (contract.output_reaches_dictionary_narrowing_sink && !resolution.empty_build_side) {
		for (idx_t rhs_idx = 0; rhs_idx < contract.rhs_output_column_count; rhs_idx++) {
			requires_global_dictionary[rhs_idx] =
			    perfect_hash_layout || sink.hash_table->RHSOutputIsGlobalDictionary(rhs_idx);
		}
	}
	binding.hash_join_probe.exact_rhs_output_probe_input_indices.assign(contract.rhs_output_column_count,
	                                                                    DConstants::INVALID_INDEX);
	for (idx_t rhs_idx = 0; rhs_idx < contract.rhs_output_column_count; rhs_idx++) {
		if (requires_global_dictionary[rhs_idx]) {
			// aliasing the probe key would replace the global dictionary the native probe emits
			continue;
		}
		const auto condition_idx = contract.rhs_output_column_indices[rhs_idx];
		if (condition_idx >= contract.equality_condition_count ||
		    condition_idx >= binding.hash_join_probe.probe_key_input_indices.size() ||
		    rhs_idx >= contract.rhs_output_types.size()) {
			continue;
		}
		const auto probe_input_idx = binding.hash_join_probe.probe_key_input_indices[condition_idx];
		if (probe_input_idx >= input.ColumnCount() ||
		    input.data[probe_input_idx].GetType() != contract.rhs_output_types[rhs_idx]) {
			continue;
		}
		binding.hash_join_probe.exact_rhs_output_probe_input_indices[rhs_idx] = probe_input_idx;
	}
	binding.hash_join_probe.lhs_probe_column_indices = contract.lhs_probe_column_indices;
	binding.hash_join_probe.lhs_probe_types = contract.lhs_probe_types;
	binding.hash_join_probe.output_types = types;
	binding.hash_join_probe.residual_sources = contract.residual_sources;
	binding.hash_join_probe.output_mode = contract.native_probe_output_mode;
	binding.hash_join_probe.correlated_mark_counts_required = contract.correlated_mark_counts_required;
	binding.hash_join_probe.correlated_mark_group_count = contract.delim_type_count;
	binding.hash_join_probe.blocker.clear();
	return ExecutionOperatorBindResult::READY;
}

bool PhysicalHashJoin::BindExecutionSink(ExecutionContext &context, DataChunk &input, OperatorSinkInput &sink_input,
                                         const ExecutionRegionHashJoinInfo &sink_info,
                                         ExecutionSinkBinding &binding) const {
	return ExecutionBindHashJoinBuild(context, sink_input, input, sink_info, binding);
}

static void ReferenceExecutionHashJoinProbeOutputColumns(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                                         DataChunk &result) {
	for (idx_t col_idx = 0; col_idx < binding.lhs_output_column_indices.size(); col_idx++) {
		auto input_col = binding.lhs_output_column_indices[col_idx];
		if (input_col >= input.ColumnCount()) {
			throw InternalException("execution native hash join probe output column index out of range");
		}
		result.data[col_idx].Reference(input.data[input_col]);
	}
}

static bool ExecutionHashJoinProbeSelectionIsIdentity(const SelectionVector &match_sel, idx_t count) {
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (match_sel.get_index(row_idx) != row_idx) {
			return false;
		}
	}
	return true;
}

static void MaterializeExecutionCorrelatedMarkJoinProbe(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                                        const SelectionVector &match_sel, idx_t count,
                                                        DataChunk &result) {
	if (count != input.size()) {
		throw InternalException("execution native correlated MARK probe must materialize every input row");
	}
	if (!binding.hash_table) {
		throw InternalException("execution native correlated MARK probe requires a hash table");
	}
	if (binding.correlated_mark_group_count + 1 != binding.probe_key_input_indices.size()) {
		throw InternalException("execution native correlated MARK probe key count mismatch");
	}
	auto &info = binding.hash_table->mark_join_info;
	if (!info.correlated_counts || info.correlated_types.size() != binding.correlated_mark_group_count) {
		throw InternalException("execution native correlated MARK probe count state is missing");
	}

	lock_guard<mutex> mj_lock(info.mj_lock);
	for (idx_t group_idx = 0; group_idx < binding.correlated_mark_group_count; group_idx++) {
		auto input_col = binding.probe_key_input_indices[group_idx];
		if (input_col >= input.ColumnCount()) {
			throw InternalException("execution native correlated MARK probe group key index out of range");
		}
		info.group_chunk.data[group_idx].Reference(input.data[input_col]);
	}
	info.group_chunk.SetChildCardinality(input.size());
	info.correlated_counts->FetchAggregates(info.group_chunk, info.result_chunk);

	ReferenceExecutionHashJoinProbeOutputColumns(binding, input, result);
	result.SetChildCardinality(input.size());

	auto last_key_input_col = binding.probe_key_input_indices[binding.correlated_mark_group_count];
	if (last_key_input_col >= input.ColumnCount()) {
		throw InternalException("execution native correlated MARK probe comparison key index out of range");
	}
	auto &last_key = input.data[last_key_input_col];
	auto &mark_vector = result.data.back();
	mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(mark_vector, count_t(input.size()));
	auto bool_result = FlatVector::GetDataMutable<bool>(mark_vector);
	auto &mask = FlatVector::ValidityMutable(mark_vector);
	switch (last_key.GetVectorType()) {
	case VectorType::CONSTANT_VECTOR:
		if (ConstantVector::IsNull(last_key)) {
			mask.SetAllInvalid(input.size());
		} else {
			mask.SetAllValid(input.size());
		}
		break;
	case VectorType::FLAT_VECTOR:
		mask.Copy(FlatVector::Validity(last_key), input.size());
		break;
	default: {
		UnifiedVectorFormat key_data;
		last_key.ToUnifiedFormat(key_data);
		if (key_data.validity.CannotHaveNull()) {
			mask.SetAllValid(input.size());
		} else {
			for (idx_t i = 0; i < input.size(); i++) {
				auto key_idx = key_data.sel->get_index(i);
				mask.Set(i, key_data.validity.RowIsValid(key_idx));
			}
		}
		break;
	}
	}

	auto count_star = FlatVector::GetData<int64_t>(info.result_chunk.data[0]);
	auto count_non_null = FlatVector::GetData<int64_t>(info.result_chunk.data[1]);
	for (idx_t i = 0; i < input.size(); i++) {
		D_ASSERT(count_star[i] >= count_non_null[i]);
		bool_result[i] = match_sel.get_index(i) != 0;
		if (!bool_result[i] && count_star[i] > count_non_null[i]) {
			mask.SetInvalid(i);
		}
		if (count_star[i] == 0) {
			mask.SetValid(i);
		}
	}
}

static void MaterializeExecutionEmptyMarkJoinProbe(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                                   idx_t count, DataChunk &result,
                                                   optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	if (count != input.size()) {
		throw InternalException("execution native empty MARK probe must materialize every input row");
	}
	{
		ExecutionOperatorStageTimer timer(recorder, "reference_probe_output");
		ReferenceExecutionHashJoinProbeOutputColumns(binding, input, result);
	}
	{
		ExecutionOperatorStageTimer timer(recorder, "empty_mark_probe_vector");
		auto &mark_vector = result.data.back();
		mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::SetSize(mark_vector, count_t(count));
		auto bool_result = FlatVector::GetDataMutable<bool>(mark_vector);
		auto &mask = FlatVector::ValidityMutable(mark_vector);
		// As in ConstructEmptyJoinResult: every row is a non-match, which is false unless the build side held only
		// NULLs, which makes it unknown
		if (binding.hash_table->has_filtered_null) {
			mask.SetAllInvalid(count);
		} else {
			mask.SetAllValid(count);
		}
		for (idx_t i = 0; i < count; i++) {
			bool_result[i] = false;
		}
	}
	result.SetChildCardinality(count);
}

void ExecutionMaterializeHashJoinProbe(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                       Vector &row_pointers, const SelectionVector &match_sel, idx_t count,
                                       DataChunk &result, optional_ptr<ExecutionOperatorStageRecorder> recorder,
                                       ExecutionHashJoinProbeOutputProof output_proof) {
	if (!binding.ready || !binding.hash_table) {
		throw InternalException("execution native hash join probe materialization requires a bound hash join table");
	}
	if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE) {
		throw InternalException("execution native regular hash join materialization received a perfect hash layout");
	}
	if (binding.output_mode == ExecutionHashJoinProbeOutputMode::NONE) {
		throw InternalException("execution native hash join probe materialization requires an output mode");
	}
	if (binding.output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY) {
		throw InternalException("execution native hash join mark-only probe does not materialize rows");
	}
	auto expected_column_count = binding.lhs_output_column_indices.size();
	if (binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    binding.output_mode == ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD) {
		expected_column_count += binding.rhs_output_column_count;
	} else if (binding.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE) {
		expected_column_count++;
	}
	if (expected_column_count != result.ColumnCount()) {
		throw InternalException("execution native hash join probe output column count mismatch");
	}
	if (binding.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE) {
		if (binding.correlated_mark_counts_required) {
			ExecutionOperatorStageTimer timer(recorder, "correlated_mark_probe");
			MaterializeExecutionCorrelatedMarkJoinProbe(binding, input, match_sel, count, result);
			return;
		}
		if (binding.empty_build_side) {
			MaterializeExecutionEmptyMarkJoinProbe(binding, input, count, result, recorder);
			return;
		}
		if (count != input.size()) {
			throw InternalException("execution native hash join MARK probe must materialize every input row");
		}
		{
			ExecutionOperatorStageTimer timer(recorder, "reference_probe_output");
			ReferenceExecutionHashJoinProbeOutputColumns(binding, input, result);
		}
		auto &mark_vector = result.data.back();
		bool *bool_result;
		ValidityMask *mask;
		{
			ExecutionOperatorStageTimer timer(recorder, "mark_probe_vector");
			mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
			FlatVector::SetSize(mark_vector, count_t(count));
			bool_result = FlatVector::GetDataMutable<bool>(mark_vector);
			mask = &FlatVector::ValidityMutable(mark_vector);
			mask->SetAllValid(count);
			for (idx_t i = 0; i < count; i++) {
				bool_result[i] = match_sel.get_index(i) != 0;
			}
		}
		{
			ExecutionOperatorStageTimer timer(recorder, "mark_probe_key_nulls");
			for (idx_t key_idx = 0; key_idx < binding.probe_key_input_indices.size(); key_idx++) {
				if (binding.hash_table->NullValuesAreEqual(key_idx)) {
					continue;
				}
				auto input_col = binding.probe_key_input_indices[key_idx];
				if (input_col >= input.ColumnCount()) {
					throw InternalException("execution native hash join MARK probe key column index out of range");
				}
				UnifiedVectorFormat key_data;
				input.data[input_col].ToUnifiedFormat(key_data);
				if (!key_data.validity.CanHaveNull()) {
					continue;
				}
				for (idx_t i = 0; i < count; i++) {
					auto key_row = key_data.sel->get_index(i);
					if (!key_data.validity.RowIsValidUnsafe(key_row)) {
						mask->SetInvalid(i);
					}
				}
			}
		}
		if (binding.hash_table->has_filtered_null) {
			ExecutionOperatorStageTimer timer(recorder, "mark_probe_build_nulls");
			for (idx_t i = 0; i < count; i++) {
				if (!bool_result[i]) {
					mask->SetInvalid(i);
				}
			}
		}
		result.SetChildCardinality(count);
		return;
	}
	const bool all_probe_rows_selected =
	    count == input.size() && ExecutionHashJoinProbeSelectionIsIdentity(match_sel, count);
	{
		ExecutionOperatorStageTimer timer(recorder,
		                                  all_probe_rows_selected ? "reference_probe_output" : "slice_probe_output");
		if (all_probe_rows_selected) {
			ReferenceExecutionHashJoinProbeOutputColumns(binding, input, result);
		} else {
			for (idx_t col_idx = 0; col_idx < binding.lhs_output_column_indices.size(); col_idx++) {
				auto input_col = binding.lhs_output_column_indices[col_idx];
				if (input_col >= input.ColumnCount()) {
					throw InternalException("execution native hash join probe output column index out of range");
				}
				result.data[col_idx].Slice(input.data[input_col], match_sel, count);
			}
		}
	}
	if (binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    binding.output_mode == ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD) {
		ExecutionOperatorStageTimer timer(recorder, "gather_build_payload");
		const auto rhs_offset = binding.lhs_output_column_indices.size();
		if (output_proof.ExactSourceFilterMatches()) {
			auto &rhs_aliases = output_proof.ExactRHSOutputProbeInputIndices();
			if (rhs_aliases.size() != binding.rhs_output_column_count) {
				throw InternalException("execution exact hash join RHS alias count mismatch");
			}
			for (idx_t rhs_idx = 0; rhs_idx < binding.rhs_output_column_count; rhs_idx++) {
				const auto input_col = rhs_aliases[rhs_idx];
				if (input_col == DConstants::INVALID_INDEX || input_col >= input.ColumnCount() ||
				    input.data[input_col].GetType() != result.data[rhs_offset + rhs_idx].GetType()) {
					throw InternalException("execution exact hash join RHS alias is invalid");
				}
				if (all_probe_rows_selected) {
					result.data[rhs_offset + rhs_idx].Reference(input.data[input_col]);
				} else {
					result.data[rhs_offset + rhs_idx].Slice(input.data[input_col], match_sel, count);
				}
			}
		} else {
			binding.hash_table->GatherRHS(row_pointers, *FlatVector::IncrementalSelectionVector(), count, result,
			                              rhs_offset, *binding.rhs_dictionary_owners);
		}
	}
	result.SetChildCardinality(count);
}

const buffer_ptr<DictionaryEntry> &ExecutionHashJoinPerfectRHSDictionary(const ExecutionHashJoinProbeBinding &binding,
                                                                         idx_t rhs_output_idx) {
	D_ASSERT(binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE);
	D_ASSERT(rhs_output_idx < binding.perfect_layout.rhs_dictionary_buffers.size());
	return binding.rhs_dictionary_owners->GetEntry(rhs_output_idx,
	                                               binding.perfect_layout.rhs_dictionary_buffers[rhs_output_idx]);
}

void ExecutionUseHashJoinProbeBloomFilter(ExecutionHashJoinProbeBinding &binding) {
	if (!binding.ready || !binding.hash_table || !binding.bloom_filter_requested) {
		throw InternalException("execution hash join probe bloom filter was not requested by its binding");
	}
	binding.hash_table->EnsureBloomFilterForProbe();
	// the finalized table already resolved its layout once: only the bloom filter words change
	if (!ExecutionGetHashJoinTableLayout(*binding.hash_table, binding.table_layout)) {
		throw InternalException("execution hash join probe lost its table layout: %s",
		                        ExecutionHashJoinTableLayoutBlocker(binding.table_layout.status));
	}
	binding.use_bloom_filter = true;
}

bool ExecutionTryDirectGatherHashJoinRHSFixedColumn(const ExecutionHashJoinProbeBinding &binding, Vector &row_pointers,
                                                    idx_t count, idx_t rhs_output_idx, Vector &result) {
	if (!binding.ready || !binding.hash_table ||
	    binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	if (rhs_output_idx >= binding.rhs_output_column_count ||
	    (rhs_output_idx < binding.rhs_output_requires_global_dictionary.size() &&
	     binding.rhs_output_requires_global_dictionary[rhs_output_idx])) {
		// a required global dictionary is emitted by the native gather, never flattened
		return false;
	}
	const auto join_output_idx = binding.lhs_output_column_indices.size() + rhs_output_idx;
	if (join_output_idx >= binding.output_types.size() || result.GetType() != binding.output_types[join_output_idx]) {
		return false;
	}
	return binding.hash_table->TryGatherRHSColumnFlat(row_pointers, *FlatVector::IncrementalSelectionVector(), count,
	                                                  rhs_output_idx, result);
}

void ExecutionGatherHashJoinRHSColumn(const ExecutionHashJoinProbeBinding &binding, Vector &row_pointers, idx_t count,
                                      idx_t rhs_output_idx, Vector &result) {
	if (ExecutionTryDirectGatherHashJoinRHSFixedColumn(binding, row_pointers, count, rhs_output_idx, result)) {
		return;
	}
	if (!binding.ready || !binding.hash_table ||
	    binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    rhs_output_idx >= binding.rhs_output_column_count) {
		throw InternalException("execution native hash join RHS gather requires a regular matched probe binding");
	}
	const auto join_output_idx = binding.lhs_output_column_indices.size() + rhs_output_idx;
	if (join_output_idx >= binding.output_types.size() || result.GetType() != binding.output_types[join_output_idx]) {
		throw InternalException("execution native hash join RHS gather type mismatch");
	}
	binding.hash_table->GatherRHSColumn(row_pointers, *FlatVector::IncrementalSelectionVector(), count, rhs_output_idx,
	                                    result, *binding.rhs_dictionary_owners);
}

bool ExecutionGetHashJoinRHSFixedColumnSource(const ExecutionHashJoinProbeBinding &binding, idx_t rhs_output_idx,
                                              ExecutionHashJoinRHSFixedColumnSource &source) {
	source = ExecutionHashJoinRHSFixedColumnSource();
	if (!binding.ready || binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    rhs_output_idx >= binding.rhs_fixed_column_sources.size()) {
		source.blocker = "hash-join-rhs-source-binding-shape";
		return false;
	}
	source = binding.rhs_fixed_column_sources[rhs_output_idx];
	return source.ready;
}

bool ExecutionMaterializeHashJoinProbeProjectionSources(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                                        Vector &row_pointers, const SelectionVector &match_sel,
                                                        idx_t count, const vector<uint8_t> &referenced_columns,
                                                        DataChunk &result,
                                                        optional_ptr<ExecutionOperatorStageRecorder> recorder,
                                                        optional_ptr<const SelectionVector> perfect_build_sel,
                                                        ExecutionHashJoinProbeOutputProof output_proof) {
	if (!binding.ready) {
		throw InternalException(
		    "execution native hash join projection-source materialization requires a bound hash join table");
	}
	const bool regular_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
	const bool perfect_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
	if ((!regular_hash_join && !perfect_hash_join) || (regular_hash_join && !binding.hash_table) ||
	    (perfect_hash_join && (!binding.perfect_layout.ready || !perfect_build_sel))) {
		return false;
	}
	if (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
		return false;
	}
	auto expected_column_count = binding.lhs_output_column_indices.size();
	if (binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		expected_column_count +=
		    regular_hash_join ? binding.rhs_output_column_count : binding.perfect_layout.rhs_output_column_count;
	}
	if (expected_column_count != result.ColumnCount() || expected_column_count != referenced_columns.size()) {
		throw InternalException("execution native hash join projection-source column count mismatch");
	}
	const bool all_probe_rows_selected =
	    regular_hash_join ? count == input.size() && ExecutionHashJoinProbeSelectionIsIdentity(match_sel, count)
	                      : binding.perfect_layout.is_build_dense && count == input.size();
	{
		ExecutionOperatorStageTimer timer(recorder,
		                                  all_probe_rows_selected ? "reference_probe_sources" : "slice_probe_sources");
		for (idx_t col_idx = 0; col_idx < binding.lhs_output_column_indices.size(); col_idx++) {
			if (!referenced_columns[col_idx]) {
				continue;
			}
			auto input_col = binding.lhs_output_column_indices[col_idx];
			if (input_col >= input.ColumnCount()) {
				throw InternalException("execution native hash join projection-source column index out of range");
			}
			if (input.data[input_col].GetType() != result.data[col_idx].GetType()) {
				return false;
			}
			if (all_probe_rows_selected) {
				result.data[col_idx].Reference(input.data[input_col]);
			} else {
				result.data[col_idx].Slice(input.data[input_col], match_sel, count);
			}
		}
	}
	if (binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		ExecutionOperatorStageTimer timer(recorder,
		                                  regular_hash_join ? "gather_build_sources" : "dictionary_build_sources");
		const auto rhs_offset = binding.lhs_output_column_indices.size();
		const auto rhs_output_count =
		    regular_hash_join ? binding.rhs_output_column_count : binding.perfect_layout.rhs_output_column_count;
		if (perfect_hash_join &&
		    binding.perfect_layout.rhs_dictionary_buffers.size() != binding.perfect_layout.rhs_output_column_count) {
			throw InternalException("execution native perfect hash join projection-source dictionary shape mismatch");
		}
		for (idx_t rhs_col_idx = 0; rhs_col_idx < rhs_output_count; rhs_col_idx++) {
			const auto output_col_idx = rhs_offset + rhs_col_idx;
			if (!referenced_columns[output_col_idx]) {
				continue;
			}
			if (regular_hash_join) {
				if (output_proof.ExactSourceFilterMatches()) {
					auto &rhs_aliases = output_proof.ExactRHSOutputProbeInputIndices();
					if (rhs_col_idx >= rhs_aliases.size()) {
						return false;
					}
					const auto input_col = rhs_aliases[rhs_col_idx];
					if (input_col == DConstants::INVALID_INDEX || input_col >= input.ColumnCount() ||
					    input.data[input_col].GetType() != result.data[output_col_idx].GetType()) {
						return false;
					}
					if (all_probe_rows_selected) {
						result.data[output_col_idx].Reference(input.data[input_col]);
					} else {
						result.data[output_col_idx].Slice(input.data[input_col], match_sel, count);
					}
				} else {
					binding.hash_table->GatherRHSColumn(row_pointers, *FlatVector::IncrementalSelectionVector(), count,
					                                    rhs_col_idx, result.data[output_col_idx],
					                                    *binding.rhs_dictionary_owners);
				}
			} else {
				auto &result_vector = result.data[output_col_idx];
				if (result_vector.GetType() != binding.perfect_layout.rhs_output_types[rhs_col_idx]) {
					throw InternalException("execution native perfect hash join projection-source RHS type mismatch");
				}
				result_vector.Dictionary(ExecutionHashJoinPerfectRHSDictionary(binding, rhs_col_idx),
				                         *perfect_build_sel, count);
			}
		}
	}
	result.SetChildCardinality(count);
	return true;
}

void ExecutionMaterializePerfectHashJoinProbe(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                              const SelectionVector &probe_sel, const SelectionVector &build_sel,
                                              idx_t count, DataChunk &result,
                                              optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	if (!binding.ready || binding.layout_kind != ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE ||
	    !binding.perfect_layout.ready) {
		throw InternalException("execution native perfect hash join materialization requires a perfect hash layout");
	}
	if (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
		throw InternalException(
		    "execution native perfect hash join materialization requires an inner probe output mode");
	}
	auto expected_column_count = binding.lhs_output_column_indices.size();
	if (binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		expected_column_count += binding.perfect_layout.rhs_output_column_count;
	}
	if (expected_column_count != result.ColumnCount()) {
		throw InternalException("execution native perfect hash join output column count mismatch");
	}
	const bool all_probe_rows_selected = binding.perfect_layout.is_build_dense && count == input.size();
	{
		ExecutionOperatorStageTimer timer(recorder,
		                                  all_probe_rows_selected ? "reference_probe_output" : "slice_probe_output");
		for (idx_t col_idx = 0; col_idx < binding.lhs_output_column_indices.size(); col_idx++) {
			auto input_col = binding.lhs_output_column_indices[col_idx];
			if (input_col >= input.ColumnCount()) {
				throw InternalException("execution native perfect hash join output column index out of range");
			}
			if (all_probe_rows_selected) {
				result.data[col_idx].Reference(input.data[input_col]);
			} else {
				result.data[col_idx].Slice(input.data[input_col], probe_sel, count);
			}
		}
	}
	if (binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		ExecutionOperatorStageTimer timer(recorder, "dictionary_build_payload");
		if (binding.perfect_layout.rhs_dictionary_buffers.size() != binding.perfect_layout.rhs_output_column_count) {
			throw InternalException("execution native perfect hash join dictionary shape mismatch");
		}
		auto rhs_offset = binding.lhs_output_column_indices.size();
		for (idx_t rhs_col_idx = 0; rhs_col_idx < binding.perfect_layout.rhs_output_column_count; rhs_col_idx++) {
			auto &result_vector = result.data[rhs_offset + rhs_col_idx];
			if (result_vector.GetType() != binding.perfect_layout.rhs_output_types[rhs_col_idx]) {
				throw InternalException("execution native perfect hash join RHS output type mismatch");
			}
			result_vector.Dictionary(ExecutionHashJoinPerfectRHSDictionary(binding, rhs_col_idx), build_sel, count);
		}
	}
	result.SetChildCardinality(count);
}

void ExecutionMaterializeHashJoinResidualSources(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                                 Vector &row_pointers, const SelectionVector &match_sel, idx_t count,
                                                 DataChunk &residual_sources,
                                                 optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	if (!binding.ready || !binding.hash_table) {
		throw InternalException("execution native hash join residual materialization requires a bound hash join table");
	}
	if (binding.residual_sources.size() != residual_sources.ColumnCount()) {
		throw InternalException("execution native hash join residual source column count mismatch");
	}
	for (auto &source : binding.residual_sources) {
		if (source.source_index >= residual_sources.ColumnCount()) {
			throw InternalException("execution native hash join residual source index out of range");
		}
		auto &target = residual_sources.data[source.source_index];
		if (target.GetType() != source.type) {
			throw InternalException("execution native hash join residual source type mismatch");
		}
		switch (source.kind) {
		case ExecutionHashJoinResidualSourceKind::PROBE: {
			ExecutionOperatorStageTimer timer(recorder, "materialize_probe_source");
			if (source.input_index >= input.ColumnCount()) {
				throw InternalException("execution native hash join residual probe source out of range");
			}
			target.Slice(input.data[source.input_index], match_sel, count);
			break;
		}
		case ExecutionHashJoinResidualSourceKind::BUILD: {
			ExecutionOperatorStageTimer timer(recorder, "materialize_build_source");
			binding.hash_table->GetDataCollection().Gather(row_pointers, *FlatVector::IncrementalSelectionVector(),
			                                               count, source.input_index, target,
			                                               *FlatVector::IncrementalSelectionVector(), nullptr);
			FlatVector::SetSize(target, count_t(count));
			break;
		}
		default:
			throw InternalException("execution native hash join residual source kind is unknown");
		}
	}
	residual_sources.SetChildCardinality(count);
}

void ExecutionMaterializeHashJoinProbeLeftUnmatched(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                                    const SelectionVector &unmatched_sel, idx_t count,
                                                    DataChunk &result,
                                                    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	if (!binding.ready || !binding.hash_table) {
		throw InternalException(
		    "execution native LEFT hash join unmatched materialization requires a bound hash join table");
	}
	if (binding.output_mode != ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD) {
		throw InternalException("execution native LEFT hash join unmatched materialization requires LEFT output mode");
	}
	auto expected_column_count = binding.lhs_output_column_indices.size() + binding.rhs_output_column_count;
	if (expected_column_count != result.ColumnCount()) {
		throw InternalException("execution native LEFT hash join unmatched output column count mismatch");
	}
	{
		ExecutionOperatorStageTimer timer(recorder, "slice_probe_output");
		for (idx_t col_idx = 0; col_idx < binding.lhs_output_column_indices.size(); col_idx++) {
			auto input_col = binding.lhs_output_column_indices[col_idx];
			if (input_col >= input.ColumnCount()) {
				throw InternalException("execution native LEFT hash join unmatched output column index out of range");
			}
			result.data[col_idx].Slice(input.data[input_col], unmatched_sel, count);
		}
	}
	{
		ExecutionOperatorStageTimer timer(recorder, "null_build_payload");
		auto rhs_offset = binding.lhs_output_column_indices.size();
		for (idx_t rhs_col_idx = 0; rhs_col_idx < binding.rhs_output_column_count; rhs_col_idx++) {
			ConstantVector::SetNull(result.data[rhs_offset + rhs_col_idx], count_t(count));
		}
	}
	result.SetChildCardinality(count);
}

OperatorResultType PhysicalHashJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<HashJoinOperatorState>();
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	D_ASSERT(sink.finalized);
	D_ASSERT(!sink.scanned_data);

	if (sink.hash_table->Count() == 0) {
		if (sink.hash_table->HasUncorrelatedMarkJoin()) {
			state.lhs_join_keys.Reset();
			state.probe_executor.Execute(input, state.lhs_join_keys);
			state.lhs_probe_data.ReferenceColumns(input, lhs_probe_columns.col_idxs);
			sink.hash_table->ConstructMarkJoinResult(state.lhs_join_keys, state.lhs_probe_data, chunk);
			return OperatorResultType::NEED_MORE_INPUT;
		}
		if (EmptyResultIfRHSIsEmpty()) {
			return OperatorResultType::FINISHED;
		}
		// for empty result, only need output columns (no predicate evaluation)
		state.lhs_output_data.ReferenceColumns(input, lhs_output_columns.col_idxs);
		ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_filtered_null, state.lhs_output_data,
		                         chunk);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	if (sink.perfect_join_executor) {
		D_ASSERT(!sink.external);
		// Lazily initialize perfect_hash_join_state on first use if needed
		if (!state.perfect_hash_join_state) {
			state.perfect_hash_join_state = sink.perfect_join_executor->GetOperatorState(context);
		}
		// for perfect hash join, when predicate is NULL, only output columns are needed
		state.lhs_output_data.ReferenceColumns(input, lhs_output_columns.col_idxs);
		return sink.perfect_join_executor->ProbePerfectHashTable(context, input, state.lhs_output_data, chunk,
		                                                         *state.perfect_hash_join_state);
	}

	if (sink.external && !state.initialized) {
		// some initialization for external hash join
		if (!sink.probe_spill) {
			sink.InitializeProbeSpill();
		}
		state.spill_state = sink.probe_spill->RegisterThread();
		state.initialized = true;
	}

	if (state.scan_structure.is_null) {
		// probe the HT, start by resolving the join keys for the left chunk
		state.lhs_join_keys.Reset();
		state.probe_executor.Execute(input, state.lhs_join_keys);

		// perform the actual probe
		if (sink.external) {
			sink.hash_table->ProbeAndSpill(state.scan_structure, state.lhs_join_keys, state.join_key_state,
			                               state.probe_state, input, *sink.probe_spill, state.spill_state,
			                               state.spill_chunk);
		} else {
			sink.hash_table->Probe(state.scan_structure, state.lhs_join_keys, state.join_key_state, state.probe_state);
		}
	}

	// pass probe data and mapping to Next
	state.lhs_probe_data.ReferenceColumns(input, lhs_probe_columns.col_idxs);
	state.scan_structure.Next(state.lhs_join_keys, state.lhs_probe_data, chunk);

	if (state.scan_structure.PointersExhausted() && chunk.size() == 0) {
		state.scan_structure.is_null = true;
		return OperatorResultType::NEED_MORE_INPUT;
	}
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
enum class HashJoinSourceStage : uint8_t { INIT, BUILD, PROBE, SCAN_HT, DONE };

class HashJoinLocalSourceState;

class HashJoinGlobalSourceState : public GlobalSourceState {
public:
	HashJoinGlobalSourceState(const PhysicalHashJoin &op, ClientContext &context);

	//! Initialize this source state using the info in the sink
	void Initialize(HashJoinGlobalSinkState &sink);
	//! Try to prepare the next stage
	bool TryPrepareNextStage(HashJoinGlobalSinkState &sink);
	//! Prepare the next build/probe/scan_ht stage for external hash join (must hold lock)
	void PrepareBuild(HashJoinGlobalSinkState &sink);
	void PrepareProbe(HashJoinGlobalSinkState &sink);
	void PrepareScanHT(HashJoinGlobalSinkState &sink);
	//! Assigns a task to a local source state
	bool AssignTask(HashJoinGlobalSinkState &sink, HashJoinLocalSourceState &lstate);

	idx_t MaxThreads() override {
		D_ASSERT(op.sink_state);
		auto &gstate = op.sink_state->Cast<HashJoinGlobalSinkState>();

		idx_t count;
		if (gstate.probe_spill) {
			count = probe_count;
		} else if (PropagatesBuildSide(op.join_type)) {
			count = gstate.hash_table->Count();
		} else {
			return 0;
		}
		return count / ((idx_t)STANDARD_VECTOR_SIZE * parallel_scan_chunk_count);
	}

	bool SupportsReuse() const override {
		return true;
	}

public:
	const PhysicalHashJoin &op;

	//! For synchronizing the external hash join
	atomic<HashJoinSourceStage> global_stage;

	//! For HT build synchronization
	idx_t build_chunk_idx = DConstants::INVALID_INDEX;
	idx_t build_chunk_count;
	idx_t build_chunk_done;
	idx_t build_chunks_per_thread = DConstants::INVALID_INDEX;

	//! For probe synchronization
	atomic<idx_t> probe_chunk_count;
	atomic<idx_t> probe_chunk_done;
	//! Last reported progress, also used while partition masks are being updated
	MonotonicProgress external_progress;

	//! To determine the number of threads
	idx_t probe_count;
	idx_t parallel_scan_chunk_count;

	//! For full/outer synchronization
	idx_t full_outer_chunk_idx = DConstants::INVALID_INDEX;
	atomic<idx_t> full_outer_chunk_count;
	atomic<idx_t> full_outer_chunk_done;
	//! Chunks of the full/outer scan that have been scanned, updated while scanning (for progress)
	atomic<idx_t> full_outer_chunk_progress;
	idx_t full_outer_chunks_per_thread = DConstants::INVALID_INDEX;

	vector<InterruptState> blocked_tasks;

private:
	void ResetState(ClientContext &context) {
		global_stage = HashJoinSourceStage::INIT;
		build_chunk_idx = DConstants::INVALID_INDEX;
		build_chunk_count = 0;
		build_chunk_done = 0;
		build_chunks_per_thread = DConstants::INVALID_INDEX;
		probe_chunk_count = 0;
		probe_chunk_done = 0;
		external_progress.Reset();
		probe_count = op.children[0].get().estimated_cardinality;
		parallel_scan_chunk_count = ClientConfig::GetConfig(context).verify_parallelism ? 1 : 120;
		full_outer_chunk_idx = DConstants::INVALID_INDEX;
		full_outer_chunk_count = 0;
		full_outer_chunk_done = 0;
		full_outer_chunk_progress = 0;
		full_outer_chunks_per_thread = DConstants::INVALID_INDEX;
		blocked_tasks.clear();
		GlobalSourceState::Reset(context);
	}

public:
	void Reset(ClientContext &context) override {
		ResetState(context);
	}
};

class HashJoinLocalSourceState : public LocalSourceState {
public:
	HashJoinLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate, const PhysicalHashJoin &op,
	                         const HashJoinGlobalSinkState &sink, Allocator &allocator);

	//! Do the work this thread has been assigned
	void ExecuteTask(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, DataChunk &chunk);
	//! Whether this thread has finished the work it has been assigned
	bool TaskFinished() const;
	//! Build, probe and scan for external hash join
	void ExternalBuild(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate);
	void ExternalProbe(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, DataChunk &chunk);
	void ExternalScanHT(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, DataChunk &chunk);

public:
	const PhysicalHashJoin &op;
	//! The stage that this thread was assigned work for
	HashJoinSourceStage local_stage;
	//! Vector with pointers here so we don't have to re-initialize
	Vector addresses;

	//! Chunks assigned to this thread for building the pointer table
	idx_t build_chunk_idx_from = DConstants::INVALID_INDEX;
	idx_t build_chunk_idx_to = DConstants::INVALID_INDEX;

	//! Local scan state for probe spill
	ColumnDataConsumerScanState probe_local_scan;
	//! Chunks for holding the scanned probe collection
	DataChunk lhs_probe_chunk;
	DataChunk lhs_join_keys;
	DataChunk lhs_probe_data;
	DataChunk lhs_output_data;
	TupleDataChunkState join_key_state;
	ExpressionExecutor lhs_join_key_executor;

	//! Scan structure for the external probe
	JoinHashTable::ScanStructure scan_structure;
	JoinHashTable::ProbeState probe_state;
	bool empty_ht_probe_in_progress = false;

	//! Chunks assigned to this thread for a full/outer scan
	idx_t full_outer_chunk_idx_from = DConstants::INVALID_INDEX;
	idx_t full_outer_chunk_idx_to = DConstants::INVALID_INDEX;
	unique_ptr<JoinHTScanState> full_outer_scan_state;
	//! Chunks of the current full/outer scan that were added to the global progress
	idx_t full_outer_chunks_reported = 0;

private:
	void ResetState() {
		local_stage = HashJoinSourceStage::INIT;
		build_chunk_idx_from = DConstants::INVALID_INDEX;
		build_chunk_idx_to = DConstants::INVALID_INDEX;
		probe_local_scan.allocator = nullptr;
		probe_local_scan.current_chunk_state.handles.clear();
		probe_local_scan.current_chunk_state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
		probe_local_scan.chunk_index = DConstants::INVALID_INDEX;
		lhs_probe_chunk.Reset();
		lhs_join_keys.Reset();
		lhs_probe_data.Reset();
		lhs_output_data.Reset();
		TupleDataCollection::InitializeChunkState(join_key_state, op.condition_types);
		scan_structure.Reset();
		empty_ht_probe_in_progress = false;
		full_outer_chunk_idx_from = DConstants::INVALID_INDEX;
		full_outer_chunk_idx_to = DConstants::INVALID_INDEX;
		full_outer_scan_state.reset();
		full_outer_chunks_reported = 0;
	}

public:
	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ExecutionContext &context, GlobalSourceState &gstate_p) override {
		ResetState();
	}
};

unique_ptr<GlobalSourceState> PhysicalHashJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<HashJoinGlobalSourceState>(*this, context);
}

unique_ptr<LocalSourceState> PhysicalHashJoin::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_uniq<HashJoinLocalSourceState>(context, gstate, *this, sink_state->Cast<HashJoinGlobalSinkState>(),
	                                           BufferAllocator::Get(context.client));
}

HashJoinGlobalSourceState::HashJoinGlobalSourceState(const PhysicalHashJoin &op, ClientContext &context) : op(op) {
	ResetState(context);
}

void HashJoinGlobalSourceState::Initialize(HashJoinGlobalSinkState &sink) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (global_stage != HashJoinSourceStage::INIT) {
		// Another thread initialized
		return;
	}

	// Finalize the probe spill
	if (sink.probe_spill) {
		sink.probe_spill->Finalize();
	}

	global_stage = HashJoinSourceStage::PROBE;
	TryPrepareNextStage(sink);
}

bool HashJoinGlobalSourceState::TryPrepareNextStage(HashJoinGlobalSinkState &sink) {
	switch (global_stage.load()) {
	case HashJoinSourceStage::BUILD:
		if (build_chunk_done == build_chunk_count) {
			sink.hash_table->GetDataCollection().VerifyEverythingPinned();
			sink.hash_table->MarkFinalized();
			PrepareProbe(sink);
			return true;
		}
		break;
	case HashJoinSourceStage::PROBE:
		if (probe_chunk_done == probe_chunk_count) {
			if (PropagatesBuildSide(op.join_type)) {
				PrepareScanHT(sink);
			} else {
				PrepareBuild(sink);
			}
			return true;
		}
		break;
	case HashJoinSourceStage::SCAN_HT:
		if (full_outer_chunk_done == full_outer_chunk_count) {
			PrepareBuild(sink);
			return true;
		}
		break;
	default:
		break;
	}
	return false;
}

void HashJoinGlobalSourceState::PrepareBuild(HashJoinGlobalSinkState &sink) {
	D_ASSERT(global_stage != HashJoinSourceStage::BUILD);
	auto &ht = *sink.hash_table;

	// Update remaining size
	sink.temporary_memory_state->SetRemainingSizeAndUpdateReservation(
	    sink.context.GetContext(), ht.GetRemainingSize() + sink.probe_side_requirement);

	// Try to put the next partitions in the block collection of the HT
	if (!sink.external || !ht.PrepareExternalFinalize(sink.GetMaxExternalHTSize())) {
		global_stage = HashJoinSourceStage::DONE;
		sink.temporary_memory_state->SetZero();
		return;
	}

	auto &data_collection = ht.GetDataCollection();
	if (data_collection.Count() == 0 && op.EmptyResultIfRHSIsEmpty()) {
		PrepareBuild(sink);
		return;
	}

	build_chunk_idx = 0;
	build_chunk_count = data_collection.ChunkCount();
	build_chunk_done = 0;

	if (ClientConfig::GetConfig(sink.context).verify_parallelism) {
		build_chunks_per_thread = 1;
	} else {
		if (KeysAreSkewed(sink)) {
			build_chunks_per_thread = build_chunk_count; // This forces single-threaded building
		} else {
			build_chunks_per_thread = // Same task size as in HashJoinFinalizeEvent
			    MaxValue<idx_t>(MinValue(build_chunk_count, HashJoinFinalizeEvent::CHUNKS_PER_TASK), 1);
		}
	}

	ht.AllocatePointerTable();
	ht.InitializePointerTable(0, ht.capacity);

	global_stage = HashJoinSourceStage::BUILD;
}

void HashJoinGlobalSourceState::PrepareProbe(HashJoinGlobalSinkState &sink) {
	sink.probe_spill->PrepareNextProbe();
	const auto &consumer = *sink.probe_spill->consumer;

	probe_chunk_count = consumer.Count() == 0 ? 0 : consumer.ChunkCount();
	probe_chunk_done = 0;

	global_stage = HashJoinSourceStage::PROBE;
	if (probe_chunk_count == 0) {
		TryPrepareNextStage(sink);
		return;
	}
}

void HashJoinGlobalSourceState::PrepareScanHT(HashJoinGlobalSinkState &sink) {
	D_ASSERT(global_stage != HashJoinSourceStage::SCAN_HT);
	auto &ht = *sink.hash_table;

	auto &data_collection = ht.GetDataCollection();
	full_outer_chunk_idx = 0;
	full_outer_chunk_count = data_collection.ChunkCount();
	full_outer_chunk_done = 0;
	full_outer_chunk_progress = 0;

	full_outer_chunks_per_thread =
	    MaxValue<idx_t>((full_outer_chunk_count + sink.num_threads - 1) / sink.num_threads, 1);

	global_stage = HashJoinSourceStage::SCAN_HT;
}

bool HashJoinGlobalSourceState::AssignTask(HashJoinGlobalSinkState &sink, HashJoinLocalSourceState &lstate) {
	D_ASSERT(lstate.TaskFinished());

	annotated_unique_lock<annotated_mutex> guard(lock);
	switch (global_stage.load()) {
	case HashJoinSourceStage::BUILD:
		if (build_chunk_idx != build_chunk_count) {
			lstate.local_stage = global_stage;
			lstate.build_chunk_idx_from = build_chunk_idx;
			build_chunk_idx = MinValue<idx_t>(build_chunk_count, build_chunk_idx + build_chunks_per_thread);
			lstate.build_chunk_idx_to = build_chunk_idx;
			return true;
		}
		break;
	case HashJoinSourceStage::PROBE:
		if (sink.probe_spill->consumer && sink.probe_spill->consumer->AssignChunk(lstate.probe_local_scan)) {
			lstate.local_stage = global_stage;
			lstate.empty_ht_probe_in_progress = false;
			return true;
		}
		guard.unlock();
		// Only touch local handles after unlocking: another thread may replace the consumer.
		lstate.probe_local_scan.current_chunk_state.handles.clear();
		return false;
	case HashJoinSourceStage::SCAN_HT:
		if (full_outer_chunk_idx != full_outer_chunk_count) {
			lstate.local_stage = global_stage;
			lstate.full_outer_chunk_idx_from = full_outer_chunk_idx;
			full_outer_chunk_idx =
			    MinValue<idx_t>(full_outer_chunk_count, full_outer_chunk_idx + full_outer_chunks_per_thread);
			lstate.full_outer_chunk_idx_to = full_outer_chunk_idx;
			return true;
		}
		break;
	case HashJoinSourceStage::DONE:
		break;
	default:
		throw InternalException("Unexpected HashJoinSourceStage in AssignTask!");
	}
	return false;
}

HashJoinLocalSourceState::HashJoinLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate,
                                                   const PhysicalHashJoin &op, const HashJoinGlobalSinkState &sink,
                                                   Allocator &allocator)
    : op(op), addresses(LogicalType::POINTER), lhs_join_key_executor(sink.context),
      scan_structure(*sink.hash_table, join_key_state) {
	lhs_probe_chunk.Initialize(allocator, sink.probe_types);
	lhs_join_keys.Initialize(allocator, op.condition_types);

	// initialize with PROBE columns (not just output)
	lhs_probe_data.InitializeEmpty(op.lhs_probe_columns.col_types);
	lhs_output_data.InitializeEmpty(op.lhs_output_columns.col_types);

	TupleDataCollection::InitializeChunkState(join_key_state, op.condition_types);

	for (auto &cond : op.conditions) {
		lhs_join_key_executor.AddExpression(cond.GetLHS());
	}
	ResetState();
}

void HashJoinLocalSourceState::ExecuteTask(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate,
                                           DataChunk &chunk) {
	switch (local_stage) {
	case HashJoinSourceStage::BUILD:
		ExternalBuild(sink, gstate);
		break;
	case HashJoinSourceStage::PROBE:
		ExternalProbe(sink, gstate, chunk);
		break;
	case HashJoinSourceStage::SCAN_HT:
		ExternalScanHT(sink, gstate, chunk);
		break;
	default:
		throw InternalException("Unexpected HashJoinSourceStage in ExecuteTask!");
	}
}

bool HashJoinLocalSourceState::TaskFinished() const {
	switch (local_stage) {
	case HashJoinSourceStage::INIT:
	case HashJoinSourceStage::BUILD:
		return true;
	case HashJoinSourceStage::PROBE:
		return scan_structure.is_null && !empty_ht_probe_in_progress;
	case HashJoinSourceStage::SCAN_HT:
		return full_outer_scan_state == nullptr;
	default:
		throw InternalException("Unexpected HashJoinSourceStage in TaskFinished!");
	}
}

void HashJoinLocalSourceState::ExternalBuild(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate) {
	D_ASSERT(local_stage == HashJoinSourceStage::BUILD);

	auto &ht = *sink.hash_table;
	ht.Finalize(build_chunk_idx_from, build_chunk_idx_to, true);

	annotated_lock_guard<annotated_mutex> guard(gstate.lock);
	gstate.build_chunk_done += build_chunk_idx_to - build_chunk_idx_from;
}

void HashJoinLocalSourceState::ExternalProbe(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate,
                                             DataChunk &chunk) {
	D_ASSERT(local_stage == HashJoinSourceStage::PROBE && sink.hash_table->finalized);

	if (!scan_structure.is_null) {
		// still have elements remaining
		scan_structure.Next(lhs_join_keys, lhs_probe_data, chunk);
		if (chunk.size() != 0 || !scan_structure.PointersExhausted()) {
			return;
		}
	}

	if (!scan_structure.is_null || empty_ht_probe_in_progress) {
		scan_structure.is_null = true;
		empty_ht_probe_in_progress = false;
		sink.probe_spill->consumer->FinishChunk(probe_local_scan);
		annotated_lock_guard<annotated_mutex> guard(gstate.lock);
		gstate.probe_chunk_done++;
		return;
	}

	// Scan input chunk for next probe
	sink.probe_spill->consumer->ScanChunk(probe_local_scan, lhs_probe_chunk);

	// Get the probe chunk columns/hashes
	lhs_join_keys.Reset();
	lhs_join_key_executor.Execute(lhs_probe_chunk, lhs_join_keys);

	// reference ALL probe columns
	lhs_probe_data.ReferenceColumns(lhs_probe_chunk, gstate.op.lhs_probe_columns.col_idxs);

	if (sink.hash_table->Count() == 0 && sink.hash_table->HasUncorrelatedMarkJoin()) {
		sink.hash_table->ConstructMarkJoinResult(lhs_join_keys, lhs_probe_data, chunk);
		empty_ht_probe_in_progress = true;
		return;
	}
	if (sink.hash_table->Count() == 0 && !gstate.op.EmptyResultIfRHSIsEmpty()) {
		// for empty result, only need output columns (no predicate evaluation)
		lhs_output_data.ReferenceColumns(lhs_probe_chunk, gstate.op.lhs_output_columns.col_idxs);
		gstate.op.ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_filtered_null,
		                                   lhs_output_data, chunk);
		empty_ht_probe_in_progress = true;
		return;
	}

	// Perform the probe
	auto precomputed_hashes = &lhs_probe_chunk.data.back();
	sink.hash_table->Probe(scan_structure, lhs_join_keys, join_key_state, probe_state, precomputed_hashes);
	scan_structure.Next(lhs_join_keys, lhs_probe_data, chunk);
}

void HashJoinLocalSourceState::ExternalScanHT(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate,
                                              DataChunk &chunk) {
	D_ASSERT(local_stage == HashJoinSourceStage::SCAN_HT);

	if (!full_outer_scan_state) {
		full_outer_scan_state =
		    make_uniq<JoinHTScanState>(sink.hash_table->GetDataCollection(), full_outer_chunk_idx_from,
		                               full_outer_chunk_idx_to, sink.hash_table->output_columns.size());
		full_outer_chunks_reported = 0;
	}
	sink.hash_table->ScanFullOuter(*full_outer_scan_state, addresses, chunk);

	auto chunks_scanned =
	    chunk.size() == 0 ? full_outer_chunk_idx_to - full_outer_chunk_idx_from : full_outer_scan_state->chunks_done;
	if (chunks_scanned > full_outer_chunks_reported) {
		gstate.full_outer_chunk_progress.fetch_add(chunks_scanned - full_outer_chunks_reported,
		                                           std::memory_order_relaxed);
		full_outer_chunks_reported = chunks_scanned;
	}
	if (chunk.size() == 0) {
		full_outer_scan_state = nullptr;
		annotated_lock_guard<annotated_mutex> guard(gstate.lock);
		gstate.full_outer_chunk_done += full_outer_chunk_idx_to - full_outer_chunk_idx_from;
	}
}

static SourceResultType ScanHashJoinSourceState(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input,
                                                const PhysicalHashJoin &op) {
	(void)context;
	auto &sink = op.sink_state->Cast<HashJoinGlobalSinkState>();
	auto &gstate = input.global_state.Cast<HashJoinGlobalSourceState>();
	auto &lstate = input.local_state.Cast<HashJoinLocalSourceState>();
	sink.scanned_data = true;

	if (!sink.external && !PropagatesBuildSide(op.join_type)) {
		ExecutionOperatorStageTimer timer(input.stage_recorder,
		                                  "source_contract.hash_join_state_scan.finish_non_producing");
		annotated_lock_guard<annotated_mutex> guard(gstate.lock);
		if (gstate.global_stage != HashJoinSourceStage::DONE) {
			gstate.global_stage = HashJoinSourceStage::DONE;
			if (sink.preserve_build_for_reuse) {
				sink.scanned_data = false;
			} else {
				sink.hash_table->Reset();
				sink.temporary_memory_state->SetZero();
			}
		}
		return SourceResultType::FINISHED;
	}

	if (gstate.global_stage == HashJoinSourceStage::INIT) {
		ExecutionOperatorStageTimer timer(input.stage_recorder, "source_contract.hash_join_state_scan.initialize");
		gstate.Initialize(sink);
	}

	// Any call to GetData must produce tuples, otherwise the pipeline executor thinks that we're done
	// Therefore, we loop until we've produced tuples, or until the operator is actually done
	while (gstate.global_stage != HashJoinSourceStage::DONE && chunk.size() == 0) {
		bool has_task = !lstate.TaskFinished();
		if (!has_task) {
			ExecutionOperatorStageTimer timer(input.stage_recorder, "source_contract.hash_join_state_scan.assign_task");
			has_task = gstate.AssignTask(sink, lstate);
		}
		if (has_task) {
			ExecutionOperatorStageTimer timer(input.stage_recorder,
			                                  "source_contract.hash_join_state_scan.execute_task");
			lstate.ExecuteTask(sink, gstate, chunk);
		} else {
			ExecutionOperatorStageTimer prepare_timer(input.stage_recorder,
			                                          "source_contract.hash_join_state_scan.prepare_next_stage");
			annotated_lock_guard<annotated_mutex> guard(gstate.lock);
			bool next_stage_ready =
			    gstate.TryPrepareNextStage(sink) || gstate.global_stage == HashJoinSourceStage::DONE;
			if (next_stage_ready) {
				gstate.UnblockTasks();
			} else if (!sink.external && gstate.global_stage == HashJoinSourceStage::SCAN_HT) {
				// Without spilling, scanning the hash table is the last stage and all of its tasks have been assigned,
				// so there is nothing left for this task to do. Do not block until the other tasks finish: this task
				// has already handed probe rows to the sink, which may in turn be waiting for this task
				// (e.g., a partitioned COPY cannot flush until every producer has combined).
				return SourceResultType::FINISHED;
			} else {
				ExecutionOperatorStageTimer timer(input.stage_recorder,
				                                  "source_contract.hash_join_state_scan.block_source");
				return gstate.BlockSource(input.interrupt_state);
			}
		}
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

SourceResultType PhysicalHashJoin::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	return ScanHashJoinSourceState(context, chunk, input, *this);
}

bool PhysicalHashJoin::SupportsExecutionSourceContract(const ExecutionRegionOpenRequest &open_request) const {
	return open_request.UsesSourceContract() && PropagatesBuildSide(join_type);
}

SourceResultType PhysicalHashJoin::GetExecutionSourceContractDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                                          OperatorSourceInput &input) const {
	return ScanHashJoinSourceState(context, chunk, input, *this);
}

ProgressData PhysicalHashJoin::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	auto &gstate = gstate_p.Cast<HashJoinGlobalSourceState>();

	ProgressData res;

	if (!sink.external) {
		if (PropagatesBuildSide(join_type)) {
			res.done = static_cast<double>(gstate.full_outer_chunk_progress.load(std::memory_order_relaxed));
			res.total = static_cast<double>(gstate.full_outer_chunk_count);
			return res;
		}
		res.done = gstate.global_stage == HashJoinSourceStage::DONE ? 1.0 : 0.0;
		res.total = 1.0;
		return res;
	}

	const auto &ht = *sink.hash_table;
	res.total = static_cast<double>(RadixPartitioning::NumberOfPartitions(ht.GetRadixBits()));

	// Progress holds the executor lock, which workers may acquire while holding this lock.
	if (!gstate.lock.try_lock()) {
		return gstate.external_progress.Update(res);
	}
	annotated_lock_guard<annotated_mutex> guard(gstate.lock, std::adopt_lock);

	res.done = static_cast<double>(ht.FinishedPartitionCount());

	const auto probe_chunk_done = static_cast<double>(gstate.probe_chunk_done);
	const auto probe_chunk_count = static_cast<double>(gstate.probe_chunk_count);
	if (probe_chunk_count != 0) {
		// Progress of the current round of probing
		auto probe_progress = probe_chunk_done / probe_chunk_count;
		// Weighed by the number of partitions
		probe_progress *= static_cast<double>(ht.CurrentPartitionCount());
		// Add it to the progress
		res.done += probe_progress;
	}

	return gstate.external_progress.Update(res);
}

InsertionOrderPreservingMap<string> PhysicalHashJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Join Type"] = EnumUtil::ToString(join_type);

	string condition_info;
	for (idx_t i = 0; i < conditions.size(); i++) {
		auto &join_condition = conditions[i];
		if (i > 0) {
			condition_info += "\n";
		}
		condition_info += StringUtil::Format("%s %s %s", join_condition.GetLHS().GetName().GetIdentifierName(),
		                                     ExpressionTypeToOperator(join_condition.GetComparisonType()),
		                                     join_condition.GetRHS().GetName().GetIdentifierName());
	}

	if (predicate) {
		if (!condition_info.empty()) {
			condition_info += "\n";
		}
		condition_info += predicate->ToString();
	}

	result["Conditions"] = condition_info;

	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
