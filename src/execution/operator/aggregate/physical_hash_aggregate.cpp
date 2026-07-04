#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/row/tuple_data_row_location_remap.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/execution_region_settings.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/operator/aggregate/distinct_aggregate_data.hpp"
#include "duckdb/execution/operator/aggregate/distinct_count_pointer_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

HashAggregateGroupingData::HashAggregateGroupingData(GroupingSet &grouping_set_p,
                                                     const GroupedAggregateData &grouped_aggregate_data,
                                                     unique_ptr<DistinctAggregateCollectionInfo> &info,
                                                     TupleDataValidityType group_validity,
                                                     TupleDataValidityType distinct_validity,
                                                     DistinctAggregateKeyStrategy distinct_key_strategy)
    : table_data(grouping_set_p, grouped_aggregate_data, group_validity) {
	if (info) {
		auto nested_validity = group_validity == TupleDataValidityType::CANNOT_HAVE_NULL_VALUES &&
		                               distinct_validity == TupleDataValidityType::CANNOT_HAVE_NULL_VALUES
		                           ? TupleDataValidityType::CANNOT_HAVE_NULL_VALUES
		                           : TupleDataValidityType::CAN_HAVE_NULL_VALUES;
		distinct_data = make_uniq<DistinctAggregateData>(*info, grouping_set_p, &grouped_aggregate_data.groups,
		                                                 nested_validity, distinct_key_strategy);
	}
}

bool HashAggregateGroupingData::HasDistinct() const {
	return distinct_data != nullptr;
}

HashAggregateGroupingGlobalState::HashAggregateGroupingGlobalState(const HashAggregateGroupingData &data,
                                                                   ClientContext &context) {
	table_state = data.table_data.GetGlobalSinkState(context);
	if (data.HasDistinct() && !data.distinct_data->UsesGroupStatePointerKeys()) {
		distinct_state = make_uniq<DistinctAggregateState>(*data.distinct_data, context);
	}
}

HashAggregateGroupingLocalState::HashAggregateGroupingLocalState(const PhysicalHashAggregate &op,
                                                                 const HashAggregateGroupingData &data,
                                                                 ExecutionContext &context) {
	table_state = data.table_data.GetLocalSinkState(context);
	if (!data.HasDistinct()) {
		return;
	}
	auto &distinct_data = *data.distinct_data;
	if (distinct_data.UsesGroupStatePointerKeys()) {
		distinct_count_scratch = make_uniq<DistinctCountPointerScratch>();
		return;
	}

	auto &distinct_indices = op.distinct_collection_info->Indices();
	D_ASSERT(!distinct_indices.empty());

	distinct_states.resize(op.distinct_collection_info->aggregates.size());
	auto &table_map = op.distinct_collection_info->table_map;

	for (auto &idx : distinct_indices) {
		idx_t table_idx = table_map[idx];
		auto &radix_table = distinct_data.radix_tables[table_idx];
		if (radix_table == nullptr) {
			// This aggregate has identical input as another aggregate, so no table is created for it
			continue;
		}
		// Initialize the states of the radix tables used for the distinct aggregates
		distinct_states[table_idx] = radix_table->GetLocalSinkState(context);
	}
}

static vector<LogicalType> CreateGroupChunkTypes(vector<unique_ptr<Expression>> &groups) {
	set<idx_t> group_indices;

	if (groups.empty()) {
		return {};
	}

	for (auto &group : groups) {
		D_ASSERT(group->GetExpressionType() == ExpressionType::BOUND_REF);
		auto &bound_ref = group->Cast<BoundReferenceExpression>();
		group_indices.insert(bound_ref.Index());
	}
	idx_t highest_index = *group_indices.rbegin();
	vector<LogicalType> types(highest_index + 1, LogicalType::SQLNULL);
	for (auto &group : groups) {
		auto &bound_ref = group->Cast<BoundReferenceExpression>();
		types[bound_ref.Index()] = bound_ref.GetReturnType();
	}
	return types;
}

static bool HashAggregateCanUseDistinctCountPointerKeys(
    const GroupedAggregateData &grouped_aggregate_data, const unique_ptr<DistinctAggregateCollectionInfo> &info,
    idx_t grouping_count, const unsafe_vector<idx_t> &non_distinct_filter, const unsafe_vector<idx_t> &distinct_filter,
    const reference_map_t<const Expression, size_t> &filter_indexes) {
	if (!info || grouping_count != 1 || grouped_aggregate_data.groups.empty() ||
	    grouped_aggregate_data.aggregates.size() != 1 || distinct_filter.size() != 1 || !non_distinct_filter.empty() ||
	    !filter_indexes.empty()) {
		return false;
	}
	if (grouped_aggregate_data.filter_count != 0 || grouped_aggregate_data.payload_types.size() != 1) {
		return false;
	}
	auto &aggregate = grouped_aggregate_data.aggregates[0]->Cast<BoundAggregateExpression>();
	if (!aggregate.IsDistinct() || aggregate.GetFilter() || aggregate.GetOrderBys() ||
	    aggregate.GetChildren().size() != 1) {
		return false;
	}
	auto &function = aggregate.Function();
	auto &child = aggregate.GetChildren()[0];
	return function.HasPrimitiveUpdateABI() &&
	       function.GetPrimitiveUpdateABI().kind == AggregatePrimitiveUpdateKind::COUNT &&
	       child->GetExpressionClass() == ExpressionClass::BOUND_REF &&
	       DistinctCountPointerPayloadTypeSupported(child->GetReturnType().InternalType());
}

static bool HashAggregateCanPlanDistinctCountPointerKeys(ClientContext &context) {
	return ExecutionRegionSettings::Enabled(context) &&
	       ExecutionRegionSettings::Policy(context) != ExecutionRegionPolicyMode::OFF;
}

static bool HashAggregateUsesDistinctCountPointerKeys(const PhysicalHashAggregate &op) {
	return op.groupings.size() == 1 && op.groupings[0].distinct_data &&
	       op.groupings[0].distinct_data->UsesGroupStatePointerKeys();
}

static bool TrySinkDistinctCountPointerKeys(ExecutionContext &context, const PhysicalHashAggregate &op,
                                            const HashAggregateGroupingData &grouping_data,
                                            HashAggregateGroupingLocalState &grouping_lstate,
                                            OperatorSinkInput &sink_input, DataChunk &input);
static void BindHashAggregateDistinctCountPointerUpdate(const PhysicalHashAggregate &op,
                                                        const HashAggregateGroupingData &grouping_data,
                                                        HashAggregateGroupingLocalState &grouping_lstate,
                                                        ExecutionDistinctCountPointerUpdateBinding &binding);

bool PhysicalHashAggregate::CanSkipRegularSink() const {
	if (HashAggregateUsesDistinctCountPointerKeys(*this)) {
		return false;
	}
	if (!filter_indexes.empty()) {
		// If we have filters, we can't skip the regular sink, because we might lose groups otherwise.
		return false;
	}
	if (grouped_aggregate_data.aggregates.empty()) {
		// When there are no aggregates, we have to add to the main ht right away
		return false;
	}
	if (!non_distinct_filter.empty()) {
		return false;
	}
	return true;
}

PhysicalHashAggregate::PhysicalHashAggregate(PhysicalPlan &physical_plan, ClientContext &context,
                                             vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
                                             idx_t estimated_cardinality)
    : PhysicalHashAggregate(physical_plan, context, std::move(types), std::move(expressions), {},
                            estimated_cardinality) {
}

PhysicalHashAggregate::PhysicalHashAggregate(PhysicalPlan &physical_plan, ClientContext &context,
                                             vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p, idx_t estimated_cardinality)
    : PhysicalHashAggregate(physical_plan, context, std::move(types), std::move(expressions), std::move(groups_p), {},
                            {}, estimated_cardinality, TupleDataValidityType::CAN_HAVE_NULL_VALUES,
                            TupleDataValidityType::CAN_HAVE_NULL_VALUES) {
}

PhysicalHashAggregate::PhysicalHashAggregate(PhysicalPlan &physical_plan, ClientContext &context,
                                             vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p,
                                             vector<GroupingSet> grouping_sets_p,
                                             vector<unsafe_vector<ProjectionIndex>> grouping_functions_p,
                                             idx_t estimated_cardinality, TupleDataValidityType group_validity,
                                             TupleDataValidityType distinct_validity)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::HASH_GROUP_BY, std::move(types), estimated_cardinality),
      grouping_sets(std::move(grouping_sets_p)) {
	// get a list of all aggregates to be computed
	const idx_t group_count = groups_p.size();
	if (grouping_sets.empty()) {
		GroupingSet set;
		for (idx_t i = 0; i < group_count; i++) {
			set.insert(ProjectionIndex(i));
		}
		grouping_sets.push_back(std::move(set));
	}
	input_group_types = CreateGroupChunkTypes(groups_p);

	grouped_aggregate_data.InitializeGroupby(std::move(groups_p), std::move(expressions),
	                                         std::move(grouping_functions_p));

	auto &aggregates = grouped_aggregate_data.aggregates;
	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	// Because everything that lives in this class should be read-only at execution time
	idx_t aggregate_input_idx = 0;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		aggregate_input_idx += aggr.GetChildren().size();
		if (aggr.GetAggregateType() == AggregateType::DISTINCT) {
			distinct_filter.push_back(i);
		} else if (aggr.GetAggregateType() == AggregateType::NON_DISTINCT) {
			non_distinct_filter.push_back(i);
		} else { // LCOV_EXCL_START
			throw NotImplementedException("AggregateType not implemented in PhysicalHashAggregate");
		} // LCOV_EXCL_STOP
	}

	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.GetFilter()) {
			auto &filter_ref = *aggr.GetFilter();
			auto &bound_ref_expr = filter_ref.Cast<BoundReferenceExpression>();
			if (!filter_indexes.count(filter_ref)) {
				// Replace the bound reference expression's index with the corresponding index of the payload chunk
				filter_indexes[filter_ref] = bound_ref_expr.Index();
				bound_ref_expr.IndexMutable() = aggregate_input_idx;
			}
			aggregate_input_idx++;
		}
	}

	distinct_collection_info = DistinctAggregateCollectionInfo::Create(grouped_aggregate_data.aggregates);
	const auto use_distinct_count_pointer_keys =
	    HashAggregateCanPlanDistinctCountPointerKeys(context) &&
	    HashAggregateCanUseDistinctCountPointerKeys(grouped_aggregate_data, distinct_collection_info,
	                                                grouping_sets.size(), non_distinct_filter, distinct_filter,
	                                                filter_indexes);
	const auto distinct_key_strategy = use_distinct_count_pointer_keys
	                                       ? DistinctAggregateKeyStrategy::GROUP_STATE_POINTER
	                                       : DistinctAggregateKeyStrategy::GROUP_KEYS;

	for (idx_t i = 0; i < grouping_sets.size(); i++) {
		groupings.emplace_back(grouping_sets[i], grouped_aggregate_data, distinct_collection_info, group_validity,
		                       distinct_validity, distinct_key_strategy);
	}
}

class DistinctCountPointerLocalRowMoveState : public TupleDataRowLocationRemap {
public:
	explicit DistinctCountPointerLocalRowMoveState(DistinctCountPointerSet &distinct_set_p)
	    : distinct_set(distinct_set_p) {
	}

	void Remap(Vector &source_addresses, const SelectionVector &source_sel, Vector &target_addresses,
	           idx_t count) override {
		if (count == 0) {
			return;
		}
		source_addresses.Flatten();
		target_addresses.Flatten();
		const auto source_data = FlatVector::GetData<uintptr_t>(source_addresses);
		const auto target_data = FlatVector::GetData<uintptr_t>(target_addresses);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			state_pointer_mappings.emplace_back(source_data[source_sel.get_index(row_idx)], target_data[row_idx]);
		}
	}

	void Flush() override {
		distinct_set.RemapStatePointers(state_pointer_mappings);
		state_pointer_mappings.clear();
	}

private:
	DistinctCountPointerSet &distinct_set;
	vector<pair<uintptr_t, uintptr_t>> state_pointer_mappings;
};

class DistinctCountPointerGlobalMergeState : public TupleDataRowLocationRemap {
public:
	void AddLocalSet(unique_ptr<DistinctCountPointerSet> local_set) {
		if (!local_set || local_set->GroupCount() == 0) {
			return;
		}
		const annotated_lock_guard<annotated_mutex> guard {lock};
		const auto set_index = local_sets.size();
		if (state_value_offset == DConstants::INVALID_INDEX) {
			state_value_offset = local_set->StateValueOffset();
		} else if (state_value_offset != local_set->StateValueOffset()) {
			throw InternalException("Distinct count pointer state offset changed during hash aggregate merge");
		}
		local_set->VisitStatePointers(
		    [&](uintptr_t source_state_pointer) { source_state_to_set_index[source_state_pointer] = set_index; });
		local_sets.emplace_back(std::move(local_set));
	}

	void Remap(Vector &source_addresses, const SelectionVector &source_sel, Vector &target_addresses,
	           idx_t count) override {
		if (count == 0) {
			return;
		}
		source_addresses.Flatten();
		target_addresses.Flatten();
		const auto source_data = FlatVector::GetData<uintptr_t>(source_addresses);
		const auto target_data = FlatVector::GetData<uintptr_t>(target_addresses);
		const annotated_lock_guard<annotated_mutex> guard {lock};
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto source_state_pointer = source_data[source_sel.get_index(row_idx)];
			auto entry = source_state_to_set_index.find(source_state_pointer);
			if (entry == source_state_to_set_index.end()) {
				continue;
			}
			auto &local_set = *local_sets[entry->second];
			if (!merged_set) {
				merged_set = make_uniq<DistinctCountPointerSet>();
			}
			const auto target_state_pointer = target_data[row_idx];
			if (!local_set.MergeStatePayloadsTo(*merged_set, source_state_pointer, target_state_pointer)) {
				throw InternalException("Distinct count pointer state remap failed during hash aggregate merge");
			}
			source_state_to_set_index.erase(entry);
		}
	}

	void Reset() {
		const annotated_lock_guard<annotated_mutex> guard {lock};
		local_sets.clear();
		source_state_to_set_index.clear();
		merged_set.reset();
		state_value_offset = DConstants::INVALID_INDEX;
	}

private:
	annotated_mutex lock;
	vector<unique_ptr<DistinctCountPointerSet>> local_sets;
	unordered_map<uintptr_t, idx_t> source_state_to_set_index;
	unique_ptr<DistinctCountPointerSet> merged_set;
	idx_t state_value_offset = DConstants::INVALID_INDEX;
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashAggregateGlobalSinkState : public GlobalSinkState {
public:
	HashAggregateGlobalSinkState(const PhysicalHashAggregate &op, ClientContext &context) : op(op) {
		grouping_states.reserve(op.groupings.size());
		distinct_count_pointer_merge_states.reserve(op.groupings.size());
		for (idx_t i = 0; i < op.groupings.size(); i++) {
			auto &grouping = op.groupings[i];
			grouping_states.emplace_back(grouping, context);
			if (grouping.HasDistinct() && grouping.distinct_data->UsesGroupStatePointerKeys()) {
				distinct_count_pointer_merge_states.emplace_back(make_uniq<DistinctCountPointerGlobalMergeState>());
			} else {
				distinct_count_pointer_merge_states.emplace_back(nullptr);
			}
		}
		vector<LogicalType> filter_types;
		for (auto &aggr : op.grouped_aggregate_data.aggregates) {
			auto &aggregate = aggr->Cast<BoundAggregateExpression>();
			for (auto &child : aggregate.GetChildren()) {
				payload_types.push_back(child->GetReturnType());
			}
			if (aggregate.GetFilter()) {
				filter_types.push_back(aggregate.GetFilter()->GetReturnType());
			}
		}
		payload_types.reserve(payload_types.size() + filter_types.size());
		payload_types.insert(payload_types.end(), filter_types.begin(), filter_types.end());
	}

	const PhysicalHashAggregate &op;
	vector<HashAggregateGroupingGlobalState> grouping_states;
	vector<unique_ptr<DistinctCountPointerGlobalMergeState>> distinct_count_pointer_merge_states;
	vector<LogicalType> payload_types;
	//! Whether or not the aggregate is finished
	bool finished = false;

	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ClientContext &context) override {
		for (idx_t grouping_idx = 0; grouping_idx < op.groupings.size(); grouping_idx++) {
			auto &grouping = op.groupings[grouping_idx];
			auto &grouping_state = grouping_states[grouping_idx];
			grouping.table_data.ResetGlobalSinkState(context, *grouping_state.table_state);
			if (!grouping.HasDistinct()) {
				continue;
			}
			auto &distinct_data = *grouping.distinct_data;
			if (distinct_data.UsesGroupStatePointerKeys()) {
				if (distinct_count_pointer_merge_states[grouping_idx]) {
					distinct_count_pointer_merge_states[grouping_idx]->Reset();
				}
				continue;
			}
			auto &distinct_state = *grouping_state.distinct_state;
			for (idx_t table_idx = 0; table_idx < distinct_data.radix_tables.size(); table_idx++) {
				auto &radix_table = distinct_data.radix_tables[table_idx];
				if (!radix_table) {
					continue;
				}
				radix_table->ResetGlobalSinkState(context, *distinct_state.radix_states[table_idx]);
			}
		}
		finished = false;
		GlobalSinkState::Reset(context);
	}
};

class HashAggregateLocalSinkState : public LocalSinkState {
public:
	HashAggregateLocalSinkState(const PhysicalHashAggregate &op, ExecutionContext &context) : op(op) {
		auto &payload_types = op.grouped_aggregate_data.payload_types;
		if (!payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(payload_types);
		}

		grouping_states.reserve(op.groupings.size());
		for (auto &grouping : op.groupings) {
			grouping_states.emplace_back(op, grouping, context);
		}
		// The filter set is only needed here for the distinct aggregates
		// the filtering of data for the regular aggregates is done within the hashtable
		vector<AggregateObject> aggregate_objects;
		for (auto &aggregate : op.grouped_aggregate_data.aggregates) {
			auto &aggr = aggregate->Cast<BoundAggregateExpression>();
			aggregate_objects.emplace_back(&aggr);
		}

		filter_set.Initialize(context.client, aggregate_objects, payload_types);
	}

	const PhysicalHashAggregate &op;
	DataChunk aggregate_input_chunk;
	vector<HashAggregateGroupingLocalState> grouping_states;
	AggregateFilterDataSet filter_set;

	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ExecutionContext &context, GlobalSinkState &gstate_p) override {
		auto &gstate = gstate_p.Cast<HashAggregateGlobalSinkState>();
		// Sink repopulates every aggregate-input column by reference before use, so we just clear it here.
		// Use Reset() rather than SetChildCardinality(0): the chunk may still hold non-flat (e.g. dictionary)
		// references from a previous iteration that SetChildCardinality cannot resize.
		aggregate_input_chunk.Reset();
		for (idx_t grouping_idx = 0; grouping_idx < op.groupings.size(); grouping_idx++) {
			auto &grouping = op.groupings[grouping_idx];
			auto &grouping_gstate = gstate.grouping_states[grouping_idx];
			auto &grouping_state = grouping_states[grouping_idx];
			grouping.table_data.ResetLocalSinkState(context, *grouping_gstate.table_state, *grouping_state.table_state);
			if (!grouping.HasDistinct()) {
				continue;
			}
			auto &distinct_data = *grouping.distinct_data;
			if (distinct_data.UsesGroupStatePointerKeys()) {
				if (grouping_state.distinct_count_scratch) {
					grouping_state.distinct_count_scratch->Reset();
				}
				continue;
			}
			auto &distinct_gstate = *grouping_gstate.distinct_state;
			for (idx_t table_idx = 0; table_idx < distinct_data.radix_tables.size(); table_idx++) {
				auto &radix_table = distinct_data.radix_tables[table_idx];
				if (!radix_table) {
					continue;
				}
				radix_table->ResetLocalSinkState(context, *distinct_gstate.radix_states[table_idx],
				                                 *grouping_state.distinct_states[table_idx]);
			}
		}
	}
};

class HashAggregateExecutionRegionSinkState : public ExecutionAggregateUpdateState {
public:
	HashAggregateExecutionRegionSinkState(ExecutionContext &context_p, const PhysicalHashAggregate &op_p,
	                                      HashAggregateGlobalSinkState &global_state_p,
	                                      HashAggregateLocalSinkState &local_state_p, InterruptState &interrupt_state_p)
	    : context(context_p), op(op_p), global_state(global_state_p), local_state(local_state_p),
	      interrupt_state(interrupt_state_p) {
	}

	SinkResultType Sink(DataChunk &input) override {
		OperatorSinkInput sink_input {global_state, local_state, interrupt_state};
		return op.Sink(context, input, sink_input);
	}

private:
	ExecutionContext &context;
	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &global_state;
	HashAggregateLocalSinkState &local_state;
	InterruptState &interrupt_state;
};

class HashAggregateDistinctCountPointerUpdateState : public ExecutionDistinctCountPointerUpdateState {
public:
	explicit HashAggregateDistinctCountPointerUpdateState(DistinctCountPointerSet &distinct_set_p)
	    : distinct_set(distinct_set_p) {
	}

	bool AddPayloads(Vector &state_pointers, Vector &payload, idx_t count, idx_t state_value_offset) override {
		return distinct_set.Add(state_pointers, payload, count, state_value_offset);
	}

	bool UseGlobalPayloadSet(idx_t expected_payload_count) override {
		return distinct_set.UseGlobalPayloadSet(expected_payload_count);
	}

	bool AddSelectedPayloads(const uintptr_t *state_pointers, const sel_t *state_sel, const sel_t *payload_sel,
	                         Vector &payload, idx_t count, idx_t state_value_offset) override {
		return distinct_set.AddSelected(state_pointers, state_sel, payload_sel, payload, count, state_value_offset);
	}

private:
	DistinctCountPointerSet &distinct_set;
};

class HashAggregateStateAddressState : public ExecutionGroupedAggregateStateAddressState {
public:
	HashAggregateStateAddressState(ExecutionContext &context_p, const PhysicalHashAggregate &op_p,
	                               HashAggregateGlobalSinkState &global_state_p,
	                               HashAggregateLocalSinkState &local_state_p, InterruptState &interrupt_state_p)
	    : context(context_p), op(op_p), global_state(global_state_p), local_state(local_state_p),
	      interrupt_state(interrupt_state_p) {
	}

private:
	struct SingleGroupingSinkState {
		const HashAggregateGroupingData &grouping;
		OperatorSinkInput input;
	};

	bool HasSingleGroupingState() const {
		return op.groupings.size() == 1 && global_state.grouping_states.size() == 1 &&
		       local_state.grouping_states.size() == 1;
	}

	bool CanUseSingleGroupingState() const {
		return !op.distinct_collection_info && HasSingleGroupingState();
	}

	bool CanResolveDistinctCountPointerAddresses(const ExecutionRegionSinkInfo &sink_info) const {
		return HasSingleGroupingState() && op.distinct_collection_info &&
		       sink_info.aggregate_contract.distinct_count_pointer_keys;
	}

	bool CanFindRowPointerGroupStateTargets(const ExecutionRegionSinkInfo &sink_info) const {
		return CanUseSingleGroupingState() || CanResolveDistinctCountPointerAddresses(sink_info);
	}

	void RequireSingleGroupingState(const char *unsupported_message, const char *shape_message) const {
		if (op.distinct_collection_info) {
			throw InternalException(unsupported_message);
		}
		RequireSingleGroupingStateShape(shape_message);
	}

	void RequireSingleGroupingStateShape(const char *shape_message) const {
		if (!HasSingleGroupingState()) {
			throw InternalException(shape_message);
		}
	}

	SingleGroupingSinkState GetSingleGroupingSinkState() {
		auto &grouping_global_state = global_state.grouping_states[0];
		auto &grouping_local_state = local_state.grouping_states[0];
		return {op.groupings[0],
		        {*grouping_global_state.table_state, *grouping_local_state.table_state, interrupt_state}};
	}

public:
	void ResolveStateAddresses(DataChunk &input, Vector &addresses,
	                           optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr) override {
		RequireSingleGroupingState("execution region hash aggregate state lookup received unsupported aggregate state",
		                           "execution region hash aggregate state lookup requires one grouping state");
		auto single_grouping = GetSingleGroupingSinkState();
		single_grouping.grouping.table_data.ResolveStateAddresses(context, input, single_grouping.input, addresses,
		                                                          recorder);
	}

	bool ReserveGroups(idx_t group_count, optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr) override {
		if (!CanUseSingleGroupingState()) {
			return false;
		}
		auto single_grouping = GetSingleGroupingSinkState();
		return single_grouping.grouping.table_data.ReserveGroups(context, single_grouping.input, group_count, recorder);
	}

	bool TryUpdateNewGroups(DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	                        const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	                        optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true,
	                        optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) override {
		if (!CanUseSingleGroupingState()) {
			return false;
		}
		auto single_grouping = GetSingleGroupingSinkState();
		return single_grouping.grouping.table_data.TryUpdateNewPrimitiveGroups(
		    context, input, single_grouping.input, sink_info, lanes, recorder, finish, dense_domain);
	}

	bool TryUpdateNewGroupsWithPayloadInput(
	    DataChunk &groups, DataChunk &payload_input, const vector<idx_t> &payload_source_indices,
	    const ExecutionRegionSinkInfo &sink_info, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true,
	    optional_ptr<Vector> precomputed_hashes = nullptr,
	    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) override {
		if (!CanUseSingleGroupingState()) {
			return false;
		}
		auto single_grouping = GetSingleGroupingSinkState();
		return single_grouping.grouping.table_data.TryUpdateNewPrimitiveGroupsWithPayloadInput(
		    context, groups, payload_input, payload_source_indices, single_grouping.input, sink_info, lanes, recorder,
		    finish, precomputed_hashes, dense_domain);
	}

	bool TryAppendNewGroups(DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	                        const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	                        optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                        bool finish = true) override {
		if (!CanUseSingleGroupingState()) {
			return false;
		}
		auto single_grouping = GetSingleGroupingSinkState();
		return single_grouping.grouping.table_data.TryAppendNewPrimitiveGroups(context, input, single_grouping.input,
		                                                                       sink_info, lanes, recorder, finish);
	}

	bool TryUpdateNewGroupsWithSelectedStateAddresses(
	    DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true,
	    optional_ptr<Vector> precomputed_hashes = nullptr,
	    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) override {
		if (!CanUseSingleGroupingState() && !CanResolveDistinctCountPointerAddresses(sink_info)) {
			return false;
		}
		auto single_grouping = GetSingleGroupingSinkState();
		return single_grouping.grouping.table_data.TryUpdateNewGroupsWithSelectedStateAddresses(
		    context, input, single_grouping.input, sink_info, update_function, update_state, recorder, finish,
		    precomputed_hashes, dense_domain);
	}

	bool TryUpdateGroupKeysWithSelectedStateAddresses(
	    DataChunk &groups, const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr, bool finish = true,
	    optional_ptr<Vector> precomputed_hashes = nullptr,
	    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) override {
		if (!CanUseSingleGroupingState() && !CanResolveDistinctCountPointerAddresses(sink_info)) {
			return false;
		}
		auto single_grouping = GetSingleGroupingSinkState();
		return single_grouping.grouping.table_data.TryUpdateGroupKeysWithSelectedStateAddresses(
		    context, groups, single_grouping.input, sink_info, update_function, update_state, recorder, finish,
		    precomputed_hashes, dense_domain);
	}

	bool TryFindOrCreateRowPointerGroupStateTargets(
	    DataChunk &payload_input, Vector &row_pointers, idx_t count,
	    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const ExecutionRegionSinkInfo &sink_info,
	    ExecutionGroupedAggregateStateTargetBatch &targets,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr) override {
		if (!CanFindRowPointerGroupStateTargets(sink_info)) {
			return false;
		}
		auto single_grouping = GetSingleGroupingSinkState();
		return single_grouping.grouping.table_data.TryFindOrCreateRowPointerGroupStateTargets(
		    context, payload_input, row_pointers, count, single_grouping.input, group_sources, sink_info, targets,
		    recorder);
	}

	bool TryFindOrCreateInputVectorGroupStateTargets(
	    DataChunk &payload_input, idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
	    const ExecutionRegionSinkInfo &sink_info, ExecutionGroupedAggregateStateTargetBatch &targets,
	    optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) override {
		if (!CanFindRowPointerGroupStateTargets(sink_info)) {
			return false;
		}
		auto single_grouping = GetSingleGroupingSinkState();
		return single_grouping.grouping.table_data.TryFindOrCreateInputVectorGroupStateTargets(
		    context, payload_input, count, single_grouping.input, group_sources, sink_info, targets, recorder,
		    dense_domain);
	}

	bool TryUpdateRowPointerGroupPayloads(DataChunk &payload_input, Vector &row_pointers, idx_t count,
	                                      const vector<ExecutionRowPointerGroupKeySource> &group_sources,
	                                      const vector<idx_t> &payload_source_indices,
	                                      const ExecutionRegionSinkInfo &sink_info,
	                                      const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	                                      optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                                      bool finish = true) override {
		if (!CanUseSingleGroupingState()) {
			return false;
		}
		auto single_grouping = GetSingleGroupingSinkState();
		return single_grouping.grouping.table_data.TryUpdateRowPointerGroupPrimitivePayloads(
		    context, payload_input, row_pointers, count, group_sources, payload_source_indices, single_grouping.input,
		    sink_info, lanes, recorder, finish);
	}

	bool TryAppendNewGroupsWithStateAddresses(DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	                                          ExecutionGroupedAggregateStateAddressUpdateFunction update_function,
	                                          void *update_state,
	                                          optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                                          bool finish = true) override {
		if (!CanUseSingleGroupingState()) {
			return false;
		}
		auto single_grouping = GetSingleGroupingSinkState();
		return single_grouping.grouping.table_data.TryAppendNewGroupsWithStateAddresses(
		    context, input, single_grouping.input, sink_info, update_function, update_state, recorder, finish);
	}

	bool TryResolveNewGroups(DataChunk &input, const ExecutionRegionSinkInfo &sink_info, Vector &addresses,
	                         optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                         bool finish = true) override {
		if (!CanUseSingleGroupingState()) {
			return false;
		}
		auto single_grouping = GetSingleGroupingSinkState();
		return single_grouping.grouping.table_data.TryResolveNewGroupAddresses(context, input, single_grouping.input,
		                                                                       sink_info, addresses, recorder, finish);
	}

	bool TryResolveDistinctCountPointerAddresses(DataChunk &input,
	                                             const ExecutionRegionSinkInfo &state_address_sink_info,
	                                             Vector &addresses,
	                                             optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                                             bool finish = true) override {
		(void)finish;
		if (!CanResolveDistinctCountPointerAddresses(state_address_sink_info)) {
			return false;
		}
		auto single_grouping = GetSingleGroupingSinkState();
		if (single_grouping.grouping.table_data.TryResolveNewGroupAddresses(
		        context, input, single_grouping.input, state_address_sink_info, addresses, recorder, false)) {
			return true;
		}
		single_grouping.grouping.table_data.ResolveStateAddresses(context, input, single_grouping.input, addresses,
		                                                          recorder);
		addresses.Flatten();
		return true;
	}

	void FinishStateUpdates() override {
		RequireSingleGroupingStateShape("execution region hash aggregate state update requires one grouping state");
		auto single_grouping = GetSingleGroupingSinkState();
		unique_ptr<DistinctCountPointerLocalRowMoveState> local_row_move_state;
		if (HashAggregateUsesDistinctCountPointerKeys(op) && local_state.grouping_states[0].distinct_count_scratch &&
		    local_state.grouping_states[0].distinct_count_scratch->distinct_set) {
			local_row_move_state = make_uniq<DistinctCountPointerLocalRowMoveState>(
			    *local_state.grouping_states[0].distinct_count_scratch->distinct_set);
		}
		single_grouping.grouping.table_data.FinishStateUpdates(
		    context, single_grouping.input,
		    local_row_move_state ? optional_ptr<TupleDataRowLocationRemap>(*local_row_move_state)
		                         : optional_ptr<TupleDataRowLocationRemap>());
		if (local_row_move_state) {
			local_row_move_state->Flush();
		}
	}

private:
	ExecutionContext &context;
	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &global_state;
	HashAggregateLocalSinkState &local_state;
	InterruptState &interrupt_state;
};

static ExecutionPrimitiveAggregateUpdateBinding
BuildHashAggregatePrimitiveUpdateBinding(const PhysicalHashAggregate &op, const ExecutionRegionSinkInfo &sink_info) {
	ExecutionPrimitiveAggregateUpdateBinding result;
	auto &contract = sink_info.aggregate_contract;
	if (!contract.grouped_state_layout_ready || contract.grouped_state_offsets.size() < sink_info.aggregates.size()) {
		result.blocker = "hash-aggregate-primitive-update-grouped-state-layout";
		return result;
	}
	result.lanes.reserve(sink_info.aggregates.size());
	for (auto &aggregate_info : sink_info.aggregates) {
		if (aggregate_info.aggregate_index >= op.grouped_aggregate_data.aggregates.size() ||
		    aggregate_info.aggregate_index >= contract.grouped_state_offsets.size()) {
			result.blocker = "hash-aggregate-primitive-update-aggregate-index-out-of-range";
			return result;
		}
		auto &aggregate =
		    op.grouped_aggregate_data.aggregates[aggregate_info.aggregate_index]->Cast<BoundAggregateExpression>();
		auto &function = aggregate.Function();
		if (!function.HasPrimitiveUpdateABI()) {
			result.blocker = "hash-aggregate-primitive-update-abi-missing";
			return result;
		}
		auto &abi = function.GetPrimitiveUpdateABI();
		if (!AggregatePrimitiveUpdateKindIsSupported(abi.kind)) {
			result.blocker = "hash-aggregate-primitive-update-kind-unsupported";
			return result;
		}
		const auto requires_payload = AggregatePrimitiveUpdateRequiresPayload(abi.kind);
		if (requires_payload) {
			if (aggregate_info.child_count != 1 || aggregate_info.child_types.empty()) {
				result.blocker = "hash-aggregate-primitive-update-requires-one-payload";
				return result;
			}
			if (AggregatePrimitiveUpdateRequiresTypedPayload(abi.kind) &&
			    aggregate_info.child_types[0].InternalType() != abi.input_type) {
				result.blocker = "hash-aggregate-primitive-update-payload-type-mismatch";
				return result;
			}
		} else if (aggregate_info.child_count != 0) {
			result.blocker = "hash-aggregate-primitive-update-requires-no-payload";
			return result;
		}
		if (function.HasStateSizeCallback() && function.GetStateSizeCallback()(function) != abi.state_size) {
			result.blocker = "hash-aggregate-primitive-update-state-size-mismatch";
			return result;
		}
		auto value_size = AggregatePrimitiveUpdateStateValueSize(abi.kind);
		if (value_size == 0 || abi.state_value_offset + value_size > abi.state_size) {
			result.blocker = "hash-aggregate-primitive-update-state-layout-invalid";
			return result;
		}
		if (AggregatePrimitiveUpdateHasStateIsSet(abi.kind) &&
		    abi.state_is_set_offset + sizeof(bool) > abi.state_size) {
			result.blocker = "hash-aggregate-primitive-update-state-layout-invalid";
			return result;
		}

		ExecutionPrimitiveAggregateUpdateLane lane;
		lane.ready = true;
		lane.kind = abi.kind;
		lane.aggregate_index = aggregate_info.aggregate_index;
		lane.payload_index = requires_payload ? aggregate_info.payload_index : DConstants::INVALID_INDEX;
		lane.payload_type = requires_payload ? aggregate_info.child_types[0].InternalType() : PhysicalType::INVALID;
		lane.state_size = abi.state_size;
		lane.state_offset = contract.grouped_state_offsets[aggregate_info.aggregate_index];
		lane.state_value_offset = abi.state_value_offset;
		lane.state_is_set_offset = abi.state_is_set_offset;
		lane.blocker.clear();
		result.lanes.push_back(lane);
	}
	result.ready = !result.lanes.empty() && result.lanes.size() == sink_info.aggregates.size();
	result.blocker = result.ready ? string() : "hash-aggregate-primitive-update-empty";
	return result;
}

static string ValidateHashAggregateExecutionSink(const ExecutionRegionSinkInfo &sink_info) {
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
		return "hash-aggregate-runtime-kind-mismatch";
	}
	auto &contract = sink_info.aggregate_contract;
	if (contract.native_state_update_contract.status != ExecutionRegionStateContractStatus::READY) {
		return contract.native_state_update_contract.blocker.empty() ? "hash-aggregate-state-update-contract-not-ready"
		                                                             : contract.native_state_update_contract.blocker;
	}
	auto blocker = ExecutionRegionAggregateNativeStateUpdateBlocker(contract, sink_info.aggregates, sink_info.groups);
	if (!blocker.empty()) {
		return blocker;
	}
	return string();
}

void PhysicalHashAggregate::SetMultiScan(GlobalSinkState &state) {
	auto &gstate = state.Cast<HashAggregateGlobalSinkState>();
	for (auto &grouping_state : gstate.grouping_states) {
		RadixPartitionedHashTable::SetMultiScan(*grouping_state.table_state);
	}
}

struct DistinctCountFinalizeFastPath {
	idx_t payload_index = DConstants::INVALID_INDEX;
	LogicalType payload_logical_type;
	PhysicalType payload_type = PhysicalType::INVALID;
	idx_t state_offset = DConstants::INVALID_INDEX;
	idx_t state_value_offset = DConstants::INVALID_INDEX;
};

static bool TryGetDistinctCountFinalizeFastPath(const PhysicalHashAggregate &op,
                                                const HashAggregateGroupingData &grouping_data, idx_t aggregate_index,
                                                idx_t payload_index, DistinctCountFinalizeFastPath &result);

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> PhysicalHashAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<HashAggregateGlobalSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<HashAggregateLocalSinkState>(*this, context);
}

void PhysicalHashAggregate::SinkDistinctGrouping(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
                                                 idx_t grouping_idx) const {
	auto &sink = input.local_state.Cast<HashAggregateLocalSinkState>();
	auto &global_sink = input.global_state.Cast<HashAggregateGlobalSinkState>();

	auto &grouping_gstate = global_sink.grouping_states[grouping_idx];
	auto &grouping_lstate = sink.grouping_states[grouping_idx];
	auto &distinct_info = *distinct_collection_info;

	auto &distinct_state = grouping_gstate.distinct_state;
	auto &distinct_data = groupings[grouping_idx].distinct_data;

	DataChunk empty_chunk;

	// Create an empty filter for Sink, since we don't need to update any aggregate states here
	unsafe_vector<idx_t> empty_filter;

	for (idx_t &idx : distinct_info.indices) {
		auto &aggregate = grouped_aggregate_data.aggregates[idx]->Cast<BoundAggregateExpression>();

		D_ASSERT(distinct_info.table_map.count(idx));
		idx_t table_idx = distinct_info.table_map[idx];
		if (!distinct_data->radix_tables[table_idx]) {
			continue;
		}
		D_ASSERT(distinct_data->radix_tables[table_idx]);
		auto &radix_table = *distinct_data->radix_tables[table_idx];
		auto &radix_global_sink = *distinct_state->radix_states[table_idx];
		auto &radix_local_sink = *grouping_lstate.distinct_states[table_idx];

		InterruptState interrupt_state;
		OperatorSinkInput sink_input {radix_global_sink, radix_local_sink, interrupt_state};

		if (aggregate.GetFilter()) {
			DataChunk filter_chunk;
			auto &filtered_data = sink.filter_set.GetFilterData(idx);
			filter_chunk.InitializeEmpty(filtered_data.filtered_payload.GetTypes());

			// Add the filter Vector (BOOL)
			auto &filter_ref = *aggregate.GetFilter();
			auto it = filter_indexes.find(filter_ref);
			D_ASSERT(it != filter_indexes.end());
			D_ASSERT(it->second < chunk.data.size());
			auto &filter_bound_ref = filter_ref.Cast<BoundReferenceExpression>();
			filter_chunk.data[filter_bound_ref.Index()].Reference(chunk.data[it->second]);

			// We cant use the AggregateFilterData::ApplyFilter method, because the chunk we need to
			// apply the filter to also has the groups, and the filtered_data.filtered_payload does not have those.
			SelectionVector sel_vec(STANDARD_VECTOR_SIZE);
			idx_t count = filtered_data.filter_executor.SelectExpression(filter_chunk, sel_vec);

			if (count == 0) {
				continue;
			}

			// Because the 'input' chunk needs to be re-used after this, we need to create
			// a duplicate of it, that we can apply the filter to
			DataChunk filtered_input;
			filtered_input.InitializeEmpty(chunk.GetTypes());

			for (idx_t group_idx = 0; group_idx < grouped_aggregate_data.groups.size(); group_idx++) {
				auto &group = grouped_aggregate_data.groups[group_idx];
				auto &bound_ref = group->Cast<BoundReferenceExpression>();
				auto &col = filtered_input.data[bound_ref.Index()];
				col.Reference(chunk.data[bound_ref.Index()]);
				col.Slice(sel_vec, count);
			}
			for (idx_t child_idx = 0; child_idx < aggregate.GetChildren().size(); child_idx++) {
				auto &child = aggregate.GetChildren()[child_idx];
				auto &bound_ref = child->Cast<BoundReferenceExpression>();
				auto &col = filtered_input.data[bound_ref.Index()];
				col.Reference(chunk.data[bound_ref.Index()]);
				col.Slice(sel_vec, count);
			}

			radix_table.Sink(context, filtered_input, sink_input, empty_chunk, empty_filter);
		} else {
			radix_table.Sink(context, chunk, sink_input, empty_chunk, empty_filter);
		}
	}
}

void PhysicalHashAggregate::SinkDistinct(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	for (idx_t i = 0; i < groupings.size(); i++) {
		SinkDistinctGrouping(context, chunk, input, i);
	}
}

SinkResultType PhysicalHashAggregate::Sink(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSinkInput &input) const {
	auto &local_state = input.local_state.Cast<HashAggregateLocalSinkState>();
	auto &global_state = input.global_state.Cast<HashAggregateGlobalSinkState>();

	const auto use_distinct_count_pointer_keys = HashAggregateUsesDistinctCountPointerKeys(*this);
	if (distinct_collection_info && !use_distinct_count_pointer_keys) {
		SinkDistinct(context, chunk, input);
	}

	if (CanSkipRegularSink()) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	DataChunk &aggregate_input_chunk = local_state.aggregate_input_chunk;
	auto &aggregates = grouped_aggregate_data.aggregates;
	idx_t aggregate_input_idx = 0;

	// Populate the aggregate child vectors
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		for (auto &child_expr : aggr.GetChildren()) {
			D_ASSERT(child_expr->GetExpressionType() == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
			D_ASSERT(bound_ref_expr.Index() < chunk.data.size());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[bound_ref_expr.Index()]);
		}
	}
	// Populate the filter vectors
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.GetFilter()) {
			auto it = filter_indexes.find(*aggr.GetFilter());
			D_ASSERT(it != filter_indexes.end());
			D_ASSERT(it->second < chunk.data.size());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[it->second]);
		}
	}

	aggregate_input_chunk.SetChildCardinality(chunk.size());
	aggregate_input_chunk.Verify(context.client.db);

	// For every grouping set there is one radix_table
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping_global_state = global_state.grouping_states[i];
		auto &grouping_local_state = local_state.grouping_states[i];
		InterruptState interrupt_state;
		OperatorSinkInput sink_input {*grouping_global_state.table_state, *grouping_local_state.table_state,
		                              interrupt_state};

		auto &grouping = groupings[i];
		auto &table = grouping.table_data;
		if (use_distinct_count_pointer_keys) {
			if (!TrySinkDistinctCountPointerKeys(context, *this, grouping, grouping_local_state, sink_input, chunk)) {
				throw InternalException("count distinct pointer-key sink failed");
			}
			continue;
		}
		table.Sink(context, chunk, sink_input, aggregate_input_chunk, non_distinct_filter);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

bool PhysicalHashAggregate::BindExecutionSink(ExecutionContext &context, DataChunk &input,
                                              OperatorSinkInput &sink_input, const ExecutionRegionSinkInfo &sink_info,
                                              ExecutionSinkBinding &binding) const {
	(void)input;
	binding = ExecutionSinkBinding();
	binding.kind = sink_info.kind;
	auto blocker = ValidateHashAggregateExecutionSink(sink_info);
	if (!blocker.empty()) {
		binding.blocker = blocker;
		binding.aggregate_update.blocker = blocker;
		return false;
	}

	auto &global_state = sink_input.global_state.Cast<HashAggregateGlobalSinkState>();
	auto &local_state = sink_input.local_state.Cast<HashAggregateLocalSinkState>();
	binding.ready = true;
	binding.aggregate_update.ready = true;
	binding.aggregate_update.state = make_shared_ptr<HashAggregateExecutionRegionSinkState>(
	    context, *this, global_state, local_state, sink_input.interrupt_state);
	binding.aggregate_update.grouped_state.ready = true;
	binding.aggregate_update.grouped_state.state = make_shared_ptr<HashAggregateStateAddressState>(
	    context, *this, global_state, local_state, sink_input.interrupt_state);
	binding.aggregate_update.grouped_state.aggregate_state_offsets = sink_info.aggregate_contract.grouped_state_offsets;
	groupings[0].table_data.GetExecutionHashAggregateLookupLayout(
	    binding.aggregate_update.grouped_state.hash_lookup_layout);
	binding.aggregate_update.grouped_state.blocker.clear();
	binding.aggregate_update.primitive = BuildHashAggregatePrimitiveUpdateBinding(*this, sink_info);
	if (HashAggregateUsesDistinctCountPointerKeys(*this)) {
		BindHashAggregateDistinctCountPointerUpdate(*this, groupings[0], local_state.grouping_states[0],
		                                            binding.aggregate_update.distinct_count_pointer);
	}
	binding.aggregate_update.blocker.clear();
	binding.blocker.clear();
	return true;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalHashAggregate::CombineDistinct(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_sink = input.global_state.Cast<HashAggregateGlobalSinkState>();
	auto &sink = input.local_state.Cast<HashAggregateLocalSinkState>();

	if (!distinct_collection_info || HashAggregateUsesDistinctCountPointerKeys(*this)) {
		return;
	}
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping_gstate = global_sink.grouping_states[i];
		auto &grouping_lstate = sink.grouping_states[i];

		auto &distinct_data = groupings[i].distinct_data;
		auto &distinct_state = grouping_gstate.distinct_state;

		const auto table_count = distinct_data->radix_tables.size();
		for (idx_t table_idx = 0; table_idx < table_count; table_idx++) {
			if (!distinct_data->radix_tables[table_idx]) {
				continue;
			}
			auto &radix_table = *distinct_data->radix_tables[table_idx];
			auto &radix_global_sink = *distinct_state->radix_states[table_idx];
			auto &radix_local_sink = *grouping_lstate.distinct_states[table_idx];

			radix_table.Combine(context, radix_global_sink, radix_local_sink);
		}
	}
}

SinkCombineResultType PhysicalHashAggregate::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<HashAggregateGlobalSinkState>();
	auto &llstate = input.local_state.Cast<HashAggregateLocalSinkState>();

	OperatorSinkCombineInput combine_distinct_input {gstate, llstate, input.interrupt_state};
	CombineDistinct(context, combine_distinct_input);

	if (CanSkipRegularSink()) {
		return SinkCombineResultType::FINISHED;
	}
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping_gstate = gstate.grouping_states[i];
		auto &grouping_lstate = llstate.grouping_states[i];

		auto &grouping = groupings[i];
		auto &table = grouping.table_data;
		unique_ptr<DistinctCountPointerLocalRowMoveState> local_row_move_state;
		if (grouping.HasDistinct() && grouping.distinct_data->UsesGroupStatePointerKeys() &&
		    grouping_lstate.distinct_count_scratch && grouping_lstate.distinct_count_scratch->distinct_set) {
			local_row_move_state =
			    make_uniq<DistinctCountPointerLocalRowMoveState>(*grouping_lstate.distinct_count_scratch->distinct_set);
		}
		table.Combine(context, *grouping_gstate.table_state, *grouping_lstate.table_state,
		              local_row_move_state ? optional_ptr<TupleDataRowLocationRemap>(*local_row_move_state)
		                                   : optional_ptr<TupleDataRowLocationRemap>());
		if (local_row_move_state) {
			local_row_move_state->Flush();
		}
		if (grouping.HasDistinct() && grouping.distinct_data->UsesGroupStatePointerKeys()) {
			auto &merge_state = gstate.distinct_count_pointer_merge_states[i];
			if (!merge_state) {
				throw InternalException("Distinct count pointer aggregate is missing global merge state");
			}
			if (grouping_lstate.distinct_count_scratch && grouping_lstate.distinct_count_scratch->distinct_set) {
				merge_state->AddLocalSet(std::move(grouping_lstate.distinct_count_scratch->distinct_set));
				grouping_lstate.distinct_count_scratch->Reset();
			}
		}
	}

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class HashAggregateFinalizeEvent : public BasePipelineEvent {
public:
	//! "Regular" Finalize Event that is scheduled after combining the thread-local distinct HTs
	HashAggregateFinalizeEvent(ClientContext &context, Pipeline *pipeline_p, const PhysicalHashAggregate &op_p,
	                           HashAggregateGlobalSinkState &gstate_p)
	    : BasePipelineEvent(*pipeline_p), context(context), op(op_p), gstate(gstate_p) {
	}

public:
	void Schedule() override;

private:
	ClientContext &context;

	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &gstate;
};

class HashAggregateFinalizeTask : public ExecutorTask {
public:
	HashAggregateFinalizeTask(ClientContext &context, Pipeline &pipeline, shared_ptr<Event> event_p,
	                          const PhysicalHashAggregate &op, HashAggregateGlobalSinkState &state_p)
	    : ExecutorTask(pipeline.executor, std::move(event_p)), context(context), pipeline(pipeline), op(op),
	      gstate(state_p) {
	}

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

	string TaskType() const override {
		return "HashAggregateFinalizeTask";
	}

private:
	ClientContext &context;
	Pipeline &pipeline;

	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &gstate;
};

void HashAggregateFinalizeEvent::Schedule() {
	vector<shared_ptr<Task>> tasks;
	tasks.push_back(make_uniq<HashAggregateFinalizeTask>(context, *pipeline, shared_from_this(), op, gstate));
	D_ASSERT(!tasks.empty());
	SetTasks(std::move(tasks));
}

TaskExecutionResult HashAggregateFinalizeTask::ExecuteTask(TaskExecutionMode mode) {
	op.FinalizeInternal(pipeline, *event, context, gstate, false);
	D_ASSERT(!gstate.finished);
	gstate.finished = true;
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

class HashAggregateDistinctFinalizeEvent : public BasePipelineEvent {
public:
	//! Distinct Finalize Event that is scheduled if we have distinct aggregates
	HashAggregateDistinctFinalizeEvent(ClientContext &context, Pipeline &pipeline_p, const PhysicalHashAggregate &op_p,
	                                   HashAggregateGlobalSinkState &gstate_p)
	    : BasePipelineEvent(pipeline_p), context(context), op(op_p), gstate(gstate_p) {
	}

public:
	void Schedule() override;
	void FinishEvent() override;

private:
	idx_t CreateGlobalSources();

private:
	ClientContext &context;

	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &gstate;

public:
	//! The GlobalSourceStates for all the radix tables of the distinct aggregates
	vector<vector<unique_ptr<GlobalSourceState>>> global_source_states;
};

class HashAggregateDistinctFinalizeTask : public ExecutorTask {
public:
	HashAggregateDistinctFinalizeTask(Pipeline &pipeline, shared_ptr<Event> event_p, const PhysicalHashAggregate &op,
	                                  HashAggregateGlobalSinkState &state_p)
	    : ExecutorTask(pipeline.executor, std::move(event_p)), pipeline(pipeline), op(op), gstate(state_p) {
	}

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

	string TaskType() const override {
		return "HashAggregateDistinctFinalizeTask";
	}

private:
	TaskExecutionResult AggregateDistinctGrouping(const idx_t grouping_idx);

private:
	Pipeline &pipeline;

	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &gstate;

	unique_ptr<LocalSinkState> local_sink_state;
	idx_t grouping_idx = 0;
	unique_ptr<LocalSourceState> radix_table_lstate;
	bool blocked = false;
	idx_t aggregation_idx = 0;
	idx_t payload_idx = 0;
	idx_t next_payload_idx = 0;
};

static bool TryGetDistinctCountFinalizeFastPath(const PhysicalHashAggregate &op,
                                                const HashAggregateGroupingData &grouping_data, idx_t aggregate_index,
                                                idx_t payload_index, DistinctCountFinalizeFastPath &result) {
	if (op.grouped_aggregate_data.aggregates.size() != 1 || aggregate_index != 0) {
		return false;
	}
	if (op.groupings.size() != 1 || op.grouped_aggregate_data.groups.empty()) {
		return false;
	}
	auto &aggregate = op.grouped_aggregate_data.aggregates[aggregate_index]->Cast<BoundAggregateExpression>();
	if (!aggregate.IsDistinct() || aggregate.GetFilter() || aggregate.GetOrderBys() ||
	    aggregate.GetChildren().size() != 1) {
		return false;
	}
	auto &function = aggregate.Function();
	if (!function.HasPrimitiveUpdateABI()) {
		return false;
	}
	auto &abi = function.GetPrimitiveUpdateABI();
	if (abi.kind != AggregatePrimitiveUpdateKind::COUNT || abi.state_size != sizeof(int64_t) ||
	    abi.state_value_offset + sizeof(int64_t) > abi.state_size) {
		return false;
	}
	if (function.HasStateSizeCallback() && function.GetStateSizeCallback()(function) != abi.state_size) {
		return false;
	}
	auto &layout = grouping_data.table_data.GetLayout();
	if (layout.AggregateCount() <= aggregate_index ||
	    layout.GetOffsets().size() <= layout.ColumnCount() + aggregate_index) {
		return false;
	}
	result.payload_index = payload_index;
	result.payload_logical_type = aggregate.GetChildren()[0]->GetReturnType();
	result.payload_type = result.payload_logical_type.InternalType();
	result.state_offset = layout.GetOffsets()[layout.ColumnCount() + aggregate_index];
	result.state_value_offset = result.state_offset + abi.state_value_offset;
	return true;
}

static ExecutionRegionSinkInfo BuildDistinctCountFinalizeSinkInfo(idx_t aggregate_index,
                                                                  const DistinctCountFinalizeFastPath &fast_path) {
	ExecutionRegionSinkInfo result;
	result.kind = ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
	result.aggregate_contract.kind = ExecutionRegionAggregateOperatorKind::HASH;
	result.aggregate_contract.aggregate_count = 1;
	result.aggregate_contract.distinct_count_pointer_keys = true;
	result.aggregate_contract.grouped_state_layout_ready = true;
	result.aggregate_contract.native_grouped_state_contract.status = ExecutionRegionStateContractStatus::READY;
	result.aggregate_contract.grouped_state_offsets.resize(aggregate_index + 1, DConstants::INVALID_INDEX);
	result.aggregate_contract.grouped_state_offsets[aggregate_index] = fast_path.state_offset;

	ExecutionRegionAggregateInput aggregate;
	aggregate.aggregate_index = aggregate_index;
	aggregate.payload_index = fast_path.payload_index;
	aggregate.child_count = 1;
	aggregate.child_indices.push_back(fast_path.payload_index);
	aggregate.child_types.push_back(fast_path.payload_logical_type);
	aggregate.primitive_update_ready = true;
	aggregate.primitive_update_kind = AggregatePrimitiveUpdateKind::COUNT;
	aggregate.primitive_update_input_type = fast_path.payload_type;
	aggregate.primitive_update_state_size = sizeof(int64_t);
	aggregate.primitive_update_state_value_offset = 0;
	result.aggregates.push_back(std::move(aggregate));
	return result;
}

static void BindHashAggregateDistinctCountPointerUpdate(const PhysicalHashAggregate &op,
                                                        const HashAggregateGroupingData &grouping_data,
                                                        HashAggregateGroupingLocalState &grouping_lstate,
                                                        ExecutionDistinctCountPointerUpdateBinding &binding) {
	binding = ExecutionDistinctCountPointerUpdateBinding();
	if (!HashAggregateUsesDistinctCountPointerKeys(op)) {
		binding.blocker = "hash-aggregate-distinct-count-pointer-contract-missing";
		return;
	}
	if (op.grouped_aggregate_data.aggregates.size() != 1 || !grouping_data.distinct_data ||
	    !grouping_data.distinct_data->UsesGroupStatePointerKeys()) {
		binding.blocker = "hash-aggregate-distinct-count-pointer-shape";
		return;
	}
	auto &aggregate = op.grouped_aggregate_data.aggregates[0]->Cast<BoundAggregateExpression>();
	if (aggregate.GetChildren().size() != 1 ||
	    aggregate.GetChildren()[0]->GetExpressionClass() != ExpressionClass::BOUND_REF) {
		binding.blocker = "hash-aggregate-distinct-count-pointer-payload-reference";
		return;
	}
	auto &child_ref = aggregate.GetChildren()[0]->Cast<BoundReferenceExpression>();
	DistinctCountFinalizeFastPath fast_path;
	if (!TryGetDistinctCountFinalizeFastPath(op, grouping_data, 0, child_ref.Index(), fast_path)) {
		binding.blocker = "hash-aggregate-distinct-count-pointer-fast-path";
		return;
	}
	if (!grouping_lstate.distinct_count_scratch || !grouping_lstate.distinct_count_scratch->distinct_set) {
		binding.blocker = "hash-aggregate-distinct-count-pointer-scratch";
		return;
	}
	binding.ready = true;
	binding.payload_index = fast_path.payload_index;
	binding.state_value_offset = fast_path.state_value_offset;
	binding.state_addresses = &grouping_lstate.distinct_count_scratch->state_addresses;
	binding.state = make_shared_ptr<HashAggregateDistinctCountPointerUpdateState>(
	    *grouping_lstate.distinct_count_scratch->distinct_set);
	binding.state_address_sink_info = BuildDistinctCountFinalizeSinkInfo(0, fast_path);
	binding.blocker.clear();
}

static ExecutionPrimitiveAggregateUpdateLane
BuildDistinctCountFinalizeLane(idx_t aggregate_index, const DistinctCountFinalizeFastPath &fast_path) {
	ExecutionPrimitiveAggregateUpdateLane result;
	result.ready = true;
	result.kind = AggregatePrimitiveUpdateKind::COUNT;
	result.aggregate_index = aggregate_index;
	result.payload_index = fast_path.payload_index;
	result.payload_type = fast_path.payload_type;
	result.state_size = sizeof(int64_t);
	result.state_offset = fast_path.state_offset;
	result.state_value_offset = 0;
	return result;
}

static bool FillDistinctCountPointerAddressesFast(ExecutionContext &context,
                                                  const HashAggregateGroupingData &grouping_data,
                                                  OperatorSinkInput &sink_input, DataChunk &input,
                                                  const DistinctCountFinalizeFastPath &fast_path,
                                                  Vector &state_pointer_vector,
                                                  optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr) {
	auto sink_info = BuildDistinctCountFinalizeSinkInfo(0, fast_path);
	return grouping_data.table_data.TryResolveNewGroupAddresses(context, input, sink_input, sink_info,
	                                                            state_pointer_vector, recorder, false);
}

static bool FillDistinctCountPointerAddressesGeneric(ExecutionContext &context,
                                                     const HashAggregateGroupingData &grouping_data,
                                                     OperatorSinkInput &sink_input, DataChunk &input,
                                                     Vector &state_pointer_vector) {
	grouping_data.table_data.ResolveStateAddresses(context, input, sink_input, state_pointer_vector);
	state_pointer_vector.Flatten();
	return true;
}

static bool TrySinkDistinctCountPointerKeys(ExecutionContext &context, const PhysicalHashAggregate &op,
                                            const HashAggregateGroupingData &grouping_data,
                                            HashAggregateGroupingLocalState &grouping_lstate,
                                            OperatorSinkInput &sink_input, DataChunk &input) {
	auto &aggregate = op.grouped_aggregate_data.aggregates[0]->Cast<BoundAggregateExpression>();
	auto &child_ref = aggregate.GetChildren()[0]->Cast<BoundReferenceExpression>();
	DistinctCountFinalizeFastPath fast_path;
	if (!TryGetDistinctCountFinalizeFastPath(op, grouping_data, 0, child_ref.Index(), fast_path)) {
		return false;
	}
	if (!grouping_data.distinct_data || !grouping_data.distinct_data->UsesGroupStatePointerKeys() ||
	    child_ref.Index() >= input.ColumnCount() || !grouping_lstate.distinct_count_scratch) {
		return false;
	}
	auto &scratch = *grouping_lstate.distinct_count_scratch;

	auto &payload = input.data[child_ref.Index()];
	auto &state_pointer_vector = scratch.state_addresses;
	state_pointer_vector.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::ValidityMutable(state_pointer_vector).SetAllValid(input.size());
	FlatVector::SetSize(state_pointer_vector, input.size());

	if (!FillDistinctCountPointerAddressesFast(context, grouping_data, sink_input, input, fast_path,
	                                           state_pointer_vector)) {
		FillDistinctCountPointerAddressesGeneric(context, grouping_data, sink_input, input, state_pointer_vector);
	}

	if (!scratch.distinct_set->Add(state_pointer_vector, payload, input.size(), fast_path.state_value_offset)) {
		return false;
	}
	DistinctCountPointerLocalRowMoveState local_row_move_state(*scratch.distinct_set);
	grouping_data.table_data.FinishStateUpdates(context, sink_input, local_row_move_state);
	local_row_move_state.Flush();
	return true;
}

static void ReferenceCompactDistinctGroups(ClientContext &context, const RadixPartitionedHashTable &table,
                                           DataChunk &distinct_rows, idx_t group_by_size, DataChunk &groups) {
	if (groups.ColumnCount() == 0) {
		groups.Initialize(context, table.group_types);
	}
	for (idx_t group_idx = 0; group_idx < group_by_size; group_idx++) {
		groups.data[group_idx].Reference(distinct_rows.data[group_idx]);
	}
	groups.SetChildCardinality(distinct_rows.size());
}

static bool TryFinalizeDistinctCountFastPath(ExecutionContext &context, const HashAggregateGroupingData &grouping_data,
                                             OperatorSinkInput &sink_input, DataChunk &group_chunk,
                                             DataChunk &distinct_rows, const DistinctCountFinalizeFastPath &fast_path) {
	const auto count = distinct_rows.size();
	if (count == 0) {
		return true;
	}
	if (group_chunk.size() != count || fast_path.payload_index >= distinct_rows.ColumnCount() ||
	    fast_path.state_value_offset == DConstants::INVALID_INDEX) {
		return false;
	}

	const auto aggregate_index = idx_t(0);
	DataChunk compact_groups;
	ReferenceCompactDistinctGroups(context.client, grouping_data.table_data, distinct_rows, group_chunk.ColumnCount(),
	                               compact_groups);
	auto sink_info = BuildDistinctCountFinalizeSinkInfo(aggregate_index, fast_path);
	auto lane = BuildDistinctCountFinalizeLane(aggregate_index, fast_path);
	vector<const ExecutionPrimitiveAggregateUpdateLane *> lanes;
	lanes.push_back(&lane);
	vector<idx_t> payload_source_indices;
	payload_source_indices.push_back(fast_path.payload_index);
	Vector group_hashes(LogicalType::HASH);
	compact_groups.Hash(group_hashes);
	if (grouping_data.table_data.TryUpdateNewPrimitiveGroupsWithPayloadInput(
	        context, compact_groups, distinct_rows, payload_source_indices, sink_input, sink_info, lanes, nullptr, true,
	        group_hashes)) {
		return true;
	}

	Vector addresses(LogicalType::POINTER);
	grouping_data.table_data.ResolveStateAddresses(context, group_chunk, sink_input, addresses);
	addresses.Flatten();
	auto state_addresses = FlatVector::GetData<uintptr_t>(addresses);

	auto &payload = distinct_rows.data[fast_path.payload_index];
	UnifiedVectorFormat payload_data;
	payload.ToUnifiedFormat(payload_data);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto payload_idx = payload_data.sel->get_index(row_idx);
		if (!payload_data.validity.RowIsValid(payload_idx)) {
			continue;
		}
		auto state = reinterpret_cast<int64_t *>(reinterpret_cast<data_ptr_t>(state_addresses[row_idx]) +
		                                         fast_path.state_value_offset);
		(*state)++;
	}

	grouping_data.table_data.FinishStateUpdates(context, sink_input);
	return true;
}

void HashAggregateDistinctFinalizeEvent::Schedule() {
	auto n_tasks = CreateGlobalSources();
	n_tasks = MinValue<idx_t>(n_tasks, NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads()));
	vector<shared_ptr<Task>> tasks;
	for (idx_t i = 0; i < n_tasks; i++) {
		tasks.push_back(make_uniq<HashAggregateDistinctFinalizeTask>(*pipeline, shared_from_this(), op, gstate));
	}
	SetTasks(std::move(tasks));
}

idx_t HashAggregateDistinctFinalizeEvent::CreateGlobalSources() {
	auto &aggregates = op.grouped_aggregate_data.aggregates;
	global_source_states.reserve(op.groupings.size());

	idx_t n_tasks = 0;
	for (idx_t grouping_idx = 0; grouping_idx < op.groupings.size(); grouping_idx++) {
		auto &grouping = op.groupings[grouping_idx];
		auto &distinct_state = *gstate.grouping_states[grouping_idx].distinct_state;
		auto &distinct_data = *grouping.distinct_data;

		vector<unique_ptr<GlobalSourceState>> aggregate_sources;
		aggregate_sources.reserve(aggregates.size());
		for (idx_t agg_idx = 0; agg_idx < aggregates.size(); agg_idx++) {
			auto &aggregate = aggregates[agg_idx];
			auto &aggr = aggregate->Cast<BoundAggregateExpression>();

			if (!aggr.IsDistinct()) {
				aggregate_sources.push_back(nullptr);
				continue;
			}
			D_ASSERT(distinct_data.info.table_map.count(agg_idx));

			auto table_idx = distinct_data.info.table_map.at(agg_idx);
			auto &radix_table_p = distinct_data.radix_tables[table_idx];
			n_tasks += radix_table_p->MaxThreads(*distinct_state.radix_states[table_idx]);
			aggregate_sources.push_back(radix_table_p->GetGlobalSourceState(context));
		}
		global_source_states.push_back(std::move(aggregate_sources));
	}

	return MaxValue<idx_t>(n_tasks, 1);
}

void HashAggregateDistinctFinalizeEvent::FinishEvent() {
	// Now that everything is added to the main ht, we can actually finalize
	auto new_event = make_shared_ptr<HashAggregateFinalizeEvent>(context, pipeline.get(), op, gstate);
	this->InsertEvent(std::move(new_event));
}

TaskExecutionResult HashAggregateDistinctFinalizeTask::ExecuteTask(TaskExecutionMode mode) {
	for (; grouping_idx < op.groupings.size(); grouping_idx++) {
		auto res = AggregateDistinctGrouping(grouping_idx);
		if (res == TaskExecutionResult::TASK_BLOCKED) {
			return res;
		}
		D_ASSERT(res == TaskExecutionResult::TASK_FINISHED);
		aggregation_idx = 0;
		payload_idx = 0;
		next_payload_idx = 0;
		local_sink_state = nullptr;
	}
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

TaskExecutionResult HashAggregateDistinctFinalizeTask::AggregateDistinctGrouping(const idx_t grouping_idx) {
	D_ASSERT(op.distinct_collection_info);
	auto &info = *op.distinct_collection_info;

	auto &grouping_data = op.groupings[grouping_idx];
	auto &grouping_state = gstate.grouping_states[grouping_idx];
	D_ASSERT(grouping_state.distinct_state);
	auto &distinct_state = *grouping_state.distinct_state;
	auto &distinct_data = *grouping_data.distinct_data;

	auto &aggregates = info.aggregates;

	// Thread-local contexts
	ThreadContext thread_context(executor.context);
	ExecutionContext execution_context(executor.context, thread_context, &pipeline);

	// Sink state to sink into global HTs
	InterruptState interrupt_state(shared_from_this());
	auto &global_sink_state = *grouping_state.table_state;
	if (!local_sink_state) {
		local_sink_state = grouping_data.table_data.GetLocalSinkState(execution_context);
	}
	OperatorSinkInput sink_input {global_sink_state, *local_sink_state, interrupt_state};

	// Create a chunk that mimics the 'input' chunk in Sink, for storing the group vectors
	DataChunk group_chunk;
	if (!op.input_group_types.empty()) {
		group_chunk.Initialize(executor.context, op.input_group_types);
	}

	const idx_t group_by_size = op.grouped_aggregate_data.groups.size();

	DataChunk aggregate_input_chunk;
	if (!gstate.payload_types.empty()) {
		aggregate_input_chunk.Initialize(executor.context, gstate.payload_types);
	}

	const auto &finalize_event = event->Cast<HashAggregateDistinctFinalizeEvent>();

	auto &agg_idx = aggregation_idx;
	for (; agg_idx < op.grouped_aggregate_data.aggregates.size(); agg_idx++) {
		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();

		if (!blocked) {
			// Forward the payload idx
			payload_idx = next_payload_idx;
			next_payload_idx = payload_idx + aggregate.GetChildren().size();
		}

		// If aggregate is not distinct, skip it
		if (!distinct_data.IsDistinct(agg_idx)) {
			continue;
		}

		D_ASSERT(distinct_data.info.table_map.count(agg_idx));
		const auto &table_idx = distinct_data.info.table_map.at(agg_idx);
		auto &radix_table = distinct_data.radix_tables[table_idx];

		auto &sink = *distinct_state.radix_states[table_idx];
		if (!blocked) {
			radix_table_lstate = radix_table->GetLocalSourceState(execution_context);
		}
		auto &local_source = *radix_table_lstate;
		OperatorSourceInput source_input {*finalize_event.global_source_states[grouping_idx][agg_idx], local_source,
		                                  interrupt_state};

		// Create a duplicate of the output_chunk, because of multi-threading we cant alter the original
		DataChunk output_chunk;
		output_chunk.Initialize(executor.context, distinct_state.distinct_output_chunks[table_idx]->GetTypes());

		// Fetch all the data from the aggregate ht, and Sink it into the main ht
		while (true) {
			output_chunk.Reset();
			group_chunk.Reset();

			auto res = radix_table->GetData(execution_context, output_chunk, sink, source_input);
			if (res == SourceResultType::FINISHED) {
				D_ASSERT(output_chunk.size() == 0);
				break;
			} else if (res == SourceResultType::BLOCKED) {
				blocked = true;
				return TaskExecutionResult::TASK_BLOCKED;
			}

			auto &grouped_aggregate_data = *distinct_data.grouped_aggregate_data[table_idx];
			for (idx_t group_idx = 0; group_idx < group_by_size; group_idx++) {
				auto &group = grouped_aggregate_data.groups[group_idx];
				auto &bound_ref_expr = group->Cast<BoundReferenceExpression>();
				group_chunk.data[bound_ref_expr.Index()].Reference(output_chunk.data[group_idx]);
			}

			DistinctCountFinalizeFastPath count_fast_path;
			if (TryGetDistinctCountFinalizeFastPath(op, grouping_data, agg_idx, group_by_size, count_fast_path)) {
				if (TryFinalizeDistinctCountFastPath(execution_context, grouping_data, sink_input, group_chunk,
				                                     output_chunk, count_fast_path)) {
					continue;
				}
			}

			aggregate_input_chunk.Reset();
			for (idx_t child_idx = 0; child_idx < grouped_aggregate_data.groups.size() - group_by_size; child_idx++) {
				aggregate_input_chunk.data[payload_idx + child_idx].Reference(
				    output_chunk.data[group_by_size + child_idx]);
			}
			aggregate_input_chunk.SetChildCardinality(output_chunk.size());

			// Sink it into the main ht
			grouping_data.table_data.Sink(execution_context, group_chunk, sink_input, aggregate_input_chunk, {agg_idx});
		}
		blocked = false;
	}
	grouping_data.table_data.Combine(execution_context, global_sink_state, *local_sink_state);
	return TaskExecutionResult::TASK_FINISHED;
}

SinkFinalizeType PhysicalHashAggregate::FinalizeDistinct(Pipeline &pipeline, Event &event, ClientContext &context,
                                                         GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<HashAggregateGlobalSinkState>();
	D_ASSERT(distinct_collection_info);

	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping = groupings[i];
		auto &distinct_data = *grouping.distinct_data;
		auto &distinct_state = *gstate.grouping_states[i].distinct_state;

		for (idx_t table_idx = 0; table_idx < distinct_data.radix_tables.size(); table_idx++) {
			if (!distinct_data.radix_tables[table_idx]) {
				continue;
			}
			auto &radix_table = distinct_data.radix_tables[table_idx];
			auto &radix_state = *distinct_state.radix_states[table_idx];
			radix_table->Finalize(context, radix_state);
		}
	}
	auto new_event = make_shared_ptr<HashAggregateDistinctFinalizeEvent>(context, pipeline, *this, gstate);
	event.InsertEvent(std::move(new_event));
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalHashAggregate::FinalizeInternal(Pipeline &pipeline, Event &event, ClientContext &context,
                                                         GlobalSinkState &gstate_p, bool check_distinct) const {
	auto &gstate = gstate_p.Cast<HashAggregateGlobalSinkState>();

	if (check_distinct && distinct_collection_info && !HashAggregateUsesDistinctCountPointerKeys(*this)) {
		// There are distinct aggregates
		// If these are partitioned those need to be combined first
		// Then we Finalize again, skipping this step
		return FinalizeDistinct(pipeline, event, context, gstate_p);
	}

	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping = groupings[i];
		auto &grouping_gstate = gstate.grouping_states[i];
		optional_ptr<TupleDataRowLocationRemap> state_remap;
		if (grouping.HasDistinct() && grouping.distinct_data->UsesGroupStatePointerKeys()) {
			auto &merge_state = gstate.distinct_count_pointer_merge_states[i];
			if (!merge_state) {
				throw InternalException("Distinct count pointer aggregate is missing global merge state");
			}
			state_remap = *merge_state;
		}
		grouping.table_data.Finalize(context, *grouping_gstate.table_state, state_remap);
	}
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalHashAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	return FinalizeInternal(pipeline, event, context, input.global_state, true);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class HashAggregateGlobalSourceState : public GlobalSourceState {
public:
	HashAggregateGlobalSourceState(ClientContext &context, const PhysicalHashAggregate &op) : op(op) {
		for (auto &grouping : op.groupings) {
			auto &rt = grouping.table_data;
			radix_states.push_back(rt.GetGlobalSourceState(context));
		}
		ResetState(context);
	}

	const PhysicalHashAggregate &op;
	atomic<idx_t> state_index;

	vector<unique_ptr<GlobalSourceState>> radix_states;

private:
	void ResetState(ClientContext &context) {
		state_index = 0;
		for (idx_t grouping_idx = 0; grouping_idx < op.groupings.size(); grouping_idx++) {
			op.groupings[grouping_idx].table_data.ResetGlobalSourceState(context, *radix_states[grouping_idx]);
		}
		GlobalSourceState::Reset(context);
	}

public:
	idx_t MaxThreads() override {
		// If there are no tables, we only need one thread.
		if (op.groupings.empty()) {
			return 1;
		}

		auto &ht_state = op.sink_state->Cast<HashAggregateGlobalSinkState>();
		idx_t threads = 0;
		for (size_t sidx = 0; sidx < op.groupings.size(); ++sidx) {
			auto &grouping = op.groupings[sidx];
			auto &grouping_gstate = ht_state.grouping_states[sidx];
			threads += grouping.table_data.MaxThreads(*grouping_gstate.table_state);
		}
		return MaxValue<idx_t>(1, threads);
	}

	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ClientContext &context) override {
		ResetState(context);
	}
};

unique_ptr<GlobalSourceState> PhysicalHashAggregate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<HashAggregateGlobalSourceState>(context, *this);
}

optional_idx PhysicalHashAggregate::FinalizedSourceCardinality() const {
	if (!sink_state) {
		return optional_idx::Invalid();
	}
	auto &sink_gstate = sink_state->Cast<HashAggregateGlobalSinkState>();
	idx_t result = 0;
	for (idx_t grouping_idx = 0; grouping_idx < groupings.size(); grouping_idx++) {
		auto cardinality = groupings[grouping_idx].table_data.FinalizedCount(
		    *sink_gstate.grouping_states[grouping_idx].table_state);
		if (!cardinality.IsValid()) {
			return optional_idx::Invalid();
		}
		result += cardinality.GetIndex();
	}
	return result;
}

class HashAggregateLocalSourceState : public LocalSourceState {
public:
	explicit HashAggregateLocalSourceState(ExecutionContext &context, const PhysicalHashAggregate &op,
	                                       GlobalSourceState &gstate)
	    : op(op) {
		for (auto &grouping : op.groupings) {
			auto &rt = grouping.table_data;
			radix_states.push_back(rt.GetLocalSourceState(context));
		}
		ResetState();
	}

	const PhysicalHashAggregate &op;
	optional_idx radix_idx;
	vector<unique_ptr<LocalSourceState>> radix_states;

private:
	void ResetState() {
		radix_idx.SetInvalid();
	}

public:
	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ExecutionContext &context, GlobalSourceState &gstate) override {
		ResetState();
		for (idx_t grouping_idx = 0; grouping_idx < op.groupings.size(); grouping_idx++) {
			op.groupings[grouping_idx].table_data.ResetLocalSourceState(context, *radix_states[grouping_idx]);
		}
	}
};

unique_ptr<LocalSourceState> PhysicalHashAggregate::GetLocalSourceState(ExecutionContext &context,
                                                                        GlobalSourceState &gstate) const {
	return make_uniq<HashAggregateLocalSourceState>(context, *this, gstate);
}

static SourceResultType ScanHashAggregateState(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input,
                                               const PhysicalHashAggregate &op) {
	auto &sink_gstate = op.sink_state->Cast<HashAggregateGlobalSinkState>();
	auto &gstate = input.global_state.Cast<HashAggregateGlobalSourceState>();
	auto &lstate = input.local_state.Cast<HashAggregateLocalSourceState>();
	while (true) {
		if (!lstate.radix_idx.IsValid()) {
			lstate.radix_idx = gstate.state_index.load();
		}
		const auto radix_idx = lstate.radix_idx.GetIndex();
		if (radix_idx >= op.groupings.size()) {
			break;
		}

		auto &grouping = op.groupings[radix_idx];
		auto &radix_table = grouping.table_data;
		auto &grouping_gstate = sink_gstate.grouping_states[radix_idx];

		OperatorSourceInput source_input {*gstate.radix_states[radix_idx], *lstate.radix_states[radix_idx],
		                                  input.interrupt_state, input.stage_recorder};
		auto res = radix_table.GetData(context, chunk, *grouping_gstate.table_state, source_input);
		if (res == SourceResultType::BLOCKED) {
			return res;
		}
		if (chunk.size() != 0) {
			return SourceResultType::HAVE_MORE_OUTPUT;
		}

		{
			ExecutionOperatorStageTimer timer(input.stage_recorder,
			                                  "source_contract.hash_aggregate_state_scan.advance_radix_table");
			// move to the next table
			annotated_lock_guard<annotated_mutex> guard(gstate.lock);
			lstate.radix_idx = lstate.radix_idx.GetIndex() + 1;
			if (lstate.radix_idx.GetIndex() > gstate.state_index) {
				// we have not yet worked on the table
				// move the global index forwards
				gstate.state_index = lstate.radix_idx.GetIndex();
			}
			lstate.radix_idx = gstate.state_index.load();
		}
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

bool PhysicalHashAggregate::SupportsExecutionSourceContract(const ExecutionRegionOpenRequest &open_request) const {
	return open_request.UsesSourceContract();
}

SourceResultType PhysicalHashAggregate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                        OperatorSourceInput &input) const {
	return ScanHashAggregateState(context, chunk, input, *this);
}

SourceResultType PhysicalHashAggregate::GetExecutionSourceContractDataInternal(ExecutionContext &context,
                                                                               DataChunk &chunk,
                                                                               OperatorSourceInput &input) const {
	return ScanHashAggregateState(context, chunk, input, *this);
}

ProgressData PhysicalHashAggregate::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &sink_gstate = sink_state->Cast<HashAggregateGlobalSinkState>();
	auto &gstate = gstate_p.Cast<HashAggregateGlobalSourceState>();
	ProgressData progress;
	for (idx_t radix_idx = 0; radix_idx < groupings.size(); radix_idx++) {
		progress.Add(groupings[radix_idx].table_data.GetProgress(
		    context, *sink_gstate.grouping_states[radix_idx].table_state, *gstate.radix_states[radix_idx]));
	}
	return progress;
}

InsertionOrderPreservingMap<string> PhysicalHashAggregate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	auto &groups = grouped_aggregate_data.groups;
	auto &aggregates = grouped_aggregate_data.aggregates;
	string groups_info;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			groups_info += "\n";
		}
		groups_info += groups[i]->GetName();
	}
	result["Groups"] = groups_info;

	string aggregate_info;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();
		if (i > 0) {
			aggregate_info += "\n";
		}
		aggregate_info += aggregates[i]->GetName();
		if (aggregate.GetFilter()) {
			aggregate_info += " Filter: " + aggregate.GetFilter()->GetName();
		}
	}
	result["Aggregates"] = aggregate_info;
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
