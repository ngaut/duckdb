#include "duckdb/execution/operator/join/physical_delim_join.hpp"

#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

namespace duckdb {

PhysicalDelimJoin::PhysicalDelimJoin(PhysicalPlan &physical_plan, PhysicalOperatorType type, vector<LogicalType> types,
                                     PhysicalOperator &original_join, PhysicalOperator &distinct,
                                     const vector<const_reference<PhysicalOperator>> &delim_scans,
                                     idx_t estimated_cardinality, optional_idx delim_idx)
    : PhysicalOperator(physical_plan, type, std::move(types), estimated_cardinality), join(original_join),
      distinct(distinct.Cast<PhysicalHashAggregate>()), delim_scans(delim_scans), delim_idx(delim_idx) {
	D_ASSERT(type == PhysicalOperatorType::LEFT_DELIM_JOIN || type == PhysicalOperatorType::RIGHT_DELIM_JOIN);
}

vector<const_reference<PhysicalOperator>> PhysicalDelimJoin::GetChildren() const {
	vector<const_reference<PhysicalOperator>> result;
	for (auto &child : children) {
		result.push_back(child.get());
	}
	result.push_back(join);
	result.push_back(distinct);
	return result;
}

InsertionOrderPreservingMap<string> PhysicalDelimJoin::ParamsToString() const {
	auto result = join.ParamsToString();
	result["Delim Index"] = StringUtil::Format("%llu", delim_idx.GetIndex());
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

static string DelimJoinNativeContractBlocker(const ExecutionContract &distinct_contract,
                                             optional_ptr<const ExecutionContract> join_contract,
                                             PhysicalOperatorType type) {
	if (distinct_contract.sink.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
		return "delim-join-distinct-sink-contract-kind-mismatch;distinct_sink_kind=" +
		       string(ExecutionRegionSinkKindToString(distinct_contract.sink.kind));
	}
	if (!distinct_contract.sink.aggregates.empty()) {
		return "delim-join-distinct-sink-has-aggregate-payloads";
	}
	auto &distinct_state_update = distinct_contract.sink.aggregate_contract.native_state_update_contract;
	if (distinct_state_update.status != ExecutionRegionStateContractStatus::READY) {
		return distinct_state_update.blocker.empty() ? "delim-join-distinct-state-update-contract-not-ready"
		                                             : distinct_state_update.blocker;
	}
	if (type != PhysicalOperatorType::RIGHT_DELIM_JOIN) {
		return string();
	}
	if (!join_contract) {
		return "delim-join-right-build-contract-missing";
	}
	if (join_contract->sink.kind != ExecutionRegionSinkKind::HASH_JOIN_BUILD) {
		return "delim-join-right-build-contract-kind-mismatch;join_sink_kind=" +
		       string(ExecutionRegionSinkKindToString(join_contract->sink.kind));
	}
	auto &join_native = join_contract->sink.hash_join_contract.native_build_contract;
	if (join_native.status != ExecutionRegionStateContractStatus::READY) {
		return join_native.blocker.empty() ? "delim-join-right-build-contract-not-ready" : join_native.blocker;
	}
	return string();
}

ExecutionContract PhysicalDelimJoin::GetExecutionContract() const {
	ExecutionContract result;
	auto distinct_contract = distinct.GetExecutionContract();
	unique_ptr<ExecutionContract> join_contract;
	if (type == PhysicalOperatorType::RIGHT_DELIM_JOIN) {
		join_contract = make_uniq<ExecutionContract>(join.GetExecutionContract());
	}

	auto join_contract_ptr =
	    join_contract ? optional_ptr<const ExecutionContract>(*join_contract) : optional_ptr<const ExecutionContract>();
	auto blocker = DelimJoinNativeContractBlocker(distinct_contract, join_contract_ptr, type);
	const bool ready = blocker.empty();

	result.sink.kind = ExecutionRegionSinkKind::DELIM_JOIN_SINK;
	result.sink.reason = "DuckDB delimiter join execution sink contract";
	result.sink.reason += ";operator=" + GetName();
	result.sink.reason += ";side=" + string(type == PhysicalOperatorType::LEFT_DELIM_JOIN ? "left" : "right");
	result.sink.reason += ";input_columns=" + std::to_string(children.empty() ? 0 : children[0].get().types.size());
	result.sink.reason += ";output_columns=" + std::to_string(types.size());
	result.sink.reason += ";delim_scans=" + std::to_string(delim_scans.size());
	result.sink.reason += ";delim_index=" + (delim_idx.IsValid() ? std::to_string(delim_idx.GetIndex()) : "invalid");
	result.sink.reason += ";distinct_sink_kind=" + string(ExecutionRegionSinkKindToString(distinct_contract.sink.kind));
	result.sink.reason += ";distinct_state_update_status=" +
	                      string(ExecutionRegionStateContractStatusToString(
	                          distinct_contract.sink.aggregate_contract.native_state_update_contract.status));
	if (join_contract) {
		result.sink.reason += ";join_sink_kind=" + string(ExecutionRegionSinkKindToString(join_contract->sink.kind));
		result.sink.reason +=
		    ";join_build_contract_status=" + string(ExecutionRegionStateContractStatusToString(
		                                         join_contract->sink.hash_join_contract.native_build_contract.status));
	} else {
		result.sink.reason += ";join_sink_kind=none";
	}

	result.sink.aggregate_contract = distinct_contract.sink.aggregate_contract;
	result.sink.aggregates = distinct_contract.sink.aggregates;
	result.sink.groups = distinct_contract.sink.groups;
	if (join_contract) {
		result.sink.hash_join_contract = join_contract->sink.hash_join_contract;
		result.sink.hash_join_keys = join_contract->sink.hash_join_keys;
	}

	result.sink.native_sink_contract.status =
	    ready ? ExecutionRegionStateContractStatus::READY : ExecutionRegionStateContractStatus::BLOCKED;
	result.sink.native_sink_contract.required_capability = "delim-join-execution-sink";
	result.sink.native_sink_contract.contract_version = "v1";
	result.sink.native_sink_contract.blocker = blocker;
	result.sink.reason += ready ? ";sink_contract_status=ready" : ";sink_contract_status=blocked";
	result.sink.reason += ";sink_required_capability=delim-join-execution-sink";
	result.sink.reason += ";sink_contract_version=v1";
	result.sink.reason += ";sink_contract_blocker=" + blocker;
	return FinalizeExecutionContract(std::move(result));
}

} // namespace duckdb
