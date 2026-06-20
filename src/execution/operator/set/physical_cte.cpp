#include "duckdb/execution/operator/set/physical_cte.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalCTE::PhysicalCTE(PhysicalPlan &physical_plan, Identifier ctename, TableIndex table_index,
                         vector<LogicalType> types, PhysicalOperator &top, PhysicalOperator &bottom,
                         idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::CTE, std::move(types), estimated_cardinality),
      table_index(table_index), ctename(std::move(ctename)) {
	children.push_back(top);
	children.push_back(bottom);
}

PhysicalCTE::~PhysicalCTE() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CTEGlobalState : public GlobalSinkState {
public:
	explicit CTEGlobalState(ClientContext &context, const PhysicalCTE &op)
	    : op(op), working_table_ref(op.working_table.get()) {
		ResetState(context);
	}
	const PhysicalCTE &op;
	optional_ptr<ColumnDataCollection> working_table_ref;

	mutex lhs_lock;

private:
	void ResetState(ClientContext &context) {
		op.working_table->Reset();
		working_table_ref = op.working_table.get();
		GlobalSinkState::Reset(context);
	}

public:
	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ClientContext &context) override {
		ResetState(context);
	}

	void MergeIT(ColumnDataCollection &input) {
		lock_guard<mutex> guard(lhs_lock);
		working_table_ref->Combine(input);
	}
};

class CTELocalState : public LocalSinkState {
public:
	explicit CTELocalState(ClientContext &context, const PhysicalCTE &op)
	    : lhs_data(context, op.working_table->Types()) {
		lhs_data.InitializeAppend(append_state);
	}

	unique_ptr<LocalSinkState> distinct_state;
	ColumnDataCollection lhs_data;
	ColumnDataAppendState append_state;

	void Append(DataChunk &input) {
		lhs_data.Append(append_state, input);
	}
};

class CTEExecutionRegionSinkState : public ExecutionAppendSinkState {
public:
	explicit CTEExecutionRegionSinkState(CTELocalState &state_p) : state(state_p) {
	}

	SinkResultType Append(DataChunk &input) override {
		state.Append(input);
		return SinkResultType::NEED_MORE_INPUT;
	}

private:
	CTELocalState &state;
};

unique_ptr<GlobalSinkState> PhysicalCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<CTEGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalCTE::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<CTELocalState>(context.client, *this);
	return std::move(state);
}

SinkResultType PhysicalCTE::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<CTELocalState>();
	lstate.Append(chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

ExecutionContract PhysicalCTE::GetExecutionContract() const {
	ExecutionContract result;
	result.sink.kind = ExecutionRegionSinkKind::MATERIALIZATION;
	result.sink.reason = "DuckDB materialization append sink contract";
	result.sink.reason += ";operator=CTE";
	result.sink.reason += ";output_columns=" + std::to_string(types.size());
	result.sink.native_sink_contract.status = ExecutionRegionStateContractStatus::READY;
	result.sink.native_sink_contract.required_capability = "materialization-append-sink";
	result.sink.native_sink_contract.contract_version = "v1";
	result.sink.native_sink_contract.blocker.clear();
	result.sink.reason += ";sink_contract_status=ready";
	result.sink.reason += ";sink_required_capability=materialization-append-sink";
	result.sink.reason += ";sink_contract_version=v1";
	result.sink.reason += ";sink_contract_blocker=none";
	result.sink.fields = BuildExecutionContractFields(result.sink.reason);
	return FinalizeExecutionContract(std::move(result));
}

bool PhysicalCTE::BindExecutionSink(ExecutionContext &context, DataChunk &input, OperatorSinkInput &sink_input,
                                    const ExecutionRegionSinkInfo &sink_info, ExecutionSinkBinding &binding) const {
	(void)context;
	(void)input;
	binding = ExecutionSinkBinding();
	binding.kind = sink_info.kind;
	if (sink_info.kind != ExecutionRegionSinkKind::MATERIALIZATION ||
	    sink_info.native_sink_contract.status != ExecutionRegionStateContractStatus::READY) {
		binding.blocker = sink_info.native_sink_contract.blocker.empty() ? "materialization-append-sink-not-ready"
		                                                                 : sink_info.native_sink_contract.blocker;
		return false;
	}
	auto &state = sink_input.local_state.Cast<CTELocalState>();
	binding.ready = true;
	binding.append_sink.ready = true;
	binding.append_sink.state = make_shared_ptr<CTEExecutionRegionSinkState>(state);
	binding.append_sink.blocker.clear();
	binding.blocker.clear();
	return true;
}

SinkCombineResultType PhysicalCTE::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<CTELocalState>();
	auto &gstate = input.global_state.Cast<CTEGlobalState>();
	gstate.MergeIT(lstate.lhs_data);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalCTE::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	D_ASSERT(children.size() == 2);
	op_state.reset();
	sink_state.reset();

	auto &state = meta_pipeline.GetState();

	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_meta_pipeline.Build(children[0]);

	for (auto &cte_scan : cte_scans) {
		state.cte_dependencies.insert(make_pair(cte_scan, reference<Pipeline>(*child_meta_pipeline.GetBasePipeline())));
	}

	// If the CTE body is a DML statement (INSERT/UPDATE/DELETE/MERGE INTO), all MetaPipelines
	// created while building children[1] (the query side) must run after the DML completes.
	// We follow the same pattern as PhysicalJoin::BuildJoinPipelines: capture the DML pipelines
	// and the current last child before building children[1], then call AddRecursiveDependencies
	// with force=true so that ordering is always enforced (not just when pipelines exceed the
	// thread count, as is the case for join build dependencies).
	vector<shared_ptr<Pipeline>> dml_pipelines;
	optional_ptr<MetaPipeline> last_child_ptr;
	if (cte_body_is_dml) {
		child_meta_pipeline.GetPipelines(dml_pipelines, false);
		last_child_ptr = meta_pipeline.GetLastChild();
	}

	children[1].get().BuildPipelines(current, meta_pipeline);

	if (last_child_ptr) {
		meta_pipeline.AddRecursiveDependencies(dml_pipelines, *last_child_ptr, true);
	}
}

vector<const_reference<PhysicalOperator>> PhysicalCTE::GetSources() const {
	return children[1].get().GetSources();
}

InsertionOrderPreservingMap<string> PhysicalCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename.GetIdentifierName();
	result["Table Index"] = StringUtil::Format("%llu", table_index.index);
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

ProgressData PhysicalCTE::GetSinkProgress(ClientContext &context, GlobalSinkState &gstate,
                                          const ProgressData source_progress) const {
	auto &state = gstate.Cast<CTEGlobalState>();
	lock_guard<mutex> guard(state.lhs_lock);
	if (!state.working_table_ref) {
		return ProgressData {0, 1, true};
	}
	auto &working_table = *state.working_table_ref;
	auto count = double(working_table.Count());
	ProgressData progress;
	progress.done = count;
	progress.total = count + source_progress.total;
	return progress;
}

} // namespace duckdb
