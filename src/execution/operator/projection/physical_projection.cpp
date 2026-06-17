#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

class ProjectionState : public OperatorState {
public:
	explicit ProjectionState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions)
	    : executor(context.client, expressions) {
	}

	ExpressionExecutor executor;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}

	bool SupportsReuse() const override {
		return true;
	}
};

class ProjectionExecutionOperatorState : public ExecutionProjectionOperatorState {
public:
	ProjectionExecutionOperatorState(ExecutionContext &context_p, const PhysicalProjection &op_p,
	                                 GlobalOperatorState &global_state_p, OperatorState &operator_state_p)
	    : context(context_p), op(op_p), global_state(global_state_p), operator_state(operator_state_p) {
	}

	OperatorResultType Project(DataChunk &input, DataChunk &output) override {
		return op.Execute(context, input, output, global_state, operator_state);
	}

private:
	ExecutionContext &context;
	const PhysicalProjection &op;
	GlobalOperatorState &global_state;
	OperatorState &operator_state;
};

PhysicalProjection::PhysicalProjection(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                       vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::PROJECTION, std::move(types), estimated_cardinality),
      select_list(std::move(select_list)) {
}

OperatorResultType PhysicalProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<ProjectionState>();
	state.executor.Execute(input, chunk);
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalProjection::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<ProjectionState>(context, select_list);
}

ExecutionContract PhysicalProjection::GetExecutionContract() const {
	ExecutionContract result;
	result.transform.projection_expressions.reserve(select_list.size());
	for (auto &expression : select_list) {
		result.transform.projection_expressions.emplace_back(*expression);
	}
	return FinalizeExecutionContract(std::move(result));
}

ExecutionOperatorReadiness
PhysicalProjection::GetExecutionOperatorReadiness(ClientContext &context,
                                                  const ExecutionRegionOperatorInfo &operator_info) const {
	(void)context;
	ExecutionOperatorReadiness readiness;
	readiness.kind = operator_info.kind;
	if (operator_info.kind != ExecutionRegionOperatorContractKind::PROJECTION) {
		readiness.status = ExecutionOperatorReadinessStatus::INVALID;
		readiness.blocker = "projection-execution-operator-kind-mismatch";
		return readiness;
	}
	readiness.status = ExecutionOperatorReadinessStatus::READY;
	readiness.blocker = "none";
	return readiness;
}

ExecutionOperatorBindResult PhysicalProjection::BindExecutionOperator(ExecutionContext &context, DataChunk &input,
                                                                      GlobalOperatorState &gstate,
                                                                      OperatorState &state,
                                                                      const ExecutionRegionOperatorInfo &operator_info,
                                                                      ExecutionOperatorBinding &binding) const {
	(void)input;
	binding = ExecutionOperatorBinding();
	binding.kind = operator_info.kind;
	if (operator_info.kind != ExecutionRegionOperatorContractKind::PROJECTION) {
		binding.blocker = "projection-execution-operator-kind-mismatch";
		return ExecutionOperatorBindResult::INVALID;
	}
	binding.ready = true;
	binding.projection.ready = true;
	binding.projection.output_types = GetTypes();
	binding.projection.state = make_shared_ptr<ProjectionExecutionOperatorState>(context, *this, gstate, state);
	return ExecutionOperatorBindResult::READY;
}

InsertionOrderPreservingMap<string> PhysicalProjection::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string projections;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			projections += "\n";
		}
		auto &expr = select_list[i];
		projections += expr->GetName();
	}
	result["__projections__"] = projections;
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
