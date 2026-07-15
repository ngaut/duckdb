#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

TableFilterKernelState::~TableFilterKernelState() {
}

TableFilterKernelProvider::~TableFilterKernelProvider() {
}

bool TableFilterKernelProvider::HasTableFilterKernels() const {
	return false;
}

bool TableFilterKernelProvider::HasTableFilterKernel(idx_t) const {
	return false;
}

unique_ptr<TableFilterKernelState> TableFilterKernelProvider::CreateTableFilterKernelState(idx_t) const {
	return nullptr;
}

static void InitializeExecutor(ClientContext &context, const Expression &expression, ExpressionFilterState &state) {
	state.executor = make_uniq<ExpressionExecutor>(context);
	state.executor->AddExpression(expression);
}

ExpressionFilterState::ExpressionFilterState(ClientContext &context, const Expression &expression) {
	InitializeExecutor(context, expression, *this);
}

unique_ptr<TableFilterState> TableFilterState::Initialize(ClientContext &context, const TableFilter &filter) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "TableFilterState::Initialize");
	return make_uniq<ExpressionFilterState>(context, *expr_filter.expr);
}

} // namespace duckdb
