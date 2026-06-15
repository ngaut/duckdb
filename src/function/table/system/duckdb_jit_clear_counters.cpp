#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/jit/manager.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBJitClearCountersData : public GlobalTableFunctionState {
	bool returned = false;
};

static unique_ptr<FunctionData> DuckDBJitClearCountersBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types,
                                                           vector<string> &names) {
	names.emplace_back("Success");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBJitClearCountersInit(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
	JitSuppressionGuard guard(context);
	JitManager::Get(context).ClearCounters();
	return make_uniq<DuckDBJitClearCountersData>();
}

static void DuckDBJitClearCountersFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBJitClearCountersData>();
	if (data.returned) {
		return;
	}
	output.data[0].Append(Value::BOOLEAN(true));
	data.returned = true;
}

void DuckDBJitClearCountersFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_jit_clear_counters", {}, DuckDBJitClearCountersFunction,
	                              DuckDBJitClearCountersBind, DuckDBJitClearCountersInit));
}

} // namespace duckdb
