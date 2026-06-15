#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/jit/manager.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBJitClearEventsData : public GlobalTableFunctionState {
	bool returned = false;
};

static unique_ptr<FunctionData> DuckDBJitClearEventsBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("Success");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBJitClearEventsInit(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	JitSuppressionGuard guard(context);
	JitManager::Get(context).ClearEvents();
	return make_uniq<DuckDBJitClearEventsData>();
}

static void DuckDBJitClearEventsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBJitClearEventsData>();
	if (data.returned) {
		return;
	}
	output.data[0].Append(Value::BOOLEAN(true));
	data.returned = true;
}

void DuckDBJitClearEventsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_jit_clear_events", {}, DuckDBJitClearEventsFunction,
	                              DuckDBJitClearEventsBind, DuckDBJitClearEventsInit));
}

} // namespace duckdb
