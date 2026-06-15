#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/jit/manager.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBJitBackendsData : public GlobalTableFunctionState {
	vector<JitBackendInfo> backends;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DuckDBJitBackendsBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("available");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("supports_regions");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("selected");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBJitBackendsInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	JitSuppressionGuard guard(context);
	auto result = make_uniq<DuckDBJitBackendsData>();
	result->backends = JitManager::Get(context).GetBackends(&context);
	return std::move(result);
}

static void DuckDBJitBackendsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBJitBackendsData>();
	idx_t count = 0;
	while (data.offset < data.backends.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.backends[data.offset++];
		output.data[0].Append(Value(entry.name));
		output.data[1].Append(Value(entry.description));
		output.data[2].Append(Value::BOOLEAN(entry.available));
		output.data[3].Append(Value::BOOLEAN(entry.supports_regions));
		output.data[4].Append(Value::BOOLEAN(entry.selected));
		count++;
	}
}

void DuckDBJitBackendsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_jit_backends", {}, DuckDBJitBackendsFunction, DuckDBJitBackendsBind,
	                              DuckDBJitBackendsInit));
}

} // namespace duckdb
