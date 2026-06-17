#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBJitClearAdmissionRulesData : public GlobalTableFunctionState {
	bool returned = false;
};

static unique_ptr<FunctionData> DuckDBJitClearAdmissionRulesBind(ClientContext &context, TableFunctionBindInput &input,
                                                                 vector<LogicalType> &return_types,
                                                                 vector<string> &names) {
	names.emplace_back("Success");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBJitClearAdmissionRulesInit(ClientContext &context,
                                                                             TableFunctionInitInput &input) {
	ExecutionRegionSuppressionGuard guard(context);
	ExecutionRegionManager::Get(context).ClearAdmissionProfileRules();
	return make_uniq<DuckDBJitClearAdmissionRulesData>();
}

static void DuckDBJitClearAdmissionRulesFunction(ClientContext &context, TableFunctionInput &data_p,
                                                 DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBJitClearAdmissionRulesData>();
	if (data.returned) {
		return;
	}
	output.data[0].Append(Value::BOOLEAN(true));
	data.returned = true;
}

void DuckDBJitClearAdmissionRulesFun::RegisterFunction(BuiltinFunctions &set) {
	auto function = TableFunction("duckdb_jit_clear_admission_rules", {}, DuckDBJitClearAdmissionRulesFunction,
	                              DuckDBJitClearAdmissionRulesBind, DuckDBJitClearAdmissionRulesInit);
	function.suppress_compiled_execution = true;
	set.AddFunction(function);
}

} // namespace duckdb
