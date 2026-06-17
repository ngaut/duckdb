#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBJitAdmissionRulesData : public GlobalTableFunctionState {
	vector<ExecutionRegionAdmissionProfileRule> rules;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DuckDBJitAdmissionRulesBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<string> &names) {
	names.emplace_back("backend_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("target");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("admission_shape_key");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("admission_min_cardinality");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("admission_proof");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBJitAdmissionRulesInit(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	ExecutionRegionSuppressionGuard guard(context);
	auto result = make_uniq<DuckDBJitAdmissionRulesData>();
	result->rules = ExecutionRegionManager::Get(context).GetAdmissionProfileRules();
	return std::move(result);
}

static void DuckDBJitAdmissionRulesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBJitAdmissionRulesData>();
	idx_t count = 0;
	while (data.offset < data.rules.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.rules[data.offset++];
		idx_t col = 0;
		output.data[col++].Append(Value(entry.backend_name));
		output.data[col++].Append(Value(ExecutionRegionCompileTargetToString(entry.rule.target)));
		output.data[col++].Append(Value(entry.rule.admission_key));
		output.data[col++].Append(Value::UBIGINT(entry.rule.min_cardinality));
		output.data[col++].Append(Value(entry.rule.proof));
		count++;
	}
}

void DuckDBJitAdmissionRulesFun::RegisterFunction(BuiltinFunctions &set) {
	auto function = TableFunction("duckdb_jit_admission_rules", {}, DuckDBJitAdmissionRulesFunction,
	                              DuckDBJitAdmissionRulesBind, DuckDBJitAdmissionRulesInit);
	function.suppress_compiled_execution = true;
	set.AddFunction(function);
}

} // namespace duckdb
