#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBJitAddAdmissionRuleBindData : public FunctionData {
	string backend_name;
	ExecutionRegionAdmissionRule rule;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<DuckDBJitAddAdmissionRuleBindData>();
		result->backend_name = backend_name;
		result->rule = rule;
		return std::move(result);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DuckDBJitAddAdmissionRuleBindData>();
		return backend_name == other.backend_name && rule.target == other.rule.target &&
		       rule.admission_key == other.rule.admission_key && rule.min_cardinality == other.rule.min_cardinality &&
		       rule.proof == other.rule.proof;
	}
};

struct DuckDBJitAddAdmissionRuleData : public GlobalTableFunctionState {
	bool returned = false;
};

static ExecutionRegionCompileTarget ParseJitAdmissionRuleTarget(const string &target) {
	auto normalized_target = StringUtil::Lower(target);
	if (normalized_target == ExecutionRegionCompileTargetToString(ExecutionRegionCompileTarget::REGION)) {
		return ExecutionRegionCompileTarget::REGION;
	}
	throw InvalidInputException("Unsupported JIT admission target \"%s\"", target);
}

static unique_ptr<FunctionData> DuckDBJitAddAdmissionRuleBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types,
                                                             vector<string> &names) {
	if (input.inputs.size() != 5) {
		throw InvalidInputException(
		    "duckdb_jit_add_admission_rule requires backend_name, target, admission_shape_key, "
		    "admission_min_cardinality, and admission_proof");
	}
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw InvalidInputException("duckdb_jit_add_admission_rule arguments cannot be NULL");
		}
	}
	auto result = make_uniq<DuckDBJitAddAdmissionRuleBindData>();
	result->backend_name = StringValue::Get(input.inputs[0]);
	result->rule.target = ParseJitAdmissionRuleTarget(StringValue::Get(input.inputs[1]));
	result->rule.admission_key = StringValue::Get(input.inputs[2]);
	result->rule.min_cardinality = NumericCast<idx_t>(input.inputs[3].GetValue<uint64_t>());
	result->rule.proof = StringValue::Get(input.inputs[4]);

	names.emplace_back("Success");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckDBJitAddAdmissionRuleInit(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	ExecutionRegionSuppressionGuard guard(context);
	auto &bind_data = input.bind_data->Cast<DuckDBJitAddAdmissionRuleBindData>();
	ExecutionRegionManager::Get(context).AddAdmissionProfileRule(bind_data.backend_name, bind_data.rule);
	return make_uniq<DuckDBJitAddAdmissionRuleData>();
}

static void DuckDBJitAddAdmissionRuleFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBJitAddAdmissionRuleData>();
	if (data.returned) {
		return;
	}
	output.data[0].Append(Value::BOOLEAN(true));
	data.returned = true;
}

void DuckDBJitAddAdmissionRuleFun::RegisterFunction(BuiltinFunctions &set) {
	auto function = TableFunction("duckdb_jit_add_admission_rule",
	                              {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                               LogicalType::UBIGINT, LogicalType::VARCHAR},
	                              DuckDBJitAddAdmissionRuleFunction, DuckDBJitAddAdmissionRuleBind,
	                              DuckDBJitAddAdmissionRuleInit);
	function.suppress_compiled_execution = true;
	set.AddFunction(function);
}

} // namespace duckdb
