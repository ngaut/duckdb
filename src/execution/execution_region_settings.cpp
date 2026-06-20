#include "duckdb/execution/execution_region_settings.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

bool ExecutionRegionSettings::Enabled(ClientContext &context) {
	return Settings::Get<EnableJitSetting>(context);
}

bool ExecutionRegionSettings::DumpIR(ClientContext &context) {
	return Settings::Get<JitDumpIrSetting>(context);
}

bool ExecutionRegionSettings::TraceDecisions(ClientContext &context) {
	return Settings::Get<JitTraceDecisionsSetting>(context);
}

bool ExecutionRegionSettings::TraceRuntime(ClientContext &context) {
	return Settings::Get<JitTraceRuntimeSetting>(context) || context.QueryProfilerIsExplainAnalyze();
}

bool ExecutionRegionSettings::TraceVectorizedBaseline(ClientContext &context) {
	return Settings::Get<JitTraceVectorizedBaselineSetting>(context);
}

bool ExecutionRegionSettings::Verify(ClientContext &context) {
	return Settings::Get<JitVerifySetting>(context);
}

bool ExecutionRegionSettings::ShouldRecordDetailedTelemetry(ClientContext &context) {
	return TraceDecisions(context) || DumpIR(context) || TraceRuntime(context);
}

idx_t ExecutionRegionSettings::EventLogSize(DatabaseInstance &db) {
	return Settings::Get<JitEventLogSizeSetting>(db);
}

string ExecutionRegionSettings::RequestedBackend(ClientContext &context) {
	return StringUtil::Lower(Settings::Get<JitBackendSetting>(context));
}

ExecutionRegionPolicyMode ExecutionRegionSettings::Policy(ClientContext &context) {
	auto policy = StringUtil::Lower(Settings::Get<JitPolicySetting>(context));
	if (policy == "auto") {
		return ExecutionRegionPolicyMode::AUTO;
	}
	if (policy == "force") {
		return ExecutionRegionPolicyMode::FORCE;
	}
	if (policy == "off") {
		return ExecutionRegionPolicyMode::OFF;
	}
	throw InvalidInputException("Invalid execution region policy \"%s\"", policy);
}

} // namespace duckdb
