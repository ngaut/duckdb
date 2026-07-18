//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_settings.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_common.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;

class ExecutionRegionSettings {
public:
	static bool Enabled(ClientContext &context);
	static bool DumpIR(ClientContext &context);
	static idx_t DebugForceDeferAfterChunks(ClientContext &context);
	static bool TraceDecisions(ClientContext &context);
	static bool TraceRuntime(ClientContext &context);
	static bool TraceVectorizedBaseline(ClientContext &context);
	static bool Verify(ClientContext &context);
	static bool ShouldRecordDetailedTelemetry(ClientContext &context);
	static bool ShouldRecordDecisionTelemetry(ClientContext &context);
	static idx_t EventLogSize(DatabaseInstance &db);
	static string RequestedBackend(ClientContext &context);
	static ExecutionRegionPolicyMode Policy(ClientContext &context);
};

} // namespace duckdb
