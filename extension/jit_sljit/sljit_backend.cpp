//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_backend.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_backend.hpp"
#include "sljit_region.hpp"
#include "sljit_region_plan.hpp"
#include "sljit_platform.hpp"

#include "duckdb/execution/execution_region_graph.hpp"

namespace duckdb {

class SljitExecutionRegionBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "sljit";
	}

	string Description() const override {
		return "SLJIT execution region backend";
	}

	bool IsAvailable() const override {
		return SljitPlatformAvailable();
	}

	bool SupportsRegions() const override {
		return true;
	}

	ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input) override {
		return AnalyzeSljitRegion(input);
	}

	ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &input) override {
		return CompileSljitRegion(Name(), input);
	}
};

void RegisterSljitExecutionRegionBackend(ExtensionLoader &loader) {
	RegisterExecutionRegionBackend(loader.GetDatabaseInstance(), make_uniq<SljitExecutionRegionBackend>(),
	                               EXECUTION_REGION_BACKEND_ABI_VERSION);
}

} // namespace duckdb
