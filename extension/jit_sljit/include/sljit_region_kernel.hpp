//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_kernel.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_state.hpp"
#include "sljit_region_runtime.hpp"

#include "duckdb/common/atomic.hpp"

namespace duckdb {

//! Private backend artifact shared by execution-bound region kernels. This
//! declaration is internal to jit_sljit and is not part of the loadable backend
//! ABI.
struct SljitNativeRegionArtifact : public ExecutionRegionArtifact {
	SljitNativeRegionArtifact(string backend_name, vector<SljitExecutableRegionOp> ops,
	                          vector<SljitExecutableScanFilter> scan_filters,
	                          ExecutionRegionScanFilterMode scan_filter_mode, SljitFullPipelineRecipePlan recipe_plan,
	                          unique_ptr<SljitExecutableNativeContinuation> native_continuation);

	string backend_name;
	//! Lazy code cells are synchronized and publish monotonically. The semantic
	//! operation graph itself is immutable after artifact construction.
	mutable vector<SljitExecutableRegionOp> ops;
	vector<SljitExecutableScanFilter> scan_filters;
	ExecutionRegionScanFilterMode scan_filter_mode;
	SljitFullPipelineRecipePlan full_pipeline_recipe_plan;
	//! Present for a region that may hand its pipeline to DuckDB's operators
	mutable unique_ptr<SljitExecutableNativeContinuation> native_continuation;
};

//! Values supplied by this execution, never retained by the database-local artifact cache.
struct SljitRegionInstanceBindings {
	vector<SljitAggregateInstanceParameters> aggregate_parameters;
	vector<SljitCastErrorIdentity> cast_errors;
	vector<shared_ptr<ExecutionRuntimeFilterIdentity>> exact_source_filters;
	vector<idx_t> source_distinct_counts;
	vector<Value> source_min_values;
	vector<Value> source_max_values;
};

//! Concrete execution kernel used by the full-pipeline template owner. Keeping
//! this type concrete preserves direct hot-loop calls while allowing artifact
//! and table-filter plumbing to compile independently.
class SljitNativeRegionKernel : public ExecutionRegionKernel {
public:
	SljitNativeRegionKernel(shared_ptr<const SljitNativeRegionArtifact> artifact, SljitRegionInstanceBindings bindings);
	~SljitNativeRegionKernel() override;

	const string &BackendName() const override;
	idx_t CodeSize() const override;
	bool HasTableFilterKernels() const override;
	bool HasTableFilterKernel(idx_t filter_index) const override;
	unique_ptr<TableFilterKernelState> CreateTableFilterKernelState(idx_t filter_index) const override;
	bool HasExecutableBody() const override;
	unique_ptr<ExecutionRegionLocalState> CreateLocalState(Allocator &allocator) const override;
	bool TryExecuteFullPipeline(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) override;
	bool SupportsChunkStep() const override;
	bool EntryReady(ExecutionRegionRuntime &runtime, string &blocker) override;
	unique_ptr<ExecutionRegionChunkStep> CreateChunkStep(ExecutionRegionRuntime &runtime) override;
	idx_t MinPassthroughRows() const override;

	//! Hands the rows the storage scan's compiled filters processed since the last call to the runtime's trace
	void RecordExecutedFilterRows(ExecutionRegionRuntime &runtime) const;
	const shared_ptr<ExecutionRuntimeFilterIdentity> &ExactSourceFilterIdentity(idx_t binding) const;
	SljitNativePerfectHashJoinProbeFunction EnsurePerfectHashJoinProbeCode(ExecutionRegionRuntime &runtime,
	                                                                       SljitExecutableHashJoinProbe &probe,
	                                                                       bool prefer_identity_selection,
	                                                                       bool direct_consumer = false);
	SljitNativeRegularHashJoinProbeFunction EnsureRegularHashJoinProbeCode(
	    ExecutionRegionRuntime &runtime, SljitExecutableHashJoinProbe &probe, bool uses_bloom_filter = false,
	    SljitHashJoinMarkSelectionMode mark_selection_mode = SljitHashJoinMarkSelectionMode::NONE);
	SljitNativeRegularHashJoinProbeFunction EnsureAllValidRegularHashJoinProbeCode(
	    ExecutionRegionRuntime &runtime, SljitExecutableHashJoinProbe &probe,
	    const SljitHashJoinProbeAllValidSpecializationKey &key,
	    SljitHashJoinMarkSelectionMode mark_selection_mode = SljitHashJoinMarkSelectionMode::NONE);

private:
	template <class FUNCTION, class BUILD>
	FUNCTION EnsureLazyHashJoinProbeCode(ExecutionRegionRuntime &runtime, SljitLazyCompiledFunction<FUNCTION> &artifact,
	                                     const char *failure_message, BUILD build);

private:
	shared_ptr<const SljitNativeRegionArtifact> artifact;
	SljitRegionInstanceBindings bindings;
	vector<SljitSharedPerfectHashPredicateClassificationCache> shared_predicate_classifications;
	mutable atomic<idx_t> executed_filter_rows {0};
};

} // namespace duckdb
