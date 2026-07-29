//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_kernel.hpp"

#include "sljit_filter_runtime.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/execution/execution_region_backend.hpp"

#include <utility>

namespace duckdb {

class SljitTableFilterKernelState : public TableFilterKernelState {
public:
	SljitTableFilterKernelState(const SljitExecutableScanFilter &scan_filter_p, atomic<idx_t> &executed_rows_p)
	    : scan_filter(scan_filter_p), executed_rows(executed_rows_p), filter_selection(STANDARD_VECTOR_SIZE),
	      alternate_selection(STANDARD_VECTOR_SIZE) {
		input.InitializeEmpty({scan_filter.input_type});
	}

	bool TrySelect(Vector &vector, UnifiedVectorFormat &format, SelectionVector &selection, idx_t scan_count,
	               idx_t &approved_tuple_count) override {
		(void)format;
		if (!scan_filter.filter || scan_count > STANDARD_VECTOR_SIZE || approved_tuple_count > scan_count) {
			return false;
		}
		if (approved_tuple_count == 0) {
			return true;
		}
		input.data[0].Reference(vector);
		input.SetChildCardinality(scan_count);
		auto input_count = approved_tuple_count;
		const SelectionVector *execute_selection = input_count == scan_count ? nullptr : &selection;
		auto output_selection = &filter_selection;
		if (execute_selection && execute_selection->data() == output_selection->data()) {
			output_selection = &alternate_selection;
		}
		idx_t selected_count;
		auto &filter = *scan_filter.filter;
		if (filter.batch_select) {
			selected_count = SljitSelectNativeStringLikeBatch(*filter.batch_select, vector, *output_selection,
			                                                  execute_selection, input_count);
		} else {
			selected_count = SljitSelectExpression(filter.expression, input, *output_selection, expression_scratch,
			                                       execute_selection, input_count, false);
		}
		if (selected_count < input_count) {
			selection.Initialize(*output_selection);
		}
		approved_tuple_count = selected_count;
		executed_rows.fetch_add(input_count, std::memory_order_relaxed);
		return true;
	}

private:
	const SljitExecutableScanFilter &scan_filter;
	atomic<idx_t> &executed_rows;
	DataChunk input;
	SelectionVector filter_selection;
	SelectionVector alternate_selection;
	SljitExpressionAdapterScratch expression_scratch;
};

SljitNativeRegionArtifact::SljitNativeRegionArtifact(
    string backend_name_p, vector<SljitExecutableRegionOp> ops_p, vector<SljitExecutableScanFilter> scan_filters_p,
    bool uses_scan_filters_p, vector<idx_t> source_distinct_counts_p, vector<Value> source_min_values_p,
    vector<Value> source_max_values_p, SljitFullPipelineRecipePlan recipe_plan_p, ExecutionRegionABI abi_p)
    : backend_name(std::move(backend_name_p)), ops(std::move(ops_p)), scan_filters(std::move(scan_filters_p)),
      uses_scan_filters(uses_scan_filters_p), source_distinct_counts(std::move(source_distinct_counts_p)),
      source_min_values(std::move(source_min_values_p)), source_max_values(std::move(source_max_values_p)),
      full_pipeline_recipe_plan(std::move(recipe_plan_p)), abi(abi_p) {
}

SljitNativeRegionKernel::SljitNativeRegionKernel(
    shared_ptr<const SljitNativeRegionArtifact> artifact_p,
    vector<shared_ptr<ExecutionRuntimeFilterIdentity>> exact_source_filter_bindings_p)
    : artifact(std::move(artifact_p)), exact_source_filter_bindings(std::move(exact_source_filter_bindings_p)),
      shared_predicate_classifications(artifact->ops.size()) {
}

SljitNativeRegionKernel::~SljitNativeRegionKernel() = default;

const string &SljitNativeRegionKernel::BackendName() const {
	return artifact->backend_name;
}

idx_t SljitNativeRegionKernel::CodeSize() const {
	idx_t result = 0;
	for (auto &op : artifact->ops) {
		result += op.CodeSize();
	}
	for (auto &scan_filter : artifact->scan_filters) {
		result += scan_filter.CodeSize();
	}
	return result;
}

bool SljitNativeRegionKernel::HasTableFilterKernels() const {
	return !artifact->scan_filters.empty();
}

bool SljitNativeRegionKernel::HasTableFilterKernel(idx_t filter_index) const {
	for (auto &scan_filter : artifact->scan_filters) {
		if (scan_filter.filter_index == filter_index) {
			return true;
		}
	}
	return false;
}

unique_ptr<TableFilterKernelState> SljitNativeRegionKernel::CreateTableFilterKernelState(idx_t filter_index) const {
	for (auto &scan_filter : artifact->scan_filters) {
		if (scan_filter.filter_index == filter_index) {
			return make_uniq<SljitTableFilterKernelState>(scan_filter, executed_filter_rows);
		}
	}
	return nullptr;
}

bool SljitNativeRegionKernel::HasExecutableBody() const {
	for (auto &op : artifact->ops) {
		if (op.HasExecutableBody()) {
			return true;
		}
	}
	return artifact->uses_scan_filters && artifact->full_pipeline_recipe_plan.HasRecipe() &&
	       artifact->full_pipeline_recipe_plan.Recipe().has_scan_filter_executable_body;
}

const shared_ptr<ExecutionRuntimeFilterIdentity> &
SljitNativeRegionKernel::ExactSourceFilterIdentity(idx_t binding) const {
	static const shared_ptr<ExecutionRuntimeFilterIdentity> no_identity;
	return binding < exact_source_filter_bindings.size() ? exact_source_filter_bindings[binding] : no_identity;
}

shared_ptr<const ExecutionRegionArtifact> CreateSljitNativeRegionArtifact(string backend_name,
                                                                          SljitExecutableRegion &&region,
                                                                          SljitFullPipelineRecipePlan recipe_plan,
                                                                          ExecutionRegionABI abi) {
	return make_shared_ptr<SljitNativeRegionArtifact>(
	    std::move(backend_name), std::move(region.ops), std::move(region.scan_filters), region.uses_scan_filters,
	    std::move(region.source_distinct_counts), std::move(region.source_min_values),
	    std::move(region.source_max_values), std::move(recipe_plan), abi);
}

unique_ptr<ExecutionRegionKernel>
InstantiateSljitNativeRegionArtifact(const shared_ptr<const ExecutionRegionArtifact> &artifact,
                                     const ExecutionRegionCompilationInput &input) {
	auto sljit_artifact_ptr = dynamic_cast<const SljitNativeRegionArtifact *>(artifact.get());
	if (!sljit_artifact_ptr || !input.lowering_plan || !input.lowering_plan->backend_plan) {
		return nullptr;
	}
	shared_ptr<const SljitNativeRegionArtifact> sljit_artifact(artifact, sljit_artifact_ptr);
	auto sljit_plan = dynamic_cast<const SljitRegionBackendPlan *>(input.lowering_plan->backend_plan.get());
	if (!sljit_plan || !sljit_plan->native_region) {
		return nullptr;
	}
	vector<shared_ptr<ExecutionRuntimeFilterIdentity>> exact_source_filter_bindings(sljit_artifact->ops.size());
	for (auto &current_op : sljit_plan->native_region->ops) {
		if (current_op.kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			continue;
		}
		auto binding = current_op.hash_join_probe.exact_source_filter_binding;
		if (binding == DConstants::INVALID_INDEX) {
			continue;
		}
		if (binding >= exact_source_filter_bindings.size() ||
		    !current_op.hash_join_probe.exact_source_filter_identity) {
			return nullptr;
		}
		exact_source_filter_bindings[binding] = current_op.hash_join_probe.exact_source_filter_identity;
	}
	return make_uniq<SljitNativeRegionKernel>(std::move(sljit_artifact), std::move(exact_source_filter_bindings));
}

} // namespace duckdb
