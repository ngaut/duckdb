//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_generated_filter_primitive_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_primitive_sequence.hpp"
#include "sljit_generated_filter_primitive.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

class SljitGeneratedFilterPrimitiveRuntime {
public:
	SljitGeneratedFilterPrimitiveRuntime(ExecutionRegionRuntime &runtime_p, vector<SljitExecutableRegionOp> &ops_p,
	                                     SljitRegionExecutionScratch &scratch_p)
	    : runtime(runtime_p), ops(ops_p), scratch(scratch_p) {
		selected_hash_join_filter_caches.resize(ops.size());
	}

	template <class EXECUTE_OUTPUT_VIEW>
	bool Execute(const SljitFullPipelinePrimitiveStep &step, const SljitRuntimeBatchView &input,
	             EXECUTE_OUTPUT_VIEW &&execute_output_view) {
		SljitRuntimeBatchView filtered_input;
		if (input.HasHashJoinSelection()) {
			auto &cache = selected_hash_join_filter_caches[step.generated_filter.filter_idx];
			if (!SljitExecuteSelectedHashJoinGeneratedFilterPrimitive(runtime, scratch, ops, step.generated_filter,
			                                                          input, selected_hash_join_filter_input, cache,
			                                                          filtered_input)) {
				return false;
			}
			return execute_output_view(filtered_input);
		}
		if (!SljitExecuteGeneratedFilterPrimitive(runtime, scratch, ops, step.generated_filter, input,
		                                          filtered_input)) {
			return false;
		}
		return execute_output_view(filtered_input);
	}

private:
	ExecutionRegionRuntime &runtime;
	vector<SljitExecutableRegionOp> &ops;
	SljitRegionExecutionScratch &scratch;
	SljitDataChunkBatch selected_hash_join_filter_input;
	vector<SljitSelectedHashJoinFilterCache> selected_hash_join_filter_caches;
};

} // namespace duckdb
