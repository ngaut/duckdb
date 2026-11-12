//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_continuation_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_filter_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_region_adapter_scratch.hpp"
#include "sljit_region_executable.hpp"

namespace duckdb {

//! Turns the chunks a region fetches through its source contract into the chunks DuckDB's scan would have produced,
//! with the source's own filters and layout repair, once the region handed its pipeline to DuckDB's operators
class SljitNativeContinuation {
public:
	explicit SljitNativeContinuation(vector<SljitExecutableRegionOp> &ops_p) : ops(ops_p), adapters(ops_p.size()) {
		for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
			auto &op = ops[op_idx];
			adapters[op_idx].resize(op.Kind() == SljitNativeRegionOpKind::PROJECTION ? op.Projections().size() : 1);
		}
	}

	DataChunk &Prepare(ExecutionRegionRuntime &runtime, DataChunk &input) {
		const auto source_rows = input.size();
		reference<DataChunk> current(input);
		for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
			auto &op = ops[op_idx];
			if (op.Kind() == SljitNativeRegionOpKind::FILTER) {
				const auto count = current.get().size();
				const auto selected =
				    SljitSelectFilter(op, current.get(), selection, adapters[op_idx][0], nullptr, count, &runtime);
				if (selected != count) {
					current.get().Slice(selection, selected);
				}
				continue;
			}
			D_ASSERT(op.Kind() == SljitNativeRegionOpKind::PROJECTION);
			auto &output = Output(runtime, op_idx);
			output.Reset();
			SljitExecuteProjection(runtime, *this, op_idx, op, current.get(), output);
			current = output;
		}
		if (runtime.TraceRuntime()) {
			runtime.RecordJitRuntimePath("source.native_continuation", current.get().size());
			runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::NATIVE_CONTINUATION, source_rows);
		}
		return current.get();
	}

	SljitExpressionAdapterScratch &ExpressionAdapterScratch(idx_t op_idx, idx_t col_idx) {
		return adapters[op_idx][col_idx];
	}

private:
	DataChunk &Output(ExecutionRegionRuntime &runtime, idx_t op_idx) {
		if (outputs.size() <= op_idx) {
			outputs.resize(op_idx + 1);
		}
		if (!outputs[op_idx]) {
			outputs[op_idx] = make_uniq<DataChunk>();
			outputs[op_idx]->Initialize(runtime.GetAllocator(), ops[op_idx].output_types);
		}
		return *outputs[op_idx];
	}

private:
	vector<SljitExecutableRegionOp> &ops;
	vector<vector<SljitExpressionAdapterScratch>> adapters;
	vector<unique_ptr<DataChunk>> outputs;
	SelectionVector selection {STANDARD_VECTOR_SIZE};
};

} // namespace duckdb
