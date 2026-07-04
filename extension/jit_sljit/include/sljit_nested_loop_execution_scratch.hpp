//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_nested_loop_execution_scratch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"
#include "sljit_scratch_access.hpp"

#include "duckdb/common/allocator.hpp"

namespace duckdb {

struct SljitNestedLoopJoinExecutionScratch {
	void Resize(idx_t count) {
		left_condition_chunks.resize(count);
		left_selections.resize(count);
		right_selections.resize(count);
		condition_chunks.resize(count);
	}

	void InitializeProbe(Allocator &allocator, idx_t op_idx, const SljitExecutableRegionOp &op) {
		left_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		right_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		SljitInitializeScratchChunk(allocator, op.nested_loop_join_probe.plan.condition_types,
		                            left_condition_chunks[op_idx]);
	}

	void InitializeBuild(Allocator &allocator, idx_t op_idx, const SljitExecutableRegionOp &op) {
		SljitInitializeScratchChunk(allocator, op.nested_loop_join_build.plan.condition_types,
		                            condition_chunks[op_idx]);
	}

	DataChunk &LeftConditionChunk(idx_t op_idx) {
		return SljitCheckedScratchPtr(left_condition_chunks, op_idx,
		                              "SLJIT nested loop join probe has no left condition scratch chunk");
	}

	SelectionVector &LeftSelection(idx_t op_idx) {
		return SljitCheckedScratchPtr(left_selections, op_idx,
		                              "SLJIT nested loop join probe has no left selection scratch");
	}

	SelectionVector &RightSelection(idx_t op_idx) {
		return SljitCheckedScratchPtr(right_selections, op_idx,
		                              "SLJIT nested loop join probe has no right selection scratch");
	}

	DataChunk &ConditionChunk(idx_t op_idx) {
		return SljitCheckedScratchPtr(condition_chunks, op_idx,
		                              "SLJIT nested loop join build has no condition scratch chunk");
	}

private:
	vector<unique_ptr<DataChunk>> left_condition_chunks;
	vector<unique_ptr<SelectionVector>> left_selections;
	vector<unique_ptr<SelectionVector>> right_selections;
	vector<unique_ptr<DataChunk>> condition_chunks;
};

} // namespace duckdb
