//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_execution_scratch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_execution_scratch_helpers.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_scratch_access.hpp"

#include "duckdb/common/allocator.hpp"

namespace duckdb {

struct SljitHashJoinExecutionScratch {
	void Resize(idx_t count) {
		build_selections.resize(count);
		row_pointers.resize(count);
		build_source_chunks.resize(count);
		build_hash_values.resize(count);
		residual_chunks.resize(count);
		residual_selections.resize(count);
		residual_match_selections.resize(count);
		residual_row_pointers.resize(count);
		sources.resize(count);
	}

	void InitializeProbe(Allocator &allocator, idx_t op_idx, const SljitExecutableRegionOp &op) {
		build_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		row_pointers[op_idx] = make_uniq<Vector>(LogicalType::POINTER);
		sources[op_idx].Resize(op.hash_join_probe.plan.keys.size());
		if (!op.hash_join_probe.plan.residual_predicate) {
			return;
		}
		residual_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		residual_match_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		residual_row_pointers[op_idx] = make_uniq<Vector>(LogicalType::POINTER);
		SljitInitializeScratchChunk(allocator, op.hash_join_probe.plan.residual_source_types, residual_chunks[op_idx]);
	}

	void InitializeBuild(idx_t op_idx) {
		build_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		build_source_chunks[op_idx] = make_uniq<DataChunk>();
		build_hash_values[op_idx] = make_uniq<Vector>(LogicalType::HASH);
	}

	Vector &RowPointers(idx_t op_idx) {
		return SljitCheckedScratchPtr(row_pointers, op_idx,
		                              "SLJIT full pipeline hash join probe has no row-pointer scratch");
	}

	SelectionVector &BuildSelection(idx_t op_idx) {
		return SljitCheckedScratchPtr(build_selections, op_idx,
		                              "SLJIT full pipeline hash join probe has no build-selection scratch");
	}

	DataChunk &BuildSourceChunk(idx_t op_idx) {
		return SljitCheckedScratchPtr(build_source_chunks, op_idx,
		                              "SLJIT full pipeline hash join build has no source-chunk scratch");
	}

	Vector &BuildHashValues(idx_t op_idx) {
		return SljitCheckedScratchPtr(build_hash_values, op_idx,
		                              "SLJIT full pipeline hash join build has no hash-value scratch");
	}

	DataChunk &ResidualChunk(idx_t op_idx) {
		return SljitCheckedScratchPtr(residual_chunks, op_idx,
		                              "SLJIT full pipeline hash join probe has no residual chunk scratch");
	}

	SelectionVector &ResidualSelection(idx_t op_idx) {
		return SljitCheckedScratchPtr(residual_selections, op_idx,
		                              "SLJIT full pipeline hash join probe has no residual selection scratch");
	}

	SelectionVector &ResidualMatchSelection(idx_t op_idx) {
		return SljitCheckedScratchPtr(residual_match_selections, op_idx,
		                              "SLJIT full pipeline hash join probe has no residual match selection scratch");
	}

	Vector &ResidualRowPointers(idx_t op_idx) {
		return SljitCheckedScratchPtr(residual_row_pointers, op_idx,
		                              "SLJIT full pipeline hash join probe has no residual row-pointer scratch");
	}

	SljitHashJoinProbeSourceScratch &Sources(idx_t op_idx) {
		return SljitCheckedScratchSlot(sources, op_idx, "SLJIT hash join probe has no source scratch slot");
	}

private:
	vector<unique_ptr<SelectionVector>> build_selections;
	vector<unique_ptr<Vector>> row_pointers;
	vector<unique_ptr<DataChunk>> build_source_chunks;
	vector<unique_ptr<Vector>> build_hash_values;
	vector<unique_ptr<DataChunk>> residual_chunks;
	vector<unique_ptr<SelectionVector>> residual_selections;
	vector<unique_ptr<SelectionVector>> residual_match_selections;
	vector<unique_ptr<Vector>> residual_row_pointers;
	vector<SljitHashJoinProbeSourceScratch> sources;
};

} // namespace duckdb
