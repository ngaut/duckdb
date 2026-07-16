//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/perfect_hash_join_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/execution_hash_join_runtime.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class HashJoinOperatorState;
class HashJoinGlobalSinkState;
class PhysicalHashJoin;
struct PerfectHashJoinStats {
	Value build_min;
	Value build_max;
	bool is_build_small = false;
	bool is_build_dense = false;
	idx_t build_range = 0;
};

//! PhysicalHashJoin represents a hash loop join between two tables
class PerfectHashJoinExecutor {
	using PerfectHashTable = vector<buffer_ptr<DictionaryEntry>>;

public:
	PerfectHashJoinExecutor(const PhysicalHashJoin &join, JoinHashTable &ht);

public:
	bool CanDoPerfectHashJoin(const PhysicalHashJoin &op, const Value &min, const Value &max);

	const LogicalType &GetKeyType() const;
	bool BuildPerfectHashTable();
	bool GetExecutionPerfectHashJoinTableLayout(ExecutionPerfectHashJoinTableLayout &layout) const;
	optional_ptr<const ExecutionPerfectHashJoinFilterLayout> GetExecutionPerfectHashJoinFilterLayout() const;
	const shared_ptr<ExecutionRuntimeFilterIdentity> &GetRuntimeFilterIdentity() const {
		return runtime_filter_identity;
	}

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context);
	OperatorResultType ProbePerfectHashTable(ExecutionContext &context, DataChunk &input, DataChunk &lhs_output_columns,
	                                         DataChunk &chunk, OperatorState &state);

	void FillSelectionVectorSwitchProbe(const Vector &source, const idx_t &count, SelectionVector &probe_sel_vec,
	                                    idx_t &probe_sel_count, optional_ptr<SelectionVector> build_sel_vec) const;
	idx_t FilterSelection(const UnifiedVectorFormat &source, const LogicalType &source_type,
	                      optional_ptr<const SelectionVector> input_sel, idx_t count,
	                      SelectionVector &result_sel) const;

private:
	template <bool BUILD_SEL_VEC>
	void FillSelectionVectorSwitchProbe(const Vector &source, const idx_t &count, SelectionVector &probe_sel_vec,
	                                    idx_t &probe_sel_count, SelectionVector *build_sel_vec) const;
	template <typename T, bool BUILD_SEL_VEC>
	void TemplatedFillSelectionVectorProbe(const Vector &source, const idx_t &count, SelectionVector &probe_sel_vec,
	                                       idx_t &probe_sel_count, SelectionVector *build_sel_vec) const;
	template <typename SOURCE, typename TARGET, bool HAS_NULL>
	idx_t TemplatedFilterSelection(const UnifiedVectorFormat &source, optional_ptr<const SelectionVector> input_sel,
	                               idx_t count, SelectionVector &result_sel) const;
	template <typename SOURCE, typename TARGET, bool HAS_NULL, bool BUILD_DENSE>
	idx_t TemplatedFilterSelectionLayoutSwitch(const UnifiedVectorFormat &source,
	                                           optional_ptr<const SelectionVector> input_sel, idx_t count,
	                                           SelectionVector &result_sel) const;
	template <typename SOURCE, typename TARGET, bool HAS_NULL, bool BUILD_DENSE, bool INPUT_SELECTED,
	          bool SOURCE_SELECTED>
	idx_t TemplatedFilterSelectionLoop(const UnifiedVectorFormat &source, const sel_t *input_selection,
	                                   const sel_t *source_selection, idx_t count, SelectionVector &result_sel) const;
	template <typename SOURCE, bool HAS_NULL>
	idx_t FilterSelectionTargetSwitch(const UnifiedVectorFormat &source, optional_ptr<const SelectionVector> input_sel,
	                                  idx_t count, SelectionVector &result_sel) const;

	bool FillSelectionVectorSwitchBuild(const Vector &source, SelectionVector &sel_vec, SelectionVector &seq_sel_vec,
	                                    idx_t count);
	template <typename T>
	bool TemplatedFillSelectionVectorBuild(const Vector &source, SelectionVector &sel_vec, SelectionVector &seq_sel_vec,
	                                       idx_t count);
	bool FullScanHashTable();
	bool PublishExecutionPerfectHashJoinFilterLayout();

private:
	const PhysicalHashJoin &join;
	JoinHashTable &ht;
	//! Columnar perfect hash table
	PerfectHashTable perfect_hash_table;
	//! Build statistics
	PerfectHashJoinStats perfect_join_statistics;
	//! Stores the occurrences of each value in the build side
	ValidityMask bitmap_build_idx;
	//! Stores the non-empty words in bitmap_build_idx so sparse range scans skip empty words
	ValidityMask bitmap_build_non_empty_words;
	//! Immutable scan-side membership contract, published once perfect-hash finalization succeeds.
	ExecutionPerfectHashJoinFilterLayout execution_filter_layout;
	//! Stores the number of unique keys in the build side
	idx_t unique_keys = 0;
	shared_ptr<ExecutionRuntimeFilterIdentity> runtime_filter_identity;
};

} // namespace duckdb
