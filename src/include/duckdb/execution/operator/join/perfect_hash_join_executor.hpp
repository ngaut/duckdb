//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/perfect_hash_join_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
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
	//! Allocate the table for the build range CanDoPerfectHashJoin accepted.
	void InitializePerfectHashTable();
	//! Fill the table from the build chunks [chunk_begin, chunk_end). Disjoint ranges may be filled concurrently.
	void FillPerfectHashTable(idx_t chunk_begin, idx_t chunk_end);
	//! Complete the table after every build chunk was filled. False if the build side has a duplicate key.
	bool FinalizePerfectHashTable();
	//! Export the native probe layout. Its runtime filter identity is the hash table's, so the exact prefix-range
	//! scan filter registered on that hash table proves membership for perfect-hash probes as well.
	bool GetExecutionPerfectHashJoinTableLayout(ExecutionPerfectHashJoinTableLayout &layout) const;

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context);
	OperatorResultType ProbePerfectHashTable(ExecutionContext &context, DataChunk &input, DataChunk &lhs_output_columns,
	                                         DataChunk &chunk, OperatorState &state);

	void FillSelectionVectorSwitchProbe(const Vector &source, const idx_t &count, SelectionVector &probe_sel_vec,
	                                    idx_t &probe_sel_count, optional_ptr<SelectionVector> build_sel_vec) const;

private:
	template <bool BUILD_SEL_VEC>
	void FillSelectionVectorSwitchProbe(const Vector &source, const idx_t &count, SelectionVector &probe_sel_vec,
	                                    idx_t &probe_sel_count, SelectionVector *build_sel_vec) const;
	template <typename T, bool BUILD_SEL_VEC>
	void TemplatedFillSelectionVectorProbe(const Vector &source, const idx_t &count, SelectionVector &probe_sel_vec,
	                                       idx_t &probe_sel_count, SelectionVector *build_sel_vec) const;

	//! Claim the table slots of the in-range keys of one build chunk. False if a slot was already claimed.
	bool ClaimBuildSlots(const Vector &keys, idx_t count, SelectionVector &slot_sel, SelectionVector &row_sel,
	                     idx_t &claimed);
	template <typename T>
	bool TemplatedClaimBuildSlots(const Vector &keys, idx_t count, SelectionVector &slot_sel, SelectionVector &row_sel,
	                              idx_t &claimed);
	bool PublishExecutionPerfectHashJoinMembershipLayout();

private:
	const PhysicalHashJoin &join;
	JoinHashTable &ht;
	//! Columnar perfect hash table
	PerfectHashTable perfect_hash_table;
	//! Build statistics
	PerfectHashJoinStats perfect_join_statistics;
	//! Stores the occurrences of each value in the build side. Fill tasks claim slots with atomic bit updates.
	ValidityMask bitmap_build_idx;
	//! Immutable membership contract for native probes, published once perfect-hash finalization succeeds.
	ExecutionPerfectHashJoinMembershipLayout membership_layout;
	//! Stores the number of unique keys in the build side
	atomic<idx_t> unique_keys {0};
	//! Set by the fill task that finds a key whose slot is already claimed
	atomic<bool> has_duplicate_key {false};
};

} // namespace duckdb
