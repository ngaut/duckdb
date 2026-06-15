//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/perfect_aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/base_aggregate_hashtable.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/common/clustered_aggregate.hpp"

namespace duckdb {

struct PerfectAggregateHashTableStateLayout {
	data_ptr_t data = nullptr;
	bool *group_is_set = nullptr;
	idx_t total_groups = 0;
	idx_t tuple_size = 0;
	idx_t aggregate_state_offset = 0;
};

class PerfectAggregateHashTable : public BaseAggregateHashTable {
public:
	PerfectAggregateHashTable(ClientContext &context, Allocator &allocator, const vector<LogicalType> &group_types,
	                          vector<LogicalType> payload_types_p, vector<AggregateObject> aggregate_objects,
	                          vector<Value> group_minima, vector<idx_t> required_bits);
	~PerfectAggregateHashTable() override;

public:
	//! Get the layout of this perfect aggregate HT
	const TupleDataLayout &GetLayout() const;

	//! Add the given data to the HT
	void AddChunk(DataChunk &groups, DataChunk &payload);

	//! Finds aggregate states for the specified group keys. The addresses vector will be filled with aggregate-state
	//! base pointers, ready for native grouped aggregate updates.
	idx_t FindOrCreateAggregateStates(DataChunk &groups, Vector &addresses_out);

	//! Expose the stable state-address layout for generated perfect-hash aggregate updates.
	PerfectAggregateHashTableStateLayout GetStateLayout();

	//! Combines the target perfect aggregate HT into this one
	void Combine(PerfectAggregateHashTable &other);

	//! Scan the HT starting from the scan_position
	void Scan(idx_t &scan_position, DataChunk &result);

protected:
	Vector addresses;
	//! The required bits per group
	vector<idx_t> required_bits;
	//! The total required bits for the HT (this determines the max capacity)
	idx_t total_required_bits;
	//! The total amount of groups
	idx_t total_groups;
	//! The tuple size
	idx_t tuple_size;
	//! The number of grouping columns
	idx_t grouping_columns;

	// The actual pointer to the data
	data_ptr_t data;
	//! The owned data of the HT
	unsafe_unique_array<data_t> owned_data;
	//! Information on whether or not a specific group has any entries
	unsafe_unique_array<bool> group_is_set;

	//! The minimum values for each of the group columns
	vector<Value> group_minima;

	//! Reused selection vector
	SelectionVector sel;

	//! The active arena allocator used by the aggregates for their internal state
	unique_ptr<ArenaAllocator> aggregate_allocator;
	//! Owning arena allocators that this HT has data from
	vector<unique_ptr<ArenaAllocator>> stored_allocators;

	ClusteredAggrState clustered_state;

private:
	//! Compute raw perfect-hash group ids into addresses_out
	uintptr_t *ComputeGroupLocationIds(DataChunk &groups, Vector &addresses_out);
	//! Validate raw group ids, mark occupied groups, and rewrite them to state addresses
	idx_t ResolveGroupStateAddresses(uintptr_t *address_data, idx_t count, idx_t state_offset);
	//! Try adding a chunk using the clustered aggregation path. Returns false if not applicable.
	bool AddChunkClustered(uintptr_t *address_data, DataChunk &payload);
	//! Destroy the perfect aggregate HT (called automatically by the destructor)
	void Destroy();
};

} // namespace duckdb
