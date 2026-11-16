//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/row_operations/row_matcher.hpp"
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include "duckdb/execution/base_aggregate_hashtable.hpp"
#include "duckdb/execution/aggregate_ht_adaptivity_state.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/clustered_aggregate.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

class BlockHandle;
class GroupedAggregateHashTable;
struct ExecutionOperatorStageRecorder;
struct ExecutionPrimitiveAggregateUpdateLane;

struct FlushMoveState;

struct GroupedAggregateProvenUniqueRange {
	//! A conservative interval covering observed integral keys. The bounded local summary keeps exact dense
	//! runs until it reaches its interval budget, then coalesces them into a safe hull.
	PhysicalType key_type = PhysicalType::INVALID;
	int64_t first_signed_key = 0;
	int64_t last_signed_key = 0;
	uint64_t first_unsigned_key = 0;
	uint64_t last_unsigned_key = 0;
};

//! GroupedAggregateHashTable is a linear probing HT that is used for computing
//! aggregates
/*!
    GroupedAggregateHashTable is a HT that is used for computing aggregates. It takes
   as input the set of groups and the types of the aggregates to compute and
   stores them in the HT. It uses linear probing for collision resolution.
*/
struct AggregateHTScanState {
public:
	AggregateHTScanState() {
	}

	idx_t partition_idx = 0;
	TupleDataScanState scan_states;
};

//! Scratch shared by mutable append probes and task-local read-only lookups.
struct AggregateHTProbeState {
public:
	AggregateHTProbeState();

	Vector hashes;
	Vector ht_offsets;
	Vector hash_salts;
	Vector addresses;
	SelectionVector group_compare_vector;
	SelectionVector no_match_vector;
	DataChunk group_chunk;
	optional_ptr<const GroupedAggregateHashTable> owner;
	bool initialized = false;
};

//! Task-local scratch state for read-only group lookups.
struct AggregateHTLookupState : public AggregateHTProbeState {
public:
	AggregateHTLookupState();

	SelectionVector missing_vector;
	TupleDataChunkState chunk_state;
	RowMatcher row_matcher;
	vector<TupleDataGatherFunction> gather_functions;
};

class GroupedAggregateHashTable : public BaseAggregateHashTable {
public:
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types,
	                          vector<LogicalType> payload_types, const vector<BoundAggregateExpression *> &aggregates,
	                          idx_t initial_capacity = InitialCapacity(), idx_t radix_bits = 0,
	                          TupleDataValidityType group_validity = TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types,
	                          vector<LogicalType> payload_types, vector<AggregateObject> aggregates,
	                          idx_t initial_capacity = InitialCapacity(), idx_t radix_bits = 0,
	                          TupleDataValidityType group_validity = TupleDataValidityType::CAN_HAVE_NULL_VALUES,
	                          idx_t trailing_region_width = 0);
	GroupedAggregateHashTable(ClientContext &context, Allocator &allocator, vector<LogicalType> group_types,
	                          TupleDataValidityType group_validity = TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	~GroupedAggregateHashTable() override;

public:
	//! The hash table load factor, when a resize is triggered
	constexpr static double LOAD_FACTOR = 1.5;

	//! Get the layout of this HT
	shared_ptr<TupleDataLayout> GetLayoutPtr();
	const TupleDataLayout &GetLayout() const;
	//! Number of groups in the HT
	idx_t Count() const;
	//! Initial capacity of the HT
	static idx_t InitialCapacity();
	//! Capacity that can hold 'count' entries without resizing
	static idx_t GetCapacityForCount(idx_t count);
	//! Current capacity of the HT
	idx_t Capacity() const;
	//! Threshold at which to resize the HT
	idx_t ResizeThreshold() const;
	static idx_t ResizeThreshold(idx_t capacity);

	//! Add the given data to the HT, computing the aggregates grouped by the
	//! data in the group chunk. When resize = true, aggregates will not be
	//! computed but instead just assigned.
	idx_t AddChunk(DataChunk &groups, DataChunk &payload, const unsafe_vector<idx_t> &filter);
	idx_t AddChunk(DataChunk &groups, Vector &group_hashes, DataChunk &payload, const unsafe_vector<idx_t> &filter);
	idx_t AddChunk(DataChunk &groups, DataChunk &payload, AggregateType filter);
	using before_update_callback_t =
	    std::function<void(const Vector &group_addresses, const SelectionVector &new_groups, idx_t new_group_count)>;
	//! Adds a chunk and invokes a callback with stable row-start addresses before updating the aggregate states.
	idx_t AddChunk(DataChunk &groups, DataChunk &payload, AggregateType filter,
	               const before_update_callback_t &before_update);
	//! Adds a chunk and returns the stable row-start addresses and input indexes of newly created groups.
	idx_t AddChunkAndGetNewGroups(DataChunk &groups, DataChunk &payload, AggregateType filter,
	                              Vector &new_group_addresses, SelectionVector &new_groups_out);
	optional_idx TryAddCompressedGroups(DataChunk &groups, DataChunk &payload, const unsafe_vector<idx_t> &filter);
	//! Resolves the rows of constant or dictionary groups, offset by address_offset, from one lookup per distinct
	//! value; an invalid result leaves the groups to the regular lookup
	optional_idx TryResolveCompressedGroups(DataChunk &groups, Vector &addresses_out, idx_t address_offset);
	optional_idx TryResolveDictionaryGroups(DataChunk &groups, Vector &addresses_out, idx_t address_offset);
	optional_idx TryResolveConstantGroups(DataChunk &groups, Vector &addresses_out, idx_t address_offset);
	//! Finds or creates the rows of the groups for a caller that updates their states itself: the input counts toward
	//! the table's adaptivity as AddChunk's does, and compressed groups take AddChunk's per-distinct-value lookup.
	//! group_hashes, when given, are the groups' hashes
	void FindOrCreateGroupRows(DataChunk &groups, Vector &addresses_out, optional_ptr<Vector> group_hashes = nullptr);

	//! Fetch the aggregates for specific groups from the HT and place them in the result
	void FetchAggregates(DataChunk &groups, DataChunk &result);

	void InitializeScan(AggregateHTScanState &scan_state);
	//! Scans group columns without reading or finalizing aggregate states.
	bool ScanGroups(AggregateHTScanState &scan_state, DataChunk &distinct_rows);
	bool Scan(AggregateHTScanState &scan_state, DataChunk &distinct_rows, DataChunk &payload_rows);

	//! Finds or creates groups in the hashtable using the specified group keys. The addresses vector will be filled
	//! with pointers to the groups in the hash table, and the new_groups selection vector will point to the newly
	//! created groups. The return value is the amount of newly created groups.
	idx_t FindOrCreateGroups(DataChunk &groups, Vector &group_hashes, Vector &addresses_out,
	                         SelectionVector &new_groups_out,
	                         optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr);
	idx_t FindOrCreateGroups(DataChunk &groups, Vector &addresses_out, SelectionVector &new_groups_out);
	void FindOrCreateGroups(DataChunk &groups, Vector &addresses_out);
	//! Finds existing groups without changing the hash table. Returns the number of matches and writes input-row
	//! indexes to found_groups_out. Matching row addresses are stored at their input-row indexes in state.addresses.
	idx_t LookupGroups(DataChunk &groups, AggregateHTLookupState &state, SelectionVector &found_groups_out) const;
	//! Gathers matched group values from state.addresses in the order specified by found_groups.
	void GatherGroups(AggregateHTLookupState &state, const SelectionVector &found_groups, idx_t found_count,
	                  DataChunk &result) const;
	//! Gathers group values from arbitrary row-start addresses.
	void GatherGroups(AggregateHTLookupState &state, Vector &addresses, const SelectionVector &groups,
	                  idx_t group_count, DataChunk &result) const;
	bool TryFindOrCreateRowPointerGroupStateTargets(DataChunk &payload_input, Vector &row_pointers, idx_t count,
	                                                const vector<ExecutionRowPointerGroupKeySource> &group_sources,
	                                                ExecutionGroupedAggregateStateTargetBatch &targets,
	                                                optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr);
	bool TryFindOrCreateInputVectorGroupStateTargets(DataChunk &payload_input, idx_t count,
	                                                 const vector<ExecutionRowPointerGroupKeySource> &group_sources,
	                                                 ExecutionGroupedAggregateStateTargetBatch &targets,
	                                                 optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr);
	bool TryAppendNewGroupsWithStateAddressesFast(DataChunk &groups,
	                                              ExecutionGroupedAggregateStateAddressUpdateFunction update_function,
	                                              void *update_state,
	                                              optional_ptr<ExecutionOperatorStageRecorder> recorder = nullptr,
	                                              optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr);

	const PartitionedTupleData &GetPartitionedData() const;
	//! Retained tuple blocks, allocator metadata, aggregate arena, and pointer table
	idx_t GetSizeInBytes() const;
	unique_ptr<PartitionedTupleData> AcquirePartitionedData();
	//! Transfers append-only rows without radix repartitioning. This succeeds only while all materialized rows are in
	//! the deferred unpartitioned collection owned by a still-valid uniqueness proof.
	unique_ptr<PartitionedTupleData> TryAcquireProvenUniqueAppendData();
	void Abandon();
	void Repartition();
	shared_ptr<ArenaAllocator> GetAggregateAllocator();

	//! Resize an empty HT, or grow an HT with no abandoned groups.
	void Resize(idx_t size);
	//! Resize the HT so the requested total group count and one append vector fit the load-factor threshold.
	bool ReserveGroups(idx_t group_count);
	//! Resize the HT so the current count plus the incoming groups fits the load-factor threshold.
	void ResizeForAdditionalGroups(idx_t additional_count);
	//! Resets the pointer table of the HT to all 0's
	void ClearPointerTable();
	//! Set the radix bits for this HT
	void SetRadixBits(idx_t radix_bits);
	//! Get the radix bits for this HT
	idx_t GetRadixBits() const;
	//! Record state-address updates that bypass group lookup but still consume sink input
	void RecordSinkCount(idx_t count);
	//! Get the total number of tuples materialized currently in this HT
	idx_t GetMaterializedCount() const;
	//! Read-only adaptivity observations; mutations follow the HT's lifecycle.
	const AggregateHTAdaptivityState &GetAdaptivityState() const {
		return adaptivity;
	}
	//! Skip lookups until resuming at an empty pointer table. Statistical admission requires final combination; a
	//! proven-unique stream may defer that requirement until its proof fails.
	void SkipLookups(bool require_final_combine = true);
	//! Resume lookups after abandoning the previous pointer table.
	void ResumeLookups();
	//! Permanently require final combination for the current append-only stream.
	void RequireFinalCombine();
	//! Continue a producer-proven fixed-width strictly increasing stream. This HT validates the published endpoints
	//! and owns conservative key bounds so the proof survives backend/runtime and local pointer-table boundaries.
	bool TryContinueProvenUniqueAppend(DataChunk &groups, ExecutionGroupedAggregateAppendProof append_proof = {});
	//! Whether append-only rows require final duplicate reconciliation.
	bool LookupsSkippedRequireFinalCombine() const;
	//! Return bounded local covering intervals when the append-only uniqueness proof is still intact.
	optional_ptr<const vector<GroupedAggregateProvenUniqueRange>> GetProvenUniqueAppendRanges() const;
	//! Enable/disable HLL; a fresh sketch must start before inserting groups
	void EnableHLL(bool enable);

	//! Executes the filter(if any) and update the aggregates
	void Combine(GroupedAggregateHashTable &other);
	//! Combines the data into this HT - combined_chunks (if set) is incremented for every combined chunk
	void Combine(TupleDataCollection &other_data, optional_ptr<atomic<idx_t>> combined_chunks = nullptr);
	//! Reset the HT for a new execution while reusing internal allocations where possible
	void ResetForNewIteration(idx_t radix_bits);

private:
	ClientContext &context;
	//! Zeroes the layout's trailing region of freshly created rows so owners find defined bytes
	void ZeroTrailingRegion(Vector &row_locations, idx_t count) const;
	//! Efficiently matches groups
	RowMatcher row_matcher;

	struct AggregateCompressedGroupState {
		AggregateCompressedGroupState();

		//! The current dictionary vector id (if any)
		string dictionary_id;
		DataChunk unique_values;
		Vector hashes;
		Vector unique_group_pointers;
		SelectionVector unique_entries;
		unique_ptr<Vector> dictionary_addresses;
		unsafe_unique_array<bool> found_entry;
		idx_t capacity = 0;
		//! Cumulative count of dict slots added to found_entry under the current id; the
		//! unique-entries walk is skipped once it reaches dict_size.
		idx_t resolved_count = 0;
		//! For the clustered-aggregate fast path: track whether every state pointer added to this
		//! dictionary shares the same high (64 - SLOT_GID_BITS) bits. The clustered path stores only
		//! the low SLOT_GID_BITS of each pointer, so if any two pointers differ in their high bits
		//! we cannot reuse the truncated form to disambiguate them and must fall back to scatter.
		//! address_high_bits starts at the sentinel ~0 ("no address seen yet"); a real high-bits
		//! value always has its low SLOT_GID_BITS cleared so ~0 cannot collide with a valid value.
		uint64_t address_high_bits = ~uint64_t(0);
		bool address_high_bits_uniform = true;
	};

	//! If we have this many or more radix bits, we use the unpartitioned data collection too
	static constexpr idx_t UNPARTITIONED_RADIX_BITS_THRESHOLD = 3;
	//! The number of radix bits to partition by
	idx_t radix_bits;
	//! The data of the HT
	unique_ptr<PartitionedTupleData> partitioned_data;
	unique_ptr<PartitionedTupleData> unpartitioned_data;
	//! Cache tuple allocations while the materialized groups and their storage stay unchanged.
	struct TupleSizeCache {
		optional_idx count;
		idx_t size = 0;
	};
	mutable TupleSizeCache tuple_size_cache;

	//! Predicates for matching groups (always ExpressionType::COMPARE_EQUAL)
	vector<ExpressionType> predicates;

	//! The number of groups in the HT
	idx_t count;
	//! The capacity of the HT. This can be increased using GroupedAggregateHashTable::Resize
	idx_t capacity;
	//! The hash map (pointer table) of the HT: allocated data and pointer into it
	HugeAllocation hash_map;
	ht_entry_t *entries;
	//! Offset of the hash column in the rows
	idx_t hash_offset;
	//! Bitmask for getting relevant bits from the hashes to determine the position
	hash_t bitmask;

	AggregateHTAdaptivityState adaptivity;

	//! The active arena allocator used by the aggregates for their internal state
	shared_ptr<ArenaAllocator> aggregate_allocator;
	//! Owning arena allocators that this HT has data from
	vector<shared_ptr<ArenaAllocator>> stored_allocators;

	//! Append state
	struct AggregateHTAppendState : public AggregateHTProbeState {
		explicit AggregateHTAppendState(ArenaAllocator &allocator);

		PartitionedTupleDataAppendState partitioned_append_state;
		PartitionedTupleDataAppendState unpartitioned_append_state;

		SelectionVector new_groups;
		SelectionVector existing_groups;
		Vector descriptor_group_hashes;
		DataChunk descriptor_group_chunk;
		DataChunk input_vector_group_chunk;
		AggregateCompressedGroupState compressed_group_state;

		RowOperationsState row_state;
	} state;

	struct AggregateDenseSingleFieldTargetCache {
		void Reset();
		void Disable();
		bool Configure(PhysicalType physical_type_p, idx_t layout_offset_p);
		bool EnsureRange(idx_t min_key, idx_t max_key, bool exact_size = false);
		bool KeyInRange(idx_t key) const;
		idx_t KeyOffset(idx_t key) const;
		uintptr_t GetAddress(idx_t key) const;
		idx_t GetPendingNewGroup(idx_t key) const;
		void SetAddress(idx_t key, uintptr_t address);
		void SetPendingNewGroup(idx_t key, idx_t group_idx);

		PhysicalType physical_type = PhysicalType::INVALID;
		idx_t layout_offset = DConstants::INVALID_INDEX;
		idx_t base_key = 0;
		bool disabled = false;
		vector<uintptr_t> addresses;
	};

	AggregateDenseSingleFieldTargetCache dense_single_field_target_cache;

	struct AggregateRowPointerDescriptorTargetCache {
		static constexpr idx_t CAPACITY = 16384;

		void Reset() {
			keys.clear();
			addresses.clear();
		}

		void Ensure() {
			if (!keys.empty()) {
				return;
			}
			keys.assign(CAPACITY, nullptr);
			addresses.assign(CAPACITY, 0);
		}

		idx_t Slot(data_ptr_t key) const {
			auto value = reinterpret_cast<uintptr_t>(key) >> 4;
			value ^= value >> 17;
			value *= 0x9E3779B97F4A7C15ULL;
			return value & (CAPACITY - 1);
		}

		uintptr_t Get(data_ptr_t key) const {
			if (keys.empty()) {
				return 0;
			}
			const auto slot = Slot(key);
			return keys[slot] == key ? addresses[slot] : 0;
		}

		void Set(data_ptr_t key, uintptr_t address) {
			D_ASSERT(key);
			D_ASSERT(address);
			Ensure();
			const auto slot = Slot(key);
			keys[slot] = key;
			addresses[slot] = address;
		}

		vector<data_ptr_t> keys;
		vector<uintptr_t> addresses;
	};

	AggregateRowPointerDescriptorTargetCache row_pointer_descriptor_target_cache;

	ClusteredAggrState clustered_state;

	//! Cold append-only proof state is kept after the existing hot hash-table members so adding the
	//! contract does not shift descriptor lookup, aggregate update, or cache state.
	bool skip_lookups_require_final_combine;
	//! Whether abandoned append epochs force final duplicate reconciliation even after
	//! lookups resume (set when a lookup epoch follows adaptive appends).
	bool epochs_require_final_combine = false;
	PhysicalType proven_unique_append_key_type;
	bool proven_unique_append_has_last_key;
	int64_t proven_unique_append_last_signed_key;
	uint64_t proven_unique_append_last_unsigned_key;
	vector<GroupedAggregateProvenUniqueRange> proven_unique_append_ranges;
	bool proven_unique_append_ranges_coalesced;

private:
	struct AggregateHTAppendTarget {
		PartitionedTupleData &data;
		PartitionedTupleDataAppendState &state;
	};

	//! Disabled the copy constructor
	GroupedAggregateHashTable(const GroupedAggregateHashTable &) = delete;
	//! Destroy the HT
	void Destroy();
	void DestroyAggregateData(PartitionedTupleData &data, PartitionedTupleDataAppendState &append_state);
	//! Initializes the PartitionedTupleData
	void InitializePartitionedData();
	//! Initializes the PartitionedTupleData that only has 1 partition
	void InitializeUnpartitionedData();
	//! Selects and prepares the canonical row-storage target. Append-only ownership can defer radix partitioning.
	AggregateHTAppendTarget PrepareAppendTarget(DataChunk &groups, idx_t group_count, bool defer_partitioning = false);
	//! Apply bitmask to get the entry in the HT
	inline idx_t ApplyBitMask(hash_t hash) const;
	//! Feed the hashes (in state.hashes) of the first new_group_count groups in state.new_groups to the HLL
	void ObserveNewGroupHashes(idx_t new_group_count);
	//! Reinserts tuples (triggered by Resize)
	void ReinsertTuples(PartitionedTupleData &data);

	void UpdateAggregates(DataChunk &payload, const unsafe_vector<idx_t> &filter, idx_t count,
	                      bool ht_offsets_valid = true);
	bool UpdateAggregatesClustered(DataChunk &payload, const unsafe_vector<idx_t> &filter, idx_t count,
	                               bool ht_offsets_valid);

	//! Does the actual group matching / creation
	idx_t AppendGroupsSkippingLookups(DataChunk &groups, Vector &group_hashes, Vector &addresses,
	                                  optional_ptr<ExecutionOperatorStageRecorder> recorder,
	                                  optional_ptr<TupleDataChunkState> source_rows);
	idx_t FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes, Vector &addresses,
	                                 SelectionVector &new_groups, optional_ptr<ExecutionOperatorStageRecorder> recorder,
	                                 optional_ptr<TupleDataChunkState> source_rows = nullptr);
	bool TryAppendNewGroupsFastInternal(DataChunk &groups, optional_ptr<Vector> addresses_out,
	                                    ExecutionGroupedAggregateStateAddressUpdateFunction address_update_function,
	                                    void *address_update_state,
	                                    optional_ptr<ExecutionOperatorStageRecorder> recorder,
	                                    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr);
	void InitializeLookupState(AggregateHTLookupState &lookup_state) const;

	//! Verify the pointer table of the HT
	void Verify();
};

} // namespace duckdb
