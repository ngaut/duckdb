#include "duckdb/execution/aggregate_hashtable.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/bswap.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

#include <array>
#include <chrono>
#include <cstring>

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

static constexpr idx_t AGGREGATE_MAX_FAST_GROUPS = 8;

static std::chrono::steady_clock::time_point
AggregateTraceStart(optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	return recorder ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
}

static void RecordAggregateTraceStage(optional_ptr<ExecutionOperatorStageRecorder> recorder, const string &stage,
                                      std::chrono::steady_clock::time_point start) {
	if (!recorder) {
		return;
	}
	auto end = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
	recorder->RecordStageRuntime(stage, elapsed);
}

static bool AggregateFastHashVectorIsValid(Vector &hashes, idx_t count) {
	return hashes.GetVectorType() == VectorType::FLAT_VECTOR &&
	       hashes.GetType().InternalType() == PhysicalType::UINT64 && hashes.size() == count;
}

static bool AggregateFastExistingMatchType(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
	case PhysicalType::INT8:
	case PhysicalType::UINT16:
	case PhysicalType::INT16:
	case PhysicalType::UINT32:
	case PhysicalType::INT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT64:
	case PhysicalType::UINT128:
	case PhysicalType::INT128:
	case PhysicalType::INTERVAL:
	case PhysicalType::VARCHAR:
		return true;
	default:
		return false;
	}
}

static bool AggregateFastAppendNewGroupType(PhysicalType type) {
	return AggregateFastExistingMatchType(type);
}

static bool AggregateFastAppendNewGroupsSupported(DataChunk &groups) {
	for (idx_t group_idx = 0; group_idx < groups.ColumnCount(); group_idx++) {
		auto &group = groups.data[group_idx];
		if (!AggregateFastAppendNewGroupType(group.GetType().InternalType()) ||
		    group.GetVectorType() != VectorType::FLAT_VECTOR ||
		    !FlatVector::Validity(group).CheckAllValid(groups.size())) {
			return false;
		}
	}
	return true;
}

static bool AggregateFastExistingValueMatches(const_data_ptr_t source_data, const data_ptr_t row_data,
                                              PhysicalType type, idx_t source_idx, idx_t layout_offset,
                                              idx_t value_size) {
	if (type == PhysicalType::VARCHAR) {
		const auto source_values = reinterpret_cast<const string_t *>(source_data);
		const auto row_value = Load<string_t>(row_data + layout_offset);
		return source_values[source_idx] == row_value;
	}
	return memcmp(source_data + source_idx * value_size, row_data + layout_offset, value_size) == 0;
}

static bool AggregateFastSourceValuesMatch(const_data_ptr_t source_data, PhysicalType type, idx_t left_idx,
                                           idx_t right_idx, idx_t value_size) {
	if (type == PhysicalType::VARCHAR) {
		const auto source_values = reinterpret_cast<const string_t *>(source_data);
		return source_values[left_idx] == source_values[right_idx];
	}
	return memcmp(source_data + left_idx * value_size, source_data + right_idx * value_size, value_size) == 0;
}

struct AggregateFastGroupSourceInfo {
	idx_t group_count = 0;
	std::array<const_data_ptr_t, AGGREGATE_MAX_FAST_GROUPS> source_data;
	std::array<PhysicalType, AGGREGATE_MAX_FAST_GROUPS> physical_types;
	std::array<idx_t, AGGREGATE_MAX_FAST_GROUPS> value_sizes;
};

static bool AggregatePrepareFastGroupSourceInfo(DataChunk &groups, const vector<LogicalType> &layout_types,
                                                idx_t chunk_size, AggregateFastGroupSourceInfo &sources) {
	const auto group_count = groups.ColumnCount();
	if (group_count == 0 || group_count > AGGREGATE_MAX_FAST_GROUPS || group_count + 1 != layout_types.size()) {
		return false;
	}
	sources.group_count = group_count;
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		auto &source = groups.data[group_idx];
		if (source.GetVectorType() != VectorType::FLAT_VECTOR) {
			return false;
		}
		if (!FlatVector::Validity(source).CheckAllValid(chunk_size)) {
			return false;
		}
		auto physical_type = layout_types[group_idx].InternalType();
		if (source.GetType().InternalType() != physical_type || !AggregateFastExistingMatchType(physical_type)) {
			return false;
		}
		sources.source_data[group_idx] = FlatVector::GetData(source);
		sources.physical_types[group_idx] = physical_type;
		sources.value_sizes[group_idx] =
		    physical_type == PhysicalType::VARCHAR ? sizeof(string_t) : GetTypeIdSize(physical_type);
	}
	return true;
}

static bool AggregateFastGroupSourceRowsMatch(const AggregateFastGroupSourceInfo &sources, idx_t row_idx,
                                              idx_t other_row_idx) {
	for (idx_t group_idx = 0; group_idx < sources.group_count; group_idx++) {
		if (!AggregateFastSourceValuesMatch(sources.source_data[group_idx], sources.physical_types[group_idx], row_idx,
		                                    other_row_idx, sources.value_sizes[group_idx])) {
			return false;
		}
	}
	return true;
}

static bool AggregateFastExistingRowMatches(const AggregateFastGroupSourceInfo &sources,
                                            const vector<idx_t> &layout_offsets, const data_ptr_t row_location,
                                            idx_t row_idx) {
	for (idx_t group_idx = 0; group_idx < sources.group_count; group_idx++) {
		if (!AggregateFastExistingValueMatches(sources.source_data[group_idx], row_location,
		                                       sources.physical_types[group_idx], row_idx, layout_offsets[group_idx],
		                                       sources.value_sizes[group_idx])) {
			return false;
		}
	}
	return true;
}

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types, vector<LogicalType> payload_types,
                                                     const vector<BoundAggregateExpression *> &bindings,
                                                     idx_t initial_capacity, idx_t radix_bits,
                                                     TupleDataValidityType group_validity)
    : GroupedAggregateHashTable(context, allocator, std::move(group_types), std::move(payload_types),
                                AggregateObject::CreateAggregateObjects(bindings), initial_capacity, radix_bits,
                                group_validity) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types,
                                                     TupleDataValidityType group_validity)
    : GroupedAggregateHashTable(context, allocator, std::move(group_types), {}, vector<AggregateObject>(),
                                InitialCapacity(), 0, group_validity) {
}

GroupedAggregateHashTable::AggregateHTAppendState::AggregateHTAppendState(ArenaAllocator &allocator)
    : hashes(LogicalType::HASH), ht_offsets(LogicalType::UBIGINT), hash_salts(LogicalType::HASH),
      new_groups(STANDARD_VECTOR_SIZE), group_compare_vector(STANDARD_VECTOR_SIZE),
      no_match_vector(STANDARD_VECTOR_SIZE), existing_groups(STANDARD_VECTOR_SIZE), addresses(LogicalType::POINTER),
      row_state(allocator) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context_p, Allocator &allocator,
                                                     vector<LogicalType> group_types_p,
                                                     vector<LogicalType> payload_types_p,
                                                     vector<AggregateObject> aggregate_objects_p,
                                                     idx_t initial_capacity, idx_t radix_bits,
                                                     TupleDataValidityType group_validity)
    : BaseAggregateHashTable(context_p, allocator, aggregate_objects_p, std::move(payload_types_p)), context(context_p),
      radix_bits(radix_bits), count(0), capacity(0), sink_count(0), skip_lookups(false), enable_hll(false),
      aggregate_allocator(make_shared_ptr<ArenaAllocator>(allocator)), state(*aggregate_allocator) {
	clustered_state.all_clustered = AllAggregatesClustered(aggregate_objects_p);
	clustered_state.n_clustered = CountAggregatesClustered(aggregate_objects_p);
	if (clustered_state.n_clustered > 1) {
		clustered_state.Initialize();
	}

	// Append hash column to the end and initialise the row layout
	group_types_p.emplace_back(LogicalType::HASH);

	auto layout = make_shared_ptr<TupleDataLayout>();
	layout->Initialize(std::move(group_types_p), std::move(aggregate_objects_p), group_validity);
	layout_ptr = std::move(layout);

	hash_offset = layout_ptr->GetOffsets()[layout_ptr->ColumnCount() - 1];

	// Partitioned data and pointer table
	InitializePartitionedData();
	if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD) {
		InitializeUnpartitionedData();
	}
	Resize(initial_capacity);

	// Predicates
	const auto expr_type =
	    layout_ptr->CannotHaveNull() ? ExpressionType::COMPARE_EQUAL : ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	predicates.resize(layout_ptr->ColumnCount() - 1, expr_type);
	row_matcher.Initialize(true, *layout_ptr, predicates);

	state.partitioned_append_state.compute_reverse_partition_sel = true;
	state.unpartitioned_append_state.compute_reverse_partition_sel = true;
}

void GroupedAggregateHashTable::InitializePartitionedData() {
	if (!partitioned_data ||
	    RadixPartitioning::RadixBitsOfPowerOfTwo(partitioned_data->PartitionCount()) != radix_bits) {
		D_ASSERT(!partitioned_data || partitioned_data->Count() == 0);
		partitioned_data = make_uniq<RadixPartitionedTupleData>(buffer_manager, layout_ptr, MemoryTag::HASH_TABLE,
		                                                        radix_bits, layout_ptr->ColumnCount() - 1);
	} else {
		partitioned_data->Reset();
	}

	D_ASSERT(GetLayout().GetAggrWidth() == layout_ptr->GetAggrWidth());
	D_ASSERT(GetLayout().GetDataWidth() == layout_ptr->GetDataWidth());
	D_ASSERT(GetLayout().GetRowWidth() == layout_ptr->GetRowWidth());

	partitioned_data->InitializeAppendState(state.partitioned_append_state,
	                                        TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
}

void GroupedAggregateHashTable::InitializeUnpartitionedData() {
	D_ASSERT(radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD);
	if (!unpartitioned_data) {
		unpartitioned_data = make_uniq<RadixPartitionedTupleData>(buffer_manager, layout_ptr, MemoryTag::HASH_TABLE,
		                                                          0ULL, layout_ptr->ColumnCount() - 1);
	} else {
		unpartitioned_data->Reset();
	}
	unpartitioned_data->InitializeAppendState(state.unpartitioned_append_state,
	                                          TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
}

const PartitionedTupleData &GroupedAggregateHashTable::GetPartitionedData() const {
	return *partitioned_data;
}

unique_ptr<PartitionedTupleData> GroupedAggregateHashTable::AcquirePartitionedData() {
	if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD) {
		// Flush/unpin unpartitioned data and append to partitioned data
		if (unpartitioned_data) {
			unpartitioned_data->FlushAppendState(state.unpartitioned_append_state);
			unpartitioned_data->Unpin();
			unpartitioned_data->Repartition(context, *partitioned_data);
		}
		InitializeUnpartitionedData();
	}

	// Flush/unpin partitioned data
	partitioned_data->FlushAppendState(state.partitioned_append_state);
	partitioned_data->Unpin();

	// Return and re-initialize
	auto result = std::move(partitioned_data);
	InitializePartitionedData();
	return result;
}

void GroupedAggregateHashTable::Abandon() {
	if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD) {
		// Flush/unpin unpartitioned data and append to partitioned data
		if (unpartitioned_data) {
			unpartitioned_data->FlushAppendState(state.unpartitioned_append_state);
			unpartitioned_data->Unpin();
			unpartitioned_data->Repartition(context, *partitioned_data);
		}
		InitializeUnpartitionedData();
	}

	// Start over
	ClearPointerTable();
	count = 0;

	// Resetting the id ensures the dict state is reset properly when needed
	state.compressed_group_state.dictionary_id = string();
}

void GroupedAggregateHashTable::Repartition() {
	auto old = AcquirePartitionedData();
	D_ASSERT(old->GetPartitions().size() != partitioned_data->GetPartitions().size());
	old->Repartition(context, *partitioned_data);
}

shared_ptr<ArenaAllocator> GroupedAggregateHashTable::GetAggregateAllocator() {
	return aggregate_allocator;
}

GroupedAggregateHashTable::~GroupedAggregateHashTable() {
	Destroy();
}

void GroupedAggregateHashTable::Destroy() {
	if (!layout_ptr->HasDestructor()) {
		return;
	}

	// There are aggregates with destructors
	if (unpartitioned_data) {
		DestroyAggregateData(*unpartitioned_data, state.unpartitioned_append_state);
	}
	if (partitioned_data) {
		DestroyAggregateData(*partitioned_data, state.partitioned_append_state);
	}
}

void GroupedAggregateHashTable::DestroyAggregateData(PartitionedTupleData &data,
                                                     PartitionedTupleDataAppendState &append_state) {
	// Call the destructor for each of the aggregates
	data.FlushAppendState(append_state);
	for (auto &data_collection : data.GetPartitions()) {
		if (data_collection->Count() == 0) {
			continue;
		}
		TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::DESTROY_AFTER_DONE, false);
		auto &row_locations = iterator.GetChunkState().row_locations;
		do {
			RowOperations::DestroyStates(state.row_state, *layout_ptr, row_locations);
		} while (iterator.Next());
		data_collection->Reset();
	}
}

shared_ptr<TupleDataLayout> GroupedAggregateHashTable::GetLayoutPtr() {
	return partitioned_data->GetLayoutPtr();
}

const TupleDataLayout &GroupedAggregateHashTable::GetLayout() const {
	return partitioned_data->GetLayout();
}

idx_t GroupedAggregateHashTable::Count() const {
	return count;
}

idx_t GroupedAggregateHashTable::InitialCapacity() {
	return STANDARD_VECTOR_SIZE * 2ULL;
}

idx_t GroupedAggregateHashTable::GetCapacityForCount(idx_t count) {
	count = MaxValue<idx_t>(InitialCapacity(), count);
	return NextPowerOfTwo(LossyNumericCast<uint64_t>(static_cast<double>(count) * LOAD_FACTOR));
}

idx_t GroupedAggregateHashTable::Capacity() const {
	return capacity;
}

idx_t GroupedAggregateHashTable::ResizeThreshold() const {
	return ResizeThreshold(Capacity());
}

idx_t GroupedAggregateHashTable::ResizeThreshold(const idx_t capacity) {
	return LossyNumericCast<idx_t>(static_cast<double>(capacity) / LOAD_FACTOR);
}

idx_t GroupedAggregateHashTable::ApplyBitMask(hash_t hash) const {
	return hash & bitmask;
}

void GroupedAggregateHashTable::Verify() {
#ifdef DEBUG
	if (skip_lookups) {
		return;
	}
	idx_t total_count = 0;
	for (idx_t i = 0; i < capacity; i++) {
		const auto &entry = entries[i];
		if (!entry.IsOccupied()) {
			continue;
		}
		auto hash = Load<hash_t>(entry.GetPointer() + hash_offset);
		D_ASSERT(entry.GetSalt() == ht_entry_t::ExtractSalt(hash));
		total_count++;
	}
	D_ASSERT(total_count == Count());
#endif
}

void GroupedAggregateHashTable::ClearPointerTable() {
	std::fill_n(entries, capacity, ht_entry_t());
}

void GroupedAggregateHashTable::SetRadixBits(idx_t radix_bits_p) {
	radix_bits = radix_bits_p;
}

idx_t GroupedAggregateHashTable::GetRadixBits() const {
	return radix_bits;
}

idx_t GroupedAggregateHashTable::GetSinkCount() const {
	return sink_count;
}

idx_t GroupedAggregateHashTable::GetMaterializedCount() const {
	auto result = partitioned_data->Count();
	if (unpartitioned_data) {
		result += unpartitioned_data->Count();
	}
	return result;
}

void GroupedAggregateHashTable::SkipLookups() {
	skip_lookups = true;
}

void GroupedAggregateHashTable::EnableHLL(bool enable) {
	enable_hll = enable;
}

bool GroupedAggregateHashTable::HLLEnabled() const {
	return enable_hll;
}

idx_t GroupedAggregateHashTable::GetHLLUpperBound() const {
	D_ASSERT(enable_hll);
	return LossyNumericCast<idx_t>((1 + HyperLogLog::GetErrorRate()) * static_cast<double>(hll.Count()));
}

void GroupedAggregateHashTable::Resize(idx_t size) {
	D_ASSERT(IsPowerOfTwo(size));
	if (Count() != 0 && size < capacity) {
		throw InternalException("Cannot downsize a non-empty hash table!");
	}
	D_ASSERT(Count() == 0 || Count() == GetMaterializedCount());

	capacity = size;
	hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(ht_entry_t));
	entries = reinterpret_cast<ht_entry_t *>(hash_map.get());
	ClearPointerTable();
	bitmask = capacity - 1;

	if (Count() != 0) {
		ReinsertTuples(*partitioned_data);
		if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD) {
			ReinsertTuples(*unpartitioned_data);
		}
	}

	Verify();
}

static void SaltIncrementAndWrap(idx_t &offset, const idx_t &salt, const idx_t &capacity_mask) {
	// How many of the uppermost bits of the salt to determine the linear probing increment
	static constexpr idx_t INCREMENT_BITS = 5;
	// Extract the bits and make sure it's odd so we cover the entire domain when adding modulo a power of two
	offset += (salt >> (64 - INCREMENT_BITS)) | 1;
	offset &= capacity_mask;
}

void GroupedAggregateHashTable::ReinsertTuples(PartitionedTupleData &data) {
	for (auto &data_collection : data.GetPartitions()) {
		if (data_collection->Count() == 0) {
			continue;
		}
		TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::ALREADY_PINNED, false);
		const auto row_locations = iterator.GetRowLocations();
		do {
			for (idx_t i = 0; i < iterator.GetCurrentChunkCount(); i++) {
				const auto &row_location = row_locations[i];
				const auto hash = Load<hash_t>(row_location + hash_offset);
				const auto salt = ht_entry_t::ExtractSalt(hash);

				// Find an empty entry
				auto ht_offset = ApplyBitMask(hash);
				D_ASSERT(ht_offset == hash % capacity);
				while (entries[ht_offset].IsOccupied()) {
					SaltIncrementAndWrap(ht_offset, salt, bitmask);
				}
				auto &entry = entries[ht_offset];
				D_ASSERT(!entry.IsOccupied());
				entry.SetSalt(salt);
				entry.SetPointer(row_location);
				D_ASSERT(entry.IsOccupied());
			}
		} while (iterator.Next());
	}
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload, AggregateType filter) {
	unsafe_vector<idx_t> aggregate_filter;

	auto &aggregates = layout_ptr->GetAggregates();
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		if (aggregate.aggr_type == filter) {
			aggregate_filter.push_back(i);
		}
	}
	return AddChunk(groups, payload, aggregate_filter);
}

GroupedAggregateHashTable::AggregateCompressedGroupState::AggregateCompressedGroupState()
    : hashes(LogicalType::HASH), unique_group_pointers(LogicalType::POINTER), unique_entries(STANDARD_VECTOR_SIZE) {
}

optional_idx GroupedAggregateHashTable::TryAddDictionaryGroups(DataChunk &groups, DataChunk &payload,
                                                               const unsafe_vector<idx_t> &filter) {
	auto result = TryResolveDictionaryGroups(groups, state.addresses, layout_ptr->GetAggrOffset());
	if (!result.IsValid()) {
		return result;
	}
	auto &aggregates = layout_ptr->GetAggregates();
	if (aggregates.empty()) {
		return result;
	}

	// ht_offsets are only valid for unique entries, not the full payload
	UpdateAggregates(payload, filter, groups.size(), false);
	return result;
}

optional_idx GroupedAggregateHashTable::TryResolveDictionaryGroups(DataChunk &groups, Vector &addresses_out,
                                                                   idx_t address_offset) {
	static constexpr idx_t MAX_DICTIONARY_SIZE_THRESHOLD = 20000;
	static constexpr idx_t DICTIONARY_THRESHOLD = 2;
	// dictionary vector - check if this is a duplicate eliminated dictionary from the storage
	const auto &dict_col = groups.data[0];
	auto opt_dict_size = DictionaryVector::DictionarySize(dict_col);
	if (!opt_dict_size.IsValid()) {
		// dict size not known - this is not a dictionary that comes from the storage
		return optional_idx();
	}
	idx_t dict_size = opt_dict_size.GetIndex();
	auto &dictionary_id = DictionaryVector::DictionaryId(dict_col);
	if (dictionary_id.empty()) {
		// dictionary has no id, we can't cache across vectors
		// only use dictionary compression if there are fewer entries than groups
		if (dict_size * DICTIONARY_THRESHOLD >= groups.size()) {
			// dictionary is too large - use regular aggregation
			return optional_idx();
		}
	} else {
		// dictionary has an id - we can cache across vectors
		// use a much larger limit for dictionary
		if (dict_size >= MAX_DICTIONARY_SIZE_THRESHOLD) {
			// dictionary is too large - use regular aggregation
			return optional_idx();
		}
	}
	const auto &dictionary_vector = DictionaryVector::Child(dict_col);
	const auto &offsets = DictionaryVector::SelVector(dict_col);
	auto &compressed_group_state = state.compressed_group_state;
	if (compressed_group_state.dictionary_id.empty() || compressed_group_state.dictionary_id != dictionary_id) {
		// new dictionary - initialize the index state
		if (dict_size > compressed_group_state.capacity) {
			compressed_group_state.dictionary_addresses = make_uniq<Vector>(LogicalType::POINTER, dict_size);
			compressed_group_state.found_entry = make_unsafe_uniq_array<bool>(dict_size);
			compressed_group_state.capacity = dict_size;
		}
		memset(compressed_group_state.found_entry.get(), 0, dict_size * sizeof(bool));
		compressed_group_state.dictionary_id = dictionary_id;
		compressed_group_state.resolved_count = 0;
		compressed_group_state.address_high_bits = ~uint64_t(0);
		compressed_group_state.address_high_bits_uniform =
		    (sizeof(uintptr_t) == sizeof(uint64_t)); // only relevant on 64-bits systems
	} else if (dict_size > compressed_group_state.capacity) {
		throw InternalException("AggregateHT - using cached dictionary data but dictionary has changed (dictionary id "
		                        "%s - dict size %d, current capacity %d)",
		                        compressed_group_state.dictionary_id, dict_size, compressed_group_state.capacity);
	}

	auto &found_entry = compressed_group_state.found_entry;
	auto &unique_entries = compressed_group_state.unique_entries;
	idx_t unique_count = 0;
	// for each of the dictionary entries - check if we have already done a look-up into the hash table
	// if we have, we can just use the cached group pointers
	// once every dict slot has been seen the walk would produce no new entries - skip it
	if (compressed_group_state.resolved_count < dict_size) {
		for (idx_t i = 0; i < groups.size(); i++) {
			auto dict_idx = offsets.get_index(i);
			unique_entries.set_index(unique_count, dict_idx);
			unique_count += !found_entry[dict_idx];
			found_entry[dict_idx] = true;
		}
		compressed_group_state.resolved_count += unique_count;
	}
	auto &unique_group_pointers = compressed_group_state.unique_group_pointers;
	idx_t new_group_count = 0;
	if (unique_count > 0) {
		auto &unique_values = compressed_group_state.unique_values;
		if (unique_values.ColumnCount() == 0) {
			unique_values.InitializeEmpty(groups.GetTypes());
		}
		// slice the dictionary
		unique_values.data[0].Slice(dictionary_vector, unique_entries, unique_count);
		unique_values.CheckCardinality(unique_count);
		// now we know which entries we are going to add - hash them
		auto &hashes = compressed_group_state.hashes;
		unique_values.Hash(hashes);

		// add the dictionary groups to the hash table
		new_group_count = FindOrCreateGroups(unique_values, hashes, unique_group_pointers, state.new_groups);
	}

	// set the addresses that we found for each of the unique groups in the main addresses vector
	auto new_dict_addresses = FlatVector::GetData<uintptr_t>(unique_group_pointers);
	// for each of the new groups, add them to the global (cached) list of addresses for the dictionary
	auto &dictionary_addresses = *compressed_group_state.dictionary_addresses;
	auto dict_addresses = FlatVector::ScatterWriter<uintptr_t>(dictionary_addresses);
	for (idx_t i = 0; i < unique_count; i++) {
		auto dict_idx = unique_entries.get_index(i);
		dict_addresses[dict_idx] = new_dict_addresses[i];
	}
	// now set up the addresses for the aggregates
	addresses_out.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_addresses = FlatVector::Writer<uintptr_t>(addresses_out, groups.size());
	if (address_offset > 0) {
		compressed_group_state.address_high_bits_uniform =
		    (sizeof(uintptr_t) == sizeof(uint64_t)); // only relevant on 64-bits systems
		compressed_group_state.address_high_bits = ~uint64_t(0);
	}
	for (idx_t i = 0; i < groups.size(); i++) {
		auto dict_idx = offsets.get_index(i);
		const uintptr_t addr = dict_addresses[dict_idx] + address_offset;
		if (address_offset > 0 && compressed_group_state.address_high_bits_uniform) {
			static constexpr uint64_t GID_HIGH_MASK = ~(ClusteredAggr::MAX_GID_COUNT - 1);
			const uint64_t high_bits = static_cast<uint64_t>(addr) & GID_HIGH_MASK;
			if (compressed_group_state.address_high_bits == ~uint64_t(0)) { // uninitialized
				compressed_group_state.address_high_bits = high_bits;
			} else if (high_bits != compressed_group_state.address_high_bits) {
				compressed_group_state.address_high_bits_uniform = false;
			}
		}
		result_addresses.WriteValue(addr);
	}
	FlatVector::SetSize(addresses_out, groups.size());

	return new_group_count;
}

optional_idx GroupedAggregateHashTable::TryAddConstantGroups(DataChunk &groups, DataChunk &payload,
                                                             const unsafe_vector<idx_t> &filter) {
	auto result = TryResolveConstantGroups(groups, state.addresses, layout_ptr->GetAggrOffset());
	if (!result.IsValid()) {
		return result;
	}
	auto &aggregates = layout_ptr->GetAggregates();
	if (aggregates.empty()) {
		state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
		return result;
	}

	UpdateAggregates(payload, filter, groups.size(), false);
	state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
	return result;
}

optional_idx GroupedAggregateHashTable::TryResolveConstantGroups(DataChunk &groups, Vector &addresses_out,
                                                                 idx_t address_offset) {
#ifndef DEBUG
	if (groups.size() <= 1) {
		// this only has a point if we have multiple groups
		return optional_idx();
	}
#endif
	// Capture row_count before Reference+SetChildCardinality, which share the buffer and would corrupt groups.size()
	const idx_t row_count = groups.size();
	auto &compressed_group_state = state.compressed_group_state;
	auto &unique_values = compressed_group_state.unique_values;
	if (unique_values.ColumnCount() == 0) {
		unique_values.InitializeEmpty(groups.GetTypes());
	}
	// slice the dictionary
	unique_values.Reference(groups);
	unique_values.SetChildCardinality(1);
	unique_values.Flatten();
	// Restore the groups chunk's buffer v_size which was corrupted to 1 by SetChildCardinality(1) above.
	// unique_values.Flatten() created new independent buffers, so restoring groups is safe.
	groups.SetChildCardinality(row_count);

	auto &hashes = compressed_group_state.hashes;
	unique_values.Hash(hashes);

	// add the single constant group to the hash table
	auto &unique_group_pointers = compressed_group_state.unique_group_pointers;
	auto new_group_count = FindOrCreateGroups(unique_values, hashes, unique_group_pointers, state.new_groups);

	// Subsequent address consumers expect a flat vector here.
	auto new_dict_addresses = FlatVector::GetData<uintptr_t>(unique_group_pointers);
	addresses_out.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_addresses = FlatVector::Writer<uintptr_t>(addresses_out, row_count);
	uintptr_t aggregate_address = new_dict_addresses[0] + address_offset;
	if (address_offset > 0) {
		static constexpr uint64_t GID_HIGH_MASK = ~(ClusteredAggr::MAX_GID_COUNT - 1);
		compressed_group_state.address_high_bits_uniform = (sizeof(uintptr_t) == sizeof(uint64_t));
		compressed_group_state.address_high_bits = static_cast<uint64_t>(aggregate_address) & GID_HIGH_MASK;
	}
	for (idx_t i = 0; i < row_count; i++) {
		result_addresses.WriteValue(aggregate_address);
	}
	FlatVector::SetSize(addresses_out, row_count);
	addresses_out.SetVectorType(VectorType::CONSTANT_VECTOR);

	return new_group_count;
}

optional_idx GroupedAggregateHashTable::TryAddCompressedGroups(DataChunk &groups, DataChunk &payload,
                                                               const unsafe_vector<idx_t> &filter) {
	// all groups must be compressed
	if (groups.AllConstant()) {
		return TryAddConstantGroups(groups, payload, filter);
	}
	if (groups.ColumnCount() == 1 && groups.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR &&
	    groups.data[0].GetType().InternalType() != PhysicalType::STRUCT) {
		return TryAddDictionaryGroups(groups, payload, filter);
	}
	return optional_idx();
}

optional_idx GroupedAggregateHashTable::TryResolveCompressedGroups(DataChunk &groups, Vector &addresses_out,
                                                                   idx_t address_offset) {
	// all groups must be compressed
	if (groups.AllConstant()) {
		return TryResolveConstantGroups(groups, addresses_out, address_offset);
	}
	if (groups.ColumnCount() == 1 && groups.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR &&
	    groups.data[0].GetType().InternalType() != PhysicalType::STRUCT) {
		return TryResolveDictionaryGroups(groups, addresses_out, address_offset);
	}
	return optional_idx();
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload, const unsafe_vector<idx_t> &filter) {
	sink_count += groups.size();

	// check if we can use an optimized path that utilizes compressed vectors
	auto result = TryAddCompressedGroups(groups, payload, filter);
	if (result.IsValid()) {
		return result.GetIndex();
	}
	// otherwise append the raw values
	groups.Hash(state.hashes);

	return AddChunk(groups, state.hashes, payload, filter);
}

bool GroupedAggregateHashTable::UpdateAggregatesClustered(DataChunk &payload, const unsafe_vector<idx_t> &filter,
                                                          idx_t count, bool ht_offsets_valid) {
	if (skip_lookups) {
		return false;
	}
	if (sizeof(uintptr_t) < sizeof(uint64_t)) {
		return false;
	}
	ClusteredAggr clustered;
	if (ht_offsets_valid) {
		if (capacity >= ClusteredAggr::MAX_GID_COUNT) {
			return false;
		}
		if (!clustered_state.TryBuild(clustered, FlatVector::GetData<uint64_t>(state.ht_offsets), count)) {
			return false;
		}
		const auto aggr_offset = layout_ptr->GetAggrOffset();
		clustered.InitializeStates([&](uint64_t gid) {
			auto slot = static_cast<idx_t>(gid);
			return entries[slot].GetPointer() + aggr_offset;
		});
	} else {
		// dictionary path: addresses are already resolved pointers — and gids are not even set then
		// But, we can use the "addresses as gids". ClusteredAggr only triggers on low gid counts (see above) and
		// for speed the TryClustered method exploits that by ignoring high gid bits. When using "addresses as gids"
		// we thus need to know that all high bits are the same (this is very typically the case).
		if (!state.compressed_group_state.address_high_bits_uniform) {
			return false;
		}
		auto addrs = FlatVector::GetData<uint64_t>(state.addresses);
		if (!clustered_state.TryBuild(clustered, addrs, count)) {
			return false;
		}
		clustered.InitializeStates([&](uint64_t gid) {
			return reinterpret_cast<data_ptr_t>(state.compressed_group_state.address_high_bits | gid);
		});
	}

	const bool skip_addresses = clustered_state.all_clustered;
	auto &aggregates = layout_ptr->GetAggregates();
	RowOperations::UpdateStatesClustered(state.row_state, aggregates, &filter_set, &filter, state.addresses, payload,
	                                     clustered, skip_addresses);
	return true;
}

void GroupedAggregateHashTable::UpdateAggregates(DataChunk &payload, const unsafe_vector<idx_t> &filter, idx_t count,
                                                 bool ht_offsets_valid) {
	if (UpdateAggregatesClustered(payload, filter, count, ht_offsets_valid)) {
		Verify();
		return;
	}

	auto &aggregates = layout_ptr->GetAggregates();
	idx_t filter_idx = 0;
	idx_t payload_idx = 0;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i];
		if (filter_idx >= filter.size() || i < filter[filter_idx]) {
			// Skip all the aggregates that are not in the filter
			payload_idx += aggr.child_count;
			VectorOperations::AddInPlace(state.addresses, NumericCast<int64_t>(aggr.payload_size));
			continue;
		}
		D_ASSERT(i == filter[filter_idx]);

		if (aggr.aggr_type != AggregateType::DISTINCT && aggr.filter) {
			RowOperations::UpdateFilteredStates(state.row_state, filter_set.GetFilterData(i), aggr, state.addresses,
			                                    payload, payload_idx);
		} else {
			RowOperations::UpdateStates(state.row_state, aggr, state.addresses, payload, payload_idx);
		}

		// Move to the next aggregate
		payload_idx += aggr.child_count;
		VectorOperations::AddInPlace(state.addresses, NumericCast<int64_t>(aggr.payload_size));
		filter_idx++;
	}

	Verify();
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, Vector &group_hashes, DataChunk &payload,
                                          const unsafe_vector<idx_t> &filter) {
	if (groups.size() == 0) {
		return 0;
	}

#ifdef DEBUG
	D_ASSERT(groups.ColumnCount() + 1 == layout_ptr->ColumnCount());
	for (idx_t i = 0; i < groups.ColumnCount(); i++) {
		D_ASSERT(groups.GetTypes()[i] == layout_ptr->GetTypes()[i]);
	}
#endif

	const auto new_group_count = FindOrCreateGroups(groups, group_hashes, state.addresses, state.new_groups);
	VectorOperations::AddInPlace(state.addresses, NumericCast<int64_t>(layout_ptr->GetAggrOffset()));

	UpdateAggregates(payload, filter, groups.size());

	return new_group_count;
}

void GroupedAggregateHashTable::FetchAggregates(DataChunk &groups, DataChunk &result) {
#ifdef DEBUG
	groups.Verify(context.db);
	D_ASSERT(groups.ColumnCount() + 1 == layout_ptr->ColumnCount());
	for (idx_t i = 0; i < result.ColumnCount(); i++) {
		D_ASSERT(result.data[i].GetType() == payload_types[i]);
	}
#endif

	result.SetChildCardinality(groups.size());
	if (groups.size() == 0) {
		return;
	}

	// Resolve the groups associated with the addresses.
	FindOrCreateGroups(groups, state.addresses);
	// now fetch the aggregates
	RowOperations::FinalizeStates(state.row_state, *layout_ptr, state.addresses, result, 0);
}

template <bool HAS_SEL>
static void GroupedAggregateHashTableInnerLoop(ht_entry_t *const entries, const idx_t capacity, const hash_t bitmask,
                                               const hash_t *const hash_salts, uint64_t *const ht_offsets,
                                               const SelectionVector *const sel_vector, const idx_t remaining_entries,
                                               SelectionVector &empty_vector, SelectionVector &compare_vector,
                                               idx_t &empty_count, idx_t &compare_count) {
	// For each remaining entry, figure out whether or not it belongs to a full or empty group
	for (idx_t i = 0; i < remaining_entries; i++) {
		const auto index = HAS_SEL ? sel_vector->get_index_unsafe(i) : i;
		const auto salt = hash_salts[index];
		auto &ht_offset = ht_offsets[index];

		idx_t inner_iteration_count;
		for (inner_iteration_count = 0; inner_iteration_count < capacity; inner_iteration_count++) {
			auto &entry = entries[ht_offset];
			if (!entry.IsOccupied()) { // Unoccupied: claim it
				entry.SetSalt(salt);
				empty_vector.set_index(empty_count++, index);
				break;
			}

			if (DUCKDB_LIKELY(entry.GetSalt() == salt)) { // Matching salt: compare groups
				compare_vector.set_index(compare_count++, index);
				break;
			}

			// Linear probing
			SaltIncrementAndWrap(ht_offset, salt, bitmask);
		}
		if (DUCKDB_UNLIKELY(inner_iteration_count == capacity)) {
			throw InternalException("Maximum inner iteration count reached in GroupedAggregateHashTable");
		}
	}
}

idx_t GroupedAggregateHashTable::FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes_v,
                                                            Vector &addresses_v, SelectionVector &new_groups_out,
                                                            optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	D_ASSERT(groups.ColumnCount() + 1 == layout_ptr->ColumnCount());
	D_ASSERT(group_hashes_v.GetType() == LogicalType::HASH);
	D_ASSERT(state.ht_offsets.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(state.ht_offsets.GetType() == LogicalType::UBIGINT);
	D_ASSERT(addresses_v.GetType() == LogicalType::POINTER);
	D_ASSERT(state.hash_salts.GetType() == LogicalType::HASH);

	// Need to fit the entire vector, and resize at threshold
	const auto chunk_size = groups.size();
	if (Count() + chunk_size > capacity || Count() + chunk_size > ResizeThreshold()) {
		auto resize_start = AggregateTraceStart(recorder);
		Verify();
		Resize(capacity * 2);
		RecordAggregateTraceStage(recorder, "find_or_create.resize", resize_start);
	}
	D_ASSERT(capacity - Count() >= chunk_size); // we need to be able to fit at least one vector of data

	// we start out with all entries [0, 1, 2, ..., chunk_size]
	const SelectionVector *sel_vector = FlatVector::IncrementalSelectionVector();

	// Make a chunk that references the groups and the hashes and convert to unified format
	auto group_format_start = AggregateTraceStart(recorder);
	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout_ptr->GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout_ptr->GetTypes().size());
	for (idx_t grp_idx = 0; grp_idx < groups.ColumnCount(); grp_idx++) {
		state.group_chunk.data[grp_idx].Reference(groups.data[grp_idx]);
	}
	state.group_chunk.data[groups.ColumnCount()].Reference(group_hashes_v);

	// convert all vectors to unified format
	TupleDataCollection::ToUnifiedFormat(state.partitioned_append_state.chunk_state, state.group_chunk);
	RecordAggregateTraceStage(recorder, "find_or_create.group_format", group_format_start);

	if (enable_hll) {
		auto hll_start = AggregateTraceStart(recorder);
		hll.Update(group_hashes_v);
		RecordAggregateTraceStage(recorder, "find_or_create.hll", hll_start);
	}

	const auto hashes = group_hashes_v.Values<hash_t>();

	addresses_v.Flatten();
	const auto addresses = FlatVector::GetDataMutable<data_ptr_t>(addresses_v);

	if (skip_lookups) {
		// Just appending now
		auto append_start = AggregateTraceStart(recorder);
		partitioned_data->AppendUnified(state.partitioned_append_state, state.group_chunk,
		                                *FlatVector::IncrementalSelectionVector(), chunk_size);
		RowOperations::InitializeStates(*layout_ptr, state.partitioned_append_state.chunk_state.row_locations,
		                                *FlatVector::IncrementalSelectionVector(), chunk_size);

		const auto row_locations =
		    FlatVector::GetData<data_ptr_t>(state.partitioned_append_state.chunk_state.row_locations);
		const auto &row_sel = state.partitioned_append_state.reverse_partition_sel;
		for (idx_t i = 0; i < chunk_size; i++) {
			const auto row_idx = row_sel.get_index_unsafe(i);
			const auto &row_location = row_locations[row_idx];
			addresses[i] = row_location;
		}
		count += chunk_size;
		FlatVector::SetSize(addresses_v, chunk_size);
		RecordAggregateTraceStage(recorder, "find_or_create.append_skip_lookup", append_start);
		return chunk_size;
	}

	// Compute the entry in the table based on the hash using a modulo,
	// and precompute the hash salts for faster comparison below
	const auto ht_offsets = FlatVector::GetDataMutable<uint64_t>(state.ht_offsets);
	const auto hash_salts = FlatVector::GetDataMutable<hash_t>(state.hash_salts);

	// We also compute the occupied count, which is essentially useless.
	// However, this loop is branchless, while the main lookup loop below is not.
	// So, by doing the lookups here, we better amortize cache misses.
	auto compute_offsets_start = AggregateTraceStart(recorder);
	idx_t occupied_count = 0;
	for (idx_t r = 0; r < chunk_size; r++) {
		const auto &hash = hashes[r].GetValue();
		auto &ht_offset = ht_offsets[r];
		ht_offset = ApplyBitMask(hash);
		occupied_count += entries[ht_offset].IsOccupied(); // Lookup
		D_ASSERT(ht_offset == hash % capacity);
		hash_salts[r] = ht_entry_t::ExtractSalt(hash);
	}
	RecordAggregateTraceStage(recorder, "find_or_create.compute_offsets", compute_offsets_start);

	SelectionVector empty_vector;
	idx_t new_group_count = 0;
	idx_t remaining_entries = chunk_size;
	idx_t iteration_count;
	for (iteration_count = 0; remaining_entries > 0 && iteration_count < capacity; iteration_count++) {
		idx_t new_entry_count = 0;
		idx_t need_compare_count = 0;
		idx_t no_match_count = 0;

		// "new_groups_out" contains ALL groups for the chunk, "empty_vector" only the groups for this iteration,
		// so it's just the same selection vector, but offset by the current "new_group_count".
		empty_vector.Initialize(new_groups_out.data() + new_group_count, new_groups_out.Capacity() - new_group_count);
		auto probe_start = AggregateTraceStart(recorder);
		if (sel_vector->IsSet()) {
			GroupedAggregateHashTableInnerLoop<true>(entries, capacity, bitmask, hash_salts, ht_offsets, sel_vector,
			                                         remaining_entries, empty_vector, state.group_compare_vector,
			                                         new_entry_count, need_compare_count);
		} else {
			GroupedAggregateHashTableInnerLoop<false>(entries, capacity, bitmask, hash_salts, ht_offsets, sel_vector,
			                                          remaining_entries, empty_vector, state.group_compare_vector,
			                                          new_entry_count, need_compare_count);
		}
		RecordAggregateTraceStage(recorder, "find_or_create.probe", probe_start);
		new_group_count += new_entry_count;

		if (DUCKDB_UNLIKELY(occupied_count > new_entry_count + need_compare_count)) {
			// We use the useless occupied_count we summed above here so the variable is used,
			// and the compiler cannot optimize away the vectorized lookups above. This should never be triggered.
			throw InternalException("Internal validation failed in GroupedAggregateHashTable");
		}
		occupied_count = 0; // Have to set to 0 for next iterations

		if (new_entry_count != 0) {
			// Append everything that belongs to an empty group
			auto append_start = AggregateTraceStart(recorder);
			optional_ptr<PartitionedTupleData> data;
			optional_ptr<PartitionedTupleDataAppendState> append_state;
			if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD &&
			    new_entry_count / RadixPartitioning::NumberOfPartitions(radix_bits) <= 4) {
				TupleDataCollection::ToUnifiedFormat(state.unpartitioned_append_state.chunk_state, state.group_chunk);
				data = unpartitioned_data.get();
				append_state = &state.unpartitioned_append_state;
			} else {
				data = partitioned_data.get();
				append_state = &state.partitioned_append_state;
			}
			data->AppendUnified(*append_state, state.group_chunk, empty_vector, new_entry_count);
			RowOperations::InitializeStates(*layout_ptr, append_state->chunk_state.row_locations,
			                                *FlatVector::IncrementalSelectionVector(), new_entry_count);

			// Set the entry pointers in the 1st part of the HT now that the data has been appended
			const auto row_locations = FlatVector::GetData<data_ptr_t>(append_state->chunk_state.row_locations);
			const auto &row_sel = append_state->reverse_partition_sel;
			for (idx_t new_entry_idx = 0; new_entry_idx < new_entry_count; new_entry_idx++) {
				const auto index = empty_vector.get_index_unsafe(new_entry_idx);
				const auto row_idx = row_sel.get_index_unsafe(index);
				const auto &row_location = row_locations[row_idx];

				auto &entry = entries[ht_offsets[index]];

				entry.SetPointer(row_location);
				addresses[index] = row_location;
			}
			RecordAggregateTraceStage(recorder, "find_or_create.append_new_groups", append_start);
		}

		if (need_compare_count != 0) {
			// Get the pointers to the rows that need to be compared
			auto compare_start = AggregateTraceStart(recorder);
			for (idx_t need_compare_idx = 0; need_compare_idx < need_compare_count; need_compare_idx++) {
				const auto index = state.group_compare_vector.get_index_unsafe(need_compare_idx);
				const auto &entry = entries[ht_offsets[index]];
				addresses[index] = entry.GetPointer();
			}

			// Perform group comparisons
			row_matcher.Match(state.group_chunk, state.partitioned_append_state.chunk_state.vector_data,
			                  state.group_compare_vector, need_compare_count, addresses_v, &state.no_match_vector,
			                  no_match_count);
			RecordAggregateTraceStage(recorder, "find_or_create.compare_groups", compare_start);
		}

		// Linear probing: each of the entries that do not match move to the next entry in the HT
		auto advance_start = AggregateTraceStart(recorder);
		for (idx_t i = 0; i < no_match_count; i++) {
			const auto index = state.no_match_vector.get_index_unsafe(i);
			auto &ht_offset = ht_offsets[index];
			const auto salt = hash_salts[index];
			SaltIncrementAndWrap(ht_offset, salt, bitmask);
		}
		RecordAggregateTraceStage(recorder, "find_or_create.advance_collisions", advance_start);
		sel_vector = &state.no_match_vector;
		remaining_entries = no_match_count;
	}
	if (iteration_count == capacity) {
		throw InternalException("Maximum outer iteration count reached in GroupedAggregateHashTable");
	}

	FlatVector::SetSize(addresses_v, chunk_size);
	count += new_group_count;
	return new_group_count;
}

bool GroupedAggregateHashTable::TryResolveExistingGroupsFastInternal(
    DataChunk &groups, Vector &group_hashes, optional_ptr<Vector> addresses_out,
    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction selected_update_function, void *selected_update_state) {
	const auto chunk_size = groups.size();
	const auto group_count = groups.ColumnCount();
	if (chunk_size == 0) {
		if (addresses_out) {
			FlatVector::SetSize(*addresses_out, 0);
		}
		return true;
	}
	if (!addresses_out && !selected_update_function) {
		return false;
	}
	if (skip_lookups || enable_hll || count == 0 || !entries || !layout_ptr->CannotHaveNull() || group_count == 0 ||
	    group_count > AGGREGATE_MAX_FAST_GROUPS || group_hashes.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	D_ASSERT(group_count + 1 == layout_ptr->ColumnCount());

	const auto &layout_types = layout_ptr->GetTypes();
	const auto &layout_offsets = layout_ptr->GetOffsets();
	AggregateFastGroupSourceInfo sources;
	if (!AggregatePrepareFastGroupSourceInfo(groups, layout_types, chunk_size, sources)) {
		return false;
	}

	data_ptr_t *addresses = nullptr;
	if (addresses_out) {
		addresses_out->SetVectorType(VectorType::FLAT_VECTOR);
		addresses = FlatVector::GetDataMutable<data_ptr_t>(*addresses_out);
	}
	uintptr_t *selected_existing_addresses = nullptr;
	if (selected_update_function) {
		state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
		selected_existing_addresses = FlatVector::GetDataMutable<uintptr_t>(state.addresses);
	}
	const auto hashes = FlatVector::GetData<hash_t>(group_hashes);
	auto emit_existing_state = [&](data_ptr_t row_location, idx_t row_idx) {
		if (addresses) {
			addresses[row_idx] = row_location;
		}
		if (selected_update_function) {
			selected_existing_addresses[row_idx] = reinterpret_cast<uintptr_t>(row_location);
		}
	};
	bool use_consecutive_reuse = false;
	const auto sample_count = MinValue<idx_t>(chunk_size, 64);
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		if (hashes[row_idx] == hashes[row_idx - 1] &&
		    AggregateFastGroupSourceRowsMatch(sources, row_idx, row_idx - 1)) {
			use_consecutive_reuse = true;
			break;
		}
	}
	idx_t last_row_idx = DConstants::INVALID_INDEX;
	data_ptr_t last_row_location = nullptr;
	for (idx_t row_idx = 0; row_idx < chunk_size; row_idx++) {
		const auto hash = hashes[row_idx];
		const auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = ApplyBitMask(hash);

		if (use_consecutive_reuse && last_row_location && hashes[row_idx] == hashes[last_row_idx] &&
		    AggregateFastGroupSourceRowsMatch(sources, row_idx, last_row_idx)) {
			emit_existing_state(last_row_location, row_idx);
			last_row_idx = row_idx;
			continue;
		}

		idx_t iteration_count;
		for (iteration_count = 0; iteration_count < capacity; iteration_count++) {
			auto &entry = entries[ht_offset];
			if (!entry.IsOccupied()) {
				return false;
			}
			if (entry.GetSalt() == salt) {
				auto row_location = entry.GetPointer();
				if (AggregateFastExistingRowMatches(sources, layout_offsets, row_location, row_idx)) {
					emit_existing_state(row_location, row_idx);
					if (use_consecutive_reuse) {
						last_row_idx = row_idx;
						last_row_location = row_location;
					}
					break;
				}
			}
			SaltIncrementAndWrap(ht_offset, salt, bitmask);
		}
		if (iteration_count == capacity) {
			throw InternalException("Maximum fast existing-group iteration count reached in GroupedAggregateHashTable");
		}
	}
	if (addresses_out) {
		FlatVector::SetSize(*addresses_out, chunk_size);
	}
	if (selected_update_function) {
		selected_update_function(selected_existing_addresses, nullptr, nullptr, chunk_size, selected_update_state);
	}
	return true;
}

bool GroupedAggregateHashTable::TryResolveExistingGroupsFast(DataChunk &groups, Vector &group_hashes,
                                                             Vector &addresses_v) {
	return TryResolveExistingGroupsFastInternal(groups, group_hashes, addresses_v, nullptr, nullptr);
}

// this is to support distinct aggregations where we need to record whether we
// have already seen a value for a group
idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &group_hashes, Vector &addresses_out,
                                                    SelectionVector &new_groups_out,
                                                    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	return FindOrCreateGroupsInternal(groups, group_hashes, addresses_out, new_groups_out, recorder);
}

void GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses) {
	// create a dummy new_groups sel vector
	FindOrCreateGroups(groups, addresses, state.new_groups);
}

idx_t GroupedAggregateHashTable::FindOrCreateGroupAddresses(DataChunk &groups, Vector &addresses_out,
                                                            optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	sink_count += groups.size();
	auto compressed_start = AggregateTraceStart(recorder);
	auto compressed_result = TryResolveCompressedGroups(groups, addresses_out);
	RecordAggregateTraceStage(recorder, "find_or_create.compressed_lookup", compressed_start);
	if (compressed_result.IsValid()) {
		addresses_out.Flatten();
		return compressed_result.GetIndex();
	}
	auto hash_start = AggregateTraceStart(recorder);
	groups.Hash(state.hashes);
	RecordAggregateTraceStage(recorder, "find_or_create.hash", hash_start);
	auto fast_existing_start = AggregateTraceStart(recorder);
	if (TryResolveExistingGroupsFast(groups, state.hashes, addresses_out)) {
		RecordAggregateTraceStage(recorder, "find_or_create.fast_existing", fast_existing_start);
		return 0;
	}
	RecordAggregateTraceStage(recorder, "find_or_create.fast_existing_miss", fast_existing_start);
	return FindOrCreateGroups(groups, state.hashes, addresses_out, state.new_groups, recorder);
}

bool GroupedAggregateHashTable::TryFindExistingGroupsSelectedStateUpdateFast(
    DataChunk &groups, ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, optional_ptr<Vector> precomputed_hashes) {
	if (!update_function) {
		return false;
	}
	optional_ptr<Vector> group_hashes = precomputed_hashes;
	if (group_hashes) {
		if (!AggregateFastHashVectorIsValid(*group_hashes, groups.size())) {
			return false;
		}
	} else {
		auto hash_start = AggregateTraceStart(recorder);
		groups.Hash(state.hashes);
		RecordAggregateTraceStage(recorder, "find_existing_selected.hash", hash_start);
		group_hashes = &state.hashes;
	}

	auto fast_existing_start = AggregateTraceStart(recorder);
	if (!TryResolveExistingGroupsFastInternal(groups, *group_hashes, nullptr, update_function, update_state)) {
		RecordAggregateTraceStage(recorder, "find_existing_selected.fast_existing_miss", fast_existing_start);
		return false;
	}
	sink_count += groups.size();
	RecordAggregateTraceStage(recorder, "find_existing_selected.fast_existing_update", fast_existing_start);
	return true;
}

bool GroupedAggregateHashTable::TryFindOrCreateGroupsFastInternal(
    DataChunk &groups, optional_ptr<Vector> addresses_out,
    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction selected_update_function, void *selected_update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, optional_ptr<Vector> precomputed_hashes) {
	const auto chunk_size = groups.size();
	const auto group_count = groups.ColumnCount();
	if (!addresses_out && !selected_update_function) {
		return false;
	}
	if (chunk_size == 0) {
		if (addresses_out) {
			FlatVector::SetSize(*addresses_out, 0);
		}
		return true;
	}
	if (skip_lookups || !entries || !layout_ptr->CannotHaveNull() || group_count == 0 ||
	    group_count > AGGREGATE_MAX_FAST_GROUPS) {
		return false;
	}
	D_ASSERT(group_count + 1 == layout_ptr->ColumnCount());

	const auto &layout_types = layout_ptr->GetTypes();
	const auto &layout_offsets = layout_ptr->GetOffsets();
	AggregateFastGroupSourceInfo sources;
	if (!AggregatePrepareFastGroupSourceInfo(groups, layout_types, chunk_size, sources)) {
		return false;
	}

	if (Count() + chunk_size > capacity || Count() + chunk_size > ResizeThreshold()) {
		auto resize_start = AggregateTraceStart(recorder);
		Verify();
		Resize(capacity * 2);
		RecordAggregateTraceStage(recorder, "find_or_create_fast.resize", resize_start);
	}

	optional_ptr<Vector> group_hashes = precomputed_hashes;
	if (group_hashes) {
		if (!AggregateFastHashVectorIsValid(*group_hashes, chunk_size)) {
			return false;
		}
	} else {
		auto hash_start = AggregateTraceStart(recorder);
		groups.Hash(state.hashes);
		RecordAggregateTraceStage(recorder, "find_or_create_fast.hash", hash_start);
		group_hashes = &state.hashes;
	}
	if (enable_hll) {
		auto hll_start = AggregateTraceStart(recorder);
		hll.Update(*group_hashes);
		RecordAggregateTraceStage(recorder, "find_or_create_fast.hll", hll_start);
	}

	data_ptr_t *addresses = nullptr;
	if (addresses_out) {
		addresses_out->SetVectorType(VectorType::FLAT_VECTOR);
		addresses = FlatVector::GetDataMutable<data_ptr_t>(*addresses_out);
	}
	uintptr_t *selected_existing_addresses = nullptr;
	if (selected_update_function) {
		state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
		selected_existing_addresses = FlatVector::GetDataMutable<uintptr_t>(state.addresses);
	}
	const auto hashes = FlatVector::GetData<hash_t>(*group_hashes);
	auto ht_offsets = FlatVector::GetDataMutable<uint64_t>(state.ht_offsets);
	auto duplicate_targets = FlatVector::GetDataMutable<hash_t>(state.hash_salts);

	idx_t new_group_count = 0;
	idx_t duplicate_count = 0;
	idx_t selected_existing_count = 0;
	auto clear_marked_entries = [&]() {
		for (idx_t marked_idx = 0; marked_idx < new_group_count; marked_idx++) {
			entries[ht_offsets[marked_idx]] = ht_entry_t();
		}
		new_group_count = 0;
	};
	auto emit_row_state = [&](data_ptr_t row_location, idx_t row_idx) {
		if (addresses) {
			addresses[row_idx] = row_location;
		}
	};
	auto emit_existing_state = [&](data_ptr_t row_location, idx_t row_idx) {
		emit_row_state(row_location, row_idx);
		if (selected_update_function) {
			selected_existing_addresses[selected_existing_count] = reinterpret_cast<uintptr_t>(row_location);
			state.existing_groups.set_index(selected_existing_count, row_idx);
			selected_existing_count++;
		}
	};
	enum class LastMatchKind : uint8_t { NONE, NEW_GROUP, EXISTING_GROUP };
	LastMatchKind last_match_kind = LastMatchKind::NONE;
	idx_t last_row_idx = DConstants::INVALID_INDEX;
	idx_t last_new_group_idx = DConstants::INVALID_INDEX;
	data_ptr_t last_existing_row_location = nullptr;
	auto emit_new_duplicate = [&](idx_t row_idx, idx_t new_group_idx) {
		state.no_match_vector.set_index(duplicate_count, row_idx);
		duplicate_targets[duplicate_count] = new_group_idx;
		duplicate_count++;
		last_row_idx = row_idx;
	};
	auto remember_new_group = [&](idx_t row_idx, idx_t new_group_idx) {
		last_match_kind = LastMatchKind::NEW_GROUP;
		last_row_idx = row_idx;
		last_new_group_idx = new_group_idx;
	};
	auto remember_existing_group = [&](idx_t row_idx, data_ptr_t row_location) {
		last_match_kind = LastMatchKind::EXISTING_GROUP;
		last_row_idx = row_idx;
		last_existing_row_location = row_location;
	};
	auto try_emit_repeated_match = [&](idx_t row_idx, bool keys_match) {
		if (!keys_match || last_match_kind == LastMatchKind::NONE) {
			return false;
		}
		if (last_match_kind == LastMatchKind::NEW_GROUP) {
			emit_new_duplicate(row_idx, last_new_group_idx);
			return true;
		}
		emit_existing_state(last_existing_row_location, row_idx);
		last_row_idx = row_idx;
		return true;
	};
	auto generic_keys_match_rows = [&](idx_t row_idx, idx_t other_row_idx) {
		return AggregateFastGroupSourceRowsMatch(sources, row_idx, other_row_idx);
	};
	auto generic_keys_match_last = [&](idx_t row_idx) {
		if (last_row_idx == DConstants::INVALID_INDEX || hashes[row_idx] != hashes[last_row_idx]) {
			return false;
		}
		return generic_keys_match_rows(row_idx, last_row_idx);
	};

	auto probe_start = AggregateTraceStart(recorder);
	bool use_consecutive_reuse = false;
	const auto sample_count = MinValue<idx_t>(chunk_size, 64);
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		if (hashes[row_idx] == hashes[row_idx - 1] && generic_keys_match_rows(row_idx, row_idx - 1)) {
			use_consecutive_reuse = true;
			break;
		}
	}
	for (idx_t row_idx = 0; row_idx < chunk_size; row_idx++) {
		const auto hash = hashes[row_idx];
		const auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = ApplyBitMask(hash);

		if (use_consecutive_reuse && try_emit_repeated_match(row_idx, generic_keys_match_last(row_idx))) {
			continue;
		}

		idx_t iteration_count;
		for (iteration_count = 0; iteration_count < capacity; iteration_count++) {
			auto &entry = entries[ht_offset];
			if (!entry.IsOccupied()) {
				ht_offsets[new_group_count] = ht_offset;
				state.new_groups.set_index(new_group_count, row_idx);
				if (use_consecutive_reuse) {
					remember_new_group(row_idx, new_group_count);
				}
				new_group_count++;
				entry.SetSalt(salt);
				break;
			}
			if (entry.GetSalt() == salt) {
				auto row_location = entry.GetPointer();
				if (cast_pointer_to_uint64(row_location) == ht_entry_t::POINTER_MASK) {
					bool found_tentative_match = false;
					for (idx_t marked_idx = 0; marked_idx < new_group_count; marked_idx++) {
						if (ht_offsets[marked_idx] != ht_offset) {
							continue;
						}
						const auto marked_row_idx = state.new_groups.get_index_unsafe(marked_idx);
						if (!AggregateFastGroupSourceRowsMatch(sources, row_idx, marked_row_idx)) {
							continue;
						}
						state.no_match_vector.set_index(duplicate_count, row_idx);
						duplicate_targets[duplicate_count] = marked_idx;
						duplicate_count++;
						if (use_consecutive_reuse) {
							remember_new_group(row_idx, marked_idx);
						}
						found_tentative_match = true;
						break;
					}
					if (found_tentative_match) {
						break;
					}
					SaltIncrementAndWrap(ht_offset, salt, bitmask);
					continue;
				}
				if (AggregateFastExistingRowMatches(sources, layout_offsets, row_location, row_idx)) {
					emit_existing_state(row_location, row_idx);
					if (use_consecutive_reuse) {
						remember_existing_group(row_idx, row_location);
					}
					break;
				}
			}
			SaltIncrementAndWrap(ht_offset, salt, bitmask);
		}
		if (iteration_count == capacity) {
			clear_marked_entries();
			throw InternalException("Maximum fast find-or-create iteration count reached in GroupedAggregateHashTable");
		}
	}
	RecordAggregateTraceStage(recorder, "find_or_create_fast.probe", probe_start);

	if (new_group_count == 0) {
		if (addresses_out) {
			FlatVector::SetSize(*addresses_out, chunk_size);
		}
		if (selected_update_function && selected_existing_count > 0) {
			auto update_start = AggregateTraceStart(recorder);
			selected_update_function(selected_existing_addresses, nullptr, state.existing_groups.data(),
			                         selected_existing_count, selected_update_state);
			RecordAggregateTraceStage(recorder, "find_or_create_fast.selected_existing_update", update_start);
		}
		sink_count += chunk_size;
		return true;
	}

	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout_ptr->GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout_ptr->GetTypes().size());
	for (idx_t group_idx = 0; group_idx < groups.ColumnCount(); group_idx++) {
		state.group_chunk.data[group_idx].Reference(groups.data[group_idx]);
	}

	auto group_format_start = AggregateTraceStart(recorder);
	state.group_chunk.data[groups.ColumnCount()].Reference(*group_hashes);
	TupleDataCollection::ToUnifiedFormat(state.partitioned_append_state.chunk_state, state.group_chunk);
	RecordAggregateTraceStage(recorder, "find_or_create_fast.group_format", group_format_start);

	auto append_start = AggregateTraceStart(recorder);
	optional_ptr<PartitionedTupleData> data;
	optional_ptr<PartitionedTupleDataAppendState> append_state;
	if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD &&
	    new_group_count / RadixPartitioning::NumberOfPartitions(radix_bits) <= 4) {
		TupleDataCollection::ToUnifiedFormat(state.unpartitioned_append_state.chunk_state, state.group_chunk);
		data = unpartitioned_data.get();
		append_state = &state.unpartitioned_append_state;
	} else {
		data = partitioned_data.get();
		append_state = &state.partitioned_append_state;
	}
	data->AppendUnified(*append_state, state.group_chunk, state.new_groups, new_group_count);
	RowOperations::InitializeStates(*layout_ptr, append_state->chunk_state.row_locations,
	                                *FlatVector::IncrementalSelectionVector(), new_group_count);
	const auto row_locations = FlatVector::GetData<data_ptr_t>(append_state->chunk_state.row_locations);
	const auto &row_sel = append_state->reverse_partition_sel;
	for (idx_t new_idx = 0; new_idx < new_group_count; new_idx++) {
		const auto row_idx = state.new_groups.get_index_unsafe(new_idx);
		const auto row_location = row_locations[row_sel.get_index_unsafe(row_idx)];
		entries[ht_offsets[new_idx]].SetPointer(row_location);
		emit_row_state(row_location, row_idx);
	}
	for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
		const auto row_idx = state.no_match_vector.get_index_unsafe(duplicate_idx);
		const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
		const auto row_location = entries[ht_offsets[marked_idx]].GetPointer();
		emit_row_state(row_location, row_idx);
	}
	if (selected_update_function) {
		auto update_start = AggregateTraceStart(recorder);
		if (selected_existing_count > 0) {
			selected_update_function(selected_existing_addresses, nullptr, state.existing_groups.data(),
			                         selected_existing_count, selected_update_state);
		}
		if (new_group_count > 0) {
			const auto state_addresses = reinterpret_cast<const uintptr_t *>(row_locations);
			selected_update_function(state_addresses, row_sel.data(), state.new_groups.data(), new_group_count,
			                         selected_update_state);
		}
		if (duplicate_count > 0) {
			for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
				const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
				selected_existing_addresses[duplicate_idx] =
				    reinterpret_cast<uintptr_t>(entries[ht_offsets[marked_idx]].GetPointer());
			}
			selected_update_function(selected_existing_addresses, nullptr, state.no_match_vector.data(),
			                         duplicate_count, selected_update_state);
		}
		RecordAggregateTraceStage(recorder, "find_or_create_fast.selected_state_update", update_start);
	}
	if (addresses_out) {
		FlatVector::SetSize(*addresses_out, chunk_size);
	}
	count += new_group_count;
	sink_count += chunk_size;
	RecordAggregateTraceStage(recorder, "find_or_create_fast.append_new_groups", append_start);
	return true;
}

bool GroupedAggregateHashTable::TryFindOrCreateGroupAddressesFast(
    DataChunk &groups, Vector &addresses_out, optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	return TryFindOrCreateGroupsFastInternal(groups, addresses_out, nullptr, nullptr, recorder);
}

bool GroupedAggregateHashTable::TryFindOrCreateGroupsSelectedStateUpdateFast(
    DataChunk &groups, ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, optional_ptr<Vector> precomputed_hashes) {
	if (!update_function) {
		return false;
	}
	return TryFindOrCreateGroupsFastInternal(groups, nullptr, update_function, update_state, recorder,
	                                         precomputed_hashes);
}

static bool AggregateDescriptorGroupKeySourcesSupported(const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                        const vector<LogicalType> &layout_types) {
	if (group_sources.empty() || group_sources.size() >= layout_types.size()) {
		return false;
	}
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		const auto &source = group_sources[group_idx];
		const auto target_physical_type = layout_types[group_idx].InternalType();
		if (!source.ready || source.target_physical_type != target_physical_type ||
		    source.target_type.InternalType() != target_physical_type ||
		    !AggregateFastExistingMatchType(target_physical_type)) {
			return false;
		}
		switch (source.cast_kind) {
		case ExecutionRowPointerGroupKeyCastKind::NONE:
			if (source.source_physical_type != source.target_physical_type) {
				return false;
			}
			break;
		case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
			if (source.source_physical_type != PhysicalType::INT64 ||
			    source.target_physical_type != PhysicalType::INT32) {
				return false;
			}
			break;
		case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
			if (source.source_physical_type != PhysicalType::INT64 ||
			    source.target_physical_type != PhysicalType::INT16) {
				return false;
			}
			break;
		case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
			if (source.source_physical_type != PhysicalType::INT32 ||
			    source.target_physical_type != PhysicalType::INT8) {
				return false;
			}
			break;
		case ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS:
			switch (source.source_physical_type) {
			case PhysicalType::INT8:
			case PhysicalType::INT16:
			case PhysicalType::INT32:
			case PhysicalType::INT64:
				break;
			default:
				return false;
			}
			switch (source.target_physical_type) {
			case PhysicalType::UINT8:
			case PhysicalType::UINT16:
			case PhysicalType::UINT32:
			case PhysicalType::UINT64:
				break;
			default:
				return false;
			}
			break;
		case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS:
			if (source.source_physical_type != PhysicalType::VARCHAR) {
				return false;
			}
			switch (source.target_physical_type) {
			case PhysicalType::UINT8:
			case PhysicalType::UINT16:
			case PhysicalType::UINT32:
			case PhysicalType::UINT64:
			case PhysicalType::UINT128:
				break;
			default:
				return false;
			}
			break;
		default:
			return false;
		}
		if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			if (source.row_layout_offset == DConstants::INVALID_INDEX) {
				return false;
			}
			if (!source.all_valid &&
			    (source.row_layout_column_idx == DConstants::INVALID_INDEX || source.row_layout_column_count == 0)) {
				return false;
			}
		} else if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			if (source.input_vector_index == DConstants::INVALID_INDEX) {
				return false;
			}
		} else {
			return false;
		}
	}
	return true;
}

static bool AggregateRowPointerGroupKeySourceIsValid(data_ptr_t row_pointer,
                                                     const ExecutionRowPointerGroupKeySource &source) {
	if (!row_pointer) {
		return false;
	}
	if (source.all_valid) {
		return true;
	}
	if (source.row_layout_column_idx == DConstants::INVALID_INDEX || source.row_layout_column_count == 0) {
		return false;
	}
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(source.row_layout_column_idx, entry_idx, idx_in_entry);
	return ValidityBytes::RowIsValid(
	    ValidityBytes(row_pointer, source.row_layout_column_count).GetValidityEntryUnsafe(entry_idx), idx_in_entry);
}

static idx_t AggregateDescriptorValueSize(PhysicalType physical_type) {
	return physical_type == PhysicalType::VARCHAR ? sizeof(string_t) : GetTypeIdSize(physical_type);
}

template <idx_t LENGTH>
static void AggregateReverseMemCpy(const data_ptr_t &__restrict dest, const const_data_ptr_t &__restrict src) {
	for (idx_t i = 0; i < LENGTH; i++) {
		dest[i] = src[LENGTH - 1 - i];
	}
}

static void AggregateReverseMemCpy(const data_ptr_t &__restrict dest, const const_data_ptr_t &__restrict src,
                                   const idx_t &length) {
	for (idx_t i = 0; i < length; i++) {
		dest[i] = src[length - 1 - i];
	}
}

template <class RESULT_TYPE>
static RESULT_TYPE AggregateStringCompressWide(const string_t &input) {
	D_ASSERT(input.GetSize() < sizeof(RESULT_TYPE));
	RESULT_TYPE result;
	const auto result_ptr = data_ptr_cast(&result);
	if (sizeof(RESULT_TYPE) <= string_t::INLINE_LENGTH) {
		AggregateReverseMemCpy<sizeof(RESULT_TYPE)>(result_ptr, const_data_ptr_cast(input.GetPrefix()));
	} else if (input.IsInlined()) {
		static constexpr auto REMAINDER = sizeof(RESULT_TYPE) - string_t::INLINE_LENGTH;
		AggregateReverseMemCpy<string_t::INLINE_LENGTH>(result_ptr + REMAINDER, const_data_ptr_cast(input.GetPrefix()));
		memset(result_ptr, '\0', REMAINDER);
	} else {
		const auto size = MinValue<idx_t>(sizeof(RESULT_TYPE), input.GetSize());
		const auto remainder = sizeof(RESULT_TYPE) - size;
		AggregateReverseMemCpy(result_ptr + remainder, data_ptr_cast(input.GetPointer()), size);
		memset(result_ptr, '\0', remainder);
	}
	result_ptr[0] = UnsafeNumericCast<data_t>(input.GetSize());
	return BSwapIfBE(result);
}

static uint8_t AggregateStringCompressUInt8(const string_t &input) {
	D_ASSERT(input.GetSize() <= sizeof(uint8_t));
	uint8_t result;
	if (input.GetSize() == 0) {
		result = 0;
	} else {
		result = UnsafeNumericCast<uint8_t>(input.GetSize() + *const_data_ptr_cast(input.GetPrefix()));
	}
	return BSwapIfBE(result);
}

template <class SRC, class DST>
static DST AggregateCheckedGroupKeyCast(SRC value) {
	DST result;
	if (!TryCast::Operation<SRC, DST>(value, result, false)) {
		throw InvalidInputException(CastExceptionText<SRC, DST>(value));
	}
	return result;
}

template <class SRC, class DST>
static bool AggregateStoreIntegralCompressedGroupKey(const_data_ptr_t source_data, idx_t source_idx, Vector &target,
                                                     idx_t target_idx,
                                                     const ExecutionRowPointerGroupKeySource &source) {
	auto source_value = Load<SRC>(source_data + source_idx * sizeof(SRC));
	SRC source_minimum;
	if (!TryCast::Operation<int64_t, SRC>(source.cast_constant, source_minimum, false)) {
		return false;
	}
	SRC compressed_value;
	if (!TrySubtractOperator::Operation<SRC, SRC, SRC>(source_value, source_minimum, compressed_value)) {
		return false;
	}
	DST target_value;
	if (!TryCast::Operation<SRC, DST>(compressed_value, target_value, false)) {
		return false;
	}
	auto target_values = FlatVector::GetDataMutable<DST>(target);
	target_values[target_idx] = target_value;
	return true;
}

template <class SRC>
static bool AggregateStoreIntegralCompressedGroupKey(const_data_ptr_t source_data, idx_t source_idx, Vector &target,
                                                     idx_t target_idx,
                                                     const ExecutionRowPointerGroupKeySource &source) {
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return AggregateStoreIntegralCompressedGroupKey<SRC, uint8_t>(source_data, source_idx, target, target_idx,
		                                                              source);
	case PhysicalType::UINT16:
		return AggregateStoreIntegralCompressedGroupKey<SRC, uint16_t>(source_data, source_idx, target, target_idx,
		                                                               source);
	case PhysicalType::UINT32:
		return AggregateStoreIntegralCompressedGroupKey<SRC, uint32_t>(source_data, source_idx, target, target_idx,
		                                                               source);
	case PhysicalType::UINT64:
		return AggregateStoreIntegralCompressedGroupKey<SRC, uint64_t>(source_data, source_idx, target, target_idx,
		                                                               source);
	default:
		return false;
	}
}

static bool AggregateStoreDescriptorGroupKeyValue(const_data_ptr_t source_data, idx_t source_idx, Vector &target,
                                                  idx_t target_idx, const ExecutionRowPointerGroupKeySource &source) {
	auto target_data = FlatVector::GetDataMutable(target);
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE: {
		if (source.source_physical_type != source.target_physical_type) {
			return false;
		}
		const auto value_size = AggregateDescriptorValueSize(source.target_physical_type);
		std::memcpy(target_data + target_idx * value_size, source_data + source_idx * value_size, value_size);
		return true;
	}
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32: {
		D_ASSERT(source.target_physical_type == PhysicalType::INT32);
		auto target_values = FlatVector::GetDataMutable<int32_t>(target);
		target_values[target_idx] =
		    AggregateCheckedGroupKeyCast<int64_t, int32_t>(Load<int64_t>(source_data + source_idx * sizeof(int64_t)));
		return true;
	}
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16: {
		D_ASSERT(source.target_physical_type == PhysicalType::INT16);
		auto target_values = FlatVector::GetDataMutable<int16_t>(target);
		target_values[target_idx] =
		    AggregateCheckedGroupKeyCast<int64_t, int16_t>(Load<int64_t>(source_data + source_idx * sizeof(int64_t)));
		return true;
	}
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8: {
		D_ASSERT(source.target_physical_type == PhysicalType::INT8);
		auto target_values = FlatVector::GetDataMutable<int8_t>(target);
		target_values[target_idx] =
		    AggregateCheckedGroupKeyCast<int32_t, int8_t>(Load<int32_t>(source_data + source_idx * sizeof(int32_t)));
		return true;
	}
	case ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS: {
		switch (source.source_physical_type) {
		case PhysicalType::INT8:
			return AggregateStoreIntegralCompressedGroupKey<int8_t>(source_data, source_idx, target, target_idx,
			                                                        source);
		case PhysicalType::INT16:
			return AggregateStoreIntegralCompressedGroupKey<int16_t>(source_data, source_idx, target, target_idx,
			                                                         source);
		case PhysicalType::INT32:
			return AggregateStoreIntegralCompressedGroupKey<int32_t>(source_data, source_idx, target, target_idx,
			                                                         source);
		case PhysicalType::INT64:
			return AggregateStoreIntegralCompressedGroupKey<int64_t>(source_data, source_idx, target, target_idx,
			                                                         source);
		default:
			return false;
		}
	}
	case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS: {
		if (source.source_physical_type != PhysicalType::VARCHAR) {
			return false;
		}
		const auto value = Load<string_t>(source_data + source_idx * sizeof(string_t));
		switch (source.target_physical_type) {
		case PhysicalType::UINT8: {
			auto target_values = FlatVector::GetDataMutable<uint8_t>(target);
			target_values[target_idx] = AggregateStringCompressUInt8(value);
			return true;
		}
		case PhysicalType::UINT16: {
			auto target_values = FlatVector::GetDataMutable<uint16_t>(target);
			target_values[target_idx] = AggregateStringCompressWide<uint16_t>(value);
			return true;
		}
		case PhysicalType::UINT32: {
			auto target_values = FlatVector::GetDataMutable<uint32_t>(target);
			target_values[target_idx] = AggregateStringCompressWide<uint32_t>(value);
			return true;
		}
		case PhysicalType::UINT64: {
			auto target_values = FlatVector::GetDataMutable<uint64_t>(target);
			target_values[target_idx] = AggregateStringCompressWide<uint64_t>(value);
			return true;
		}
		case PhysicalType::UINT128: {
			auto target_values = FlatVector::GetDataMutable<uhugeint_t>(target);
			target_values[target_idx] = AggregateStringCompressWide<uhugeint_t>(value);
			return true;
		}
		default:
			return false;
		}
	}
	default:
		return false;
	}
}

struct AggregateDescriptorValue {
	std::array<data_t, 32> data;

	data_ptr_t Ptr() {
		return data.data();
	}

	const_data_ptr_t Ptr() const {
		return data.data();
	}
};

template <class SRC, class DST>
static bool AggregateLoadIntegralCompressedGroupKeyValue(const_data_ptr_t source_data, idx_t source_idx,
                                                         AggregateDescriptorValue &target,
                                                         const ExecutionRowPointerGroupKeySource &source) {
	auto source_value = Load<SRC>(source_data + source_idx * sizeof(SRC));
	SRC source_minimum;
	if (!TryCast::Operation<int64_t, SRC>(source.cast_constant, source_minimum, false)) {
		return false;
	}
	SRC compressed_value;
	if (!TrySubtractOperator::Operation<SRC, SRC, SRC>(source_value, source_minimum, compressed_value)) {
		return false;
	}
	DST target_value;
	if (!TryCast::Operation<SRC, DST>(compressed_value, target_value, false)) {
		return false;
	}
	Store<DST>(target_value, target.Ptr());
	return true;
}

template <class SRC>
static bool AggregateLoadIntegralCompressedGroupKeyValue(const_data_ptr_t source_data, idx_t source_idx,
                                                         AggregateDescriptorValue &target,
                                                         const ExecutionRowPointerGroupKeySource &source) {
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return AggregateLoadIntegralCompressedGroupKeyValue<SRC, uint8_t>(source_data, source_idx, target, source);
	case PhysicalType::UINT16:
		return AggregateLoadIntegralCompressedGroupKeyValue<SRC, uint16_t>(source_data, source_idx, target, source);
	case PhysicalType::UINT32:
		return AggregateLoadIntegralCompressedGroupKeyValue<SRC, uint32_t>(source_data, source_idx, target, source);
	case PhysicalType::UINT64:
		return AggregateLoadIntegralCompressedGroupKeyValue<SRC, uint64_t>(source_data, source_idx, target, source);
	default:
		return false;
	}
}

static bool AggregateLoadDescriptorGroupKeyValue(const_data_ptr_t source_data, idx_t source_idx,
                                                 AggregateDescriptorValue &target,
                                                 const ExecutionRowPointerGroupKeySource &source) {
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE: {
		if (source.source_physical_type != source.target_physical_type) {
			return false;
		}
		const auto value_size = AggregateDescriptorValueSize(source.target_physical_type);
		std::memcpy(target.Ptr(), source_data + source_idx * value_size, value_size);
		return true;
	}
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32: {
		D_ASSERT(source.target_physical_type == PhysicalType::INT32);
		const auto target_value =
		    AggregateCheckedGroupKeyCast<int64_t, int32_t>(Load<int64_t>(source_data + source_idx * sizeof(int64_t)));
		Store<int32_t>(target_value, target.Ptr());
		return true;
	}
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16: {
		D_ASSERT(source.target_physical_type == PhysicalType::INT16);
		const auto target_value =
		    AggregateCheckedGroupKeyCast<int64_t, int16_t>(Load<int64_t>(source_data + source_idx * sizeof(int64_t)));
		Store<int16_t>(target_value, target.Ptr());
		return true;
	}
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8: {
		D_ASSERT(source.target_physical_type == PhysicalType::INT8);
		const auto target_value =
		    AggregateCheckedGroupKeyCast<int32_t, int8_t>(Load<int32_t>(source_data + source_idx * sizeof(int32_t)));
		Store<int8_t>(target_value, target.Ptr());
		return true;
	}
	case ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS: {
		switch (source.source_physical_type) {
		case PhysicalType::INT8:
			return AggregateLoadIntegralCompressedGroupKeyValue<int8_t>(source_data, source_idx, target, source);
		case PhysicalType::INT16:
			return AggregateLoadIntegralCompressedGroupKeyValue<int16_t>(source_data, source_idx, target, source);
		case PhysicalType::INT32:
			return AggregateLoadIntegralCompressedGroupKeyValue<int32_t>(source_data, source_idx, target, source);
		case PhysicalType::INT64:
			return AggregateLoadIntegralCompressedGroupKeyValue<int64_t>(source_data, source_idx, target, source);
		default:
			return false;
		}
	}
	case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS: {
		if (source.source_physical_type != PhysicalType::VARCHAR) {
			return false;
		}
		const auto value = Load<string_t>(source_data + source_idx * sizeof(string_t));
		switch (source.target_physical_type) {
		case PhysicalType::UINT8:
			Store<uint8_t>(AggregateStringCompressUInt8(value), target.Ptr());
			return true;
		case PhysicalType::UINT16:
			Store<uint16_t>(AggregateStringCompressWide<uint16_t>(value), target.Ptr());
			return true;
		case PhysicalType::UINT32:
			Store<uint32_t>(AggregateStringCompressWide<uint32_t>(value), target.Ptr());
			return true;
		case PhysicalType::UINT64:
			Store<uint64_t>(AggregateStringCompressWide<uint64_t>(value), target.Ptr());
			return true;
		case PhysicalType::UINT128:
			Store<uhugeint_t>(AggregateStringCompressWide<uhugeint_t>(value), target.Ptr());
			return true;
		default:
			return false;
		}
	}
	default:
		return false;
	}
}

static bool AggregateDescriptorGroupChunkMatches(DataChunk &chunk, const vector<LogicalType> &group_types) {
	if (chunk.ColumnCount() != group_types.size()) {
		return false;
	}
	for (idx_t group_idx = 0; group_idx < group_types.size(); group_idx++) {
		if (chunk.data[group_idx].GetType() != group_types[group_idx]) {
			return false;
		}
	}
	return true;
}

static void AggregateEnsureDescriptorGroupChunk(Allocator &allocator, DataChunk &chunk,
                                                const vector<LogicalType> &group_types) {
	if (AggregateDescriptorGroupChunkMatches(chunk, group_types)) {
		return;
	}
	chunk.Destroy();
	chunk.Initialize(allocator, group_types);
}

struct AggregateRowPointerDescriptorSourceInfo {
	idx_t group_count = 0;
	const data_ptr_t *row_pointers = nullptr;
	DataChunk *payload_input = nullptr;
	std::array<UnifiedVectorFormat, AGGREGATE_MAX_FAST_GROUPS> input_formats;
};

static bool
AggregateRowPointerDescriptorSourcesCanProbeDirect(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	static constexpr idx_t DIRECT_DESCRIPTOR_GROUP_LIMIT = 3;
	return group_sources.size() <= DIRECT_DESCRIPTOR_GROUP_LIMIT;
}

static bool
AggregatePrepareRowPointerDescriptorSourceInfo(DataChunk &payload_input, Vector &row_pointers, idx_t count,
                                               const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                               const vector<LogicalType> &layout_types,
                                               AggregateRowPointerDescriptorSourceInfo &info) {
	if (!AggregateDescriptorGroupKeySourcesSupported(group_sources, layout_types) ||
	    !AggregateRowPointerDescriptorSourcesCanProbeDirect(group_sources)) {
		return false;
	}
	info.group_count = group_sources.size();
	info.row_pointers = FlatVector::GetData<data_ptr_t>(row_pointers);
	info.payload_input = &payload_input;
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			continue;
		}
		if (source.input_vector_index >= payload_input.ColumnCount()) {
			return false;
		}
		auto &input = payload_input.data[source.input_vector_index];
		if (input.GetType().InternalType() != source.source_physical_type ||
		    input.GetType().InternalType() != source.target_physical_type) {
			return false;
		}
		input.ToUnifiedFormat(info.input_formats[group_idx]);
	}
	return true;
}

static bool AggregateDescriptorSourceValueData(const AggregateRowPointerDescriptorSourceInfo &info,
                                               const ExecutionRowPointerGroupKeySource &source, idx_t group_idx,
                                               idx_t row_idx, const_data_ptr_t &source_data, idx_t &source_idx) {
	if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
		auto row_pointer = info.row_pointers[row_idx];
		if (!AggregateRowPointerGroupKeySourceIsValid(row_pointer, source)) {
			return false;
		}
		source_data = row_pointer + source.row_layout_offset;
		source_idx = 0;
		return true;
	}
	if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
		auto &format = info.input_formats[group_idx];
		source_idx = format.sel->get_index(row_idx);
		if (!format.validity.RowIsValid(source_idx)) {
			return false;
		}
		source_data = format.data;
		return true;
	}
	return false;
}

static hash_t AggregateHashDescriptorValue(const_data_ptr_t source_data, idx_t source_idx, PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
		return Hash(Load<bool>(source_data + source_idx * sizeof(bool)));
	case PhysicalType::UINT8:
		return Hash(Load<uint8_t>(source_data + source_idx * sizeof(uint8_t)));
	case PhysicalType::INT8:
		return Hash(Load<int8_t>(source_data + source_idx * sizeof(int8_t)));
	case PhysicalType::UINT16:
		return Hash(Load<uint16_t>(source_data + source_idx * sizeof(uint16_t)));
	case PhysicalType::INT16:
		return Hash(Load<int16_t>(source_data + source_idx * sizeof(int16_t)));
	case PhysicalType::UINT32:
		return Hash(Load<uint32_t>(source_data + source_idx * sizeof(uint32_t)));
	case PhysicalType::INT32:
		return Hash(Load<int32_t>(source_data + source_idx * sizeof(int32_t)));
	case PhysicalType::UINT64:
		return Hash(Load<uint64_t>(source_data + source_idx * sizeof(uint64_t)));
	case PhysicalType::INT64:
		return Hash(Load<int64_t>(source_data + source_idx * sizeof(int64_t)));
	case PhysicalType::UINT128:
		return Hash(Load<uhugeint_t>(source_data + source_idx * sizeof(uhugeint_t)));
	case PhysicalType::INT128:
		return Hash(Load<hugeint_t>(source_data + source_idx * sizeof(hugeint_t)));
	case PhysicalType::INTERVAL:
		return Hash(Load<interval_t>(source_data + source_idx * sizeof(interval_t)));
	case PhysicalType::VARCHAR:
		return Hash(Load<string_t>(source_data + source_idx * sizeof(string_t)));
	default:
		throw InternalException("Unsupported direct row-pointer aggregate descriptor hash type");
	}
}

static bool AggregateHashDescriptorRows(const AggregateRowPointerDescriptorSourceInfo &info,
                                        const vector<ExecutionRowPointerGroupKeySource> &group_sources, idx_t count,
                                        Vector &hashes) {
	hashes.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(hashes, count_t(count));
	auto hash_data = FlatVector::GetDataMutable<hash_t>(hashes);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		hash_t result = 0;
		for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
			auto &source = group_sources[group_idx];
			const_data_ptr_t source_data;
			idx_t source_idx;
			if (!AggregateDescriptorSourceValueData(info, source, group_idx, row_idx, source_data, source_idx)) {
				return false;
			}
			hash_t value_hash;
			if (source.cast_kind == ExecutionRowPointerGroupKeyCastKind::NONE &&
			    source.source_physical_type == source.target_physical_type) {
				const auto value_size = AggregateDescriptorValueSize(source.target_physical_type);
				value_hash =
				    AggregateHashDescriptorValue(source_data + source_idx * value_size, 0, source.target_physical_type);
			} else {
				AggregateDescriptorValue value;
				if (!AggregateLoadDescriptorGroupKeyValue(source_data, source_idx, value, source)) {
					return false;
				}
				value_hash = AggregateHashDescriptorValue(value.Ptr(), 0, source.target_physical_type);
			}
			result = group_idx == 0 ? value_hash : CombineHashScalar(result, value_hash);
		}
		hash_data[row_idx] = result;
	}
	return true;
}

static bool AggregateFillDescriptorGroupChunkAndHashes(Allocator &allocator, DataChunk &payload_input,
                                                       Vector &row_pointers, idx_t count,
                                                       const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                       const vector<LogicalType> &layout_types, DataChunk &groups,
                                                       Vector &hashes) {
	if (!AggregateDescriptorGroupKeySourcesSupported(group_sources, layout_types)) {
		return false;
	}

	AggregateRowPointerDescriptorSourceInfo info;
	info.group_count = group_sources.size();
	info.row_pointers = FlatVector::GetData<data_ptr_t>(row_pointers);
	info.payload_input = &payload_input;
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			continue;
		}
		if (source.input_vector_index >= payload_input.ColumnCount()) {
			return false;
		}
		auto &input = payload_input.data[source.input_vector_index];
		if (input.GetType().InternalType() != source.source_physical_type) {
			return false;
		}
		input.ToUnifiedFormat(info.input_formats[group_idx]);
	}

	vector<LogicalType> group_types;
	group_types.reserve(group_sources.size());
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		group_types.push_back(layout_types[group_idx]);
	}
	AggregateEnsureDescriptorGroupChunk(allocator, groups, group_types);

	bool row_pointer_no_cast_sources = row_pointers.GetVectorType() == VectorType::FLAT_VECTOR;
	for (auto &source : group_sources) {
		if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD ||
		    source.cast_kind != ExecutionRowPointerGroupKeyCastKind::NONE ||
		    source.source_physical_type != source.target_physical_type || !source.all_valid ||
		    source.row_layout_offset == DConstants::INVALID_INDEX) {
			row_pointer_no_cast_sources = false;
			break;
		}
	}
	if (row_pointer_no_cast_sources) {
		std::array<data_ptr_t, AGGREGATE_MAX_FAST_GROUPS> target_data;
		std::array<idx_t, AGGREGATE_MAX_FAST_GROUPS> value_sizes;
		for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
			auto &target = groups.data[group_idx];
			target.SetVectorType(VectorType::FLAT_VECTOR);
			FlatVector::ValidityMutable(target).SetAllValid(count);
			FlatVector::SetSize(target, count_t(count));
			target_data[group_idx] = FlatVector::GetDataMutable(target);
			value_sizes[group_idx] = AggregateDescriptorValueSize(group_sources[group_idx].target_physical_type);
		}

		hashes.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::SetSize(hashes, count_t(count));
		auto hash_data = FlatVector::GetDataMutable<hash_t>(hashes);
		const auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto row_pointer = row_pointer_data[row_idx];
			if (!row_pointer) {
				return false;
			}
			hash_t result = 0;
			for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
				auto &source = group_sources[group_idx];
				const auto value_size = value_sizes[group_idx];
				auto source_value = row_pointer + source.row_layout_offset;
				std::memcpy(target_data[group_idx] + row_idx * value_size, source_value, value_size);
				const auto value_hash = AggregateHashDescriptorValue(source_value, 0, source.target_physical_type);
				result = group_idx == 0 ? value_hash : CombineHashScalar(result, value_hash);
			}
			hash_data[row_idx] = result;
		}
		groups.SetChildCardinality(count);
		return true;
	}

	hashes.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(hashes, count_t(count));
	auto hash_data = FlatVector::GetDataMutable<hash_t>(hashes);
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		auto &target = groups.data[group_idx];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::ValidityMutable(target).SetAllValid(count);
		FlatVector::SetSize(target, count_t(count));
		auto target_data = FlatVector::GetDataMutable(target);
		const auto value_size = AggregateDescriptorValueSize(source.target_physical_type);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const_data_ptr_t source_data;
			idx_t source_idx;
			if (!AggregateDescriptorSourceValueData(info, source, group_idx, row_idx, source_data, source_idx)) {
				return false;
			}
			hash_t value_hash;
			if (source.cast_kind == ExecutionRowPointerGroupKeyCastKind::NONE &&
			    source.source_physical_type == source.target_physical_type) {
				auto source_value = source_data + source_idx * value_size;
				std::memcpy(target_data + row_idx * value_size, source_value, value_size);
				value_hash = AggregateHashDescriptorValue(source_value, 0, source.target_physical_type);
			} else {
				AggregateDescriptorValue value;
				if (!AggregateLoadDescriptorGroupKeyValue(source_data, source_idx, value, source)) {
					return false;
				}
				std::memcpy(target_data + row_idx * value_size, value.Ptr(), value_size);
				value_hash = AggregateHashDescriptorValue(value.Ptr(), 0, source.target_physical_type);
			}
			hash_data[row_idx] = group_idx == 0 ? value_hash : CombineHashScalar(hash_data[row_idx], value_hash);
		}
	}
	groups.SetChildCardinality(count);
	return true;
}

static bool AggregateDescriptorValuesMatch(const AggregateDescriptorValue &left, const AggregateDescriptorValue &right,
                                           PhysicalType type) {
	if (type == PhysicalType::VARCHAR) {
		return Load<string_t>(left.Ptr()) == Load<string_t>(right.Ptr());
	}
	return memcmp(left.Ptr(), right.Ptr(), AggregateDescriptorValueSize(type)) == 0;
}

static bool AggregateDescriptorSourceValuesMatch(const AggregateRowPointerDescriptorSourceInfo &info,
                                                 const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                 idx_t row_idx, idx_t other_row_idx) {
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		const_data_ptr_t left_data;
		const_data_ptr_t right_data;
		idx_t left_idx;
		idx_t right_idx;
		AggregateDescriptorValue left_value;
		AggregateDescriptorValue right_value;
		if (!AggregateDescriptorSourceValueData(info, source, group_idx, row_idx, left_data, left_idx) ||
		    !AggregateDescriptorSourceValueData(info, source, group_idx, other_row_idx, right_data, right_idx)) {
			return false;
		}
		if (!AggregateLoadDescriptorGroupKeyValue(left_data, left_idx, left_value, source) ||
		    !AggregateLoadDescriptorGroupKeyValue(right_data, right_idx, right_value, source) ||
		    !AggregateDescriptorValuesMatch(left_value, right_value, source.target_physical_type)) {
			return false;
		}
	}
	return true;
}

static bool AggregateDescriptorSourceRowMatchesStored(const AggregateRowPointerDescriptorSourceInfo &info,
                                                      const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                      const vector<idx_t> &layout_offsets, data_ptr_t row_location,
                                                      idx_t row_idx) {
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		const_data_ptr_t source_data;
		idx_t source_idx;
		AggregateDescriptorValue value;
		if (!AggregateDescriptorSourceValueData(info, source, group_idx, row_idx, source_data, source_idx)) {
			return false;
		}
		if (!AggregateLoadDescriptorGroupKeyValue(source_data, source_idx, value, source)) {
			return false;
		}
		const auto value_size = AggregateDescriptorValueSize(source.target_physical_type);
		if (!AggregateFastExistingValueMatches(value.Ptr(), row_location, source.target_physical_type, 0,
		                                       layout_offsets[group_idx], value_size)) {
			return false;
		}
	}
	return true;
}

static bool AggregateFillSelectedDescriptorGroupChunk(Allocator &allocator,
                                                      const AggregateRowPointerDescriptorSourceInfo &info,
                                                      const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                      const SelectionVector &selected_rows, idx_t selected_count,
                                                      idx_t count, const vector<LogicalType> &layout_types,
                                                      DataChunk &groups) {
	vector<LogicalType> group_types;
	group_types.reserve(group_sources.size());
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		group_types.push_back(layout_types[group_idx]);
	}
	AggregateEnsureDescriptorGroupChunk(allocator, groups, group_types);
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		auto &target = groups.data[group_idx];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::ValidityMutable(target).SetAllValid(count);
		FlatVector::SetSize(target, count_t(count));
		for (idx_t selected_idx = 0; selected_idx < selected_count; selected_idx++) {
			const auto row_idx = selected_rows.get_index_unsafe(selected_idx);
			const_data_ptr_t source_data;
			idx_t source_idx;
			if (!AggregateDescriptorSourceValueData(info, source, group_idx, row_idx, source_data, source_idx) ||
			    !AggregateStoreDescriptorGroupKeyValue(source_data, source_idx, target, row_idx, source)) {
				return false;
			}
		}
	}
	groups.SetChildCardinality(count);
	return true;
}

bool GroupedAggregateHashTable::TryFindOrCreateGroupsRowPointerSelectedStateUpdateDirect(
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction selected_update_function, void *selected_update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	if (!selected_update_function || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	if (count == 0) {
		return true;
	}
	if (payload_input.size() != count || skip_lookups || !entries || !layout_ptr->CannotHaveNull()) {
		return false;
	}

	AggregateRowPointerDescriptorSourceInfo sources;
	if (!AggregatePrepareRowPointerDescriptorSourceInfo(payload_input, row_pointers, count, group_sources,
	                                                    layout_ptr->GetTypes(), sources)) {
		return false;
	}

	auto hash_start = AggregateTraceStart(recorder);
	if (!AggregateHashDescriptorRows(sources, group_sources, count, state.hashes)) {
		RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_descriptor.hash_miss", hash_start);
		return false;
	}
	RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_descriptor.hash", hash_start);
	if (enable_hll) {
		auto hll_start = AggregateTraceStart(recorder);
		hll.Update(state.hashes);
		RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_descriptor.hll", hll_start);
	}

	if (Count() + count > capacity || Count() + count > ResizeThreshold()) {
		auto resize_start = AggregateTraceStart(recorder);
		Verify();
		Resize(capacity * 2);
		RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_descriptor.resize", resize_start);
	}

	state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
	auto selected_addresses = FlatVector::GetDataMutable<uintptr_t>(state.addresses);
	const auto hashes = FlatVector::GetData<hash_t>(state.hashes);
	auto ht_offsets = FlatVector::GetDataMutable<uint64_t>(state.ht_offsets);
	auto duplicate_targets = FlatVector::GetDataMutable<hash_t>(state.hash_salts);

	idx_t new_group_count = 0;
	idx_t duplicate_count = 0;
	idx_t selected_existing_count = 0;
	auto clear_marked_entries = [&]() {
		for (idx_t marked_idx = 0; marked_idx < new_group_count; marked_idx++) {
			entries[ht_offsets[marked_idx]] = ht_entry_t();
		}
		new_group_count = 0;
	};
	auto emit_existing_state = [&](data_ptr_t row_location, idx_t row_idx) {
		selected_addresses[selected_existing_count] = reinterpret_cast<uintptr_t>(row_location);
		state.existing_groups.set_index(selected_existing_count, row_idx);
		selected_existing_count++;
	};
	enum class LastMatchKind : uint8_t { NONE, NEW_GROUP, EXISTING_GROUP };
	LastMatchKind last_match_kind = LastMatchKind::NONE;
	idx_t last_row_idx = DConstants::INVALID_INDEX;
	idx_t last_new_group_idx = DConstants::INVALID_INDEX;
	data_ptr_t last_existing_row_location = nullptr;
	auto emit_new_duplicate = [&](idx_t row_idx, idx_t new_group_idx) {
		state.no_match_vector.set_index(duplicate_count, row_idx);
		duplicate_targets[duplicate_count] = new_group_idx;
		duplicate_count++;
		last_row_idx = row_idx;
	};
	auto remember_new_group = [&](idx_t row_idx, idx_t new_group_idx) {
		last_match_kind = LastMatchKind::NEW_GROUP;
		last_row_idx = row_idx;
		last_new_group_idx = new_group_idx;
	};
	auto remember_existing_group = [&](idx_t row_idx, data_ptr_t row_location) {
		last_match_kind = LastMatchKind::EXISTING_GROUP;
		last_row_idx = row_idx;
		last_existing_row_location = row_location;
	};
	auto try_emit_repeated_match = [&](idx_t row_idx) {
		if (last_match_kind == LastMatchKind::NONE || last_row_idx == DConstants::INVALID_INDEX ||
		    hashes[row_idx] != hashes[last_row_idx] ||
		    !AggregateDescriptorSourceValuesMatch(sources, group_sources, row_idx, last_row_idx)) {
			return false;
		}
		if (last_match_kind == LastMatchKind::NEW_GROUP) {
			emit_new_duplicate(row_idx, last_new_group_idx);
			return true;
		}
		emit_existing_state(last_existing_row_location, row_idx);
		last_row_idx = row_idx;
		return true;
	};

	auto probe_start = AggregateTraceStart(recorder);
	bool use_consecutive_reuse = false;
	const auto sample_count = MinValue<idx_t>(count, 64);
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		if (hashes[row_idx] == hashes[row_idx - 1] &&
		    AggregateDescriptorSourceValuesMatch(sources, group_sources, row_idx, row_idx - 1)) {
			use_consecutive_reuse = true;
			break;
		}
	}
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto hash = hashes[row_idx];
		const auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = ApplyBitMask(hash);

		if (use_consecutive_reuse && try_emit_repeated_match(row_idx)) {
			continue;
		}

		idx_t iteration_count;
		for (iteration_count = 0; iteration_count < capacity; iteration_count++) {
			auto &entry = entries[ht_offset];
			if (!entry.IsOccupied()) {
				ht_offsets[new_group_count] = ht_offset;
				state.new_groups.set_index(new_group_count, row_idx);
				if (use_consecutive_reuse) {
					remember_new_group(row_idx, new_group_count);
				}
				new_group_count++;
				entry.SetSalt(salt);
				break;
			}
			if (entry.GetSalt() == salt) {
				auto row_location = entry.GetPointer();
				if (cast_pointer_to_uint64(row_location) == ht_entry_t::POINTER_MASK) {
					bool found_tentative_match = false;
					for (idx_t marked_idx = 0; marked_idx < new_group_count; marked_idx++) {
						if (ht_offsets[marked_idx] != ht_offset) {
							continue;
						}
						const auto marked_row_idx = state.new_groups.get_index_unsafe(marked_idx);
						if (!AggregateDescriptorSourceValuesMatch(sources, group_sources, row_idx, marked_row_idx)) {
							continue;
						}
						state.no_match_vector.set_index(duplicate_count, row_idx);
						duplicate_targets[duplicate_count] = marked_idx;
						duplicate_count++;
						if (use_consecutive_reuse) {
							remember_new_group(row_idx, marked_idx);
						}
						found_tentative_match = true;
						break;
					}
					if (found_tentative_match) {
						break;
					}
					SaltIncrementAndWrap(ht_offset, salt, bitmask);
					continue;
				}
				if (AggregateDescriptorSourceRowMatchesStored(sources, group_sources, layout_ptr->GetOffsets(),
				                                              row_location, row_idx)) {
					emit_existing_state(row_location, row_idx);
					if (use_consecutive_reuse) {
						remember_existing_group(row_idx, row_location);
					}
					break;
				}
			}
			SaltIncrementAndWrap(ht_offset, salt, bitmask);
		}
		if (iteration_count == capacity) {
			clear_marked_entries();
			throw InternalException("Maximum direct row-pointer find-or-create iteration count reached in "
			                        "GroupedAggregateHashTable");
		}
	}
	RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_descriptor.probe", probe_start);

	if (new_group_count == 0) {
		if (selected_existing_count > 0) {
			auto update_start = AggregateTraceStart(recorder);
			selected_update_function(selected_addresses, nullptr, state.existing_groups.data(), selected_existing_count,
			                         selected_update_state);
			RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_descriptor.selected_state_update",
			                          update_start);
		}
		sink_count += count;
		return true;
	}

	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout_ptr->GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout_ptr->GetTypes().size());
	auto append_fill_start = AggregateTraceStart(recorder);
	if (!AggregateFillSelectedDescriptorGroupChunk(allocator, sources, group_sources, state.new_groups, new_group_count,
	                                               count, layout_ptr->GetTypes(), state.descriptor_group_chunk)) {
		clear_marked_entries();
		RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_descriptor.append_key_fill_miss",
		                          append_fill_start);
		return false;
	}
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		state.group_chunk.data[group_idx].Reference(state.descriptor_group_chunk.data[group_idx]);
	}
	state.group_chunk.data[group_sources.size()].Reference(state.hashes);
	TupleDataCollection::ToUnifiedFormat(state.partitioned_append_state.chunk_state, state.group_chunk);
	RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_descriptor.append_key_fill", append_fill_start);

	auto append_start = AggregateTraceStart(recorder);
	optional_ptr<PartitionedTupleData> data;
	optional_ptr<PartitionedTupleDataAppendState> append_state;
	if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD &&
	    new_group_count / RadixPartitioning::NumberOfPartitions(radix_bits) <= 4) {
		TupleDataCollection::ToUnifiedFormat(state.unpartitioned_append_state.chunk_state, state.group_chunk);
		data = unpartitioned_data.get();
		append_state = &state.unpartitioned_append_state;
	} else {
		data = partitioned_data.get();
		append_state = &state.partitioned_append_state;
	}
	data->AppendUnified(*append_state, state.group_chunk, state.new_groups, new_group_count);
	RowOperations::InitializeStates(*layout_ptr, append_state->chunk_state.row_locations,
	                                *FlatVector::IncrementalSelectionVector(), new_group_count);
	const auto row_locations = FlatVector::GetData<data_ptr_t>(append_state->chunk_state.row_locations);
	const auto &row_sel = append_state->reverse_partition_sel;
	for (idx_t new_idx = 0; new_idx < new_group_count; new_idx++) {
		const auto row_idx = state.new_groups.get_index_unsafe(new_idx);
		const auto row_location = row_locations[row_sel.get_index_unsafe(row_idx)];
		entries[ht_offsets[new_idx]].SetPointer(row_location);
	}
	for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
		const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
		D_ASSERT(entries[ht_offsets[marked_idx]].GetPointer());
	}
	if (selected_existing_count > 0) {
		auto update_start = AggregateTraceStart(recorder);
		selected_update_function(selected_addresses, nullptr, state.existing_groups.data(), selected_existing_count,
		                         selected_update_state);
		RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_descriptor.selected_existing_update",
		                          update_start);
	}
	if (new_group_count > 0) {
		auto update_start = AggregateTraceStart(recorder);
		for (idx_t new_idx = 0; new_idx < new_group_count; new_idx++) {
			const auto row_idx = state.new_groups.get_index_unsafe(new_idx);
			selected_addresses[new_idx] = reinterpret_cast<uintptr_t>(row_locations[row_sel.get_index_unsafe(row_idx)]);
		}
		selected_update_function(selected_addresses, nullptr, state.new_groups.data(), new_group_count,
		                         selected_update_state);
		RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_descriptor.selected_new_update", update_start);
	}
	if (duplicate_count > 0) {
		auto update_start = AggregateTraceStart(recorder);
		for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
			const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
			selected_addresses[duplicate_idx] =
			    reinterpret_cast<uintptr_t>(entries[ht_offsets[marked_idx]].GetPointer());
		}
		selected_update_function(selected_addresses, nullptr, state.no_match_vector.data(), duplicate_count,
		                         selected_update_state);
		RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_descriptor.selected_duplicate_update",
		                          update_start);
	}
	this->count += new_group_count;
	sink_count += count;
	RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_descriptor.append_new_groups", append_start);
	return true;
}

bool GroupedAggregateHashTable::TryFindOrCreateGroupsRowPointerSelectedStateUpdateFast(
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	if (!update_function || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	if (count == 0) {
		return true;
	}
	if (payload_input.size() != count || skip_lookups || !entries || !layout_ptr->CannotHaveNull() ||
	    !AggregateDescriptorGroupKeySourcesSupported(group_sources, layout_ptr->GetTypes())) {
		return false;
	}
	if (TryFindOrCreateGroupsRowPointerSelectedStateUpdateDirect(payload_input, row_pointers, count, group_sources,
	                                                             update_function, update_state, recorder)) {
		return true;
	}
	auto fill_hash_start = AggregateTraceStart(recorder);
	if (!AggregateFillDescriptorGroupChunkAndHashes(allocator, payload_input, row_pointers, count, group_sources,
	                                                layout_ptr->GetTypes(), state.descriptor_group_chunk,
	                                                state.hashes)) {
		RecordAggregateTraceStage(recorder, "find_or_create_descriptor_keys.fill_hash_miss", fill_hash_start);
		return false;
	}
	RecordAggregateTraceStage(recorder, "find_or_create_descriptor_keys.fill_hash", fill_hash_start);

	return TryFindOrCreateGroupsFastInternal(state.descriptor_group_chunk, nullptr, update_function, update_state,
	                                         recorder, optional_ptr<Vector>(&state.hashes));
}

bool GroupedAggregateHashTable::TryAppendNewGroupAddressesFast(DataChunk &groups, Vector &addresses_out,
                                                               optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	return TryAppendNewGroupsFastInternal(groups, addresses_out, nullptr, nullptr, recorder);
}

bool GroupedAggregateHashTable::TryAppendNewGroupsFastInternal(
    DataChunk &groups, optional_ptr<Vector> addresses_out,
    ExecutionGroupedAggregateStateAddressUpdateFunction address_update_function, void *address_update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	const auto chunk_size = groups.size();
	if (!addresses_out && !address_update_function) {
		return false;
	}
	if (chunk_size == 0) {
		if (addresses_out) {
			FlatVector::SetSize(*addresses_out, 0);
		}
		return true;
	}
	if (skip_lookups || enable_hll || !entries || !layout_ptr->CannotHaveNull() || groups.ColumnCount() == 0 ||
	    !AggregateFastAppendNewGroupsSupported(groups)) {
		return false;
	}

	if (Count() + chunk_size > capacity || Count() + chunk_size > ResizeThreshold()) {
		auto resize_start = AggregateTraceStart(recorder);
		Verify();
		Resize(capacity * 2);
		RecordAggregateTraceStage(recorder, "find_new.resize", resize_start);
	}
	D_ASSERT(capacity - Count() >= chunk_size);

	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout_ptr->GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout_ptr->GetTypes().size());
	for (idx_t group_idx = 0; group_idx < groups.ColumnCount(); group_idx++) {
		state.group_chunk.data[group_idx].Reference(groups.data[group_idx]);
	}
	auto hash_start = AggregateTraceStart(recorder);
	groups.Hash(state.hashes);
	RecordAggregateTraceStage(recorder, "find_new.hash", hash_start);
	auto group_format_start = AggregateTraceStart(recorder);
	state.group_chunk.data[groups.ColumnCount()].Reference(state.hashes);
	TupleDataCollection::ToUnifiedFormat(state.partitioned_append_state.chunk_state, state.group_chunk);
	RecordAggregateTraceStage(recorder, "find_new.group_format", group_format_start);

	const auto hashes = state.hashes.Values<hash_t>();
	auto ht_offsets = FlatVector::GetDataMutable<uint64_t>(state.ht_offsets);
	auto hash_salts = FlatVector::GetDataMutable<hash_t>(state.hash_salts);
	auto mark_start = AggregateTraceStart(recorder);
	idx_t marked_count = 0;
	for (idx_t row_idx = 0; row_idx < chunk_size; row_idx++) {
		const auto hash = hashes[row_idx].GetValue();
		const auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = ApplyBitMask(hash);
		idx_t iteration_count = 0;
		for (; iteration_count < capacity; iteration_count++) {
			auto &entry = entries[ht_offset];
			if (!entry.IsOccupied()) {
				ht_offsets[marked_count] = ht_offset;
				hash_salts[marked_count] = salt;
				entry.SetSalt(salt);
				marked_count++;
				break;
			}
			if (entry.GetSalt() == salt) {
				for (idx_t marked_idx = 0; marked_idx < marked_count; marked_idx++) {
					entries[ht_offsets[marked_idx]] = ht_entry_t();
				}
				RecordAggregateTraceStage(recorder, "find_new.mark_empty_miss", mark_start);
				return false;
			}
			SaltIncrementAndWrap(ht_offset, salt, bitmask);
		}
		if (iteration_count == capacity) {
			for (idx_t marked_idx = 0; marked_idx < marked_count; marked_idx++) {
				entries[ht_offsets[marked_idx]] = ht_entry_t();
			}
			throw InternalException("Maximum fast new-group iteration count reached in GroupedAggregateHashTable");
		}
	}
	RecordAggregateTraceStage(recorder, "find_new.mark_empty", mark_start);

	auto append_start = AggregateTraceStart(recorder);
	data_ptr_t *addresses = nullptr;
	if (addresses_out) {
		addresses_out->Flatten();
		addresses = FlatVector::GetDataMutable<data_ptr_t>(*addresses_out);
	}
	optional_ptr<PartitionedTupleData> data;
	optional_ptr<PartitionedTupleDataAppendState> append_state;
	if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD &&
	    chunk_size / RadixPartitioning::NumberOfPartitions(radix_bits) <= 4) {
		TupleDataCollection::ToUnifiedFormat(state.unpartitioned_append_state.chunk_state, state.group_chunk);
		data = unpartitioned_data.get();
		append_state = &state.unpartitioned_append_state;
	} else {
		data = partitioned_data.get();
		append_state = &state.partitioned_append_state;
	}
	data->AppendUnified(*append_state, state.group_chunk, *FlatVector::IncrementalSelectionVector(), chunk_size);
	RowOperations::InitializeStates(*layout_ptr, append_state->chunk_state.row_locations,
	                                *FlatVector::IncrementalSelectionVector(), chunk_size);
	const auto row_locations = FlatVector::GetData<data_ptr_t>(append_state->chunk_state.row_locations);
	const auto &row_sel = append_state->reverse_partition_sel;
	for (idx_t row_idx = 0; row_idx < chunk_size; row_idx++) {
		const auto row_location = row_locations[row_sel.get_index_unsafe(row_idx)];
		entries[ht_offsets[row_idx]].SetPointer(row_location);
		if (addresses) {
			addresses[row_idx] = row_location;
		}
	}
	if (addresses_out) {
		FlatVector::SetSize(*addresses_out, chunk_size);
	}
	count += chunk_size;
	sink_count += chunk_size;
	RecordAggregateTraceStage(recorder, "find_new.append_new_groups", append_start);

	if (address_update_function) {
		auto update_start = AggregateTraceStart(recorder);
		const auto state_addresses = reinterpret_cast<const uintptr_t *>(row_locations);
		address_update_function(state_addresses, row_sel.data(), chunk_size, address_update_state);
		RecordAggregateTraceStage(recorder, "find_new.state_address_update", update_start);
	}
	return true;
}

bool GroupedAggregateHashTable::TryAppendNewGroupsWithStateAddressesFast(
    DataChunk &groups, ExecutionGroupedAggregateStateAddressUpdateFunction update_function, void *update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	if (!update_function) {
		return false;
	}
	return TryAppendNewGroupsFastInternal(groups, nullptr, update_function, update_state, recorder);
}

bool GroupedAggregateHashTable::GetExecutionHashAggregateLookupLayout(
    ExecutionHashAggregateLookupLayout &layout) const {
	if (!layout_ptr) {
		layout = ExecutionHashAggregateLookupLayout();
		layout.blocker = "hash-aggregate-layout-missing";
		return false;
	}
	if (!ExecutionBuildHashAggregateLookupLayout(*layout_ptr, layout)) {
		return false;
	}

	layout.pointer_table_ready = entries;
	layout.in_memory = layout.pointer_table_ready;
	layout.skip_lookups = skip_lookups;
	layout.capacity = capacity;
	layout.bitmask = bitmask;
	layout.entries = entries;
	if (!layout.pointer_table_ready) {
		layout.blocker = "hash-aggregate-pointer-table-missing";
		return false;
	}
	layout.ready = layout.table_layout_ready && layout.pointer_table_ready && layout.append_contract_ready &&
	               layout.row_compare_contract_ready && layout.backend_lowering_ready;
	return layout.pointer_table_ready;
}

idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	groups.Hash(state.hashes);
	return FindOrCreateGroups(groups, state.hashes, addresses_out, new_groups_out);
}

struct FlushMoveState {
	explicit FlushMoveState(TupleDataCollection &collection_p)
	    : collection(collection_p), hashes(LogicalType::HASH), group_addresses(LogicalType::POINTER),
	      new_groups_sel(STANDARD_VECTOR_SIZE) {
		const auto &layout = collection.GetLayout();
		vector<column_t> column_ids;
		column_ids.reserve(layout.ColumnCount() - 1);
		for (idx_t col_idx = 0; col_idx < layout.ColumnCount() - 1; col_idx++) {
			column_ids.emplace_back(col_idx);
		}
		collection.InitializeScan(scan_state, column_ids, TupleDataPinProperties::DESTROY_AFTER_DONE);
		collection.InitializeScanChunk(scan_state, groups);
		hash_col_idx = layout.ColumnCount() - 1;
	}

	bool Scan() {
		if (collection.Scan(scan_state, groups)) {
			collection.Gather(scan_state.chunk_state.row_locations, *FlatVector::IncrementalSelectionVector(),
			                  groups.size(), hash_col_idx, hashes, *FlatVector::IncrementalSelectionVector(), nullptr);
			return true;
		}

		collection.FinalizePinState(scan_state.pin_state);
		return false;
	}

	TupleDataCollection &collection;
	TupleDataScanState scan_state;
	DataChunk groups;

	idx_t hash_col_idx;
	Vector hashes;

	Vector group_addresses;
	SelectionVector new_groups_sel;
};

void GroupedAggregateHashTable::Combine(GroupedAggregateHashTable &other) {
	auto other_partitioned_data = other.AcquirePartitionedData();
	auto other_data = other_partitioned_data->GetUnpartitioned();
	Combine(*other_data);

	// Inherit ownership to all stored aggregate allocators
	stored_allocators.emplace_back(other.aggregate_allocator);
	for (const auto &stored_allocator : other.stored_allocators) {
		stored_allocators.emplace_back(stored_allocator);
	}
}

void GroupedAggregateHashTable::Combine(TupleDataCollection &other_data, optional_ptr<atomic<double>> progress) {
	D_ASSERT(other_data.GetLayout().GetAggrWidth() == layout_ptr->GetAggrWidth());
	D_ASSERT(other_data.GetLayout().GetDataWidth() == layout_ptr->GetDataWidth());
	D_ASSERT(other_data.GetLayout().GetRowWidth() == layout_ptr->GetRowWidth());

	if (other_data.Count() == 0) {
		return;
	}

	FlushMoveState fm_state(other_data);

	idx_t chunk_idx = 0;
	const auto chunk_count = other_data.ChunkCount();
	while (fm_state.Scan()) {
		// Check for interrupts with each chunk
		context.InterruptCheck();
		FindOrCreateGroups(fm_state.groups, fm_state.hashes, fm_state.group_addresses, fm_state.new_groups_sel);
		RowOperations::CombineStates(state.row_state, *layout_ptr, fm_state.scan_state.chunk_state.row_locations,
		                             fm_state.group_addresses);
		if (layout_ptr->HasDestructor()) {
			RowOperations::DestroyStates(state.row_state, *layout_ptr, fm_state.scan_state.chunk_state.row_locations);
		}

		if (progress) {
			*progress = static_cast<double>(++chunk_idx) / static_cast<double>(chunk_count);
		}
	}

	Verify();
}

void GroupedAggregateHashTable::InitializeScan(AggregateHTScanState &scan_state) {
	scan_state.partition_idx = 0;
	vector<idx_t> group_indexes(layout_ptr->ColumnCount() - 1);
	for (idx_t i = 0; i < group_indexes.size(); i++) {
		group_indexes[i] = i;
	}

	auto &partition = partitioned_data->GetPartitions()[scan_state.partition_idx];
	partition->InitializeScan(scan_state.scan_states, group_indexes);
}

bool GroupedAggregateHashTable::Scan(AggregateHTScanState &scan_state, DataChunk &distinct_rows,
                                     DataChunk &payload_rows) {
	if (scan_state.partition_idx >= partitioned_data->PartitionCount()) {
		return false;
	}

	payload_rows.Reset();
	distinct_rows.Reset();
	auto &current_partition = partitioned_data->GetPartitions()[scan_state.partition_idx];

	if (current_partition->Scan(scan_state.scan_states, distinct_rows)) {
		FetchAggregates(distinct_rows, payload_rows);
		return true;
	} else {
		if (++(scan_state.partition_idx) >= partitioned_data->PartitionCount()) {
			return false;
		} else {
			auto &new_partition = partitioned_data->GetPartitions()[scan_state.partition_idx];
			new_partition->InitializeScan(scan_state.scan_states);
			return true;
		}
	}
}

void GroupedAggregateHashTable::ResetForNewIteration(idx_t initial_capacity, idx_t radix_bits_p) {
	// Save the previous iteration's group count before destroying aggregate states.
	// This lets us size the pointer table based on actual prior data rather than the
	// global sink capacity, which is typically much larger than recursive iteration sizes.
	const auto prev_count = count;
	Destroy();
	aggregate_allocator->Reset();
	stored_allocators.clear();

	const auto reuse_partitioned_data = partitioned_data && RadixPartitioning::RadixBitsOfPowerOfTwo(
	                                                            partitioned_data->PartitionCount()) == radix_bits_p;
	const auto reuse_unpartitioned_data =
	    radix_bits_p >= UNPARTITIONED_RADIX_BITS_THRESHOLD && unpartitioned_data &&
	    RadixPartitioning::RadixBitsOfPowerOfTwo(unpartitioned_data->PartitionCount()) == 0;

	radix_bits = radix_bits_p;
	if (reuse_partitioned_data) {
		if (partitioned_data->Count() != 0) {
			partitioned_data->Reset();
		}
		partitioned_data->ResetAppendState(state.partitioned_append_state,
		                                   TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
	} else {
		InitializePartitionedData();
	}
	if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD) {
		if (reuse_unpartitioned_data) {
			if (unpartitioned_data->Count() != 0) {
				unpartitioned_data->Reset();
			}
			unpartitioned_data->ResetAppendState(state.unpartitioned_append_state,
			                                     TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
		} else {
			InitializeUnpartitionedData();
		}
	} else {
		unpartitioned_data.reset();
	}

	count = 0;
	sink_count = 0;
	skip_lookups = false;
	enable_hll = false;
	hll = HyperLogLog();
	state.compressed_group_state.dictionary_id = string();

	// Compute effective capacity based on the previous iteration's actual group count.
	// This avoids clearing a large pointer table (O(max_capacity)) when prior iterations
	// processed few rows. The HT will grow during the iteration if more groups are encountered.
	const auto effective_capacity = GetCapacityForCount(prev_count);
	if (!hash_map.IsSet() || capacity < effective_capacity) {
		Resize(effective_capacity);
	} else {
		// Logically shrink to effective_capacity: only zero the portion we need.
		// Entries beyond effective_capacity are unreachable (bitmask limits all accesses),
		// so leaving them uncleared is safe. The physical buffer is reused in place.
		capacity = effective_capacity;
		entries = reinterpret_cast<ht_entry_t *>(hash_map.get());
		bitmask = capacity - 1;
		ClearPointerTable();
	}
}
} // namespace duckdb
