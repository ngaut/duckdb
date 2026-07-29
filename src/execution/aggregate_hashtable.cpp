#include "duckdb/execution/aggregate_hashtable.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"
#include "duckdb/execution/execution_region_settings.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

#include <array>
#include <chrono>
#include <cstring>
#include <type_traits>
#include <utility>

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

static constexpr idx_t AGGREGATE_MAX_FAST_GROUPS = 8;
static constexpr idx_t AGGREGATE_ROW_POINTER_DENSE_TARGET_MAX_ENTRIES = EXECUTION_DENSE_GROUP_DOMAIN_MAX_TARGET_ENTRIES;
static constexpr idx_t AGGREGATE_DENSE_TARGET_MAX_RANGE_PER_GROUP = 2;

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

static bool AggregateStoredGroupKeyIsValid(data_ptr_t row_location, const TupleDataLayout &layout, idx_t group_idx) {
	if (layout.CannotHaveNull()) {
		return true;
	}
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(group_idx, entry_idx, idx_in_entry);
	return ValidityBytes::RowIsValid(
	    ValidityBytes(row_location, layout.ColumnCount()).GetValidityEntryUnsafe(entry_idx), idx_in_entry);
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
	std::array<UnifiedVectorFormat, AGGREGATE_MAX_FAST_GROUPS> formats;
	std::array<const_data_ptr_t, AGGREGATE_MAX_FAST_GROUPS> source_data;
	std::array<const SelectionVector *, AGGREGATE_MAX_FAST_GROUPS> source_sel;
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
		auto physical_type = layout_types[group_idx].InternalType();
		if (source.GetType().InternalType() != physical_type || !AggregateFastExistingMatchType(physical_type)) {
			return false;
		}
		source.ToUnifiedFormat(sources.formats[group_idx]);
		auto &format = sources.formats[group_idx];
		if (format.physical_type != physical_type) {
			return false;
		}
		if (format.validity.CanHaveNull()) {
			for (idx_t row_idx = 0; row_idx < chunk_size; row_idx++) {
				const auto source_idx = format.sel->get_index(row_idx);
				if (!format.validity.RowIsValid(source_idx)) {
					return false;
				}
			}
		}
		sources.source_data[group_idx] = format.data;
		sources.source_sel[group_idx] = format.sel;
		sources.physical_types[group_idx] = physical_type;
		sources.value_sizes[group_idx] =
		    physical_type == PhysicalType::VARCHAR ? sizeof(string_t) : GetTypeIdSize(physical_type);
	}
	return true;
}

static idx_t AggregateFastGroupSourceIndex(const AggregateFastGroupSourceInfo &sources, idx_t group_idx,
                                           idx_t row_idx) {
	return sources.source_sel[group_idx]->get_index(row_idx);
}

static bool AggregateFastGroupSourceRowsMatch(const AggregateFastGroupSourceInfo &sources, idx_t row_idx,
                                              idx_t other_row_idx) {
	for (idx_t group_idx = 0; group_idx < sources.group_count; group_idx++) {
		const auto source_idx = AggregateFastGroupSourceIndex(sources, group_idx, row_idx);
		const auto other_source_idx = AggregateFastGroupSourceIndex(sources, group_idx, other_row_idx);
		if (!AggregateFastSourceValuesMatch(sources.source_data[group_idx], sources.physical_types[group_idx],
		                                    source_idx, other_source_idx, sources.value_sizes[group_idx])) {
			return false;
		}
	}
	return true;
}

static bool AggregateFastExistingRowMatches(const AggregateFastGroupSourceInfo &sources, const TupleDataLayout &layout,
                                            const data_ptr_t row_location, idx_t row_idx) {
	const auto &layout_offsets = layout.GetOffsets();
	for (idx_t group_idx = 0; group_idx < sources.group_count; group_idx++) {
		if (!AggregateStoredGroupKeyIsValid(row_location, layout, group_idx)) {
			return false;
		}
		const auto source_idx = AggregateFastGroupSourceIndex(sources, group_idx, row_idx);
		if (!AggregateFastExistingValueMatches(sources.source_data[group_idx], row_location,
		                                       sources.physical_types[group_idx], source_idx, layout_offsets[group_idx],
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
      descriptor_group_hashes(LogicalType::HASH), row_state(allocator) {
}

void GroupedAggregateHashTable::AggregateDenseSingleFieldTargetCache::Reset() {
	physical_type = PhysicalType::INVALID;
	layout_offset = DConstants::INVALID_INDEX;
	base_key = 0;
	disabled = false;
	addresses.clear();
}

void GroupedAggregateHashTable::AggregateDenseSingleFieldTargetCache::Disable() {
	Reset();
	disabled = true;
}

bool GroupedAggregateHashTable::AggregateDenseSingleFieldTargetCache::Configure(PhysicalType physical_type_p,
                                                                                idx_t layout_offset_p) {
	if (disabled) {
		return false;
	}
	if (physical_type == PhysicalType::INVALID) {
		physical_type = physical_type_p;
		layout_offset = layout_offset_p;
		return true;
	}
	if (physical_type != physical_type_p || layout_offset != layout_offset_p) {
		Disable();
		return false;
	}
	return true;
}

bool GroupedAggregateHashTable::AggregateDenseSingleFieldTargetCache::EnsureRange(idx_t min_key, idx_t max_key,
                                                                                  bool exact_size) {
	if (disabled || max_key < min_key) {
		return false;
	}
	const auto required_key_count = max_key - min_key + 1;
	if (required_key_count > AGGREGATE_ROW_POINTER_DENSE_TARGET_MAX_ENTRIES) {
		return false;
	}
	if (addresses.empty()) {
		base_key = min_key;
		idx_t new_size = required_key_count;
		if (!exact_size) {
			new_size = STANDARD_VECTOR_SIZE;
			while (new_size < required_key_count) {
				new_size *= 2;
			}
		}
		if (new_size > AGGREGATE_ROW_POINTER_DENSE_TARGET_MAX_ENTRIES) {
			new_size = AGGREGATE_ROW_POINTER_DENSE_TARGET_MAX_ENTRIES;
		}
		addresses.resize(new_size, 0);
		return true;
	}

	const auto current_key_count = MinValue<idx_t>(addresses.size(), NumericLimits<idx_t>::Maximum() - base_key + 1);
	const auto current_last_key = base_key + current_key_count - 1;
	const auto new_base_key = MinValue(base_key, min_key);
	const auto new_last_key = MaxValue(current_last_key, max_key);
	const auto new_key_count = new_last_key - new_base_key + 1;
	if (new_key_count > AGGREGATE_ROW_POINTER_DENSE_TARGET_MAX_ENTRIES) {
		return false;
	}
	if (new_base_key == base_key && addresses.size() >= new_key_count) {
		return true;
	}

	idx_t new_size = new_key_count;
	if (!exact_size) {
		new_size = addresses.size();
		while (new_size < new_key_count) {
			new_size *= 2;
		}
	}
	if (new_size > AGGREGATE_ROW_POINTER_DENSE_TARGET_MAX_ENTRIES) {
		new_size = AGGREGATE_ROW_POINTER_DENSE_TARGET_MAX_ENTRIES;
	}
	vector<uintptr_t> new_addresses(new_size, 0);
	const auto old_offset = base_key - new_base_key;
	for (idx_t old_idx = 0; old_idx < current_key_count; old_idx++) {
		new_addresses[old_offset + old_idx] = addresses[old_idx];
	}
	base_key = new_base_key;
	addresses = std::move(new_addresses);
	return true;
}

bool GroupedAggregateHashTable::AggregateDenseSingleFieldTargetCache::KeyInRange(idx_t key) const {
	return !addresses.empty() && key >= base_key && key - base_key < addresses.size();
}

idx_t GroupedAggregateHashTable::AggregateDenseSingleFieldTargetCache::KeyOffset(idx_t key) const {
	D_ASSERT(KeyInRange(key));
	return key - base_key;
}

uintptr_t GroupedAggregateHashTable::AggregateDenseSingleFieldTargetCache::GetAddress(idx_t key) const {
	const auto entry = addresses[KeyOffset(key)];
	return (entry & 1) == 0 ? entry : 0;
}

idx_t GroupedAggregateHashTable::AggregateDenseSingleFieldTargetCache::GetPendingNewGroup(idx_t key) const {
	const auto entry = addresses[KeyOffset(key)];
	return (entry & 1) != 0 ? entry >> 1 : DConstants::INVALID_INDEX;
}

void GroupedAggregateHashTable::AggregateDenseSingleFieldTargetCache::SetAddress(idx_t key, uintptr_t address) {
	D_ASSERT((address & 1) == 0);
	addresses[KeyOffset(key)] = address;
}

void GroupedAggregateHashTable::AggregateDenseSingleFieldTargetCache::SetPendingNewGroup(idx_t key, idx_t group_idx) {
	auto &entry = addresses[KeyOffset(key)];
	if (group_idx == DConstants::INVALID_INDEX) {
		if ((entry & 1) != 0) {
			entry = 0;
		}
		return;
	}
	D_ASSERT(entry == 0);
	D_ASSERT(group_idx <= (NumericLimits<uintptr_t>::Maximum() >> 1));
	entry = (group_idx << 1) | 1;
}

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context_p, Allocator &allocator,
                                                     vector<LogicalType> group_types_p,
                                                     vector<LogicalType> payload_types_p,
                                                     vector<AggregateObject> aggregate_objects_p,
                                                     idx_t initial_capacity, idx_t radix_bits,
                                                     TupleDataValidityType group_validity)
    : BaseAggregateHashTable(context_p, allocator, aggregate_objects_p, std::move(payload_types_p)), context(context_p),
      radix_bits(radix_bits), count(0), capacity(0), sink_count(0),
      require_canonical_group_hash(ExecutionRegionSettings::AdaptiveAb(context_p) ||
                                   ExecutionRegionSettings::DebugForceDeferAfterChunks(context_p) != 0),
      skip_lookups(false), enable_hll(false), aggregate_allocator(make_shared_ptr<ArenaAllocator>(allocator)),
      state(*aggregate_allocator), skip_lookups_require_final_combine(false),
      proven_unique_append_key_type(PhysicalType::INVALID), proven_unique_append_has_last_key(false),
      proven_unique_append_last_signed_key(0), proven_unique_append_last_unsigned_key(0),
      proven_unique_append_ranges_coalesced(false) {
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
	if ((layout_ptr->GetRowWidth() & 1) != 0) {
		// Pending dense-cache entries use the low pointer bit. Odd row strides cannot preserve that invariant.
		dense_single_field_target_cache.Disable();
	}
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
	if (!unpartitioned_data) {
		unpartitioned_data = make_uniq<RadixPartitionedTupleData>(buffer_manager, layout_ptr, MemoryTag::HASH_TABLE,
		                                                          0ULL, layout_ptr->ColumnCount() - 1);
	} else {
		unpartitioned_data->Reset();
	}
	unpartitioned_data->InitializeAppendState(state.unpartitioned_append_state,
	                                          TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
}

GroupedAggregateHashTable::AggregateHTAppendTarget
GroupedAggregateHashTable::PrepareAppendTarget(DataChunk &groups, idx_t group_count, bool defer_partitioning) {
	const bool defer_radix_partitioning = defer_partitioning && radix_bits > 0;
	if (defer_radix_partitioning && !unpartitioned_data) {
		InitializeUnpartitionedData();
	}
	const bool use_unpartitioned =
	    defer_radix_partitioning || (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD &&
	                                 group_count / RadixPartitioning::NumberOfPartitions(radix_bits) <= 4);
	if (use_unpartitioned) {
		D_ASSERT(unpartitioned_data);
		TupleDataCollection::ToUnifiedFormat(state.unpartitioned_append_state.chunk_state, groups);
		return {*unpartitioned_data, state.unpartitioned_append_state};
	}
	TupleDataCollection::ToUnifiedFormat(state.partitioned_append_state.chunk_state, groups);
	return {*partitioned_data, state.partitioned_append_state};
}

const PartitionedTupleData &GroupedAggregateHashTable::GetPartitionedData() const {
	return *partitioned_data;
}

unique_ptr<PartitionedTupleData>
GroupedAggregateHashTable::AcquirePartitionedData(optional_ptr<TupleDataRowLocationRemap> row_location_remap) {
	dense_single_field_target_cache.Disable();
	row_pointer_descriptor_target_cache.Reset();
	// The dictionary group cache stores row pointers; acquiring the data hands those rows
	// to a collection that may copy them, so the cache must not survive.
	state.compressed_group_state.dictionary_id = string();
	const bool needs_unpartitioned_data = radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD || unpartitioned_data;
	if (unpartitioned_data) {
		// Flush/unpin unpartitioned data and append to partitioned data
		unpartitioned_data->FlushAppendState(state.unpartitioned_append_state);
		unpartitioned_data->Unpin();
		unpartitioned_data->Repartition(context, *partitioned_data, row_location_remap);
	}
	if (needs_unpartitioned_data) {
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

unique_ptr<PartitionedTupleData> GroupedAggregateHashTable::TryAcquireProvenUniqueAppendData(
    optional_ptr<TupleDataRowLocationRemap> row_location_remap) {
	// A coalesced interval envelope still proves local duplicate freedom. Keep the rows unpartitioned until the global
	// finalizer knows whether local envelopes are disjoint; only a failed global proof needs radix repartitioning.
	if (!GetProvenUniqueAppendRanges() || !unpartitioned_data || partitioned_data->Count() != 0) {
		return nullptr;
	}
	unpartitioned_data->FlushAppendState(state.unpartitioned_append_state);
	unpartitioned_data->Unpin();
	auto result = std::move(unpartitioned_data);
	if (row_location_remap) {
		row_location_remap->Flush();
	}
	return result;
}

void GroupedAggregateHashTable::Abandon(optional_ptr<TupleDataRowLocationRemap> row_location_remap) {
	dense_single_field_target_cache.Reset();
	row_pointer_descriptor_target_cache.Reset();
	const bool needs_unpartitioned_data = radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD || unpartitioned_data;
	if (unpartitioned_data) {
		// Flush/unpin unpartitioned data and append to partitioned data
		unpartitioned_data->FlushAppendState(state.unpartitioned_append_state);
		unpartitioned_data->Unpin();
		unpartitioned_data->Repartition(context, *partitioned_data, row_location_remap);
	}
	if (needs_unpartitioned_data) {
		InitializeUnpartitionedData();
	}

	// Start over
	ClearPointerTable();
	count = 0;

	// Resetting the id ensures the dict state is reset properly when needed
	state.compressed_group_state.dictionary_id = string();
}

void GroupedAggregateHashTable::Repartition(optional_ptr<TupleDataRowLocationRemap> row_location_remap) {
	dense_single_field_target_cache.Disable();
	row_pointer_descriptor_target_cache.Reset();
	auto old = AcquirePartitionedData(row_location_remap);
	D_ASSERT(old->GetPartitions().size() != partitioned_data->GetPartitions().size());
	old->Repartition(context, *partitioned_data, row_location_remap);
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

void GroupedAggregateHashTable::RecordSinkCount(idx_t count) {
	sink_count += count;
}

idx_t GroupedAggregateHashTable::GetMaterializedCount() const {
	auto result = partitioned_data->Count();
	if (unpartitioned_data) {
		result += unpartitioned_data->Count();
	}
	return result;
}

void GroupedAggregateHashTable::SkipLookups(bool require_final_combine) {
	skip_lookups = true;
	skip_lookups_require_final_combine = skip_lookups_require_final_combine || require_final_combine;
	enable_hll = false;
	dense_single_field_target_cache.Disable();
}

bool GroupedAggregateHashTable::LookupsSkipped() const {
	return skip_lookups;
}

void GroupedAggregateHashTable::RequireFinalCombine() {
	if (skip_lookups) {
		skip_lookups_require_final_combine = true;
	}
}

static constexpr idx_t MAX_PROVEN_UNIQUE_APPEND_INTERVALS = STANDARD_VECTOR_SIZE;

template <class KEY_TYPE>
static void AggregateAppendProvenUniqueRange(vector<GroupedAggregateProvenUniqueRange> &ranges, PhysicalType key_type,
                                             KEY_TYPE first_key, KEY_TYPE last_key) {
	GroupedAggregateProvenUniqueRange range;
	range.key_type = key_type;
	if constexpr (std::is_signed<KEY_TYPE>::value) {
		range.first_signed_key = static_cast<int64_t>(first_key);
		range.last_signed_key = static_cast<int64_t>(last_key);
	} else {
		range.first_unsigned_key = static_cast<uint64_t>(first_key);
		range.last_unsigned_key = static_cast<uint64_t>(last_key);
	}
	ranges.push_back(range);
}

template <class KEY_TYPE>
static void AggregateExtendProvenUniqueRange(GroupedAggregateProvenUniqueRange &range, KEY_TYPE key) {
	if constexpr (std::is_signed<KEY_TYPE>::value) {
		range.last_signed_key = static_cast<int64_t>(key);
	} else {
		range.last_unsigned_key = static_cast<uint64_t>(key);
	}
}

template <class KEY_TYPE>
static void AggregateRecordProvenUniqueRange(vector<GroupedAggregateProvenUniqueRange> &ranges, PhysicalType key_type,
                                             KEY_TYPE first_key, KEY_TYPE last_key, bool has_previous,
                                             KEY_TYPE previous, bool &ranges_coalesced) {
	if (ranges_coalesced) {
		D_ASSERT(ranges.size() == 1);
		AggregateExtendProvenUniqueRange(ranges.front(), last_key);
		return;
	}
	const auto adjacent = has_previous && previous != NumericLimits<KEY_TYPE>::Maximum() &&
	                      first_key == static_cast<KEY_TYPE>(previous + 1);
	if (adjacent) {
		D_ASSERT(!ranges.empty());
		AggregateExtendProvenUniqueRange(ranges.back(), last_key);
		return;
	}
	if (ranges.size() >= MAX_PROVEN_UNIQUE_APPEND_INTERVALS) {
		D_ASSERT(!ranges.empty());
		AggregateExtendProvenUniqueRange(ranges.front(), last_key);
		ranges.resize(1);
		ranges_coalesced = true;
		return;
	}
	AggregateAppendProvenUniqueRange(ranges, key_type, first_key, last_key);
}

template <class KEY_TYPE>
static bool AggregateTryContinueProducerProvenUniqueSummaryTyped(DataChunk &groups, PhysicalType key_type,
                                                                 bool &has_last_key, int64_t &last_signed_key,
                                                                 uint64_t &last_unsigned_key,
                                                                 vector<GroupedAggregateProvenUniqueRange> &ranges,
                                                                 bool &ranges_coalesced) {
	if (groups.ColumnCount() != 1 || groups.size() == 0) {
		return false;
	}
	auto &group = groups.data[0];
	if (group.GetVectorType() != VectorType::FLAT_VECTOR || !FlatVector::Validity(group).CheckAllValid(groups.size())) {
		return false;
	}
	const auto data = FlatVector::GetData<KEY_TYPE>(group);
	const auto first_key = data[0];
	const auto last_key = data[groups.size() - 1];
	if (groups.size() > 1 && last_key <= first_key) {
		return false;
	}
	const auto previous_stream_key = std::is_signed<KEY_TYPE>::value ? static_cast<KEY_TYPE>(last_signed_key)
	                                                                 : static_cast<KEY_TYPE>(last_unsigned_key);
	if (has_last_key && first_key <= previous_stream_key) {
		return false;
	}
	// The producer proves every intermediate transition, so one conservative
	// endpoint interval covers the batch without rescanning it. Keeping batch
	// intervals separate until the bounded summary fills preserves scheduler
	// gaps for global disjointness; capacity coalescing remains conservative.
	AggregateRecordProvenUniqueRange(ranges, key_type, first_key, last_key, has_last_key, previous_stream_key,
	                                 ranges_coalesced);
	if constexpr (std::is_signed<KEY_TYPE>::value) {
		last_signed_key = static_cast<int64_t>(last_key);
	} else {
		last_unsigned_key = static_cast<uint64_t>(last_key);
	}
	has_last_key = true;
	return true;
}

template <class KEY_TYPE>
static bool AggregateTryContinueProvenUniqueAppendTyped(DataChunk &groups, PhysicalType key_type, bool &has_last_key,
                                                        int64_t &last_signed_key, uint64_t &last_unsigned_key,
                                                        vector<GroupedAggregateProvenUniqueRange> &ranges,
                                                        bool &ranges_coalesced,
                                                        ExecutionGroupedAggregateAppendProof append_proof) {
	if (groups.ColumnCount() != 1 || groups.size() == 0) {
		return false;
	}
	if (append_proof.groups_strictly_increasing &&
	    AggregateTryContinueProducerProvenUniqueSummaryTyped<KEY_TYPE>(groups, key_type, has_last_key, last_signed_key,
	                                                                   last_unsigned_key, ranges, ranges_coalesced)) {
		return true;
	}
	UnifiedVectorFormat format;
	groups.data[0].ToUnifiedFormat(format);
	auto data = UnifiedVectorFormat::GetData<KEY_TYPE>(format);
	auto sel = format.sel;
	const bool can_have_null = format.validity.CanHaveNull();
	const auto previous_stream_key = std::is_signed<KEY_TYPE>::value ? static_cast<KEY_TYPE>(last_signed_key)
	                                                                 : static_cast<KEY_TYPE>(last_unsigned_key);
	bool has_chunk_key = false;
	KEY_TYPE first_key {};
	KEY_TYPE last_key {};
	for (idx_t row_idx = 0; row_idx < groups.size(); row_idx++) {
		const auto source_idx = sel->get_index(row_idx);
		if (can_have_null && !format.validity.RowIsValid(source_idx)) {
			return false;
		}
		const auto key = data[source_idx];
		if (!has_chunk_key) {
			if (has_last_key && key <= previous_stream_key) {
				return false;
			}
			first_key = key;
		} else if (key <= last_key) {
			return false;
		}
		last_key = key;
		has_chunk_key = true;
	}

	using UNSIGNED_KEY_TYPE = typename std::make_unsigned<KEY_TYPE>::type;
	const auto observed_span = static_cast<UNSIGNED_KEY_TYPE>(last_key) - static_cast<UNSIGNED_KEY_TYPE>(first_key);
	const auto dense = observed_span == static_cast<UNSIGNED_KEY_TYPE>(groups.size() - 1);
	if (dense) {
		AggregateRecordProvenUniqueRange(ranges, key_type, first_key, last_key, has_last_key, previous_stream_key,
		                                 ranges_coalesced);
	} else {
		KEY_TYPE run_first = first_key;
		KEY_TYPE run_last = first_key;
		bool has_previous_range = has_last_key;
		KEY_TYPE previous_range_key = previous_stream_key;
		for (idx_t row_idx = 1; row_idx < groups.size(); row_idx++) {
			const auto key = data[sel->get_index(row_idx)];
			const auto adjacent =
			    run_last != NumericLimits<KEY_TYPE>::Maximum() && key == static_cast<KEY_TYPE>(run_last + 1);
			if (!adjacent) {
				AggregateRecordProvenUniqueRange(ranges, key_type, run_first, run_last, has_previous_range,
				                                 previous_range_key, ranges_coalesced);
				has_previous_range = true;
				previous_range_key = run_last;
				run_first = key;
			}
			run_last = key;
		}
		AggregateRecordProvenUniqueRange(ranges, key_type, run_first, run_last, has_previous_range, previous_range_key,
		                                 ranges_coalesced);
	}
	if constexpr (std::is_signed<KEY_TYPE>::value) {
		last_signed_key = static_cast<int64_t>(last_key);
	} else {
		last_unsigned_key = static_cast<uint64_t>(last_key);
	}
	has_last_key = true;
	return true;
}

bool GroupedAggregateHashTable::TryContinueProvenUniqueAppend(DataChunk &groups,
                                                              ExecutionGroupedAggregateAppendProof append_proof) {
	if (skip_lookups_require_final_combine || (sink_count != 0 && !skip_lookups) || groups.ColumnCount() != 1) {
		return false;
	}
	const auto key_type = groups.data[0].GetType().InternalType();
	if (proven_unique_append_key_type != PhysicalType::INVALID && proven_unique_append_key_type != key_type) {
		RequireFinalCombine();
		return false;
	}
	proven_unique_append_key_type = key_type;
	bool continued;
	switch (key_type) {
	case PhysicalType::INT8:
		continued = AggregateTryContinueProvenUniqueAppendTyped<int8_t>(
		    groups, key_type, proven_unique_append_has_last_key, proven_unique_append_last_signed_key,
		    proven_unique_append_last_unsigned_key, proven_unique_append_ranges, proven_unique_append_ranges_coalesced,
		    append_proof);
		break;
	case PhysicalType::INT16:
		continued = AggregateTryContinueProvenUniqueAppendTyped<int16_t>(
		    groups, key_type, proven_unique_append_has_last_key, proven_unique_append_last_signed_key,
		    proven_unique_append_last_unsigned_key, proven_unique_append_ranges, proven_unique_append_ranges_coalesced,
		    append_proof);
		break;
	case PhysicalType::INT32:
		continued = AggregateTryContinueProvenUniqueAppendTyped<int32_t>(
		    groups, key_type, proven_unique_append_has_last_key, proven_unique_append_last_signed_key,
		    proven_unique_append_last_unsigned_key, proven_unique_append_ranges, proven_unique_append_ranges_coalesced,
		    append_proof);
		break;
	case PhysicalType::INT64:
		continued = AggregateTryContinueProvenUniqueAppendTyped<int64_t>(
		    groups, key_type, proven_unique_append_has_last_key, proven_unique_append_last_signed_key,
		    proven_unique_append_last_unsigned_key, proven_unique_append_ranges, proven_unique_append_ranges_coalesced,
		    append_proof);
		break;
	case PhysicalType::UINT8:
		continued = AggregateTryContinueProvenUniqueAppendTyped<uint8_t>(
		    groups, key_type, proven_unique_append_has_last_key, proven_unique_append_last_signed_key,
		    proven_unique_append_last_unsigned_key, proven_unique_append_ranges, proven_unique_append_ranges_coalesced,
		    append_proof);
		break;
	case PhysicalType::UINT16:
		continued = AggregateTryContinueProvenUniqueAppendTyped<uint16_t>(
		    groups, key_type, proven_unique_append_has_last_key, proven_unique_append_last_signed_key,
		    proven_unique_append_last_unsigned_key, proven_unique_append_ranges, proven_unique_append_ranges_coalesced,
		    append_proof);
		break;
	case PhysicalType::UINT32:
		continued = AggregateTryContinueProvenUniqueAppendTyped<uint32_t>(
		    groups, key_type, proven_unique_append_has_last_key, proven_unique_append_last_signed_key,
		    proven_unique_append_last_unsigned_key, proven_unique_append_ranges, proven_unique_append_ranges_coalesced,
		    append_proof);
		break;
	case PhysicalType::UINT64:
		continued = AggregateTryContinueProvenUniqueAppendTyped<uint64_t>(
		    groups, key_type, proven_unique_append_has_last_key, proven_unique_append_last_signed_key,
		    proven_unique_append_last_unsigned_key, proven_unique_append_ranges, proven_unique_append_ranges_coalesced,
		    append_proof);
		break;
	default:
		continued = false;
		break;
	}
	if (!continued) {
		RequireFinalCombine();
		return false;
	}
	if (!skip_lookups) {
		SkipLookups(false);
	}
	return true;
}

bool GroupedAggregateHashTable::LookupsSkippedRequireFinalCombine() const {
	return (skip_lookups && skip_lookups_require_final_combine) || epochs_require_final_combine;
}

void GroupedAggregateHashTable::EnsureLookupEpoch() {
	if (!skip_lookups) {
		return;
	}
	// Adaptive appends left rows the pointer table does not cover, so lookups may only
	// run against a fresh epoch. The abandoned epochs can duplicate future groups,
	// which makes the final combine mandatory and voids any uniqueness proof.
	Abandon();
	skip_lookups = false;
	epochs_require_final_combine = true;
}

optional_ptr<const vector<GroupedAggregateProvenUniqueRange>>
GroupedAggregateHashTable::GetProvenUniqueAppendRanges() const {
	if (!skip_lookups || skip_lookups_require_final_combine || epochs_require_final_combine ||
	    !proven_unique_append_has_last_key || proven_unique_append_ranges.empty()) {
		return nullptr;
	}
	return &proven_unique_append_ranges;
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
	// The pointer table indexes only the current epoch. Adaptive skip-lookups appends
	// materialize rows the table never covered, and abandoning retains them for the
	// final combine, so reinsertion here — which walks ALL materialized rows — cannot
	// be used to grow the table: it would reinsert abandoned rows, oversubscribe the
	// new table, and leave the probe loop unable to terminate. Start a fresh, larger
	// epoch instead. The retained rows stay materialized and the final combine merges
	// them, exactly as it does for the epoch EnsureLookupEpoch abandons.
	if (Count() != 0 && Count() != GetMaterializedCount()) {
		epochs_require_final_combine = true;
		Abandon();
	}

	capacity = size;
	hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(ht_entry_t));
	entries = reinterpret_cast<ht_entry_t *>(hash_map.get());
	ClearPointerTable();
	bitmask = capacity - 1;

	if (Count() != 0) {
		ReinsertTuples(*partitioned_data);
		if (unpartitioned_data) {
			ReinsertTuples(*unpartitioned_data);
		}
	}

	Verify();
}

bool GroupedAggregateHashTable::ReserveGroups(idx_t group_count) {
	if (skip_lookups) {
		return false;
	}
	if (group_count <= Count()) {
		return false;
	}
	const auto append_slack = MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericLimits<idx_t>::Maximum() - group_count);
	const auto target_count = group_count + append_slack;
	if (target_count <= capacity && target_count <= ResizeThreshold()) {
		return false;
	}
	const auto target_capacity = MaxValue(capacity * 2, GetCapacityForCount(target_count));
	Resize(target_capacity);
	return true;
}

void GroupedAggregateHashTable::ResizeForAdditionalGroups(idx_t additional_count) {
	const auto target_count = Count() + additional_count;
	const auto target_capacity = MaxValue(capacity * 2, GetCapacityForCount(target_count));
	Resize(target_capacity);
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
				idx_t probe_count = 0;
				while (entries[ht_offset].IsOccupied()) {
					if (DUCKDB_UNLIKELY(++probe_count == capacity)) {
						throw InternalException("Reinserting tuples cannot find an empty hash table entry");
					}
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
	EnsureLookupEpoch();
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
		ResizeForAdditionalGroups(chunk_size);
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
	if (skip_lookups || enable_hll || count == 0 || !entries || group_count == 0 ||
	    group_count > AGGREGATE_MAX_FAST_GROUPS || group_hashes.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	D_ASSERT(group_count + 1 == layout_ptr->ColumnCount());

	const auto &layout_types = layout_ptr->GetTypes();
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
				if (AggregateFastExistingRowMatches(sources, *layout_ptr, row_location, row_idx)) {
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
	EnsureLookupEpoch();
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

template <class T>
static bool AggregateDenseSingleFieldValueKey(T value, idx_t &key) {
	if constexpr (std::is_same<T, bool>::value) {
		key = value ? 1 : 0;
		return true;
	} else {
		if constexpr (std::is_signed<T>::value) {
			if (value < 0) {
				return false;
			}
		}
		using UNSIGNED_T = typename std::make_unsigned<T>::type;
		auto unsigned_value = static_cast<UNSIGNED_T>(value);
		if (unsigned_value > NumericLimits<idx_t>::Maximum()) {
			return false;
		}
		key = static_cast<idx_t>(unsigned_value);
		return true;
	}
}

template <class T>
static bool AggregateDenseSingleFieldGroupKey(const AggregateFastGroupSourceInfo &sources, idx_t row_idx, idx_t &key) {
	if (sources.group_count != 1) {
		return false;
	}
	const auto source_idx = AggregateFastGroupSourceIndex(sources, 0, row_idx);
	return AggregateDenseSingleFieldValueKey(Load<T>(sources.source_data[0] + source_idx * sizeof(T)), key);
}

static idx_t AggregateDenseTargetMaxRange(idx_t group_count) {
	if (group_count > NumericLimits<idx_t>::Maximum() / AGGREGATE_DENSE_TARGET_MAX_RANGE_PER_GROUP) {
		return NumericLimits<idx_t>::Maximum();
	}
	return MaxValue<idx_t>(STANDARD_VECTOR_SIZE, group_count * AGGREGATE_DENSE_TARGET_MAX_RANGE_PER_GROUP);
}

static bool AggregateDenseKeyRange(idx_t min_key, idx_t max_key, idx_t &key_range) {
	if (max_key < min_key || max_key - min_key == NumericLimits<idx_t>::Maximum()) {
		return false;
	}
	key_range = max_key - min_key + 1;
	return true;
}

static bool AggregateDenseDomainCanPrepare(const ExecutionDenseGroupDomain &domain, PhysicalType physical_type,
                                           idx_t &domain_range) {
	if (!domain.ready || domain.physical_type != physical_type || domain.distinct_count == 0) {
		return false;
	}
	if (!AggregateDenseKeyRange(domain.min_key, domain.max_key, domain_range)) {
		return false;
	}
	return domain_range <= AGGREGATE_ROW_POINTER_DENSE_TARGET_MAX_ENTRIES;
}

static bool AggregateDenseDomainCanAdmit(const ExecutionDenseGroupDomain &domain, PhysicalType physical_type,
                                         idx_t chunk_min_key, idx_t chunk_max_key, idx_t &domain_range) {
	if (!domain.ready || domain.physical_type != physical_type || domain.distinct_count == 0 ||
	    domain.min_key > chunk_min_key || domain.max_key < chunk_max_key) {
		return false;
	}
	return AggregateDenseDomainCanPrepare(domain, physical_type, domain_range);
}

//! The single authority over the integral group types the dense single-field fast
//! paths support. Every dense single-field dispatch routes through this switch, so a
//! newly supported type is one case here instead of a sweep over per-operation
//! switches. OP::Run<T> receives the group type's storage type; an unsupported type
//! returns the fallback.
template <class OP, class RESULT, class... ARGS>
static RESULT AggregateDispatchDenseIntegralType(PhysicalType physical_type, RESULT fallback, ARGS &&... args) {
	switch (physical_type) {
	case PhysicalType::BOOL:
		return OP::template Run<bool>(std::forward<ARGS>(args)...);
	case PhysicalType::UINT8:
		return OP::template Run<uint8_t>(std::forward<ARGS>(args)...);
	case PhysicalType::INT8:
		return OP::template Run<int8_t>(std::forward<ARGS>(args)...);
	case PhysicalType::UINT16:
		return OP::template Run<uint16_t>(std::forward<ARGS>(args)...);
	case PhysicalType::INT16:
		return OP::template Run<int16_t>(std::forward<ARGS>(args)...);
	case PhysicalType::UINT32:
		return OP::template Run<uint32_t>(std::forward<ARGS>(args)...);
	case PhysicalType::INT32:
		return OP::template Run<int32_t>(std::forward<ARGS>(args)...);
	case PhysicalType::UINT64:
		return OP::template Run<uint64_t>(std::forward<ARGS>(args)...);
	case PhysicalType::INT64:
		return OP::template Run<int64_t>(std::forward<ARGS>(args)...);
	default:
		return fallback;
	}
}

struct AggregateDenseIntegralTypeSupportedOp {
	template <class T>
	static bool Run() {
		return true;
	}
};

static bool AggregateDenseIntegralTypeSupported(PhysicalType physical_type) {
	return AggregateDispatchDenseIntegralType<AggregateDenseIntegralTypeSupportedOp>(physical_type, false);
}

template <class T, class CACHE>
static bool
AggregateTryPrepareDenseSingleFieldTargetCacheTemplated(const AggregateFastGroupSourceInfo &sources, idx_t chunk_size,
                                                        idx_t existing_count, idx_t layout_offset, CACHE &cache,
                                                        optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	if (sources.group_count != 1) {
		return false;
	}
	if (chunk_size > STANDARD_VECTOR_SIZE) {
		if (cache.physical_type != PhysicalType::INVALID || existing_count != 0) {
			cache.Disable();
		}
		return false;
	}
	if (existing_count != 0 && cache.physical_type == PhysicalType::INVALID) {
		cache.Disable();
		return false;
	}
	if (!cache.Configure(sources.physical_types[0], layout_offset)) {
		return false;
	}
	idx_t min_key = NumericLimits<idx_t>::Maximum();
	idx_t max_key = 0;
	for (idx_t row_idx = 0; row_idx < chunk_size; row_idx++) {
		idx_t key;
		if (!AggregateDenseSingleFieldGroupKey<T>(sources, row_idx, key)) {
			cache.Disable();
			return false;
		}
		min_key = MinValue(min_key, key);
		max_key = MaxValue(max_key, key);
	}
	if (max_key == NumericLimits<idx_t>::Maximum()) {
		cache.Disable();
		return false;
	}
	idx_t key_range;
	if (!AggregateDenseKeyRange(min_key, max_key, key_range)) {
		cache.Disable();
		return false;
	}
	idx_t cache_min_key = min_key;
	idx_t cache_max_key = max_key;
	idx_t max_range = AggregateDenseTargetMaxRange(existing_count + chunk_size);
	bool exact_cache_range = false;
	if (dense_domain) {
		idx_t domain_range;
		if (AggregateDenseDomainCanAdmit(*dense_domain, sources.physical_types[0], min_key, max_key, domain_range)) {
			cache_min_key = dense_domain->min_key;
			cache_max_key = dense_domain->max_key;
			key_range = domain_range;
			max_range = domain_range;
			exact_cache_range = true;
		}
	}
	if (key_range > max_range || !cache.EnsureRange(cache_min_key, cache_max_key, exact_cache_range)) {
		if (cache.physical_type != PhysicalType::INVALID || existing_count != 0) {
			cache.Disable();
		}
		return false;
	}
	return true;
}

struct AggregateTryPrepareDenseSingleFieldTargetCacheOp {
	template <class T, class... ARGS>
	static auto Run(ARGS &&... args)
	    -> decltype(AggregateTryPrepareDenseSingleFieldTargetCacheTemplated<T>(std::forward<ARGS>(args)...)) {
		return AggregateTryPrepareDenseSingleFieldTargetCacheTemplated<T>(std::forward<ARGS>(args)...);
	}
};

template <class CACHE>
static bool AggregateTryPrepareDenseSingleFieldTargetCache(
    DataChunk &groups, const vector<LogicalType> &layout_types, const vector<idx_t> &layout_offsets, idx_t chunk_size,
    idx_t existing_count, CACHE &cache, AggregateFastGroupSourceInfo &sources,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) {
	if (groups.ColumnCount() != 1 || layout_types.size() != 2 ||
	    layout_types[0].InternalType() != groups.data[0].GetType().InternalType()) {
		return false;
	}
	if (!AggregatePrepareFastGroupSourceInfo(groups, layout_types, chunk_size, sources) ||
	    sources.physical_types[0] != layout_types[0].InternalType()) {
		return false;
	}
	const auto layout_offset = layout_offsets[0];
	return AggregateDispatchDenseIntegralType<AggregateTryPrepareDenseSingleFieldTargetCacheOp>(
	    sources.physical_types[0], false, sources, chunk_size, existing_count, layout_offset, cache, dense_domain);
}

template <class T, class CACHE>
static bool AggregatePopulateDenseSingleFieldTargetCacheTemplated(const AggregateFastGroupSourceInfo &sources,
                                                                  const data_ptr_t *row_locations,
                                                                  const SelectionVector &row_sel, idx_t chunk_size,
                                                                  CACHE &cache) {
	if (chunk_size > row_sel.Capacity()) {
		return false;
	}
	for (idx_t row_idx = 0; row_idx < chunk_size; row_idx++) {
		idx_t key;
		const auto key_loaded = AggregateDenseSingleFieldGroupKey<T>(sources, row_idx, key);
		if (!key_loaded || !cache.KeyInRange(key)) {
			return false;
		}
		const auto row_location = row_locations[row_sel.get_index_unsafe(row_idx)];
		cache.SetAddress(key, reinterpret_cast<uintptr_t>(row_location));
		cache.SetPendingNewGroup(key, DConstants::INVALID_INDEX);
	}
	return true;
}

struct AggregatePopulateDenseSingleFieldTargetCacheOp {
	template <class T, class... ARGS>
	static auto Run(ARGS &&... args)
	    -> decltype(AggregatePopulateDenseSingleFieldTargetCacheTemplated<T>(std::forward<ARGS>(args)...)) {
		return AggregatePopulateDenseSingleFieldTargetCacheTemplated<T>(std::forward<ARGS>(args)...);
	}
};

template <class CACHE>
static bool AggregatePopulateDenseSingleFieldTargetCache(const AggregateFastGroupSourceInfo &sources,
                                                         const data_ptr_t *row_locations,
                                                         const SelectionVector &row_sel, idx_t chunk_size,
                                                         CACHE &cache) {
	if (!AggregateDenseIntegralTypeSupported(sources.physical_types[0])) {
		throw InternalException("Unsupported dense single-field target cache type");
	}
	return AggregateDispatchDenseIntegralType<AggregatePopulateDenseSingleFieldTargetCacheOp>(
	    sources.physical_types[0], false, sources, row_locations, row_sel, chunk_size, cache);
}

enum class AggregateDenseAppendProof : uint8_t { UNAVAILABLE, PROVEN_NEW, DUPLICATE_EXISTING, DUPLICATE_INPUT };

template <class T, class CACHE>
static AggregateDenseAppendProof
AggregateProveDenseSingleFieldAppendKeysNewTemplated(const AggregateFastGroupSourceInfo &sources, idx_t chunk_size,
                                                     CACHE &cache) {
	for (idx_t row_idx = 0; row_idx < chunk_size; row_idx++) {
		idx_t key;
		if (!AggregateDenseSingleFieldGroupKey<T>(sources, row_idx, key) || !cache.KeyInRange(key)) {
			for (idx_t clear_idx = 0; clear_idx < row_idx; clear_idx++) {
				const auto loaded = AggregateDenseSingleFieldGroupKey<T>(sources, clear_idx, key);
				D_ASSERT(loaded && cache.KeyInRange(key));
				cache.SetPendingNewGroup(key, DConstants::INVALID_INDEX);
			}
			return AggregateDenseAppendProof::UNAVAILABLE;
		}
		const auto existing_address = cache.GetAddress(key);
		const auto pending_group = cache.GetPendingNewGroup(key);
		if (existing_address != 0 || pending_group != DConstants::INVALID_INDEX) {
			for (idx_t clear_idx = 0; clear_idx < row_idx; clear_idx++) {
				const auto loaded = AggregateDenseSingleFieldGroupKey<T>(sources, clear_idx, key);
				D_ASSERT(loaded && cache.KeyInRange(key));
				cache.SetPendingNewGroup(key, DConstants::INVALID_INDEX);
			}
			return existing_address != 0 ? AggregateDenseAppendProof::DUPLICATE_EXISTING
			                             : AggregateDenseAppendProof::DUPLICATE_INPUT;
		}
		cache.SetPendingNewGroup(key, row_idx);
	}
	return AggregateDenseAppendProof::PROVEN_NEW;
}

struct AggregateProveDenseSingleFieldAppendKeysNewOp {
	template <class T, class... ARGS>
	static auto Run(ARGS &&... args)
	    -> decltype(AggregateProveDenseSingleFieldAppendKeysNewTemplated<T>(std::forward<ARGS>(args)...)) {
		return AggregateProveDenseSingleFieldAppendKeysNewTemplated<T>(std::forward<ARGS>(args)...);
	}
};

template <class CACHE>
static AggregateDenseAppendProof
AggregateProveDenseSingleFieldAppendKeysNew(const AggregateFastGroupSourceInfo &sources, idx_t chunk_size,
                                            CACHE &cache) {
	return AggregateDispatchDenseIntegralType<AggregateProveDenseSingleFieldAppendKeysNewOp>(
	    sources.physical_types[0], AggregateDenseAppendProof::UNAVAILABLE, sources, chunk_size, cache);
}

template <class T, class CACHE>
static bool AggregateRebuildDenseSingleFieldTargetCacheFromData(PartitionedTupleData &data, idx_t layout_offset,
                                                                CACHE &cache) {
	for (auto &data_collection : data.GetPartitions()) {
		if (data_collection->Count() == 0) {
			continue;
		}
		TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::ALREADY_PINNED, false);
		const auto row_locations = iterator.GetRowLocations();
		do {
			for (idx_t row_idx = 0; row_idx < iterator.GetCurrentChunkCount(); row_idx++) {
				const auto row_location = row_locations[row_idx];
				idx_t key;
				if (!AggregateDenseSingleFieldValueKey(Load<T>(row_location + layout_offset), key) ||
				    !cache.KeyInRange(key)) {
					return false;
				}
				cache.SetAddress(key, reinterpret_cast<uintptr_t>(row_location));
				cache.SetPendingNewGroup(key, DConstants::INVALID_INDEX);
			}
		} while (iterator.Next());
	}
	return true;
}

template <class T>
bool GroupedAggregateHashTable::TryFindOrCreateSingleFieldGroupsDenseTemplated(
    DataChunk &groups, optional_ptr<Vector> addresses_out,
    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction selected_update_function, void *selected_update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, optional_ptr<SelectionVector> duplicates_out,
    idx_t *duplicate_count_out, optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	auto record_miss = [&](const char *stage_name) {
		auto miss_start = AggregateTraceStart(recorder);
		RecordAggregateTraceStage(recorder, stage_name, miss_start);
		return false;
	};
	const auto chunk_size = groups.size();
	if (!addresses_out && !selected_update_function && !duplicates_out) {
		return record_miss("find_or_create_dense_miss.consumer");
	}
	if (duplicate_count_out) {
		*duplicate_count_out = 0;
	}
	if (chunk_size == 0) {
		if (addresses_out) {
			FlatVector::SetSize(*addresses_out, 0);
		}
		return true;
	}
	if (dense_single_field_target_cache.disabled && dense_domain && dense_domain->ready) {
		dense_single_field_target_cache.Reset();
	}
	if (skip_lookups || !entries || groups.ColumnCount() != 1 || dense_single_field_target_cache.disabled) {
		return record_miss("find_or_create_dense_miss.state");
	}
	const auto &layout_types = layout_ptr->GetTypes();
	const auto &layout_offsets = layout_ptr->GetOffsets();
	if (layout_types.size() != 2 || layout_types[0].InternalType() != groups.data[0].GetType().InternalType()) {
		return record_miss("find_or_create_dense_miss.layout");
	}
	AggregateFastGroupSourceInfo sources;
	if (!AggregatePrepareFastGroupSourceInfo(groups, layout_types, chunk_size, sources) ||
	    sources.physical_types[0] != layout_types[0].InternalType()) {
		return record_miss("find_or_create_dense_miss.source");
	}
	auto &cache = dense_single_field_target_cache;
	const auto layout_offset = layout_offsets[0];
	const auto cache_was_empty = cache.addresses.empty();
	if (!AggregateTryPrepareDenseSingleFieldTargetCacheTemplated<T>(sources, chunk_size, Count(), layout_offset, cache,
	                                                                dense_domain)) {
		return record_miss("find_or_create_dense_miss.cache");
	}
	if (cache_was_empty && Count() != 0) {
		auto rebuild_start = AggregateTraceStart(recorder);
		if (!AggregateRebuildDenseSingleFieldTargetCacheFromData<T>(*partitioned_data, layout_offset, cache) ||
		    (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD &&
		     !AggregateRebuildDenseSingleFieldTargetCacheFromData<T>(*unpartitioned_data, layout_offset, cache))) {
			cache.Disable();
			RecordAggregateTraceStage(recorder, "find_or_create_dense_miss.cache_rebuild", rebuild_start);
			return false;
		}
		RecordAggregateTraceStage(recorder, "find_or_create_dense.cache_rebuild", rebuild_start);
	}

	if (Count() + chunk_size > capacity || Count() + chunk_size > ResizeThreshold()) {
		auto resize_start = AggregateTraceStart(recorder);
		Verify();
		ResizeForAdditionalGroups(chunk_size);
		RecordAggregateTraceStage(recorder, "find_or_create_dense.resize", resize_start);
	}

	state.hashes.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(state.hashes, count_t(chunk_size));
	auto hashes = FlatVector::GetDataMutable<hash_t>(state.hashes);
	state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(state.addresses, count_t(chunk_size));
	auto selected_addresses = FlatVector::GetDataMutable<uintptr_t>(state.addresses);
	auto ht_offsets = FlatVector::GetDataMutable<uint64_t>(state.ht_offsets);
	auto duplicate_targets = FlatVector::GetDataMutable<hash_t>(state.hash_salts);
	data_ptr_t *addresses = nullptr;
	if (addresses_out) {
		addresses_out->SetVectorType(VectorType::FLAT_VECTOR);
		addresses = FlatVector::GetDataMutable<data_ptr_t>(*addresses_out);
	}

	idx_t new_group_count = 0;
	idx_t duplicate_count = 0;
	idx_t emitted_duplicate_count = 0;
	idx_t selected_existing_count = 0;
	auto dense_key = [&](idx_t row_idx) {
		idx_t key;
		const auto key_loaded = AggregateDenseSingleFieldGroupKey<T>(sources, row_idx, key);
		D_ASSERT(key_loaded);
		return key;
	};
	auto clear_marked_entries = [&]() {
		for (idx_t marked_idx = 0; marked_idx < new_group_count; marked_idx++) {
			entries[ht_offsets[marked_idx]] = ht_entry_t();
			cache.SetPendingNewGroup(dense_key(state.new_groups.get_index_unsafe(marked_idx)),
			                         DConstants::INVALID_INDEX);
		}
		new_group_count = 0;
		duplicate_count = 0;
	};
	auto emit_row_state = [&](data_ptr_t row_location, idx_t row_idx) {
		if (addresses) {
			addresses[row_idx] = row_location;
		}
	};
	auto emit_duplicate = [&](idx_t row_idx) {
		if (duplicates_out) {
			duplicates_out->set_index(emitted_duplicate_count, row_idx);
		}
		emitted_duplicate_count++;
	};
	auto emit_existing_state = [&](data_ptr_t row_location, idx_t row_idx) {
		emit_row_state(row_location, row_idx);
		emit_duplicate(row_idx);
		if (selected_update_function) {
			selected_addresses[selected_existing_count] = reinterpret_cast<uintptr_t>(row_location);
			state.existing_groups.set_index(selected_existing_count, row_idx);
			selected_existing_count++;
		}
	};
	auto emit_new_duplicate = [&](idx_t row_idx, idx_t new_group_idx) {
		state.no_match_vector.set_index(duplicate_count, row_idx);
		duplicate_targets[duplicate_count] = new_group_idx;
		duplicate_count++;
		emit_duplicate(row_idx);
	};
	auto stored_key_matches = [&](data_ptr_t row_location, T value) {
		if (!AggregateStoredGroupKeyIsValid(row_location, *layout_ptr, 0)) {
			return false;
		}
		return Load<T>(row_location + layout_offset) == value;
	};

	auto probe_start = AggregateTraceStart(recorder);
	for (idx_t row_idx = 0; row_idx < chunk_size; row_idx++) {
		const auto key = dense_key(row_idx);
		if (!cache.KeyInRange(key)) {
			clear_marked_entries();
			cache.Disable();
			return false;
		}
		T value;
		hash_t hash;
		bool hash_ready = false;
		const auto source_idx = AggregateFastGroupSourceIndex(sources, 0, row_idx);
		if (enable_hll) {
			value = Load<T>(sources.source_data[0] + source_idx * sizeof(T));
			hash = Hash(value);
			hashes[row_idx] = hash;
			hash_ready = true;
		}
		const auto cached_address = cache.GetAddress(key);
		if (cached_address != 0) {
			emit_existing_state(reinterpret_cast<data_ptr_t>(cached_address), row_idx);
			continue;
		}
		const auto pending_group_idx = cache.GetPendingNewGroup(key);
		if (pending_group_idx != DConstants::INVALID_INDEX) {
			emit_new_duplicate(row_idx, pending_group_idx);
			continue;
		}

		if (!hash_ready) {
			value = Load<T>(sources.source_data[0] + source_idx * sizeof(T));
			hash = Hash(value);
			hashes[row_idx] = hash;
		}
		const auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = ApplyBitMask(hash);

		idx_t iteration_count;
		for (iteration_count = 0; iteration_count < capacity; iteration_count++) {
			auto &entry = entries[ht_offset];
			if (!entry.IsOccupied()) {
				ht_offsets[new_group_count] = ht_offset;
				state.new_groups.set_index(new_group_count, row_idx);
				cache.SetPendingNewGroup(key, new_group_count);
				new_group_count++;
				entry.SetSalt(salt);
				break;
			}
			if (entry.GetSalt() == salt) {
				auto row_location = entry.GetPointer();
				if (cast_pointer_to_uint64(row_location) != ht_entry_t::POINTER_MASK &&
				    stored_key_matches(row_location, value)) {
					const auto address = reinterpret_cast<uintptr_t>(row_location);
					cache.SetAddress(key, address);
					emit_existing_state(row_location, row_idx);
					break;
				}
			}
			SaltIncrementAndWrap(ht_offset, salt, bitmask);
		}
		if (iteration_count == capacity) {
			clear_marked_entries();
			throw InternalException("Maximum dense single-field group find-or-create iteration count reached in "
			                        "GroupedAggregateHashTable");
		}
	}
	RecordAggregateTraceStage(recorder, "find_or_create_dense.probe", probe_start);

	if (enable_hll) {
		auto hll_start = AggregateTraceStart(recorder);
		hll.Update(state.hashes);
		RecordAggregateTraceStage(recorder, "find_or_create_dense.hll", hll_start);
	}

	if (new_group_count == 0) {
		if (addresses_out) {
			FlatVector::SetSize(*addresses_out, count_t(chunk_size));
		}
		if (selected_update_function && selected_existing_count > 0) {
			auto update_start = AggregateTraceStart(recorder);
			const auto existing_selection =
			    selected_existing_count == chunk_size ? nullptr : state.existing_groups.data();
			selected_update_function(selected_addresses, nullptr, existing_selection, selected_existing_count,
			                         selected_update_state);
			RecordAggregateTraceStage(recorder, "find_or_create_dense.selected_existing_update", update_start);
		}
		sink_count += chunk_size;
		if (duplicate_count_out) {
			*duplicate_count_out = emitted_duplicate_count;
		}
		return true;
	}

	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout_ptr->GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout_ptr->GetTypes().size());
	state.group_chunk.data[0].Reference(groups.data[0]);

	auto group_format_start = AggregateTraceStart(recorder);
	state.group_chunk.data[1].Reference(state.hashes);
	TupleDataCollection::ToUnifiedFormat(state.partitioned_append_state.chunk_state, state.group_chunk);
	RecordAggregateTraceStage(recorder, "find_or_create_dense.group_format", group_format_start);

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
		const auto address = reinterpret_cast<uintptr_t>(row_location);
		const auto key = dense_key(row_idx);
		cache.SetAddress(key, address);
		cache.SetPendingNewGroup(key, DConstants::INVALID_INDEX);
		emit_row_state(row_location, row_idx);
	}
	for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
		const auto row_idx = state.no_match_vector.get_index_unsafe(duplicate_idx);
		const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
		emit_row_state(entries[ht_offsets[marked_idx]].GetPointer(), row_idx);
	}
	if (selected_update_function) {
		auto update_start = AggregateTraceStart(recorder);
		if (selected_existing_count > 0) {
			selected_update_function(selected_addresses, nullptr, state.existing_groups.data(), selected_existing_count,
			                         selected_update_state);
		}
		if (new_group_count > 0) {
			const auto state_addresses = reinterpret_cast<const uintptr_t *>(row_locations);
			selected_update_function(state_addresses, row_sel.data(), state.new_groups.data(), new_group_count,
			                         selected_update_state);
		}
		if (duplicate_count > 0) {
			for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
				const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
				selected_addresses[duplicate_idx] =
				    reinterpret_cast<uintptr_t>(entries[ht_offsets[marked_idx]].GetPointer());
			}
			selected_update_function(selected_addresses, nullptr, state.no_match_vector.data(), duplicate_count,
			                         selected_update_state);
		}
		RecordAggregateTraceStage(recorder, "find_or_create_dense.selected_state_update", update_start);
	}
	if (addresses_out) {
		FlatVector::SetSize(*addresses_out, count_t(chunk_size));
	}
	count += new_group_count;
	sink_count += chunk_size;
	if (duplicate_count_out) {
		*duplicate_count_out = emitted_duplicate_count;
	}
	RecordAggregateTraceStage(recorder, "find_or_create_dense.append_new_groups", append_start);
	return true;
}

struct GroupedAggregateHashTable::SingleFieldGroupsDenseDispatchOp {
	template <class T, class... ARGS>
	static bool Run(GroupedAggregateHashTable &ht, ARGS &&... args) {
		return ht.TryFindOrCreateSingleFieldGroupsDenseTemplated<T>(std::forward<ARGS>(args)...);
	}
};

bool GroupedAggregateHashTable::TryFindOrCreateSingleFieldGroupsDense(
    DataChunk &groups, optional_ptr<Vector> addresses_out,
    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction selected_update_function, void *selected_update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, optional_ptr<SelectionVector> duplicates_out,
    idx_t *duplicate_count_out, optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	if (groups.ColumnCount() != 1) {
		auto miss_start = AggregateTraceStart(recorder);
		RecordAggregateTraceStage(recorder, "find_or_create_dense_miss.vector", miss_start);
		return false;
	}
	if (!AggregateDenseIntegralTypeSupported(groups.data[0].GetType().InternalType())) {
		auto miss_start = AggregateTraceStart(recorder);
		RecordAggregateTraceStage(recorder, "find_or_create_dense_miss.physical_type", miss_start);
		return false;
	}
	return AggregateDispatchDenseIntegralType<SingleFieldGroupsDenseDispatchOp>(
	    groups.data[0].GetType().InternalType(), false, *this, groups, addresses_out, selected_update_function,
	    selected_update_state, recorder, duplicates_out, duplicate_count_out, dense_domain);
}

bool GroupedAggregateHashTable::TryFindOrCreateGroupsFastInternal(
    DataChunk &groups, optional_ptr<Vector> addresses_out,
    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction selected_update_function, void *selected_update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, optional_ptr<Vector> precomputed_hashes,
    optional_ptr<SelectionVector> duplicates_out, idx_t *duplicate_count_out,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain, bool insert_only) {
	EnsureLookupEpoch();
	const auto chunk_size = groups.size();
	const auto group_count = groups.ColumnCount();
	if (!insert_only && !addresses_out && !selected_update_function && !duplicates_out) {
		return false;
	}
	if (duplicate_count_out) {
		*duplicate_count_out = 0;
	}
	if (chunk_size == 0) {
		if (addresses_out) {
			FlatVector::SetSize(*addresses_out, 0);
		}
		return true;
	}
	if (!entries || group_count == 0 || group_count > AGGREGATE_MAX_FAST_GROUPS) {
		return false;
	}
	D_ASSERT(group_count + 1 == layout_ptr->ColumnCount());

	if (!insert_only &&
	    TryFindOrCreateSingleFieldGroupsDense(groups, addresses_out, selected_update_function, selected_update_state,
	                                          recorder, duplicates_out, duplicate_count_out, dense_domain)) {
		return true;
	}

	const auto &layout_types = layout_ptr->GetTypes();
	AggregateFastGroupSourceInfo sources;
	if (!AggregatePrepareFastGroupSourceInfo(groups, layout_types, chunk_size, sources)) {
		return false;
	}

	if (Count() + chunk_size > capacity || Count() + chunk_size > ResizeThreshold()) {
		auto resize_start = AggregateTraceStart(recorder);
		Verify();
		ResizeForAdditionalGroups(chunk_size);
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
	idx_t emitted_duplicate_count = 0;
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
	auto emit_duplicate = [&](idx_t row_idx) {
		if (insert_only) {
			return;
		}
		if (duplicates_out) {
			duplicates_out->set_index(emitted_duplicate_count, row_idx);
		}
		emitted_duplicate_count++;
	};
	auto emit_existing_state = [&](data_ptr_t row_location, idx_t row_idx) {
		emit_row_state(row_location, row_idx);
		emit_duplicate(row_idx);
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
		if (!insert_only) {
			state.no_match_vector.set_index(duplicate_count, row_idx);
			duplicate_targets[duplicate_count] = new_group_idx;
			duplicate_count++;
			emit_duplicate(row_idx);
		}
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
	for (idx_t row_idx = 0; row_idx < chunk_size; row_idx++) {
		const auto hash = hashes[row_idx];
		const auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = ApplyBitMask(hash);

		if (try_emit_repeated_match(row_idx, generic_keys_match_last(row_idx))) {
			continue;
		}

		idx_t iteration_count;
		for (iteration_count = 0; iteration_count < capacity; iteration_count++) {
			auto &entry = entries[ht_offset];
			if (!entry.IsOccupied()) {
				ht_offsets[new_group_count] = ht_offset;
				state.new_groups.set_index(new_group_count, row_idx);
				remember_new_group(row_idx, new_group_count);
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
						if (!insert_only) {
							state.no_match_vector.set_index(duplicate_count, row_idx);
							duplicate_targets[duplicate_count] = marked_idx;
							duplicate_count++;
						}
						remember_new_group(row_idx, marked_idx);
						found_tentative_match = true;
						break;
					}
					if (found_tentative_match) {
						break;
					}
					SaltIncrementAndWrap(ht_offset, salt, bitmask);
					continue;
				}
				if (AggregateFastExistingRowMatches(sources, *layout_ptr, row_location, row_idx)) {
					emit_existing_state(row_location, row_idx);
					remember_existing_group(row_idx, row_location);
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
			const auto existing_selection =
			    selected_existing_count == chunk_size ? nullptr : state.existing_groups.data();
			selected_update_function(selected_existing_addresses, nullptr, existing_selection, selected_existing_count,
			                         selected_update_state);
			RecordAggregateTraceStage(recorder, "find_or_create_fast.selected_existing_update", update_start);
		}
		sink_count += chunk_size;
		if (duplicate_count_out) {
			*duplicate_count_out = emitted_duplicate_count;
		}
		return true;
	}

	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout_ptr->GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout_ptr->GetTypes().size());
	for (idx_t group_idx = 0; group_idx < groups.ColumnCount(); group_idx++) {
		state.group_chunk.data[group_idx].Reference(groups.data[group_idx]);
	}

	state.group_chunk.data[groups.ColumnCount()].Reference(*group_hashes);

	auto append_start = AggregateTraceStart(recorder);
	const bool defer_insert_only_partitioning = insert_only && radix_bits > 0;
	auto group_format_start = AggregateTraceStart(recorder);
	auto target = PrepareAppendTarget(state.group_chunk, new_group_count, defer_insert_only_partitioning);
	RecordAggregateTraceStage(recorder,
	                          defer_insert_only_partitioning ? "find_or_create_fast.deferred_group_format"
	                                                         : "find_or_create_fast.group_format",
	                          group_format_start);
	auto storage_append_start = AggregateTraceStart(recorder);
	const auto fixed_width_append = target.data.TryAppendUnifiedFixedWidthSinglePartition(
	    target.state, state.group_chunk, state.new_groups, new_group_count);
	if (!fixed_width_append) {
		if (!target.data.TryAppendUnifiedSinglePartition(target.state, state.group_chunk, state.new_groups,
		                                                 new_group_count)) {
			target.data.AppendUnified(target.state, state.group_chunk, state.new_groups, new_group_count);
		}
	}
	RecordAggregateTraceStage(recorder,
	                          fixed_width_append ? "find_or_create_fast.fixed_width_storage_append"
	                                             : "find_or_create_fast.storage_append",
	                          storage_append_start);
	if (!layout_ptr->GetAggregates().empty()) {
		auto initialize_start = AggregateTraceStart(recorder);
		RowOperations::InitializeStates(*layout_ptr, target.state.chunk_state.row_locations,
		                                *FlatVector::IncrementalSelectionVector(), new_group_count);
		RecordAggregateTraceStage(recorder, "find_or_create_fast.initialize_states", initialize_start);
	}
	const auto row_locations = FlatVector::GetData<data_ptr_t>(target.state.chunk_state.row_locations);
	const auto &row_sel = target.state.reverse_partition_sel;
	auto publish_start = AggregateTraceStart(recorder);
	for (idx_t new_idx = 0; new_idx < new_group_count; new_idx++) {
		const auto row_idx = state.new_groups.get_index_unsafe(new_idx);
		const auto row_location = row_locations[row_sel.get_index_unsafe(row_idx)];
		entries[ht_offsets[new_idx]].SetPointer(row_location);
		emit_row_state(row_location, row_idx);
	}
	RecordAggregateTraceStage(recorder, "find_or_create_fast.publish_new_groups", publish_start);
	if (!insert_only) {
		for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
			const auto row_idx = state.no_match_vector.get_index_unsafe(duplicate_idx);
			const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
			const auto row_location = entries[ht_offsets[marked_idx]].GetPointer();
			emit_row_state(row_location, row_idx);
		}
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
	if (duplicate_count_out) {
		*duplicate_count_out = emitted_duplicate_count;
	}
	RecordAggregateTraceStage(recorder, "find_or_create_fast.append_new_groups", append_start);
	return true;
}

bool GroupedAggregateHashTable::TryFindOrCreateGroupAddressesFast(
    DataChunk &groups, Vector &addresses_out, optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	return TryFindOrCreateGroupsFastInternal(groups, addresses_out, nullptr, nullptr, recorder);
}

bool GroupedAggregateHashTable::TryFindOrCreateGroupsFast(DataChunk &groups, Vector &addresses_out,
                                                          SelectionVector &new_groups_out, idx_t &new_group_count,
                                                          optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	const auto count_before = Count();
	if (!TryFindOrCreateGroupsFastInternal(groups, addresses_out, nullptr, nullptr, recorder)) {
		new_group_count = 0;
		return false;
	}
	new_group_count = Count() - count_before;
	for (idx_t new_idx = 0; new_idx < new_group_count; new_idx++) {
		new_groups_out.set_index(new_idx, state.new_groups.get_index_unsafe(new_idx));
	}
	return true;
}

bool GroupedAggregateHashTable::TryInsertGroupsFast(DataChunk &groups,
                                                    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	if (!layout_ptr->GetAggregates().empty()) {
		return false;
	}
	return TryFindOrCreateGroupsFastInternal(groups, nullptr, nullptr, nullptr, recorder, nullptr, nullptr, nullptr,
	                                         nullptr, true);
}

bool GroupedAggregateHashTable::TryFindOrCreateGroupsSelectedStateUpdateFast(
    DataChunk &groups, ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, optional_ptr<Vector> precomputed_hashes,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	if (!update_function) {
		return false;
	}
	return TryFindOrCreateGroupsFastInternal(groups, nullptr, update_function, update_state, recorder,
	                                         precomputed_hashes, nullptr, nullptr, dense_domain);
}

static bool AggregateDescriptorGroupKeySourcesSupported(const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                        const vector<LogicalType> &layout_types) {
	if (group_sources.empty() || group_sources.size() >= layout_types.size()) {
		return false;
	}
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		const auto &source = group_sources[group_idx];
		const auto target_physical_type = layout_types[group_idx].InternalType();
		if (!source.ready || source.HasOutputTransform() || source.target_physical_type != target_physical_type ||
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
		case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
			if (source.source_physical_type != PhysicalType::INT32 || source.source_type.id() != LogicalTypeId::DATE) {
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
		case ExecutionRowPointerGroupKeyCastKind::STRING_SUBSTRING:
			if (source.source_physical_type != PhysicalType::VARCHAR ||
			    source.target_physical_type != PhysicalType::VARCHAR) {
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

static constexpr hash_t AGGREGATE_DESCRIPTOR_NULL_HASH = 0xbf58476d1ce4e5b9;

static idx_t AggregateDescriptorValueSize(PhysicalType physical_type) {
	return physical_type == PhysicalType::VARCHAR ? sizeof(string_t) : GetTypeIdSize(physical_type);
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
static bool AggregateIntegralCompressedGroupKeyValue(const_data_ptr_t source_data, idx_t source_idx,
                                                     const ExecutionRowPointerGroupKeySource &source,
                                                     DST &target_value) {
	auto source_value = Load<SRC>(source_data + source_idx * sizeof(SRC));
	SRC source_minimum;
	if (!TryCast::Operation<int64_t, SRC>(source.cast_constant, source_minimum, false)) {
		return false;
	}
	SRC compressed_value;
	if (!TrySubtractOperator::Operation<SRC, SRC, SRC>(source_value, source_minimum, compressed_value)) {
		return false;
	}
	if (!TryCast::Operation<SRC, DST>(compressed_value, target_value, false)) {
		return false;
	}
	return true;
}

static int64_t AggregateExtractDateYearGroupKey(int32_t days) {
	int32_t year = Date::EPOCH_YEAR;
	while (days < 0) {
		days += Date::DAYS_PER_YEAR_INTERVAL;
		year -= Date::YEAR_INTERVAL;
	}
	while (days >= Date::DAYS_PER_YEAR_INTERVAL) {
		days -= Date::DAYS_PER_YEAR_INTERVAL;
		year += Date::YEAR_INTERVAL;
	}
	auto year_offset = days / 365;
	while (days < Date::CUMULATIVE_YEAR_DAYS[year_offset]) {
		year_offset--;
		D_ASSERT(year_offset >= 0);
	}
	return year + year_offset;
}

template <class DST>
static bool AggregateDateYearCompressedGroupKeyValue(const_data_ptr_t source_data, idx_t source_idx,
                                                     const ExecutionRowPointerGroupKeySource &source,
                                                     DST &target_value) {
	const auto days = Load<int32_t>(source_data + source_idx * sizeof(int32_t));
	int64_t compressed_value;
	if (!TrySubtractOperator::Operation<int64_t, int64_t, int64_t>(AggregateExtractDateYearGroupKey(days),
	                                                               source.cast_constant, compressed_value)) {
		return false;
	}
	if (!TryCast::Operation<int64_t, DST>(compressed_value, target_value, false)) {
		return false;
	}
	return true;
}

template <class SRC, class DST>
static bool AggregateStoreIntegralCompressedGroupKey(const_data_ptr_t source_data, idx_t source_idx, Vector &target,
                                                     idx_t target_idx,
                                                     const ExecutionRowPointerGroupKeySource &source) {
	DST target_value;
	if (!AggregateIntegralCompressedGroupKeyValue<SRC, DST>(source_data, source_idx, source, target_value)) {
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

template <class DST>
static bool AggregateStoreDateYearCompressedGroupKey(const_data_ptr_t source_data, idx_t source_idx, Vector &target,
                                                     idx_t target_idx,
                                                     const ExecutionRowPointerGroupKeySource &source) {
	DST target_value;
	if (!AggregateDateYearCompressedGroupKeyValue<DST>(source_data, source_idx, source, target_value)) {
		return false;
	}
	auto target_values = FlatVector::GetDataMutable<DST>(target);
	target_values[target_idx] = target_value;
	return true;
}

static bool AggregateStoreDateYearCompressedGroupKey(const_data_ptr_t source_data, idx_t source_idx, Vector &target,
                                                     idx_t target_idx,
                                                     const ExecutionRowPointerGroupKeySource &source) {
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return AggregateStoreDateYearCompressedGroupKey<uint8_t>(source_data, source_idx, target, target_idx, source);
	case PhysicalType::UINT16:
		return AggregateStoreDateYearCompressedGroupKey<uint16_t>(source_data, source_idx, target, target_idx, source);
	case PhysicalType::UINT32:
		return AggregateStoreDateYearCompressedGroupKey<uint32_t>(source_data, source_idx, target, target_idx, source);
	case PhysicalType::UINT64:
		return AggregateStoreDateYearCompressedGroupKey<uint64_t>(source_data, source_idx, target, target_idx, source);
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
		const auto value = Load<int64_t>(source_data + source_idx * sizeof(int64_t));
		target_values[target_idx] = source.unchecked_integral_cast
		                                ? static_cast<int32_t>(value)
		                                : AggregateCheckedGroupKeyCast<int64_t, int32_t>(value);
		return true;
	}
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16: {
		D_ASSERT(source.target_physical_type == PhysicalType::INT16);
		auto target_values = FlatVector::GetDataMutable<int16_t>(target);
		const auto value = Load<int64_t>(source_data + source_idx * sizeof(int64_t));
		target_values[target_idx] = source.unchecked_integral_cast
		                                ? static_cast<int16_t>(value)
		                                : AggregateCheckedGroupKeyCast<int64_t, int16_t>(value);
		return true;
	}
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8: {
		D_ASSERT(source.target_physical_type == PhysicalType::INT8);
		auto target_values = FlatVector::GetDataMutable<int8_t>(target);
		const auto value = Load<int32_t>(source_data + source_idx * sizeof(int32_t));
		target_values[target_idx] = source.unchecked_integral_cast
		                                ? static_cast<int8_t>(value)
		                                : AggregateCheckedGroupKeyCast<int32_t, int8_t>(value);
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
	case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
		if (source.source_physical_type != PhysicalType::INT32 || source.source_type.id() != LogicalTypeId::DATE) {
			return false;
		}
		return AggregateStoreDateYearCompressedGroupKey(source_data, source_idx, target, target_idx, source);
	case ExecutionRowPointerGroupKeyCastKind::STRING_SUBSTRING: {
		if (source.source_physical_type != PhysicalType::VARCHAR ||
		    source.target_physical_type != PhysicalType::VARCHAR) {
			return false;
		}
		auto target_values = FlatVector::GetDataMutable<string_t>(target);
		const auto value = Load<string_t>(source_data + source_idx * sizeof(string_t));
		target_values[target_idx] = SubstringPrefixUnicode(value, source.string_substring_length);
		return true;
	}
	case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS: {
		if (source.source_physical_type != PhysicalType::VARCHAR) {
			return false;
		}
		const auto value = Load<string_t>(source_data + source_idx * sizeof(string_t));
		switch (source.target_physical_type) {
		case PhysicalType::UINT8: {
			auto target_values = FlatVector::GetDataMutable<uint8_t>(target);
			target_values[target_idx] = StringCompressUInt8Value(value);
			return true;
		}
		case PhysicalType::UINT16: {
			auto target_values = FlatVector::GetDataMutable<uint16_t>(target);
			target_values[target_idx] = StringCompressValue<uint16_t>(value);
			return true;
		}
		case PhysicalType::UINT32: {
			auto target_values = FlatVector::GetDataMutable<uint32_t>(target);
			target_values[target_idx] = StringCompressValue<uint32_t>(value);
			return true;
		}
		case PhysicalType::UINT64: {
			auto target_values = FlatVector::GetDataMutable<uint64_t>(target);
			target_values[target_idx] = StringCompressValue<uint64_t>(value);
			return true;
		}
		case PhysicalType::UINT128: {
			auto target_values = FlatVector::GetDataMutable<uhugeint_t>(target);
			target_values[target_idx] = StringCompressValue<uhugeint_t>(value);
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
	std::array<UnifiedVectorFormat, AGGREGATE_MAX_FAST_GROUPS> input_formats;
	vector<UnifiedVectorFormat> overflow_input_formats;

	void InitializeInputFormats(idx_t group_count_p) {
		group_count = group_count_p;
		if (group_count > AGGREGATE_MAX_FAST_GROUPS) {
			overflow_input_formats.resize(group_count);
		} else {
			overflow_input_formats.clear();
		}
	}

	UnifiedVectorFormat &InputFormat(idx_t group_idx) {
		return group_count > AGGREGATE_MAX_FAST_GROUPS ? overflow_input_formats[group_idx] : input_formats[group_idx];
	}

	const UnifiedVectorFormat &InputFormat(idx_t group_idx) const {
		return group_count > AGGREGATE_MAX_FAST_GROUPS ? overflow_input_formats[group_idx] : input_formats[group_idx];
	}
};

enum class AggregateRowPointerGroupLookupStrategy : uint8_t {
	SINGLE_ROW_POINTER_FIELD_TARGETS,
	DIRECT_DESCRIPTOR_TARGETS,
	MATERIALIZED_DESCRIPTOR_TARGETS
};

static bool
AggregateRowPointerDescriptorSourcesCanMaterialize(DataChunk &payload_input, Vector &row_pointers,
                                                   const vector<ExecutionRowPointerGroupKeySource> &group_sources);

static bool AggregateDescriptorSourcesCanMaterialize(DataChunk &payload_input, optional_ptr<Vector> row_pointers,
                                                     const vector<ExecutionRowPointerGroupKeySource> &group_sources);

static bool AggregateDescriptorSourcesNeedRowPointers(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	for (auto &source : group_sources) {
		if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			return true;
		}
	}
	return false;
}

static bool AggregateRowPointerSingleFieldDirectTypeSupported(PhysicalType type) {
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
		return true;
	default:
		return false;
	}
}

static bool AggregateRowPointerSingleFieldDirectCastSupported(const ExecutionRowPointerGroupKeySource &source) {
	if (!AggregateRowPointerSingleFieldDirectTypeSupported(source.target_physical_type)) {
		return false;
	}
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		return source.source_physical_type == source.target_physical_type;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		return source.source_physical_type == PhysicalType::INT64 && source.target_physical_type == PhysicalType::INT32;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		return source.source_physical_type == PhysicalType::INT64 && source.target_physical_type == PhysicalType::INT16;
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		return source.source_physical_type == PhysicalType::INT32 && source.target_physical_type == PhysicalType::INT8;
	case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
		return source.source_physical_type == PhysicalType::INT32 && source.source_type.id() == LogicalTypeId::DATE &&
		       (source.target_physical_type == PhysicalType::UINT8 ||
		        source.target_physical_type == PhysicalType::UINT16 ||
		        source.target_physical_type == PhysicalType::UINT32 ||
		        source.target_physical_type == PhysicalType::UINT64);
	case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS:
		return source.source_physical_type == PhysicalType::VARCHAR &&
		       (source.target_physical_type == PhysicalType::UINT8 ||
		        source.target_physical_type == PhysicalType::UINT16 ||
		        source.target_physical_type == PhysicalType::UINT32 ||
		        source.target_physical_type == PhysicalType::UINT64);
	case ExecutionRowPointerGroupKeyCastKind::STRING_SUBSTRING:
		return source.source_physical_type == PhysicalType::VARCHAR &&
		       source.target_physical_type == PhysicalType::VARCHAR;
	default:
		return false;
	}
}

static bool
AggregateRowPointerSingleFieldCanProbeDirect(DataChunk &payload_input, Vector &row_pointers,
                                             const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (group_sources.size() != 1 ||
	    !AggregateRowPointerDescriptorSourcesCanMaterialize(payload_input, row_pointers, group_sources)) {
		return false;
	}
	auto &source = group_sources[0];
	return source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD &&
	       AggregateRowPointerSingleFieldDirectCastSupported(source);
}

static bool
AggregateRowPointerDescriptorSourcesCanMaterialize(DataChunk &payload_input, Vector &row_pointers,
                                                   const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	return AggregateDescriptorSourcesCanMaterialize(payload_input, optional_ptr<Vector>(&row_pointers), group_sources);
}

static bool AggregateDescriptorSourcesCanMaterialize(DataChunk &payload_input, optional_ptr<Vector> row_pointers,
                                                     const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (AggregateDescriptorSourcesNeedRowPointers(group_sources) &&
	    (!row_pointers || row_pointers->GetVectorType() != VectorType::FLAT_VECTOR)) {
		return false;
	}
	for (auto &source : group_sources) {
		if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			continue;
		}
		if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			return false;
		}
		if (source.input_vector_index == DConstants::INVALID_INDEX ||
		    source.input_vector_index >= payload_input.ColumnCount()) {
			return false;
		}
		auto &input = payload_input.data[source.input_vector_index];
		if (input.GetType().InternalType() != source.source_physical_type) {
			return false;
		}
	}
	return true;
}

static bool AggregateDescriptorSourcesCanProbeDirect(DataChunk &payload_input, optional_ptr<Vector> row_pointers,
                                                     const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	return group_sources.size() <= AGGREGATE_MAX_FAST_GROUPS &&
	       AggregateDescriptorSourcesCanMaterialize(payload_input, row_pointers, group_sources);
}

static bool
AggregateInputVectorDescriptorSourcesCanProbeDirect(DataChunk &payload_input,
                                                    const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	for (auto &source : group_sources) {
		if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			return false;
		}
	}
	return AggregateDescriptorSourcesCanProbeDirect(payload_input, nullptr, group_sources);
}

static bool
AggregateRowPointerDescriptorSourcesCanProbeDirect(DataChunk &payload_input, Vector &row_pointers,
                                                   const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	return AggregateDescriptorSourcesCanProbeDirect(payload_input, optional_ptr<Vector>(&row_pointers), group_sources);
}

static bool
AggregateDescriptorSourcesCanUseDirectTargets(DataChunk &payload_input, optional_ptr<Vector> row_pointers,
                                              const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (row_pointers) {
		return AggregateRowPointerDescriptorSourcesCanProbeDirect(payload_input, *row_pointers, group_sources);
	}
	return AggregateInputVectorDescriptorSourcesCanProbeDirect(payload_input, group_sources);
}

static AggregateRowPointerGroupLookupStrategy
AggregateSelectRowPointerGroupLookupStrategy(DataChunk &payload_input, Vector &row_pointers,
                                             const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (AggregateRowPointerSingleFieldCanProbeDirect(payload_input, row_pointers, group_sources)) {
		return AggregateRowPointerGroupLookupStrategy::SINGLE_ROW_POINTER_FIELD_TARGETS;
	}
	return AggregateRowPointerDescriptorSourcesCanProbeDirect(payload_input, row_pointers, group_sources)
	           ? AggregateRowPointerGroupLookupStrategy::DIRECT_DESCRIPTOR_TARGETS
	           : AggregateRowPointerGroupLookupStrategy::MATERIALIZED_DESCRIPTOR_TARGETS;
}

static bool AggregatePrepareDescriptorSourceInfo(DataChunk &payload_input, optional_ptr<Vector> row_pointers,
                                                 const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                 const vector<LogicalType> &layout_types,
                                                 AggregateRowPointerDescriptorSourceInfo &info) {
	if (!AggregateDescriptorGroupKeySourcesSupported(group_sources, layout_types) ||
	    !AggregateDescriptorSourcesCanMaterialize(payload_input, row_pointers, group_sources)) {
		return false;
	}
	info.row_pointers = row_pointers ? FlatVector::GetData<data_ptr_t>(*row_pointers) : nullptr;
	info.InitializeInputFormats(group_sources.size());
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			continue;
		}
		auto &input = payload_input.data[source.input_vector_index];
		input.ToUnifiedFormat(info.InputFormat(group_idx));
	}
	return true;
}

static bool AggregatePrepareRowPointerDescriptorSourceInfo(
    DataChunk &payload_input, Vector &row_pointers, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<LogicalType> &layout_types, AggregateRowPointerDescriptorSourceInfo &info) {
	return AggregatePrepareDescriptorSourceInfo(payload_input, optional_ptr<Vector>(&row_pointers), group_sources,
	                                            layout_types, info);
}

static bool AggregateDescriptorSourceValueData(const AggregateRowPointerDescriptorSourceInfo &info,
                                               const ExecutionRowPointerGroupKeySource &source, idx_t group_idx,
                                               idx_t row_idx, const_data_ptr_t &source_data, idx_t &source_idx,
                                               bool &source_is_valid) {
	source_data = nullptr;
	source_idx = 0;
	source_is_valid = false;
	if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
		if (!info.row_pointers) {
			return false;
		}
		auto row_pointer = info.row_pointers[row_idx];
		if (!row_pointer) {
			return true;
		}
		if (!source.all_valid &&
		    (source.row_layout_column_idx == DConstants::INVALID_INDEX || source.row_layout_column_count == 0)) {
			return false;
		}
		if (!source.all_valid && !AggregateRowPointerGroupKeySourceIsValid(row_pointer, source)) {
			return true;
		}
		source_is_valid = true;
		source_data = row_pointer + source.row_layout_offset;
		return true;
	}
	if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
		auto &format = info.InputFormat(group_idx);
		source_idx = format.sel->get_index(row_idx);
		if (!format.validity.RowIsValid(source_idx)) {
			return true;
		}
		source_is_valid = true;
		source_data = format.data;
		return true;
	}
	return false;
}

static bool
AggregateDescriptorSourcesUseOnlyRowPointerFields(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	return ExecutionRowPointerGroupKeySourcesAreRowPointerFields(group_sources);
}

static bool AggregateDescriptorSourcesAreAllValid(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	for (auto &source : group_sources) {
		if (!source.all_valid) {
			return false;
		}
	}
	return !group_sources.empty();
}

static bool AggregateDescriptorLeadingKeyHashSourceSupported(const ExecutionRowPointerGroupKeySource &source) {
	if (source.cast_kind != ExecutionRowPointerGroupKeyCastKind::NONE ||
	    source.source_physical_type != source.target_physical_type) {
		return false;
	}
	switch (source.target_physical_type) {
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
		return true;
	default:
		return false;
	}
}

static bool AggregateDescriptorLeadingKeyHashIsJoinKey(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (group_sources.size() < 2) {
		return false;
	}
	auto &source = group_sources[0];
	return source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR &&
	       source.hash_join_condition_idx != DConstants::INVALID_INDEX &&
	       !ExecutionRowPointerGroupKeySourcesAreRowPointerFields(group_sources);
}

static bool AggregateDescriptorCanUseLeadingKeyHash(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (group_sources.empty() || !AggregateDescriptorLeadingKeyHashSourceSupported(group_sources[0])) {
		return false;
	}
	if (AggregateDescriptorLeadingKeyHashIsJoinKey(group_sources)) {
		return true;
	}
	return group_sources.size() >= 4;
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
	case PhysicalType::UINT128: {
		const auto value = Load<uhugeint_t>(source_data + source_idx * sizeof(uhugeint_t));
		return MurmurHash64(value.lower) ^ MurmurHash64(value.upper);
	}
	case PhysicalType::INT128: {
		const auto value = Load<hugeint_t>(source_data + source_idx * sizeof(hugeint_t));
		return MurmurHash64(value.lower) ^ MurmurHash64(static_cast<uint64_t>(value.upper));
	}
	case PhysicalType::INTERVAL:
		return Hash(Load<interval_t>(source_data + source_idx * sizeof(interval_t)));
	case PhysicalType::VARCHAR:
		return Hash(Load<string_t>(source_data + source_idx * sizeof(string_t)));
	default:
		throw InternalException("Unsupported direct row-pointer aggregate descriptor hash type");
	}
}

template <class T>
static bool AggregateDescriptorRawSourceValuesMatch(const_data_ptr_t left_data, idx_t left_idx,
                                                    const_data_ptr_t right_data, idx_t right_idx) {
	return Load<T>(left_data + left_idx * sizeof(T)) == Load<T>(right_data + right_idx * sizeof(T));
}

static bool AggregateDescriptorRawStringValuesMatch(const_data_ptr_t left_data, idx_t left_idx,
                                                    const_data_ptr_t right_data, idx_t right_idx) {
	const auto left_values = reinterpret_cast<const string_t *>(left_data);
	const auto right_values = reinterpret_cast<const string_t *>(right_data);
	return left_values[left_idx] == right_values[right_idx];
}

static bool AggregateDescriptorRawSourceValuesMatch(const_data_ptr_t left_data, idx_t left_idx,
                                                    const_data_ptr_t right_data, idx_t right_idx, PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
		return AggregateDescriptorRawSourceValuesMatch<bool>(left_data, left_idx, right_data, right_idx);
	case PhysicalType::UINT8:
		return AggregateDescriptorRawSourceValuesMatch<uint8_t>(left_data, left_idx, right_data, right_idx);
	case PhysicalType::INT8:
		return AggregateDescriptorRawSourceValuesMatch<int8_t>(left_data, left_idx, right_data, right_idx);
	case PhysicalType::UINT16:
		return AggregateDescriptorRawSourceValuesMatch<uint16_t>(left_data, left_idx, right_data, right_idx);
	case PhysicalType::INT16:
		return AggregateDescriptorRawSourceValuesMatch<int16_t>(left_data, left_idx, right_data, right_idx);
	case PhysicalType::UINT32:
		return AggregateDescriptorRawSourceValuesMatch<uint32_t>(left_data, left_idx, right_data, right_idx);
	case PhysicalType::INT32:
		return AggregateDescriptorRawSourceValuesMatch<int32_t>(left_data, left_idx, right_data, right_idx);
	case PhysicalType::UINT64:
		return AggregateDescriptorRawSourceValuesMatch<uint64_t>(left_data, left_idx, right_data, right_idx);
	case PhysicalType::INT64:
		return AggregateDescriptorRawSourceValuesMatch<int64_t>(left_data, left_idx, right_data, right_idx);
	case PhysicalType::UINT128:
		return AggregateDescriptorRawSourceValuesMatch<uhugeint_t>(left_data, left_idx, right_data, right_idx);
	case PhysicalType::INT128:
		return AggregateDescriptorRawSourceValuesMatch<hugeint_t>(left_data, left_idx, right_data, right_idx);
	case PhysicalType::INTERVAL:
		return AggregateDescriptorRawSourceValuesMatch<interval_t>(left_data, left_idx, right_data, right_idx);
	case PhysicalType::VARCHAR:
		return AggregateDescriptorRawStringValuesMatch(left_data, left_idx, right_data, right_idx);
	default:
		throw InternalException("Unsupported direct row-pointer aggregate descriptor compare type");
	}
}

template <class T>
static bool AggregateDescriptorRawSourceValueMatchesStored(const_data_ptr_t source_data, idx_t source_idx,
                                                           data_ptr_t row_location, idx_t layout_offset) {
	return Load<T>(source_data + source_idx * sizeof(T)) == Load<T>(row_location + layout_offset);
}

static bool AggregateDescriptorRawStringValueMatchesStored(const_data_ptr_t source_data, idx_t source_idx,
                                                           data_ptr_t row_location, idx_t layout_offset) {
	const auto source_values = reinterpret_cast<const string_t *>(source_data);
	const auto row_value = Load<string_t>(row_location + layout_offset);
	return source_values[source_idx] == row_value;
}

static bool AggregateDescriptorRawSourceValueMatchesStored(const_data_ptr_t source_data, idx_t source_idx,
                                                           data_ptr_t row_location, idx_t layout_offset,
                                                           PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
		return AggregateDescriptorRawSourceValueMatchesStored<bool>(source_data, source_idx, row_location,
		                                                            layout_offset);
	case PhysicalType::UINT8:
		return AggregateDescriptorRawSourceValueMatchesStored<uint8_t>(source_data, source_idx, row_location,
		                                                               layout_offset);
	case PhysicalType::INT8:
		return AggregateDescriptorRawSourceValueMatchesStored<int8_t>(source_data, source_idx, row_location,
		                                                              layout_offset);
	case PhysicalType::UINT16:
		return AggregateDescriptorRawSourceValueMatchesStored<uint16_t>(source_data, source_idx, row_location,
		                                                                layout_offset);
	case PhysicalType::INT16:
		return AggregateDescriptorRawSourceValueMatchesStored<int16_t>(source_data, source_idx, row_location,
		                                                               layout_offset);
	case PhysicalType::UINT32:
		return AggregateDescriptorRawSourceValueMatchesStored<uint32_t>(source_data, source_idx, row_location,
		                                                                layout_offset);
	case PhysicalType::INT32:
		return AggregateDescriptorRawSourceValueMatchesStored<int32_t>(source_data, source_idx, row_location,
		                                                               layout_offset);
	case PhysicalType::UINT64:
		return AggregateDescriptorRawSourceValueMatchesStored<uint64_t>(source_data, source_idx, row_location,
		                                                                layout_offset);
	case PhysicalType::INT64:
		return AggregateDescriptorRawSourceValueMatchesStored<int64_t>(source_data, source_idx, row_location,
		                                                               layout_offset);
	case PhysicalType::UINT128:
		return AggregateDescriptorRawSourceValueMatchesStored<uhugeint_t>(source_data, source_idx, row_location,
		                                                                  layout_offset);
	case PhysicalType::INT128:
		return AggregateDescriptorRawSourceValueMatchesStored<hugeint_t>(source_data, source_idx, row_location,
		                                                                 layout_offset);
	case PhysicalType::INTERVAL:
		return AggregateDescriptorRawSourceValueMatchesStored<interval_t>(source_data, source_idx, row_location,
		                                                                  layout_offset);
	case PhysicalType::VARCHAR:
		return AggregateDescriptorRawStringValueMatchesStored(source_data, source_idx, row_location, layout_offset);
	default:
		throw InternalException("Unsupported direct row-pointer aggregate descriptor stored compare type");
	}
}

template <class SRC, class DST>
static bool AggregateHashIntegralCompressedDescriptorValue(const_data_ptr_t source_data, idx_t source_idx,
                                                           const ExecutionRowPointerGroupKeySource &source,
                                                           hash_t &value_hash) {
	DST target_value;
	if (!AggregateIntegralCompressedGroupKeyValue<SRC, DST>(source_data, source_idx, source, target_value)) {
		return false;
	}
	value_hash = Hash(target_value);
	return true;
}

template <class SRC>
static bool AggregateHashIntegralCompressedDescriptorValue(const_data_ptr_t source_data, idx_t source_idx,
                                                           const ExecutionRowPointerGroupKeySource &source,
                                                           hash_t &value_hash) {
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return AggregateHashIntegralCompressedDescriptorValue<SRC, uint8_t>(source_data, source_idx, source,
		                                                                    value_hash);
	case PhysicalType::UINT16:
		return AggregateHashIntegralCompressedDescriptorValue<SRC, uint16_t>(source_data, source_idx, source,
		                                                                     value_hash);
	case PhysicalType::UINT32:
		return AggregateHashIntegralCompressedDescriptorValue<SRC, uint32_t>(source_data, source_idx, source,
		                                                                     value_hash);
	case PhysicalType::UINT64:
		return AggregateHashIntegralCompressedDescriptorValue<SRC, uint64_t>(source_data, source_idx, source,
		                                                                     value_hash);
	default:
		return false;
	}
}

static bool AggregateHashIntegralCompressedDescriptorValue(const_data_ptr_t source_data, idx_t source_idx,
                                                           const ExecutionRowPointerGroupKeySource &source,
                                                           hash_t &value_hash) {
	switch (source.source_physical_type) {
	case PhysicalType::INT8:
		return AggregateHashIntegralCompressedDescriptorValue<int8_t>(source_data, source_idx, source, value_hash);
	case PhysicalType::INT16:
		return AggregateHashIntegralCompressedDescriptorValue<int16_t>(source_data, source_idx, source, value_hash);
	case PhysicalType::INT32:
		return AggregateHashIntegralCompressedDescriptorValue<int32_t>(source_data, source_idx, source, value_hash);
	case PhysicalType::INT64:
		return AggregateHashIntegralCompressedDescriptorValue<int64_t>(source_data, source_idx, source, value_hash);
	default:
		return false;
	}
}

template <class DST>
static bool AggregateHashDateYearCompressedDescriptorValue(const_data_ptr_t source_data, idx_t source_idx,
                                                           const ExecutionRowPointerGroupKeySource &source,
                                                           hash_t &value_hash) {
	DST target_value;
	if (!AggregateDateYearCompressedGroupKeyValue<DST>(source_data, source_idx, source, target_value)) {
		return false;
	}
	value_hash = Hash(target_value);
	return true;
}

static bool AggregateHashDateYearCompressedDescriptorValue(const_data_ptr_t source_data, idx_t source_idx,
                                                           const ExecutionRowPointerGroupKeySource &source,
                                                           hash_t &value_hash) {
	if (source.source_physical_type != PhysicalType::INT32 || source.source_type.id() != LogicalTypeId::DATE) {
		return false;
	}
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return AggregateHashDateYearCompressedDescriptorValue<uint8_t>(source_data, source_idx, source, value_hash);
	case PhysicalType::UINT16:
		return AggregateHashDateYearCompressedDescriptorValue<uint16_t>(source_data, source_idx, source, value_hash);
	case PhysicalType::UINT32:
		return AggregateHashDateYearCompressedDescriptorValue<uint32_t>(source_data, source_idx, source, value_hash);
	case PhysicalType::UINT64:
		return AggregateHashDateYearCompressedDescriptorValue<uint64_t>(source_data, source_idx, source, value_hash);
	default:
		return false;
	}
}

template <class RESULT_TYPE>
static bool AggregateHashStringCompressedDescriptorValue(const string_t &value, hash_t &value_hash) {
	value_hash = Hash(StringCompressValue<RESULT_TYPE>(value));
	return true;
}

static bool AggregateHashStringCompressedDescriptorValue(const_data_ptr_t source_data, idx_t source_idx,
                                                         const ExecutionRowPointerGroupKeySource &source,
                                                         hash_t &value_hash) {
	if (source.source_physical_type != PhysicalType::VARCHAR) {
		return false;
	}
	const auto value = Load<string_t>(source_data + source_idx * sizeof(string_t));
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		value_hash = Hash(StringCompressUInt8Value(value));
		return true;
	case PhysicalType::UINT16:
		return AggregateHashStringCompressedDescriptorValue<uint16_t>(value, value_hash);
	case PhysicalType::UINT32:
		return AggregateHashStringCompressedDescriptorValue<uint32_t>(value, value_hash);
	case PhysicalType::UINT64:
		return AggregateHashStringCompressedDescriptorValue<uint64_t>(value, value_hash);
	case PhysicalType::UINT128: {
		const auto compressed_value = StringCompressValue<uhugeint_t>(value);
		value_hash = MurmurHash64(compressed_value.lower) ^ MurmurHash64(compressed_value.upper);
		return true;
	}
	default:
		return false;
	}
}

static bool AggregateHashDescriptorSourceValue(const_data_ptr_t source_data, idx_t source_idx,
                                               const ExecutionRowPointerGroupKeySource &source, hash_t &value_hash) {
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		if (source.source_physical_type != source.target_physical_type) {
			return false;
		}
		value_hash = AggregateHashDescriptorValue(source_data, source_idx, source.target_physical_type);
		return true;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32: {
		D_ASSERT(source.target_physical_type == PhysicalType::INT32);
		const auto value = Load<int64_t>(source_data + source_idx * sizeof(int64_t));
		value_hash = Hash(source.unchecked_integral_cast ? static_cast<int32_t>(value)
		                                                 : AggregateCheckedGroupKeyCast<int64_t, int32_t>(value));
		return true;
	}
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		D_ASSERT(source.target_physical_type == PhysicalType::INT16);
		{
			const auto value = Load<int64_t>(source_data + source_idx * sizeof(int64_t));
			value_hash = Hash(source.unchecked_integral_cast ? static_cast<int16_t>(value)
			                                                 : AggregateCheckedGroupKeyCast<int64_t, int16_t>(value));
		}
		return true;
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		D_ASSERT(source.target_physical_type == PhysicalType::INT8);
		{
			const auto value = Load<int32_t>(source_data + source_idx * sizeof(int32_t));
			value_hash = Hash(source.unchecked_integral_cast ? static_cast<int8_t>(value)
			                                                 : AggregateCheckedGroupKeyCast<int32_t, int8_t>(value));
		}
		return true;
	case ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS:
		return AggregateHashIntegralCompressedDescriptorValue(source_data, source_idx, source, value_hash);
	case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
		return AggregateHashDateYearCompressedDescriptorValue(source_data, source_idx, source, value_hash);
	case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS:
		return AggregateHashStringCompressedDescriptorValue(source_data, source_idx, source, value_hash);
	case ExecutionRowPointerGroupKeyCastKind::STRING_SUBSTRING: {
		if (source.source_physical_type != PhysicalType::VARCHAR ||
		    source.target_physical_type != PhysicalType::VARCHAR) {
			return false;
		}
		const auto value = Load<string_t>(source_data + source_idx * sizeof(string_t));
		value_hash = Hash(SubstringPrefixUnicode(value, source.string_substring_length));
		return true;
	}
	default:
		return false;
	}
}

template <class SRC, class DST>
static bool AggregateIntegralCompressedDescriptorValuesMatch(const_data_ptr_t left_data, idx_t left_idx,
                                                             const_data_ptr_t right_data, idx_t right_idx,
                                                             const ExecutionRowPointerGroupKeySource &source) {
	DST left_value;
	DST right_value;
	return AggregateIntegralCompressedGroupKeyValue<SRC, DST>(left_data, left_idx, source, left_value) &&
	       AggregateIntegralCompressedGroupKeyValue<SRC, DST>(right_data, right_idx, source, right_value) &&
	       left_value == right_value;
}

template <class SRC>
static bool AggregateIntegralCompressedDescriptorValuesMatch(const_data_ptr_t left_data, idx_t left_idx,
                                                             const_data_ptr_t right_data, idx_t right_idx,
                                                             const ExecutionRowPointerGroupKeySource &source) {
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return AggregateIntegralCompressedDescriptorValuesMatch<SRC, uint8_t>(left_data, left_idx, right_data,
		                                                                      right_idx, source);
	case PhysicalType::UINT16:
		return AggregateIntegralCompressedDescriptorValuesMatch<SRC, uint16_t>(left_data, left_idx, right_data,
		                                                                       right_idx, source);
	case PhysicalType::UINT32:
		return AggregateIntegralCompressedDescriptorValuesMatch<SRC, uint32_t>(left_data, left_idx, right_data,
		                                                                       right_idx, source);
	case PhysicalType::UINT64:
		return AggregateIntegralCompressedDescriptorValuesMatch<SRC, uint64_t>(left_data, left_idx, right_data,
		                                                                       right_idx, source);
	default:
		return false;
	}
}

static bool AggregateIntegralCompressedDescriptorValuesMatch(const_data_ptr_t left_data, idx_t left_idx,
                                                             const_data_ptr_t right_data, idx_t right_idx,
                                                             const ExecutionRowPointerGroupKeySource &source) {
	switch (source.source_physical_type) {
	case PhysicalType::INT8:
		return AggregateIntegralCompressedDescriptorValuesMatch<int8_t>(left_data, left_idx, right_data, right_idx,
		                                                                source);
	case PhysicalType::INT16:
		return AggregateIntegralCompressedDescriptorValuesMatch<int16_t>(left_data, left_idx, right_data, right_idx,
		                                                                 source);
	case PhysicalType::INT32:
		return AggregateIntegralCompressedDescriptorValuesMatch<int32_t>(left_data, left_idx, right_data, right_idx,
		                                                                 source);
	case PhysicalType::INT64:
		return AggregateIntegralCompressedDescriptorValuesMatch<int64_t>(left_data, left_idx, right_data, right_idx,
		                                                                 source);
	default:
		return false;
	}
}

template <class DST>
static bool AggregateDateYearCompressedDescriptorValuesMatch(const_data_ptr_t left_data, idx_t left_idx,
                                                             const_data_ptr_t right_data, idx_t right_idx,
                                                             const ExecutionRowPointerGroupKeySource &source) {
	DST left_value;
	DST right_value;
	return AggregateDateYearCompressedGroupKeyValue<DST>(left_data, left_idx, source, left_value) &&
	       AggregateDateYearCompressedGroupKeyValue<DST>(right_data, right_idx, source, right_value) &&
	       left_value == right_value;
}

static bool AggregateDateYearCompressedDescriptorValuesMatch(const_data_ptr_t left_data, idx_t left_idx,
                                                             const_data_ptr_t right_data, idx_t right_idx,
                                                             const ExecutionRowPointerGroupKeySource &source) {
	if (source.source_physical_type != PhysicalType::INT32 || source.source_type.id() != LogicalTypeId::DATE) {
		return false;
	}
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return AggregateDateYearCompressedDescriptorValuesMatch<uint8_t>(left_data, left_idx, right_data, right_idx,
		                                                                 source);
	case PhysicalType::UINT16:
		return AggregateDateYearCompressedDescriptorValuesMatch<uint16_t>(left_data, left_idx, right_data, right_idx,
		                                                                  source);
	case PhysicalType::UINT32:
		return AggregateDateYearCompressedDescriptorValuesMatch<uint32_t>(left_data, left_idx, right_data, right_idx,
		                                                                  source);
	case PhysicalType::UINT64:
		return AggregateDateYearCompressedDescriptorValuesMatch<uint64_t>(left_data, left_idx, right_data, right_idx,
		                                                                  source);
	default:
		return false;
	}
}

template <class RESULT_TYPE>
static bool AggregateStringCompressedDescriptorValuesMatch(const string_t &left, const string_t &right) {
	return StringCompressValue<RESULT_TYPE>(left) == StringCompressValue<RESULT_TYPE>(right);
}

static bool AggregateStringCompressedDescriptorValuesMatch(const_data_ptr_t left_data, idx_t left_idx,
                                                           const_data_ptr_t right_data, idx_t right_idx,
                                                           const ExecutionRowPointerGroupKeySource &source) {
	if (source.source_physical_type != PhysicalType::VARCHAR) {
		return false;
	}
	const auto left = Load<string_t>(left_data + left_idx * sizeof(string_t));
	const auto right = Load<string_t>(right_data + right_idx * sizeof(string_t));
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return StringCompressUInt8Value(left) == StringCompressUInt8Value(right);
	case PhysicalType::UINT16:
		return AggregateStringCompressedDescriptorValuesMatch<uint16_t>(left, right);
	case PhysicalType::UINT32:
		return AggregateStringCompressedDescriptorValuesMatch<uint32_t>(left, right);
	case PhysicalType::UINT64:
		return AggregateStringCompressedDescriptorValuesMatch<uint64_t>(left, right);
	case PhysicalType::UINT128:
		return AggregateStringCompressedDescriptorValuesMatch<uhugeint_t>(left, right);
	default:
		return false;
	}
}

static bool AggregateDescriptorSourceValuesMatch(const ExecutionRowPointerGroupKeySource &source,
                                                 const_data_ptr_t left_data, idx_t left_idx,
                                                 const_data_ptr_t right_data, idx_t right_idx) {
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		if (source.source_physical_type != source.target_physical_type) {
			return false;
		}
		return AggregateDescriptorRawSourceValuesMatch(left_data, left_idx, right_data, right_idx,
		                                               source.target_physical_type);
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		D_ASSERT(source.target_physical_type == PhysicalType::INT32);
		if (source.unchecked_integral_cast) {
			return static_cast<int32_t>(Load<int64_t>(left_data + left_idx * sizeof(int64_t))) ==
			       static_cast<int32_t>(Load<int64_t>(right_data + right_idx * sizeof(int64_t)));
		}
		return AggregateCheckedGroupKeyCast<int64_t, int32_t>(Load<int64_t>(left_data + left_idx * sizeof(int64_t))) ==
		       AggregateCheckedGroupKeyCast<int64_t, int32_t>(Load<int64_t>(right_data + right_idx * sizeof(int64_t)));
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		D_ASSERT(source.target_physical_type == PhysicalType::INT16);
		if (source.unchecked_integral_cast) {
			return static_cast<int16_t>(Load<int64_t>(left_data + left_idx * sizeof(int64_t))) ==
			       static_cast<int16_t>(Load<int64_t>(right_data + right_idx * sizeof(int64_t)));
		}
		return AggregateCheckedGroupKeyCast<int64_t, int16_t>(Load<int64_t>(left_data + left_idx * sizeof(int64_t))) ==
		       AggregateCheckedGroupKeyCast<int64_t, int16_t>(Load<int64_t>(right_data + right_idx * sizeof(int64_t)));
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		D_ASSERT(source.target_physical_type == PhysicalType::INT8);
		if (source.unchecked_integral_cast) {
			return static_cast<int8_t>(Load<int32_t>(left_data + left_idx * sizeof(int32_t))) ==
			       static_cast<int8_t>(Load<int32_t>(right_data + right_idx * sizeof(int32_t)));
		}
		return AggregateCheckedGroupKeyCast<int32_t, int8_t>(Load<int32_t>(left_data + left_idx * sizeof(int32_t))) ==
		       AggregateCheckedGroupKeyCast<int32_t, int8_t>(Load<int32_t>(right_data + right_idx * sizeof(int32_t)));
	case ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS:
		return AggregateIntegralCompressedDescriptorValuesMatch(left_data, left_idx, right_data, right_idx, source);
	case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
		return AggregateDateYearCompressedDescriptorValuesMatch(left_data, left_idx, right_data, right_idx, source);
	case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS:
		return AggregateStringCompressedDescriptorValuesMatch(left_data, left_idx, right_data, right_idx, source);
	case ExecutionRowPointerGroupKeyCastKind::STRING_SUBSTRING: {
		if (source.source_physical_type != PhysicalType::VARCHAR ||
		    source.target_physical_type != PhysicalType::VARCHAR) {
			return false;
		}
		const auto left = Load<string_t>(left_data + left_idx * sizeof(string_t));
		const auto right = Load<string_t>(right_data + right_idx * sizeof(string_t));
		return SubstringPrefixUnicode(left, source.string_substring_length) ==
		       SubstringPrefixUnicode(right, source.string_substring_length);
	}
	default:
		return false;
	}
}

bool ExecutionRowPointerGroupKeySourcesAreRowPointerFields(
    const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	for (auto &source : group_sources) {
		if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			return false;
		}
	}
	return !group_sources.empty();
}

static bool AggregateDescriptorSourceRepeatsWithRowPointer(const ExecutionRowPointerGroupKeySource &source) {
	if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
		return true;
	}
	return source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR &&
	       source.input_vector_repeats_with_row_pointer;
}

static bool
AggregateDescriptorSourcesRepeatWithRowPointer(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	for (auto &source : group_sources) {
		if (!AggregateDescriptorSourceRepeatsWithRowPointer(source)) {
			return false;
		}
	}
	return !group_sources.empty();
}

static void AggregateSelectFlatRowPointers(const data_ptr_t *__restrict source_row_pointers,
                                           const SelectionVector &selection, idx_t count, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(result, count_t(count));
	auto target_row_pointers = FlatVector::GetDataMutable<data_ptr_t>(result);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		target_row_pointers[row_idx] = source_row_pointers[selection.get_index_unsafe(row_idx)];
	}
}

static bool AggregateDescriptorSourceRepeatsBySourceIndex(const AggregateRowPointerDescriptorSourceInfo &info,
                                                          const ExecutionRowPointerGroupKeySource &source,
                                                          idx_t group_idx, idx_t row_idx, idx_t other_row_idx) {
	if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
		if (!info.row_pointers) {
			return false;
		}
		return info.row_pointers[row_idx] == info.row_pointers[other_row_idx];
	}
	if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
		return false;
	}
	auto &format = info.InputFormat(group_idx);
	return format.sel->get_index(row_idx) == format.sel->get_index(other_row_idx);
}

static bool
AggregateDescriptorSourcesRepeatBySourceIndex(const AggregateRowPointerDescriptorSourceInfo &info,
                                              const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                              idx_t row_idx, idx_t other_row_idx) {
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		if (!AggregateDescriptorSourceRepeatsBySourceIndex(info, group_sources[group_idx], group_idx, row_idx,
		                                                   other_row_idx)) {
			return false;
		}
	}
	return !group_sources.empty();
}

bool ExecutionRowPointerGroupKeysEqual(data_ptr_t left_row_pointer, data_ptr_t right_row_pointer,
                                       const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (!left_row_pointer || !right_row_pointer ||
	    !ExecutionRowPointerGroupKeySourcesAreRowPointerFields(group_sources)) {
		return false;
	}
	if (left_row_pointer == right_row_pointer) {
		for (auto &source : group_sources) {
			if (!AggregateRowPointerGroupKeySourceIsValid(left_row_pointer, source)) {
				return false;
			}
		}
		return true;
	}
	for (auto &source : group_sources) {
		if (!AggregateRowPointerGroupKeySourceIsValid(left_row_pointer, source) ||
		    !AggregateRowPointerGroupKeySourceIsValid(right_row_pointer, source)) {
			return false;
		}
		const auto left_data = left_row_pointer + source.row_layout_offset;
		const auto right_data = right_row_pointer + source.row_layout_offset;
		if (!AggregateDescriptorSourceValuesMatch(source, left_data, 0, right_data, 0)) {
			return false;
		}
	}
	return true;
}

template <class SRC, class DST>
static bool AggregateIntegralCompressedDescriptorValueMatchesStored(const_data_ptr_t source_data, idx_t source_idx,
                                                                    data_ptr_t row_location, idx_t layout_offset,
                                                                    const ExecutionRowPointerGroupKeySource &source) {
	DST target_value;
	if (!AggregateIntegralCompressedGroupKeyValue<SRC, DST>(source_data, source_idx, source, target_value)) {
		return false;
	}
	return Load<DST>(row_location + layout_offset) == target_value;
}

template <class SRC>
static bool AggregateIntegralCompressedDescriptorValueMatchesStored(const_data_ptr_t source_data, idx_t source_idx,
                                                                    data_ptr_t row_location, idx_t layout_offset,
                                                                    const ExecutionRowPointerGroupKeySource &source) {
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return AggregateIntegralCompressedDescriptorValueMatchesStored<SRC, uint8_t>(
		    source_data, source_idx, row_location, layout_offset, source);
	case PhysicalType::UINT16:
		return AggregateIntegralCompressedDescriptorValueMatchesStored<SRC, uint16_t>(
		    source_data, source_idx, row_location, layout_offset, source);
	case PhysicalType::UINT32:
		return AggregateIntegralCompressedDescriptorValueMatchesStored<SRC, uint32_t>(
		    source_data, source_idx, row_location, layout_offset, source);
	case PhysicalType::UINT64:
		return AggregateIntegralCompressedDescriptorValueMatchesStored<SRC, uint64_t>(
		    source_data, source_idx, row_location, layout_offset, source);
	default:
		return false;
	}
}

static bool AggregateIntegralCompressedDescriptorValueMatchesStored(const_data_ptr_t source_data, idx_t source_idx,
                                                                    data_ptr_t row_location, idx_t layout_offset,
                                                                    const ExecutionRowPointerGroupKeySource &source) {
	switch (source.source_physical_type) {
	case PhysicalType::INT8:
		return AggregateIntegralCompressedDescriptorValueMatchesStored<int8_t>(source_data, source_idx, row_location,
		                                                                       layout_offset, source);
	case PhysicalType::INT16:
		return AggregateIntegralCompressedDescriptorValueMatchesStored<int16_t>(source_data, source_idx, row_location,
		                                                                        layout_offset, source);
	case PhysicalType::INT32:
		return AggregateIntegralCompressedDescriptorValueMatchesStored<int32_t>(source_data, source_idx, row_location,
		                                                                        layout_offset, source);
	case PhysicalType::INT64:
		return AggregateIntegralCompressedDescriptorValueMatchesStored<int64_t>(source_data, source_idx, row_location,
		                                                                        layout_offset, source);
	default:
		return false;
	}
}

template <class DST>
static bool AggregateDateYearCompressedDescriptorValueMatchesStored(const_data_ptr_t source_data, idx_t source_idx,
                                                                    data_ptr_t row_location, idx_t layout_offset,
                                                                    const ExecutionRowPointerGroupKeySource &source) {
	DST target_value;
	if (!AggregateDateYearCompressedGroupKeyValue<DST>(source_data, source_idx, source, target_value)) {
		return false;
	}
	return Load<DST>(row_location + layout_offset) == target_value;
}

static bool AggregateDateYearCompressedDescriptorValueMatchesStored(const_data_ptr_t source_data, idx_t source_idx,
                                                                    data_ptr_t row_location, idx_t layout_offset,
                                                                    const ExecutionRowPointerGroupKeySource &source) {
	if (source.source_physical_type != PhysicalType::INT32 || source.source_type.id() != LogicalTypeId::DATE) {
		return false;
	}
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return AggregateDateYearCompressedDescriptorValueMatchesStored<uint8_t>(source_data, source_idx, row_location,
		                                                                        layout_offset, source);
	case PhysicalType::UINT16:
		return AggregateDateYearCompressedDescriptorValueMatchesStored<uint16_t>(source_data, source_idx, row_location,
		                                                                         layout_offset, source);
	case PhysicalType::UINT32:
		return AggregateDateYearCompressedDescriptorValueMatchesStored<uint32_t>(source_data, source_idx, row_location,
		                                                                         layout_offset, source);
	case PhysicalType::UINT64:
		return AggregateDateYearCompressedDescriptorValueMatchesStored<uint64_t>(source_data, source_idx, row_location,
		                                                                         layout_offset, source);
	default:
		return false;
	}
}

template <class RESULT_TYPE>
static bool AggregateStringCompressedDescriptorValueMatchesStored(const string_t &value, data_ptr_t row_location,
                                                                  idx_t layout_offset) {
	return Load<RESULT_TYPE>(row_location + layout_offset) == StringCompressValue<RESULT_TYPE>(value);
}

static bool AggregateStringCompressedDescriptorValueMatchesStored(const_data_ptr_t source_data, idx_t source_idx,
                                                                  data_ptr_t row_location, idx_t layout_offset,
                                                                  const ExecutionRowPointerGroupKeySource &source) {
	if (source.source_physical_type != PhysicalType::VARCHAR) {
		return false;
	}
	const auto value = Load<string_t>(source_data + source_idx * sizeof(string_t));
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return Load<uint8_t>(row_location + layout_offset) == StringCompressUInt8Value(value);
	case PhysicalType::UINT16:
		return AggregateStringCompressedDescriptorValueMatchesStored<uint16_t>(value, row_location, layout_offset);
	case PhysicalType::UINT32:
		return AggregateStringCompressedDescriptorValueMatchesStored<uint32_t>(value, row_location, layout_offset);
	case PhysicalType::UINT64:
		return AggregateStringCompressedDescriptorValueMatchesStored<uint64_t>(value, row_location, layout_offset);
	case PhysicalType::UINT128:
		return AggregateStringCompressedDescriptorValueMatchesStored<uhugeint_t>(value, row_location, layout_offset);
	default:
		return false;
	}
}

static bool AggregateDescriptorSourceValueMatchesStored(const ExecutionRowPointerGroupKeySource &source,
                                                        const_data_ptr_t source_data, idx_t source_idx,
                                                        data_ptr_t row_location, idx_t layout_offset) {
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		if (source.source_physical_type != source.target_physical_type) {
			return false;
		}
		return AggregateDescriptorRawSourceValueMatchesStored(source_data, source_idx, row_location, layout_offset,
		                                                      source.target_physical_type);
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32: {
		D_ASSERT(source.target_physical_type == PhysicalType::INT32);
		const auto value = Load<int64_t>(source_data + source_idx * sizeof(int64_t));
		const auto target_value = source.unchecked_integral_cast
		                              ? static_cast<int32_t>(value)
		                              : AggregateCheckedGroupKeyCast<int64_t, int32_t>(value);
		return Load<int32_t>(row_location + layout_offset) == target_value;
	}
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16: {
		D_ASSERT(source.target_physical_type == PhysicalType::INT16);
		const auto value = Load<int64_t>(source_data + source_idx * sizeof(int64_t));
		const auto target_value = source.unchecked_integral_cast
		                              ? static_cast<int16_t>(value)
		                              : AggregateCheckedGroupKeyCast<int64_t, int16_t>(value);
		return Load<int16_t>(row_location + layout_offset) == target_value;
	}
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8: {
		D_ASSERT(source.target_physical_type == PhysicalType::INT8);
		const auto value = Load<int32_t>(source_data + source_idx * sizeof(int32_t));
		const auto target_value = source.unchecked_integral_cast ? static_cast<int8_t>(value)
		                                                         : AggregateCheckedGroupKeyCast<int32_t, int8_t>(value);
		return Load<int8_t>(row_location + layout_offset) == target_value;
	}
	case ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS:
		return AggregateIntegralCompressedDescriptorValueMatchesStored(source_data, source_idx, row_location,
		                                                               layout_offset, source);
	case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
		return AggregateDateYearCompressedDescriptorValueMatchesStored(source_data, source_idx, row_location,
		                                                               layout_offset, source);
	case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS:
		return AggregateStringCompressedDescriptorValueMatchesStored(source_data, source_idx, row_location,
		                                                             layout_offset, source);
	case ExecutionRowPointerGroupKeyCastKind::STRING_SUBSTRING: {
		if (source.source_physical_type != PhysicalType::VARCHAR ||
		    source.target_physical_type != PhysicalType::VARCHAR) {
			return false;
		}
		const auto value = Load<string_t>(source_data + source_idx * sizeof(string_t));
		const auto target_value = SubstringPrefixUnicode(value, source.string_substring_length);
		return target_value == Load<string_t>(row_location + layout_offset);
	}
	default:
		return false;
	}
}

static bool AggregateHashDescriptorRows(const AggregateRowPointerDescriptorSourceInfo &info,
                                        const vector<ExecutionRowPointerGroupKeySource> &group_sources, idx_t count,
                                        Vector &hashes, bool require_canonical_group_hash) {
	hashes.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(hashes, count_t(count));
	auto hash_data = FlatVector::GetDataMutable<hash_t>(hashes);
	const auto can_reuse_row_pointer_hashes =
	    info.row_pointers && AggregateDescriptorSourcesRepeatWithRowPointer(group_sources);
	bool can_reuse_source_index_hashes = false;
	if (!can_reuse_row_pointer_hashes) {
		const auto sample_count = MinValue<idx_t>(count, 64);
		for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
			if (AggregateDescriptorSourcesRepeatBySourceIndex(info, group_sources, row_idx, row_idx - 1)) {
				can_reuse_source_index_hashes = true;
				break;
			}
		}
	}
	// The stored hash is also the probe hash, the partition-routing key, and the value the final
	// combine re-reads verbatim, so it must be a canonical function of the full group key whenever
	// two runners can feed one table: a mid-query handoff (adaptive A/B, forced defer) mixes native
	// rows (full-column DataChunk::Hash) with compiled rows, and a leading-key-only truncation would
	// then split one group across two stored hashes into two partitions that never merge. When no
	// handoff can occur the compiled runner owns the table alone, so the cheaper leading-key hash is
	// sound. Each per-column descriptor hash already equals VectorOperations::Hash, so folding every
	// column yields exactly the canonical hash.
	const auto use_leading_key_hash =
	    !require_canonical_group_hash && AggregateDescriptorCanUseLeadingKeyHash(group_sources);
	const idx_t hash_group_count = use_leading_key_hash ? 1 : group_sources.size();
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (can_reuse_row_pointer_hashes && row_idx > 0 &&
		    info.row_pointers[row_idx] == info.row_pointers[row_idx - 1]) {
			hash_data[row_idx] = hash_data[row_idx - 1];
			continue;
		}
		if (can_reuse_source_index_hashes && row_idx > 0 &&
		    AggregateDescriptorSourcesRepeatBySourceIndex(info, group_sources, row_idx, row_idx - 1)) {
			hash_data[row_idx] = hash_data[row_idx - 1];
			continue;
		}
		hash_t result = 0;
		for (idx_t group_idx = 0; group_idx < hash_group_count; group_idx++) {
			auto &source = group_sources[group_idx];
			const_data_ptr_t source_data;
			idx_t source_idx;
			bool source_is_valid;
			if (!AggregateDescriptorSourceValueData(info, source, group_idx, row_idx, source_data, source_idx,
			                                        source_is_valid)) {
				return false;
			}
			hash_t value_hash;
			if (source_is_valid) {
				if (!AggregateHashDescriptorSourceValue(source_data, source_idx, source, value_hash)) {
					return false;
				}
			} else {
				value_hash = AGGREGATE_DESCRIPTOR_NULL_HASH;
			}
			result = group_idx == 0 ? value_hash : CombineHashScalar(result, value_hash);
		}
		hash_data[row_idx] = result;
	}
	return true;
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
		bool left_is_valid;
		bool right_is_valid;
		if (!AggregateDescriptorSourceValueData(info, source, group_idx, row_idx, left_data, left_idx, left_is_valid) ||
		    !AggregateDescriptorSourceValueData(info, source, group_idx, other_row_idx, right_data, right_idx,
		                                        right_is_valid)) {
			return false;
		}
		if (left_is_valid != right_is_valid) {
			return false;
		}
		if (!left_is_valid) {
			continue;
		}
		if (!AggregateDescriptorSourceValuesMatch(source, left_data, left_idx, right_data, right_idx)) {
			return false;
		}
	}
	return true;
}

static bool AggregateDescriptorSourceRowMatchesStored(const AggregateRowPointerDescriptorSourceInfo &info,
                                                      const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                      const TupleDataLayout &layout, data_ptr_t row_location,
                                                      idx_t row_idx, bool descriptor_sources_are_all_valid) {
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		const_data_ptr_t source_data;
		idx_t source_idx;
		bool source_is_valid;
		if (!AggregateDescriptorSourceValueData(info, source, group_idx, row_idx, source_data, source_idx,
		                                        source_is_valid)) {
			return false;
		}
		if (descriptor_sources_are_all_valid) {
			if (!source_is_valid) {
				return false;
			}
		} else {
			const auto stored_is_valid = AggregateStoredGroupKeyIsValid(row_location, layout, group_idx);
			if (source_is_valid != stored_is_valid) {
				return false;
			}
			if (!source_is_valid) {
				continue;
			}
		}
		if (!AggregateDescriptorSourceValueMatchesStored(source, source_data, source_idx, row_location,
		                                                 layout.GetOffsets()[group_idx])) {
			return false;
		}
	}
	return true;
}

static bool AggregateFillDescriptorGroupChunk(Allocator &allocator, const AggregateRowPointerDescriptorSourceInfo &info,
                                              const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                              optional_ptr<const SelectionVector> selected_rows, idx_t selected_count,
                                              idx_t count, const vector<LogicalType> &layout_types, DataChunk &groups) {
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
			const auto row_idx = selected_rows ? selected_rows->get_index_unsafe(selected_idx) : selected_idx;
			const_data_ptr_t source_data;
			idx_t source_idx;
			bool source_is_valid;
			if (!AggregateDescriptorSourceValueData(info, source, group_idx, row_idx, source_data, source_idx,
			                                        source_is_valid)) {
				return false;
			}
			if (!source_is_valid) {
				FlatVector::ValidityMutable(target).SetInvalid(row_idx);
				continue;
			}
			if (!AggregateStoreDescriptorGroupKeyValue(source_data, source_idx, target, row_idx, source)) {
				return false;
			}
		}
	}
	groups.SetChildCardinality(count);
	return true;
}

static bool AggregateFillCompactRowPointerFieldDescriptorGroupChunk(
    Allocator &allocator, const AggregateRowPointerDescriptorSourceInfo &info,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const SelectionVector &selected_rows,
    idx_t selected_count, const vector<LogicalType> &layout_types, DataChunk &groups) {
	vector<LogicalType> group_types;
	group_types.reserve(group_sources.size());
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		group_types.push_back(layout_types[group_idx]);
	}
	AggregateEnsureDescriptorGroupChunk(allocator, groups, group_types);
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD ||
		    source.row_layout_offset == DConstants::INVALID_INDEX ||
		    (!source.all_valid &&
		     (source.row_layout_column_idx == DConstants::INVALID_INDEX || source.row_layout_column_count == 0))) {
			return false;
		}
		auto &target = groups.data[group_idx];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::ValidityMutable(target).SetAllValid(selected_count);
		FlatVector::SetSize(target, count_t(selected_count));
		if (source.cast_kind == ExecutionRowPointerGroupKeyCastKind::NONE &&
		    source.source_physical_type == source.target_physical_type) {
			const auto value_size = AggregateDescriptorValueSize(source.target_physical_type);
			auto target_data = FlatVector::GetDataMutable(target);
			for (idx_t selected_idx = 0; selected_idx < selected_count; selected_idx++) {
				const auto row_idx = selected_rows.get_index_unsafe(selected_idx);
				auto row_pointer = info.row_pointers[row_idx];
				if (!row_pointer ||
				    (!source.all_valid && !AggregateRowPointerGroupKeySourceIsValid(row_pointer, source))) {
					FlatVector::ValidityMutable(target).SetInvalid(selected_idx);
					continue;
				}
				std::memcpy(target_data + selected_idx * value_size, row_pointer + source.row_layout_offset,
				            value_size);
			}
			continue;
		}
		for (idx_t selected_idx = 0; selected_idx < selected_count; selected_idx++) {
			const auto row_idx = selected_rows.get_index_unsafe(selected_idx);
			auto row_pointer = info.row_pointers[row_idx];
			if (!row_pointer || (!source.all_valid && !AggregateRowPointerGroupKeySourceIsValid(row_pointer, source))) {
				FlatVector::ValidityMutable(target).SetInvalid(selected_idx);
				continue;
			}
			if (!AggregateStoreDescriptorGroupKeyValue(row_pointer + source.row_layout_offset, 0, target, selected_idx,
			                                           source)) {
				return false;
			}
		}
	}
	groups.SetChildCardinality(selected_count);
	return true;
}

static bool
AggregateDescriptorSourcesUseOnlyInputVectors(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	for (auto &source : group_sources) {
		if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			return false;
		}
	}
	return !group_sources.empty();
}

static bool AggregateFillCompactInputVectorRawGroupSource(const AggregateRowPointerDescriptorSourceInfo &info,
                                                          const ExecutionRowPointerGroupKeySource &source,
                                                          idx_t group_idx, const SelectionVector &selected_rows,
                                                          idx_t selected_count, Vector &target) {
	if (source.cast_kind != ExecutionRowPointerGroupKeyCastKind::NONE ||
	    source.source_physical_type != source.target_physical_type) {
		return false;
	}
	const auto value_size = AggregateDescriptorValueSize(source.target_physical_type);
	auto &format = info.InputFormat(group_idx);
	auto target_data = FlatVector::GetDataMutable(target);
	for (idx_t selected_idx = 0; selected_idx < selected_count; selected_idx++) {
		const auto row_idx = selected_rows.get_index_unsafe(selected_idx);
		const auto source_idx = format.sel->get_index(row_idx);
		if (!format.validity.RowIsValid(source_idx)) {
			FlatVector::ValidityMutable(target).SetInvalid(selected_idx);
			continue;
		}
		std::memcpy(target_data + selected_idx * value_size, format.data + source_idx * value_size, value_size);
	}
	return true;
}

template <class SRC, class DST>
static void AggregateFillCompactInputVectorIntegralCastGroupSource(const AggregateRowPointerDescriptorSourceInfo &info,
                                                                   const ExecutionRowPointerGroupKeySource &source,
                                                                   idx_t group_idx,
                                                                   const SelectionVector &selected_rows,
                                                                   idx_t selected_count, Vector &target) {
	auto &format = info.InputFormat(group_idx);
	auto target_values = FlatVector::GetDataMutable<DST>(target);
	for (idx_t selected_idx = 0; selected_idx < selected_count; selected_idx++) {
		const auto row_idx = selected_rows.get_index_unsafe(selected_idx);
		const auto source_idx = format.sel->get_index(row_idx);
		if (!format.validity.RowIsValid(source_idx)) {
			FlatVector::ValidityMutable(target).SetInvalid(selected_idx);
			continue;
		}
		const auto value = Load<SRC>(format.data + source_idx * sizeof(SRC));
		target_values[selected_idx] =
		    source.unchecked_integral_cast ? static_cast<DST>(value) : AggregateCheckedGroupKeyCast<SRC, DST>(value);
	}
}

static bool AggregateFillCompactInputVectorGroupSource(const AggregateRowPointerDescriptorSourceInfo &info,
                                                       const ExecutionRowPointerGroupKeySource &source, idx_t group_idx,
                                                       const SelectionVector &selected_rows, idx_t selected_count,
                                                       Vector &target) {
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		return AggregateFillCompactInputVectorRawGroupSource(info, source, group_idx, selected_rows, selected_count,
		                                                     target);
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		if (source.source_physical_type != PhysicalType::INT64 || source.target_physical_type != PhysicalType::INT32) {
			return false;
		}
		AggregateFillCompactInputVectorIntegralCastGroupSource<int64_t, int32_t>(info, source, group_idx, selected_rows,
		                                                                         selected_count, target);
		return true;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		if (source.source_physical_type != PhysicalType::INT64 || source.target_physical_type != PhysicalType::INT16) {
			return false;
		}
		AggregateFillCompactInputVectorIntegralCastGroupSource<int64_t, int16_t>(info, source, group_idx, selected_rows,
		                                                                         selected_count, target);
		return true;
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		if (source.source_physical_type != PhysicalType::INT32 || source.target_physical_type != PhysicalType::INT8) {
			return false;
		}
		AggregateFillCompactInputVectorIntegralCastGroupSource<int32_t, int8_t>(info, source, group_idx, selected_rows,
		                                                                        selected_count, target);
		return true;
	default:
		return false;
	}
}

static bool AggregateFillCompactInputVectorDescriptorGroupChunk(
    Allocator &allocator, const AggregateRowPointerDescriptorSourceInfo &info,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const SelectionVector &selected_rows,
    idx_t selected_count, const vector<LogicalType> &layout_types, DataChunk &groups) {
	if (!AggregateDescriptorSourcesUseOnlyInputVectors(group_sources)) {
		return false;
	}
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
		FlatVector::ValidityMutable(target).SetAllValid(selected_count);
		FlatVector::SetSize(target, count_t(selected_count));
		if (!AggregateFillCompactInputVectorGroupSource(info, source, group_idx, selected_rows, selected_count,
		                                                target)) {
			return false;
		}
	}
	groups.SetChildCardinality(selected_count);
	return true;
}

static bool AggregateFillCompactDescriptorGroupChunk(Allocator &allocator,
                                                     const AggregateRowPointerDescriptorSourceInfo &info,
                                                     const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                     const SelectionVector &selected_rows, idx_t selected_count,
                                                     const vector<LogicalType> &layout_types, const hash_t *hashes,
                                                     DataChunk &groups, Vector &group_hashes) {
	if (AggregateDescriptorSourcesUseOnlyRowPointerFields(group_sources) &&
	    !AggregateFillCompactRowPointerFieldDescriptorGroupChunk(allocator, info, group_sources, selected_rows,
	                                                             selected_count, layout_types, groups)) {
		return false;
	}
	if (AggregateDescriptorSourcesUseOnlyInputVectors(group_sources) &&
	    !AggregateFillCompactInputVectorDescriptorGroupChunk(allocator, info, group_sources, selected_rows,
	                                                         selected_count, layout_types, groups)) {
		return false;
	}
	if (!AggregateDescriptorSourcesUseOnlyRowPointerFields(group_sources) &&
	    !AggregateDescriptorSourcesUseOnlyInputVectors(group_sources)) {
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
			FlatVector::ValidityMutable(target).SetAllValid(selected_count);
			FlatVector::SetSize(target, count_t(selected_count));
			for (idx_t selected_idx = 0; selected_idx < selected_count; selected_idx++) {
				const auto row_idx = selected_rows.get_index_unsafe(selected_idx);
				const_data_ptr_t source_data;
				idx_t source_idx;
				bool source_is_valid;
				if (!AggregateDescriptorSourceValueData(info, source, group_idx, row_idx, source_data, source_idx,
				                                        source_is_valid)) {
					return false;
				}
				if (!source_is_valid) {
					FlatVector::ValidityMutable(target).SetInvalid(selected_idx);
					continue;
				}
				if (!AggregateStoreDescriptorGroupKeyValue(source_data, source_idx, target, selected_idx, source)) {
					return false;
				}
			}
		}
		groups.SetChildCardinality(selected_count);
	}

	group_hashes.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(group_hashes, count_t(selected_count));
	auto compact_hashes = FlatVector::GetDataMutable<hash_t>(group_hashes);
	for (idx_t selected_idx = 0; selected_idx < selected_count; selected_idx++) {
		const auto row_idx = selected_rows.get_index_unsafe(selected_idx);
		compact_hashes[selected_idx] = hashes[row_idx];
	}
	return true;
}

static bool
AggregateCanReferenceInputVectorDescriptorGroupChunk(DataChunk &payload_input, optional_ptr<Vector> row_pointers,
                                                     const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                     const vector<LogicalType> &layout_types) {
	if (row_pointers || group_sources.empty() || group_sources.size() >= layout_types.size()) {
		return false;
	}
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR ||
		    source.cast_kind != ExecutionRowPointerGroupKeyCastKind::NONE ||
		    source.source_physical_type != source.target_physical_type ||
		    source.target_physical_type != layout_types[group_idx].InternalType() ||
		    source.target_type != layout_types[group_idx] || source.input_vector_index == DConstants::INVALID_INDEX ||
		    source.input_vector_index >= payload_input.ColumnCount()) {
			return false;
		}
		auto &input = payload_input.data[source.input_vector_index];
		if (input.GetType() != layout_types[group_idx]) {
			return false;
		}
	}
	return true;
}

static bool
AggregateTryReferenceInputVectorDescriptorGroupChunk(DataChunk &payload_input, optional_ptr<Vector> row_pointers,
                                                     const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                     const vector<LogicalType> &layout_types, Vector &hashes,
                                                     idx_t count, DataChunk &groups) {
	if (!AggregateCanReferenceInputVectorDescriptorGroupChunk(payload_input, row_pointers, group_sources,
	                                                          layout_types)) {
		return false;
	}
	if (groups.ColumnCount() != layout_types.size()) {
		return false;
	}
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		groups.data[group_idx].Reference(payload_input.data[source.input_vector_index]);
	}
	groups.data[group_sources.size()].Reference(hashes);
	groups.SetChildCardinality(count);
	return true;
}

static bool AggregateFillDescriptorGroupChunk(Allocator &allocator, DataChunk &payload_input, Vector &row_pointers,
                                              idx_t count,
                                              const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                              const vector<LogicalType> &layout_types, DataChunk &groups) {
	AggregateRowPointerDescriptorSourceInfo info;
	if (!AggregatePrepareRowPointerDescriptorSourceInfo(payload_input, row_pointers, group_sources, layout_types,
	                                                    info)) {
		return false;
	}
	return AggregateFillDescriptorGroupChunk(allocator, info, group_sources, nullptr, count, count, layout_types,
	                                         groups);
}

template <class T>
static bool AggregateDescriptorDenseKeyFromTargetValue(T value, idx_t &key) {
	if constexpr (std::is_same<T, bool>::value) {
		key = value ? 1 : 0;
		return true;
	} else {
		if constexpr (std::is_signed<T>::value) {
			if (value < 0) {
				return false;
			}
		}
		using UNSIGNED_T = typename std::make_unsigned<T>::type;
		auto unsigned_value = static_cast<UNSIGNED_T>(value);
		if (unsigned_value > NumericLimits<idx_t>::Maximum()) {
			return false;
		}
		key = static_cast<idx_t>(unsigned_value);
		return true;
	}
}

template <class T>
bool GroupedAggregateHashTable::TryFindOrCreateSingleInputVectorGroupsDenseTemplated(
    DataChunk &payload_input, idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    optional_ptr<ExecutionGroupedAggregateStateTargetBatch> targets,
    optional_ptr<const ExecutionPrimitiveAggregateUpdateLane> count_one_lane,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	if (targets) {
		targets->Reset();
	}
	if (static_cast<bool>(targets) == static_cast<bool>(count_one_lane)) {
		return false;
	}
	if (count_one_lane && (!count_one_lane->ready || count_one_lane->state_size == 0 ||
	                       (count_one_lane->kind != AggregatePrimitiveUpdateKind::COUNT &&
	                        count_one_lane->kind != AggregatePrimitiveUpdateKind::COUNT_STAR) ||
	                       count_one_lane->state_value_offset + sizeof(int64_t) > count_one_lane->state_size)) {
		return false;
	}
	if (count == 0) {
		return true;
	}
	if (count > STANDARD_VECTOR_SIZE) {
		dense_single_field_target_cache.Disable();
		return false;
	}
	if (dense_single_field_target_cache.disabled && dense_domain && dense_domain->ready) {
		dense_single_field_target_cache.Reset();
	}
	if (enable_hll || skip_lookups || !entries || group_sources.size() != 1 ||
	    dense_single_field_target_cache.disabled || payload_input.size() != count) {
		return false;
	}
	auto &source = group_sources[0];
	if (!source.ready || source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR ||
	    source.target_physical_type != layout_ptr->GetTypes()[0].InternalType()) {
		return false;
	}
	AggregateRowPointerDescriptorSourceInfo sources;
	if (!AggregatePrepareDescriptorSourceInfo(payload_input, nullptr, group_sources, layout_ptr->GetTypes(), sources)) {
		return false;
	}
	auto &input_format = sources.InputFormat(0);
	auto load_target_value = [&](idx_t row_idx, T &value) {
		const auto source_idx = input_format.sel->get_index(row_idx);
		if (!input_format.validity.RowIsValid(source_idx)) {
			return false;
		}
		switch (source.cast_kind) {
		case ExecutionRowPointerGroupKeyCastKind::NONE:
			if (source.source_physical_type != source.target_physical_type) {
				return false;
			}
			value = Load<T>(input_format.data + source_idx * sizeof(T));
			return true;
		case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
			if constexpr (std::is_same<T, int32_t>::value) {
				const auto source_value = Load<int64_t>(input_format.data + source_idx * sizeof(int64_t));
				if (source.unchecked_integral_cast) {
					value = static_cast<int32_t>(source_value);
					return true;
				}
				return TryCast::Operation<int64_t, int32_t>(source_value, value, false);
			}
			return false;
		case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
			if constexpr (std::is_same<T, int16_t>::value) {
				const auto source_value = Load<int64_t>(input_format.data + source_idx * sizeof(int64_t));
				if (source.unchecked_integral_cast) {
					value = static_cast<int16_t>(source_value);
					return true;
				}
				return TryCast::Operation<int64_t, int16_t>(source_value, value, false);
			}
			return false;
		case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
			if constexpr (std::is_same<T, int8_t>::value) {
				const auto source_value = Load<int32_t>(input_format.data + source_idx * sizeof(int32_t));
				if (source.unchecked_integral_cast) {
					value = static_cast<int8_t>(source_value);
					return true;
				}
				return TryCast::Operation<int32_t, int8_t>(source_value, value, false);
			}
			return false;
		case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
			if constexpr (std::is_same<T, uint8_t>::value || std::is_same<T, uint16_t>::value ||
			              std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value) {
				return AggregateDateYearCompressedGroupKeyValue<T>(input_format.data, source_idx, source, value);
			}
			return false;
		default:
			return false;
		}
	};

	auto &cache = dense_single_field_target_cache;
	const auto layout_offset = layout_ptr->GetOffsets()[0];
	if (!cache.Configure(source.target_physical_type, layout_offset)) {
		return false;
	}
	auto dense_value_and_key = [&](idx_t row_idx, T &value, idx_t &key) {
		const auto loaded =
		    load_target_value(row_idx, value) && AggregateDescriptorDenseKeyFromTargetValue<T>(value, key);
		D_ASSERT(loaded);
		return loaded;
	};
	auto dense_key = [&](idx_t row_idx) {
		T value;
		idx_t key;
		dense_value_and_key(row_idx, value, key);
		return key;
	};
	const auto cache_was_empty = cache.addresses.empty();
	idx_t key_range = 0;
	idx_t cache_min_key = 0;
	idx_t cache_max_key = 0;
	idx_t max_range = 0;
	bool exact_cache_range = false;
	if (dense_domain && AggregateDenseDomainCanPrepare(*dense_domain, source.target_physical_type, key_range)) {
		cache_min_key = dense_domain->min_key;
		cache_max_key = dense_domain->max_key;
		max_range = key_range;
		exact_cache_range = true;
	} else {
		idx_t min_key = NumericLimits<idx_t>::Maximum();
		idx_t max_key = 0;
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			T value;
			idx_t key;
			if (!dense_value_and_key(row_idx, value, key)) {
				cache.Disable();
				return false;
			}
			min_key = MinValue(min_key, key);
			max_key = MaxValue(max_key, key);
		}
		if (max_key == NumericLimits<idx_t>::Maximum() || !AggregateDenseKeyRange(min_key, max_key, key_range)) {
			cache.Disable();
			return false;
		}
		cache_min_key = min_key;
		cache_max_key = max_key;
		max_range = AggregateDenseTargetMaxRange(Count() + count);
	}
	if (key_range > max_range || !cache.EnsureRange(cache_min_key, cache_max_key, exact_cache_range)) {
		if (cache.physical_type != PhysicalType::INVALID || Count() != 0) {
			cache.Disable();
		}
		return false;
	}
	if (cache_was_empty && Count() != 0) {
		auto rebuild_start = AggregateTraceStart(recorder);
		if (!AggregateRebuildDenseSingleFieldTargetCacheFromData<T>(*partitioned_data, layout_offset, cache) ||
		    (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD &&
		     !AggregateRebuildDenseSingleFieldTargetCacheFromData<T>(*unpartitioned_data, layout_offset, cache))) {
			cache.Disable();
			RecordAggregateTraceStage(recorder, "find_or_create_input_vector_dense_miss.cache_rebuild", rebuild_start);
			return false;
		}
		RecordAggregateTraceStage(recorder, "find_or_create_input_vector_dense.cache_rebuild", rebuild_start);
	}

	if (Count() + count > capacity || Count() + count > ResizeThreshold()) {
		auto resize_start = AggregateTraceStart(recorder);
		Verify();
		ResizeForAdditionalGroups(count);
		RecordAggregateTraceStage(recorder, "find_or_create_input_vector_dense.resize", resize_start);
	}

	state.hashes.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(state.hashes, count_t(count));
	auto hashes = FlatVector::GetDataMutable<hash_t>(state.hashes);
	state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(state.addresses, count_t(count));
	auto selected_addresses = FlatVector::GetDataMutable<uintptr_t>(state.addresses);
	auto ht_offsets = FlatVector::GetDataMutable<uint64_t>(state.ht_offsets);
	auto duplicate_targets = FlatVector::GetDataMutable<hash_t>(state.hash_salts);

	idx_t new_group_count = 0;
	idx_t duplicate_count = 0;
	enum class LastDenseTargetMatchKind : uint8_t { NONE, NEW_GROUP, EXISTING_GROUP };
	LastDenseTargetMatchKind last_match_kind = LastDenseTargetMatchKind::NONE;
	idx_t last_key = DConstants::INVALID_INDEX;
	idx_t last_new_group_idx = DConstants::INVALID_INDEX;
	uintptr_t last_existing_address = 0;
	auto clear_marked_entries = [&]() {
		for (idx_t marked_idx = 0; marked_idx < new_group_count; marked_idx++) {
			entries[ht_offsets[marked_idx]] = ht_entry_t();
			cache.SetPendingNewGroup(dense_key(state.new_groups.get_index_unsafe(marked_idx)),
			                         DConstants::INVALID_INDEX);
		}
		new_group_count = 0;
		duplicate_count = 0;
		last_match_kind = LastDenseTargetMatchKind::NONE;
	};
	auto emit_new_duplicate = [&](idx_t row_idx, idx_t new_group_idx) {
		state.no_match_vector.set_index(duplicate_count, row_idx);
		duplicate_targets[duplicate_count] = new_group_idx;
		duplicate_count++;
	};
	auto remember_new_group = [&](idx_t key, idx_t new_group_idx) {
		last_match_kind = LastDenseTargetMatchKind::NEW_GROUP;
		last_key = key;
		last_new_group_idx = new_group_idx;
	};
	auto remember_existing_group = [&](idx_t key, uintptr_t address) {
		last_match_kind = LastDenseTargetMatchKind::EXISTING_GROUP;
		last_key = key;
		last_existing_address = address;
	};
	auto consume_state_address = [&](idx_t row_idx, uintptr_t address) {
		selected_addresses[row_idx] = address;
	};
	auto update_count_one_states = [&]() {
		if (!count_one_lane) {
			return;
		}
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto state_address = reinterpret_cast<data_ptr_t>(selected_addresses[row_idx]);
			auto state_base = state_address + count_one_lane->state_offset;
			auto count_ptr = reinterpret_cast<int64_t *>(state_base + count_one_lane->state_value_offset);
			(*count_ptr)++;
		}
	};
	auto try_emit_repeated_group = [&](idx_t key, idx_t row_idx) {
		if (last_match_kind == LastDenseTargetMatchKind::NONE || key != last_key) {
			return false;
		}
		if (last_match_kind == LastDenseTargetMatchKind::NEW_GROUP) {
			emit_new_duplicate(row_idx, last_new_group_idx);
			return true;
		}
		consume_state_address(row_idx, last_existing_address);
		return true;
	};
	auto stored_key_matches = [&](data_ptr_t row_location, T value) {
		if (!AggregateStoredGroupKeyIsValid(row_location, *layout_ptr, 0)) {
			return false;
		}
		return Load<T>(row_location + layout_offset) == value;
	};

	auto probe_start = AggregateTraceStart(recorder);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		T value;
		idx_t key;
		if (!dense_value_and_key(row_idx, value, key) || !cache.KeyInRange(key)) {
			clear_marked_entries();
			cache.Disable();
			return false;
		}
		if (try_emit_repeated_group(key, row_idx)) {
			continue;
		}
		const auto cached_address = cache.GetAddress(key);
		if (cached_address != 0) {
			consume_state_address(row_idx, cached_address);
			remember_existing_group(key, cached_address);
			continue;
		}
		const auto pending_group_idx = cache.GetPendingNewGroup(key);
		if (pending_group_idx != DConstants::INVALID_INDEX) {
			emit_new_duplicate(row_idx, pending_group_idx);
			remember_new_group(key, pending_group_idx);
			continue;
		}

		const auto hash = Hash(value);
		hashes[row_idx] = hash;
		const auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = ApplyBitMask(hash);

		idx_t iteration_count;
		for (iteration_count = 0; iteration_count < capacity; iteration_count++) {
			auto &entry = entries[ht_offset];
			if (!entry.IsOccupied()) {
				ht_offsets[new_group_count] = ht_offset;
				state.new_groups.set_index(new_group_count, row_idx);
				cache.SetPendingNewGroup(key, new_group_count);
				remember_new_group(key, new_group_count);
				new_group_count++;
				entry.SetSalt(salt);
				break;
			}
			if (entry.GetSalt() == salt) {
				auto row_location = entry.GetPointer();
				if (cast_pointer_to_uint64(row_location) != ht_entry_t::POINTER_MASK &&
				    stored_key_matches(row_location, value)) {
					const auto address = reinterpret_cast<uintptr_t>(row_location);
					cache.SetAddress(key, address);
					consume_state_address(row_idx, address);
					remember_existing_group(key, address);
					break;
				}
			}
			SaltIncrementAndWrap(ht_offset, salt, bitmask);
		}
		if (iteration_count == capacity) {
			clear_marked_entries();
			throw InternalException("Maximum dense input-vector group iteration count reached in "
			                        "GroupedAggregateHashTable");
		}
	}
	RecordAggregateTraceStage(recorder, "find_or_create_input_vector_dense.probe", probe_start);

	if (new_group_count == 0) {
		update_count_one_states();
		if (targets) {
			auto target_start = AggregateTraceStart(recorder);
			targets->InputOrder().Set(selected_addresses, nullptr, count);
			RecordAggregateTraceStage(recorder, "find_or_create_input_vector_dense.emit_existing_targets",
			                          target_start);
		}
		sink_count += count;
		return true;
	}

	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout_ptr->GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout_ptr->GetTypes().size());
	auto append_fill_start = AggregateTraceStart(recorder);
	if (!AggregateFillDescriptorGroupChunk(allocator, sources, group_sources,
	                                       optional_ptr<const SelectionVector>(&state.new_groups), new_group_count,
	                                       count, layout_ptr->GetTypes(), state.descriptor_group_chunk)) {
		clear_marked_entries();
		RecordAggregateTraceStage(recorder, "find_or_create_input_vector_dense.append_key_fill_miss",
		                          append_fill_start);
		return false;
	}
	state.group_chunk.data[0].Reference(state.descriptor_group_chunk.data[0]);
	state.group_chunk.data[1].Reference(state.hashes);
	TupleDataCollection::ToUnifiedFormat(state.partitioned_append_state.chunk_state, state.group_chunk);
	RecordAggregateTraceStage(recorder, "find_or_create_input_vector_dense.append_key_fill", append_fill_start);

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
		const auto address = reinterpret_cast<uintptr_t>(row_location);
		const auto key = dense_key(row_idx);
		cache.SetAddress(key, address);
		cache.SetPendingNewGroup(key, DConstants::INVALID_INDEX);
		consume_state_address(row_idx, address);
	}
	for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
		const auto row_idx = state.no_match_vector.get_index_unsafe(duplicate_idx);
		const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
		const auto address = reinterpret_cast<uintptr_t>(entries[ht_offsets[marked_idx]].GetPointer());
		consume_state_address(row_idx, address);
	}
	update_count_one_states();
	if (targets) {
		auto target_start = AggregateTraceStart(recorder);
		targets->InputOrder().Set(selected_addresses, nullptr, count);
		RecordAggregateTraceStage(recorder, "find_or_create_input_vector_dense.emit_targets", target_start);
	}
	this->count += new_group_count;
	sink_count += count;
	RecordAggregateTraceStage(recorder, "find_or_create_input_vector_dense.append_new_groups", append_start);
	return true;
}

bool GroupedAggregateHashTable::TryFindOrCreateRowPointerSingleInputVectorGroupStateTargetsDense(
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, ExecutionGroupedAggregateStateTargetBatch &targets,
    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	if (row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	return TryFindOrCreateSingleInputVectorGroupStateTargetsDense(payload_input, count, group_sources, targets,
	                                                              recorder);
}

struct GroupedAggregateHashTable::SingleInputVectorGroupsDenseDispatchOp {
	template <class T, class... ARGS>
	static bool Run(GroupedAggregateHashTable &ht, ARGS &&... args) {
		return ht.TryFindOrCreateSingleInputVectorGroupsDenseTemplated<T>(std::forward<ARGS>(args)...);
	}
};

bool GroupedAggregateHashTable::TryFindOrCreateSingleInputVectorGroupStateTargetsDense(
    DataChunk &payload_input, idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    ExecutionGroupedAggregateStateTargetBatch &targets, optional_ptr<ExecutionOperatorStageRecorder> recorder,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	if (group_sources.size() != 1) {
		return false;
	}
	return AggregateDispatchDenseIntegralType<SingleInputVectorGroupsDenseDispatchOp>(
	    group_sources[0].target_physical_type, false, *this, payload_input, count, group_sources, &targets, nullptr,
	    recorder, dense_domain);
}

bool GroupedAggregateHashTable::TryUpdateSingleInputVectorGroupCountOneDense(
    DataChunk &payload_input, idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const ExecutionPrimitiveAggregateUpdateLane &lane, optional_ptr<ExecutionOperatorStageRecorder> recorder,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	if (group_sources.size() != 1) {
		return false;
	}
	return AggregateDispatchDenseIntegralType<SingleInputVectorGroupsDenseDispatchOp>(
	    group_sources[0].target_physical_type, false, *this, payload_input, count, group_sources, nullptr, &lane,
	    recorder, dense_domain);
}

template <class T>
struct AggregateRowPointerSingleFieldKey {
	bool valid = false;
	T value = T();
};

template <class T>
static bool AggregateRowPointerSingleFieldKeyValue(const_data_ptr_t source_data,
                                                   const ExecutionRowPointerGroupKeySource &source, T &value) {
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		if (source.source_physical_type != source.target_physical_type) {
			return false;
		}
		value = Load<T>(source_data);
		return true;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		if constexpr (std::is_same<T, int32_t>::value) {
			const auto source_value = Load<int64_t>(source_data);
			value = source.unchecked_integral_cast ? static_cast<int32_t>(source_value)
			                                       : AggregateCheckedGroupKeyCast<int64_t, int32_t>(source_value);
			return true;
		}
		return false;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		if constexpr (std::is_same<T, int16_t>::value) {
			const auto source_value = Load<int64_t>(source_data);
			value = source.unchecked_integral_cast ? static_cast<int16_t>(source_value)
			                                       : AggregateCheckedGroupKeyCast<int64_t, int16_t>(source_value);
			return true;
		}
		return false;
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		if constexpr (std::is_same<T, int8_t>::value) {
			const auto source_value = Load<int32_t>(source_data);
			value = source.unchecked_integral_cast ? static_cast<int8_t>(source_value)
			                                       : AggregateCheckedGroupKeyCast<int32_t, int8_t>(source_value);
			return true;
		}
		return false;
	case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
		if constexpr (std::is_same<T, uint8_t>::value || std::is_same<T, uint16_t>::value ||
		              std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value) {
			return AggregateDateYearCompressedGroupKeyValue<T>(source_data, 0, source, value);
		}
		return false;
	case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS:
		if (source.source_physical_type != PhysicalType::VARCHAR) {
			return false;
		}
		{
			const auto source_value = Load<string_t>(source_data);
			if constexpr (std::is_same<T, uint8_t>::value) {
				value = StringCompressUInt8Value(source_value);
				return true;
			} else if constexpr (std::is_same<T, uint16_t>::value) {
				value = StringCompressValue<uint16_t>(source_value);
				return true;
			} else if constexpr (std::is_same<T, uint32_t>::value) {
				value = StringCompressValue<uint32_t>(source_value);
				return true;
			} else if constexpr (std::is_same<T, uint64_t>::value) {
				value = StringCompressValue<uint64_t>(source_value);
				return true;
			}
		}
		return false;
	default:
		return false;
	}
}

template <class T>
static bool AggregateRowPointerSingleFieldKeyFromPointer(data_ptr_t row_pointer,
                                                         const ExecutionRowPointerGroupKeySource &source,
                                                         AggregateRowPointerSingleFieldKey<T> &key) {
	key = AggregateRowPointerSingleFieldKey<T>();
	if (!row_pointer) {
		return true;
	}
	if (!source.all_valid && !AggregateRowPointerGroupKeySourceIsValid(row_pointer, source)) {
		return true;
	}
	key.valid = true;
	if (!AggregateRowPointerSingleFieldKeyValue(row_pointer + source.row_layout_offset, source, key.value)) {
		return false;
	}
	return true;
}

template <class T>
static bool AggregateRowPointerDenseSingleFieldKey(data_ptr_t row_pointer,
                                                   const ExecutionRowPointerGroupKeySource &source, idx_t &key) {
	AggregateRowPointerSingleFieldKey<T> source_key;
	if (!AggregateRowPointerSingleFieldKeyFromPointer(row_pointer, source, source_key) || !source_key.valid) {
		return false;
	}
	return AggregateDescriptorDenseKeyFromTargetValue<T>(source_key.value, key);
}

template <class T>
static bool AggregateRowPointerSingleFieldKeysMatch(const AggregateRowPointerSingleFieldKey<T> &left,
                                                    const AggregateRowPointerSingleFieldKey<T> &right) {
	if (left.valid != right.valid) {
		return false;
	}
	return !left.valid || left.value == right.value;
}

template <class T>
struct AggregateRowPointerSparseSingleFieldTargetCacheEntry {
	bool occupied = false;
	hash_t hash = 0;
	AggregateRowPointerSingleFieldKey<T> key;
	data_ptr_t existing_row_location = nullptr;
	idx_t new_group_idx = DConstants::INVALID_INDEX;
};

template <class T>
struct AggregateRowPointerSparseSingleFieldTargetCache {
	void Initialize(idx_t count) {
		idx_t capacity = 1;
		while (capacity < count * 2) {
			capacity *= 2;
		}
		entries.clear();
		entries.resize(capacity);
		mask = capacity - 1;
	}

	bool Lookup(hash_t hash, const AggregateRowPointerSingleFieldKey<T> &key, data_ptr_t &existing_row_location,
	            idx_t &new_group_idx) const {
		D_ASSERT(!entries.empty());
		auto entry_idx = static_cast<idx_t>(hash) & mask;
		for (idx_t probe_idx = 0; probe_idx < entries.size(); probe_idx++) {
			auto &entry = entries[entry_idx];
			if (!entry.occupied) {
				return false;
			}
			if (entry.hash == hash && AggregateRowPointerSingleFieldKeysMatch(entry.key, key)) {
				existing_row_location = entry.existing_row_location;
				new_group_idx = entry.new_group_idx;
				return true;
			}
			entry_idx = (entry_idx + 1) & mask;
		}
		return false;
	}

	void InsertExisting(hash_t hash, const AggregateRowPointerSingleFieldKey<T> &key, data_ptr_t row_location) {
		Insert(hash, key, row_location, DConstants::INVALID_INDEX);
	}

	void InsertNew(hash_t hash, const AggregateRowPointerSingleFieldKey<T> &key, idx_t new_group_idx) {
		Insert(hash, key, nullptr, new_group_idx);
	}

	void Insert(hash_t hash, const AggregateRowPointerSingleFieldKey<T> &key, data_ptr_t existing_row_location,
	            idx_t new_group_idx) {
		D_ASSERT(!entries.empty());
		auto entry_idx = static_cast<idx_t>(hash) & mask;
		for (idx_t probe_idx = 0; probe_idx < entries.size(); probe_idx++) {
			auto &entry = entries[entry_idx];
			if (!entry.occupied) {
				entry.occupied = true;
				entry.hash = hash;
				entry.key = key;
				entry.existing_row_location = existing_row_location;
				entry.new_group_idx = new_group_idx;
				return;
			}
			if (entry.hash == hash && AggregateRowPointerSingleFieldKeysMatch(entry.key, key)) {
				entry.existing_row_location = existing_row_location;
				entry.new_group_idx = new_group_idx;
				return;
			}
			entry_idx = (entry_idx + 1) & mask;
		}
		throw InternalException("Sparse row-pointer single-field aggregate target cache is full");
	}

	vector<AggregateRowPointerSparseSingleFieldTargetCacheEntry<T>> entries;
	idx_t mask = 0;
};

template <class T>
static bool AggregateRowPointerSingleFieldBatchHasSparseRepeats(const data_ptr_t *row_pointer_data, idx_t count,
                                                                const ExecutionRowPointerGroupKeySource &source) {
	const auto sample_count = MinValue<idx_t>(count, 64);
	std::array<AggregateRowPointerSingleFieldKey<T>, 64> sample_keys;
	std::array<hash_t, 64> sample_hashes;
	for (idx_t row_idx = 0; row_idx < sample_count; row_idx++) {
		if (!AggregateRowPointerSingleFieldKeyFromPointer(row_pointer_data[row_idx], source, sample_keys[row_idx])) {
			return false;
		}
		sample_hashes[row_idx] =
		    sample_keys[row_idx].valid ? Hash(sample_keys[row_idx].value) : AGGREGATE_DESCRIPTOR_NULL_HASH;
		for (idx_t prev_idx = 0; prev_idx < row_idx; prev_idx++) {
			if (sample_hashes[prev_idx] == sample_hashes[row_idx] &&
			    AggregateRowPointerSingleFieldKeysMatch(sample_keys[prev_idx], sample_keys[row_idx])) {
				return true;
			}
		}
	}
	return false;
}

template <class T>
bool GroupedAggregateHashTable::TryFindOrCreateRowPointerSingleFieldGroupStateTargetsDense(
    Vector &row_pointers, idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const ExecutionRowPointerGroupKeySource &source, ExecutionGroupedAggregateStateTargetBatch &targets,
    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	if (enable_hll || !entries || skip_lookups || dense_single_field_target_cache.disabled) {
		return false;
	}
	if (count > STANDARD_VECTOR_SIZE) {
		dense_single_field_target_cache.Disable();
		return false;
	}
	auto &cache = dense_single_field_target_cache;
	const auto layout_offset = layout_ptr->GetOffsets()[0];
	if (Count() != 0 && cache.physical_type == PhysicalType::INVALID) {
		cache.Disable();
		return false;
	}
	if (!cache.Configure(source.target_physical_type, layout_offset)) {
		return false;
	}

	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	idx_t min_key = NumericLimits<idx_t>::Maximum();
	idx_t max_key = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		idx_t key;
		if (!AggregateRowPointerDenseSingleFieldKey<T>(row_pointer_data[row_idx], source, key)) {
			cache.Disable();
			return false;
		}
		min_key = MinValue(min_key, key);
		max_key = MaxValue(max_key, key);
	}
	idx_t key_range;
	if (max_key == NumericLimits<idx_t>::Maximum() || !AggregateDenseKeyRange(min_key, max_key, key_range) ||
	    key_range > AggregateDenseTargetMaxRange(Count() + count) || !cache.EnsureRange(min_key, max_key)) {
		cache.Disable();
		return false;
	}

	if (Count() + count > capacity || Count() + count > ResizeThreshold()) {
		auto resize_start = AggregateTraceStart(recorder);
		Verify();
		ResizeForAdditionalGroups(count);
		RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_single_field_dense.resize", resize_start);
	}

	state.hashes.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(state.hashes, count_t(count));
	auto hashes = FlatVector::GetDataMutable<hash_t>(state.hashes);
	state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(state.addresses, count_t(count));
	auto selected_addresses = FlatVector::GetDataMutable<uintptr_t>(state.addresses);
	auto ht_offsets = FlatVector::GetDataMutable<uint64_t>(state.ht_offsets);
	auto duplicate_targets = FlatVector::GetDataMutable<hash_t>(state.hash_salts);

	idx_t new_group_count = 0;
	idx_t duplicate_count = 0;
	auto dense_key = [&](idx_t row_idx) {
		idx_t key;
		const auto key_loaded = AggregateRowPointerDenseSingleFieldKey<T>(row_pointer_data[row_idx], source, key);
		D_ASSERT(key_loaded);
		return key;
	};
	auto clear_marked_entries = [&]() {
		for (idx_t marked_idx = 0; marked_idx < new_group_count; marked_idx++) {
			entries[ht_offsets[marked_idx]] = ht_entry_t();
			cache.SetPendingNewGroup(dense_key(state.new_groups.get_index_unsafe(marked_idx)),
			                         DConstants::INVALID_INDEX);
		}
		new_group_count = 0;
		duplicate_count = 0;
	};
	auto stored_key_matches = [&](data_ptr_t row_location, T value) {
		return Load<T>(row_location + layout_offset) == value;
	};

	auto probe_start = AggregateTraceStart(recorder);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto key = dense_key(row_idx);
		const auto cached_address = cache.GetAddress(key);
		if (cached_address != 0) {
			selected_addresses[row_idx] = cached_address;
			continue;
		}
		const auto pending_group_idx = cache.GetPendingNewGroup(key);
		if (pending_group_idx != DConstants::INVALID_INDEX) {
			state.no_match_vector.set_index(duplicate_count, row_idx);
			duplicate_targets[duplicate_count] = pending_group_idx;
			duplicate_count++;
			continue;
		}

		AggregateRowPointerSingleFieldKey<T> source_key;
		const auto source_loaded =
		    AggregateRowPointerSingleFieldKeyFromPointer(row_pointer_data[row_idx], source, source_key);
		D_ASSERT(source_loaded && source_key.valid);
		const auto value = source_key.value;
		const auto hash = Hash(value);
		hashes[row_idx] = hash;
		const auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = ApplyBitMask(hash);

		idx_t iteration_count;
		for (iteration_count = 0; iteration_count < capacity; iteration_count++) {
			auto &entry = entries[ht_offset];
			if (!entry.IsOccupied()) {
				ht_offsets[new_group_count] = ht_offset;
				state.new_groups.set_index(new_group_count, row_idx);
				cache.SetPendingNewGroup(key, new_group_count);
				new_group_count++;
				entry.SetSalt(salt);
				break;
			}
			if (entry.GetSalt() == salt) {
				auto row_location = entry.GetPointer();
				if (cast_pointer_to_uint64(row_location) != ht_entry_t::POINTER_MASK &&
				    stored_key_matches(row_location, value)) {
					const auto address = reinterpret_cast<uintptr_t>(row_location);
					cache.SetAddress(key, address);
					selected_addresses[row_idx] = address;
					break;
				}
			}
			SaltIncrementAndWrap(ht_offset, salt, bitmask);
		}
		if (iteration_count == capacity) {
			clear_marked_entries();
			throw InternalException("Maximum dense single-field row-pointer find-or-create iteration count reached in "
			                        "GroupedAggregateHashTable");
		}
	}
	RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_single_field_dense.probe", probe_start);

	if (new_group_count == 0) {
		auto target_start = AggregateTraceStart(recorder);
		targets.InputOrder().Set(selected_addresses, nullptr, count);
		RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_single_field_dense.emit_existing_targets",
		                          target_start);
		sink_count += count;
		return true;
	}

	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout_ptr->GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout_ptr->GetTypes().size());
	AggregateRowPointerDescriptorSourceInfo sources;
	sources.row_pointers = row_pointer_data;
	sources.InitializeInputFormats(group_sources.size());
	auto append_fill_start = AggregateTraceStart(recorder);
	if (!AggregateFillDescriptorGroupChunk(allocator, sources, group_sources,
	                                       optional_ptr<const SelectionVector>(&state.new_groups), new_group_count,
	                                       count, layout_ptr->GetTypes(), state.descriptor_group_chunk)) {
		clear_marked_entries();
		RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_single_field_dense.append_key_fill_miss",
		                          append_fill_start);
		return false;
	}
	state.group_chunk.data[0].Reference(state.descriptor_group_chunk.data[0]);
	state.group_chunk.data[1].Reference(state.hashes);
	TupleDataCollection::ToUnifiedFormat(state.partitioned_append_state.chunk_state, state.group_chunk);
	RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_single_field_dense.append_key_fill",
	                          append_fill_start);

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
		const auto address = reinterpret_cast<uintptr_t>(row_location);
		const auto key = dense_key(row_idx);
		cache.SetAddress(key, address);
		cache.SetPendingNewGroup(key, DConstants::INVALID_INDEX);
		selected_addresses[row_idx] = address;
	}
	for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
		const auto row_idx = state.no_match_vector.get_index_unsafe(duplicate_idx);
		const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
		selected_addresses[row_idx] = reinterpret_cast<uintptr_t>(entries[ht_offsets[marked_idx]].GetPointer());
	}
	auto target_start = AggregateTraceStart(recorder);
	targets.InputOrder().Set(selected_addresses, nullptr, count);
	RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_single_field_dense.emit_targets", target_start);
	this->count += new_group_count;
	sink_count += count;
	RecordAggregateTraceStage(recorder, "find_or_create_row_pointer_single_field_dense.append_new_groups",
	                          append_start);
	return true;
}

template <class T, class SOURCE>
bool GroupedAggregateHashTable::TryFindOrCreateSingleFieldGroupStateTargetsDirectTemplated(
    idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    ExecutionGroupedAggregateStateTargetBatch &targets, optional_ptr<ExecutionOperatorStageRecorder> recorder,
    SOURCE &source) {
	if (Count() + count > capacity || Count() + count > ResizeThreshold()) {
		auto resize_start = AggregateTraceStart(recorder);
		Verify();
		ResizeForAdditionalGroups(count);
		RecordAggregateTraceStage(recorder, source.StageName("resize"), resize_start);
	}

	state.hashes.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(state.hashes, count_t(count));
	auto hashes = FlatVector::GetDataMutable<hash_t>(state.hashes);
	state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(state.addresses, count_t(count));
	auto selected_addresses = FlatVector::GetDataMutable<uintptr_t>(state.addresses);
	auto ht_offsets = FlatVector::GetDataMutable<uint64_t>(state.ht_offsets);
	auto duplicate_targets = FlatVector::GetDataMutable<hash_t>(state.hash_salts);

	idx_t new_group_count = 0;
	idx_t duplicate_count = 0;
	auto clear_marked_entries = [&]() {
		for (idx_t marked_idx = 0; marked_idx < new_group_count; marked_idx++) {
			entries[ht_offsets[marked_idx]] = ht_entry_t();
		}
		new_group_count = 0;
	};
	auto emit_existing_state = [&](data_ptr_t row_location, idx_t row_idx) {
		selected_addresses[row_idx] = reinterpret_cast<uintptr_t>(row_location);
	};
	auto emit_new_duplicate = [&](idx_t row_idx, idx_t new_group_idx) {
		state.no_match_vector.set_index(duplicate_count, row_idx);
		duplicate_targets[duplicate_count] = new_group_idx;
		duplicate_count++;
	};

	enum class LastMatchKind : uint8_t { NONE, NEW_GROUP, EXISTING_GROUP };
	LastMatchKind last_match_kind = LastMatchKind::NONE;
	hash_t last_hash = 0;
	AggregateRowPointerSingleFieldKey<T> last_key;
	idx_t last_new_group_idx = DConstants::INVALID_INDEX;
	data_ptr_t last_existing_row_location = nullptr;
	auto remember_new_group = [&](idx_t new_group_idx, hash_t hash, AggregateRowPointerSingleFieldKey<T> key) {
		last_match_kind = LastMatchKind::NEW_GROUP;
		last_hash = hash;
		last_key = key;
		last_new_group_idx = new_group_idx;
	};
	auto remember_existing_group = [&](data_ptr_t row_location, hash_t hash, AggregateRowPointerSingleFieldKey<T> key) {
		last_match_kind = LastMatchKind::EXISTING_GROUP;
		last_hash = hash;
		last_key = key;
		last_existing_row_location = row_location;
	};
	auto try_emit_repeated_match = [&](idx_t row_idx, hash_t hash, const AggregateRowPointerSingleFieldKey<T> &key) {
		if (last_match_kind == LastMatchKind::NONE || hash != last_hash ||
		    !AggregateRowPointerSingleFieldKeysMatch(key, last_key)) {
			return false;
		}
		if (last_match_kind == LastMatchKind::NEW_GROUP) {
			emit_new_duplicate(row_idx, last_new_group_idx);
			return true;
		}
		emit_existing_state(last_existing_row_location, row_idx);
		return true;
	};

	auto probe_start = AggregateTraceStart(recorder);
	const bool use_sparse_cache = source.BatchHasSparseRepeats(count);
	AggregateRowPointerSparseSingleFieldTargetCache<T> sparse_cache;
	if (use_sparse_cache) {
		sparse_cache.Initialize(count);
		RecordAggregateTraceStage(recorder, source.StageName("sparse_cache_enable"), probe_start);
	}
	bool use_consecutive_reuse = false;
	if (!use_sparse_cache) {
		const auto sample_count = MinValue<idx_t>(count, 64);
		for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
			if (AggregateRowPointerSingleFieldKeysMatch(source.Key(row_idx), source.Key(row_idx - 1))) {
				use_consecutive_reuse = true;
				break;
			}
		}
	}
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto key = source.Key(row_idx);
		const auto hash = key.valid ? Hash(key.value) : AGGREGATE_DESCRIPTOR_NULL_HASH;
		hashes[row_idx] = hash;
		const auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = ApplyBitMask(hash);

		if (use_consecutive_reuse && try_emit_repeated_match(row_idx, hash, key)) {
			continue;
		}
		if (use_sparse_cache) {
			data_ptr_t cached_existing_row_location = nullptr;
			idx_t cached_new_group_idx = DConstants::INVALID_INDEX;
			if (sparse_cache.Lookup(hash, key, cached_existing_row_location, cached_new_group_idx)) {
				if (cached_new_group_idx != DConstants::INVALID_INDEX) {
					emit_new_duplicate(row_idx, cached_new_group_idx);
				} else {
					emit_existing_state(cached_existing_row_location, row_idx);
				}
				continue;
			}
		}

		idx_t iteration_count;
		for (iteration_count = 0; iteration_count < capacity; iteration_count++) {
			auto &entry = entries[ht_offset];
			if (!entry.IsOccupied()) {
				ht_offsets[new_group_count] = ht_offset;
				state.new_groups.set_index(new_group_count, row_idx);
				if (use_sparse_cache) {
					sparse_cache.InsertNew(hash, key, new_group_count);
				}
				if (use_consecutive_reuse) {
					remember_new_group(new_group_count, hash, key);
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
						if (!AggregateRowPointerSingleFieldKeysMatch(source.Key(marked_row_idx), key)) {
							continue;
						}
						emit_new_duplicate(row_idx, marked_idx);
						if (use_sparse_cache) {
							sparse_cache.InsertNew(hash, key, marked_idx);
						}
						if (use_consecutive_reuse) {
							remember_new_group(marked_idx, hash, key);
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
				if (source.StoredKeyMatches(row_location, key)) {
					emit_existing_state(row_location, row_idx);
					if (use_sparse_cache) {
						sparse_cache.InsertExisting(hash, key, row_location);
					}
					if (use_consecutive_reuse) {
						remember_existing_group(row_location, hash, key);
					}
					break;
				}
			}
			SaltIncrementAndWrap(ht_offset, salt, bitmask);
		}
		if (iteration_count == capacity) {
			clear_marked_entries();
			throw InternalException(source.IterationLimitMessage());
		}
	}
	RecordAggregateTraceStage(recorder, source.StageName("probe"), probe_start);
	if (enable_hll) {
		auto hll_start = AggregateTraceStart(recorder);
		hll.Update(state.hashes);
		RecordAggregateTraceStage(recorder, source.StageName("hll"), hll_start);
	}

	if (new_group_count == 0) {
		auto target_start = AggregateTraceStart(recorder);
		targets.InputOrder().Set(selected_addresses, nullptr, count);
		RecordAggregateTraceStage(recorder, source.StageName("emit_existing_targets"), target_start);
		sink_count += count;
		return true;
	}

	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout_ptr->GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout_ptr->GetTypes().size());
	auto append_fill_start = AggregateTraceStart(recorder);
	if (!source.FillGroupChunk(allocator, state, group_sources, state.new_groups, new_group_count, count,
	                           layout_ptr->GetTypes(), hashes)) {
		clear_marked_entries();
		RecordAggregateTraceStage(recorder, source.StageName("append_key_fill_miss"), append_fill_start);
		return false;
	}
	TupleDataCollection::ToUnifiedFormat(state.partitioned_append_state.chunk_state, state.group_chunk);
	RecordAggregateTraceStage(recorder, source.StageName("append_key_fill"), append_fill_start);

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
	source.AppendUnified(*data, *append_state, state, new_group_count);
	RowOperations::InitializeStates(*layout_ptr, append_state->chunk_state.row_locations,
	                                *FlatVector::IncrementalSelectionVector(), new_group_count);
	const auto row_locations = FlatVector::GetData<data_ptr_t>(append_state->chunk_state.row_locations);
	const auto &row_sel = append_state->reverse_partition_sel;
	for (idx_t new_idx = 0; new_idx < new_group_count; new_idx++) {
		const auto row_idx = state.new_groups.get_index_unsafe(new_idx);
		const auto row_location = source.AppendedRowLocation(row_locations, row_sel, new_idx, row_idx);
		entries[ht_offsets[new_idx]].SetPointer(row_location);
		selected_addresses[row_idx] = reinterpret_cast<uintptr_t>(row_location);
	}
	for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
		const auto row_idx = state.no_match_vector.get_index_unsafe(duplicate_idx);
		const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
		selected_addresses[row_idx] = reinterpret_cast<uintptr_t>(entries[ht_offsets[marked_idx]].GetPointer());
	}
	auto target_start = AggregateTraceStart(recorder);
	targets.InputOrder().Set(selected_addresses, nullptr, count);
	RecordAggregateTraceStage(recorder, source.StageName("emit_targets"), target_start);
	this->count += new_group_count;
	sink_count += count;
	RecordAggregateTraceStage(recorder, source.StageName("append_new_groups"), append_start);
	return true;
}

template <class T>
bool GroupedAggregateHashTable::TryFindOrCreateRowPointerSingleFieldGroupStateTargetsDirectTemplated(
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, ExecutionGroupedAggregateStateTargetBatch &targets,
    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	targets.Reset();
	if (!AggregateRowPointerSingleFieldCanProbeDirect(payload_input, row_pointers, group_sources)) {
		return false;
	}
	if (count == 0) {
		return true;
	}
	if (payload_input.size() != count || skip_lookups || !entries) {
		return false;
	}

	auto &source = group_sources[0];
	if (TryFindOrCreateRowPointerSingleFieldGroupStateTargetsDense<T>(row_pointers, count, group_sources, source,
	                                                                  targets, recorder)) {
		return true;
	}
	const auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	const auto layout_offset = layout_ptr->GetOffsets()[0];

	struct RowPointerSingleFieldSource {
		const data_ptr_t *row_pointer_data;
		const ExecutionRowPointerGroupKeySource &source;
		idx_t layout_offset;
		const TupleDataLayout &layout;

		string StageName(const char *suffix) const {
			return StringUtil::Format("find_or_create_row_pointer_single_field.%s", suffix);
		}

		const char *IterationLimitMessage() const {
			return "Maximum single-field row-pointer find-or-create iteration count reached in "
			       "GroupedAggregateHashTable";
		}

		AggregateRowPointerSingleFieldKey<T> Key(idx_t row_idx) const {
			AggregateRowPointerSingleFieldKey<T> key;
			const auto loaded = AggregateRowPointerSingleFieldKeyFromPointer(row_pointer_data[row_idx], source, key);
			D_ASSERT(loaded);
			return key;
		}

		bool StoredKeyMatches(data_ptr_t row_location, const AggregateRowPointerSingleFieldKey<T> &key) const {
			const auto stored_is_valid = source.all_valid || AggregateStoredGroupKeyIsValid(row_location, layout, 0);
			if (key.valid != stored_is_valid) {
				return false;
			}
			return !key.valid || Load<T>(row_location + layout_offset) == key.value;
		}

		bool BatchHasSparseRepeats(idx_t count) const {
			return AggregateRowPointerSingleFieldBatchHasSparseRepeats<T>(row_pointer_data, count, source);
		}

		bool FillGroupChunk(Allocator &allocator, AggregateHTAppendState &state,
		                    const vector<ExecutionRowPointerGroupKeySource> &group_sources,
		                    const SelectionVector &new_groups, idx_t new_group_count, idx_t count,
		                    const vector<LogicalType> &layout_types, const hash_t *hashes) const {
			(void)hashes;
			AggregateRowPointerDescriptorSourceInfo sources;
			sources.row_pointers = row_pointer_data;
			sources.InitializeInputFormats(group_sources.size());
			if (!AggregateFillDescriptorGroupChunk(allocator, sources, group_sources,
			                                       optional_ptr<const SelectionVector>(&new_groups), new_group_count,
			                                       count, layout_types, state.descriptor_group_chunk)) {
				return false;
			}
			state.group_chunk.data[0].Reference(state.descriptor_group_chunk.data[0]);
			state.group_chunk.data[1].Reference(state.hashes);
			return true;
		}

		void AppendUnified(PartitionedTupleData &data, PartitionedTupleDataAppendState &append_state,
		                   AggregateHTAppendState &state, idx_t new_group_count) const {
			data.AppendUnified(append_state, state.group_chunk, state.new_groups, new_group_count);
		}

		data_ptr_t AppendedRowLocation(const data_ptr_t *row_locations, const SelectionVector &row_sel, idx_t new_idx,
		                               idx_t row_idx) const {
			(void)new_idx;
			return row_locations[row_sel.get_index_unsafe(row_idx)];
		}
	};

	RowPointerSingleFieldSource row_pointer_source {row_pointer_data, source, layout_offset, *layout_ptr};
	return TryFindOrCreateSingleFieldGroupStateTargetsDirectTemplated<T>(count, group_sources, targets, recorder,
	                                                                     row_pointer_source);
}

struct GroupedAggregateHashTable::RowPointerSingleFieldDirectDispatchOp {
	template <class T, class... ARGS>
	static bool Run(GroupedAggregateHashTable &ht, ARGS &&... args) {
		return ht.TryFindOrCreateRowPointerSingleFieldGroupStateTargetsDirectTemplated<T>(std::forward<ARGS>(args)...);
	}
};

bool GroupedAggregateHashTable::TryFindOrCreateRowPointerSingleFieldGroupStateTargetsDirect(
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, ExecutionGroupedAggregateStateTargetBatch &targets,
    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	if (!AggregateRowPointerSingleFieldCanProbeDirect(payload_input, row_pointers, group_sources)) {
		return false;
	}
	return AggregateDispatchDenseIntegralType<RowPointerSingleFieldDirectDispatchOp>(
	    group_sources[0].target_physical_type, false, *this, payload_input, row_pointers, count, group_sources, targets,
	    recorder);
}

static bool
AggregateInputVectorSingleFieldCanProbeDirect(DataChunk &payload_input,
                                              const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (group_sources.size() != 1) {
		return false;
	}
	auto &source = group_sources[0];
	return source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR &&
	       source.input_vector_index != DConstants::INVALID_INDEX &&
	       source.input_vector_index < payload_input.ColumnCount() &&
	       AggregateInputVectorDescriptorSourcesCanProbeDirect(payload_input, group_sources);
}

template <class T>
static bool AggregateInputVectorSingleFieldKeyValue(const UnifiedVectorFormat &format, idx_t row_idx,
                                                    const ExecutionRowPointerGroupKeySource &source,
                                                    AggregateRowPointerSingleFieldKey<T> &key) {
	key = AggregateRowPointerSingleFieldKey<T>();
	const auto source_idx = format.sel->get_index(row_idx);
	if (!format.validity.RowIsValid(source_idx)) {
		return true;
	}
	key.valid = true;
	const auto value_size = AggregateDescriptorValueSize(source.source_physical_type);
	return AggregateRowPointerSingleFieldKeyValue(format.data + source_idx * value_size, source, key.value);
}

template <class T>
static bool AggregateInputVectorSingleFieldBatchHasSparseRepeats(const UnifiedVectorFormat &format, idx_t count,
                                                                 const ExecutionRowPointerGroupKeySource &source) {
	const auto sample_count = MinValue<idx_t>(count, 64);
	std::array<AggregateRowPointerSingleFieldKey<T>, 64> sample_keys;
	std::array<hash_t, 64> sample_hashes;
	for (idx_t row_idx = 0; row_idx < sample_count; row_idx++) {
		if (!AggregateInputVectorSingleFieldKeyValue(format, row_idx, source, sample_keys[row_idx])) {
			return false;
		}
		sample_hashes[row_idx] =
		    sample_keys[row_idx].valid ? Hash(sample_keys[row_idx].value) : AGGREGATE_DESCRIPTOR_NULL_HASH;
		for (idx_t prev_idx = 0; prev_idx < row_idx; prev_idx++) {
			if (sample_hashes[prev_idx] == sample_hashes[row_idx] &&
			    AggregateRowPointerSingleFieldKeysMatch(sample_keys[prev_idx], sample_keys[row_idx])) {
				return true;
			}
		}
	}
	return false;
}

template <class T>
bool GroupedAggregateHashTable::TryFindOrCreateInputVectorSingleFieldGroupStateTargetsDirectTemplated(
    DataChunk &payload_input, idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    ExecutionGroupedAggregateStateTargetBatch &targets, optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	targets.Reset();
	if (!AggregateInputVectorSingleFieldCanProbeDirect(payload_input, group_sources)) {
		return false;
	}
	if (count == 0) {
		return true;
	}
	if (payload_input.size() != count || skip_lookups || !entries) {
		return false;
	}

	auto &source = group_sources[0];
	AggregateRowPointerDescriptorSourceInfo sources;
	if (!AggregatePrepareDescriptorSourceInfo(payload_input, nullptr, group_sources, layout_ptr->GetTypes(), sources)) {
		return false;
	}
	auto &format = sources.InputFormat(0);
	const auto layout_offset = layout_ptr->GetOffsets()[0];

	struct InputVectorSingleFieldSource {
		const UnifiedVectorFormat &format;
		const ExecutionRowPointerGroupKeySource &source;
		const AggregateRowPointerDescriptorSourceInfo &sources;
		idx_t layout_offset;
		const TupleDataLayout &layout;

		string StageName(const char *suffix) const {
			return StringUtil::Format("find_or_create_input_vector_single_field.%s", suffix);
		}

		const char *IterationLimitMessage() const {
			return "Maximum single-field input-vector find-or-create iteration count reached in "
			       "GroupedAggregateHashTable";
		}

		AggregateRowPointerSingleFieldKey<T> Key(idx_t row_idx) const {
			AggregateRowPointerSingleFieldKey<T> key;
			const auto loaded = AggregateInputVectorSingleFieldKeyValue(format, row_idx, source, key);
			D_ASSERT(loaded);
			return key;
		}

		bool StoredKeyMatches(data_ptr_t row_location, const AggregateRowPointerSingleFieldKey<T> &key) const {
			const auto stored_is_valid = source.all_valid || AggregateStoredGroupKeyIsValid(row_location, layout, 0);
			if (key.valid != stored_is_valid) {
				return false;
			}
			return !key.valid || Load<T>(row_location + layout_offset) == key.value;
		}

		bool BatchHasSparseRepeats(idx_t count) const {
			return AggregateInputVectorSingleFieldBatchHasSparseRepeats<T>(format, count, source);
		}

		bool FillGroupChunk(Allocator &allocator, AggregateHTAppendState &state,
		                    const vector<ExecutionRowPointerGroupKeySource> &group_sources,
		                    const SelectionVector &new_groups, idx_t new_group_count, idx_t count,
		                    const vector<LogicalType> &layout_types, const hash_t *hashes) const {
			(void)allocator;
			(void)count;
			if (!AggregateFillCompactDescriptorGroupChunk(
			        allocator, sources, group_sources, new_groups, new_group_count, layout_types, hashes,
			        state.descriptor_group_chunk, state.descriptor_group_hashes)) {
				return false;
			}
			state.group_chunk.data[0].Reference(state.descriptor_group_chunk.data[0]);
			state.group_chunk.data[1].Reference(state.descriptor_group_hashes);
			state.group_chunk.SetChildCardinality(new_group_count);
			return true;
		}

		void AppendUnified(PartitionedTupleData &data, PartitionedTupleDataAppendState &append_state,
		                   AggregateHTAppendState &state, idx_t new_group_count) const {
			data.AppendUnified(append_state, state.group_chunk, *FlatVector::IncrementalSelectionVector(),
			                   new_group_count);
		}

		data_ptr_t AppendedRowLocation(const data_ptr_t *row_locations, const SelectionVector &row_sel, idx_t new_idx,
		                               idx_t row_idx) const {
			(void)row_idx;
			return row_locations[row_sel.get_index_unsafe(new_idx)];
		}
	};

	InputVectorSingleFieldSource input_vector_source {format, source, sources, layout_offset, *layout_ptr};
	return TryFindOrCreateSingleFieldGroupStateTargetsDirectTemplated<T>(count, group_sources, targets, recorder,
	                                                                     input_vector_source);
}

struct GroupedAggregateHashTable::InputVectorSingleFieldDirectDispatchOp {
	template <class T, class... ARGS>
	static bool Run(GroupedAggregateHashTable &ht, ARGS &&... args) {
		return ht.TryFindOrCreateInputVectorSingleFieldGroupStateTargetsDirectTemplated<T>(std::forward<ARGS>(args)...);
	}
};

bool GroupedAggregateHashTable::TryFindOrCreateInputVectorSingleFieldGroupStateTargetsDirect(
    DataChunk &payload_input, idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    ExecutionGroupedAggregateStateTargetBatch &targets, optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	if (!AggregateInputVectorSingleFieldCanProbeDirect(payload_input, group_sources)) {
		return false;
	}
	return AggregateDispatchDenseIntegralType<InputVectorSingleFieldDirectDispatchOp>(
	    group_sources[0].target_physical_type, false, *this, payload_input, count, group_sources, targets, recorder);
}

struct AggregateStringPrefixTargetKey {
	bool is_null = false;
	string_t value;

	bool operator==(const AggregateStringPrefixTargetKey &other) const {
		if (is_null || other.is_null) {
			return is_null == other.is_null;
		}
		return value == other.value;
	}
};

struct AggregateStringPrefixTargetKeyHash {
	size_t operator()(const AggregateStringPrefixTargetKey &key) const {
		return key.is_null ? AGGREGATE_DESCRIPTOR_NULL_HASH : Hash(key.value);
	}
};

struct AggregateStringPrefixTargetMatch {
	bool new_group = false;
	idx_t new_group_idx = DConstants::INVALID_INDEX;
	data_ptr_t row_location = nullptr;
};

struct AggregateAsciiPrefixTargetKey {
	bool is_null = false;
	uint8_t length = 0;
	uint64_t bytes = 0;
	hash_t hash = 0;

	bool operator==(const AggregateAsciiPrefixTargetKey &other) const {
		return is_null == other.is_null && length == other.length && bytes == other.bytes;
	}
};

struct AggregateAsciiPrefixTargetKeyHash {
	size_t operator()(const AggregateAsciiPrefixTargetKey &key) const {
		return key.is_null ? AGGREGATE_DESCRIPTOR_NULL_HASH : key.hash;
	}
};

static idx_t AggregateEstimateRepeatedKeyCacheSize(idx_t count, idx_t sample_count, idx_t sample_key_count) {
	if (count == 0 || sample_count == 0 || sample_key_count == 0) {
		return 0;
	}
	const auto estimated = (sample_key_count * count + sample_count - 1) / sample_count;
	return MinValue<idx_t>(count, MaxValue<idx_t>(estimated, MinValue<idx_t>(count, 8)));
}

static bool AggregateLoadAsciiPrefixTargetKey(const UnifiedVectorFormat &source_format, const string_t *source_data,
                                              idx_t row_idx, idx_t prefix_length, AggregateAsciiPrefixTargetKey &key) {
	if (prefix_length > sizeof(uint64_t)) {
		return false;
	}
	const auto source_idx = source_format.sel->get_index(row_idx);
	if (!source_format.validity.RowIsValid(source_idx)) {
		key.is_null = true;
		key.length = 0;
		key.bytes = 0;
		key.hash = AGGREGATE_DESCRIPTOR_NULL_HASH;
		return true;
	}
	const auto value = source_data[source_idx];
	const auto prefix_size = UnsafeNumericCast<uint8_t>(MinValue<idx_t>(value.GetSize(), prefix_length));
	auto input_data = value.GetData();
	for (idx_t byte_idx = 0; byte_idx < prefix_size; byte_idx++) {
		if (input_data[byte_idx] & 0x80) {
			return false;
		}
	}
	key.is_null = false;
	key.length = prefix_size;
	key.bytes = 0;
	if (prefix_size > 0) {
		std::memcpy(&key.bytes, input_data, prefix_size);
	}
	key.hash = Hash(prefix_size == 0 ? string_t("", 0) : string_t(input_data, prefix_size));
	return true;
}

static bool AggregateStoredAsciiPrefixKeyMatches(data_ptr_t row_location, const TupleDataLayout &layout,
                                                 const AggregateAsciiPrefixTargetKey &key) {
	const auto stored_is_valid = AggregateStoredGroupKeyIsValid(row_location, layout, 0);
	if (key.is_null || !stored_is_valid) {
		return key.is_null == !stored_is_valid;
	}
	const auto stored_value = Load<string_t>(row_location + layout.GetOffsets()[0]);
	if (stored_value.GetSize() != key.length) {
		return false;
	}
	return key.length == 0 || std::memcmp(stored_value.GetData(), &key.bytes, key.length) == 0;
}

bool GroupedAggregateHashTable::TryFindOrCreateInputVectorStringPrefixGroupStateTargets(
    DataChunk &payload_input, idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    ExecutionGroupedAggregateStateTargetBatch &targets, optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	targets.Reset();
	if (count < 2 || payload_input.size() != count || skip_lookups || !entries || group_sources.size() != 1 ||
	    layout_ptr->GetTypes().empty() || layout_ptr->GetTypes()[0].InternalType() != PhysicalType::VARCHAR) {
		return false;
	}
	auto &source = group_sources[0];
	if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR ||
	    source.cast_kind != ExecutionRowPointerGroupKeyCastKind::STRING_SUBSTRING ||
	    source.source_physical_type != PhysicalType::VARCHAR || source.target_physical_type != PhysicalType::VARCHAR ||
	    source.input_vector_index == DConstants::INVALID_INDEX ||
	    source.input_vector_index >= payload_input.ColumnCount()) {
		return false;
	}
	auto &source_vector = payload_input.data[source.input_vector_index];
	if (source_vector.GetType().InternalType() != PhysicalType::VARCHAR) {
		return false;
	}

	UnifiedVectorFormat source_format;
	source_vector.ToUnifiedFormat(source_format);
	auto source_data = UnifiedVectorFormat::GetData<string_t>(source_format);
	const auto try_find_existing_ascii_prefix_targets = [&]() {
		if (Count() == 0 || source.string_substring_length > sizeof(uint64_t)) {
			return false;
		}
		const auto ascii_sample_count = MinValue<idx_t>(count, 64);
		std::array<AggregateAsciiPrefixTargetKey, 64> sample_keys;
		idx_t sample_key_count = 0;
		bool has_sample_repeat = false;
		for (idx_t row_idx = 0; row_idx < ascii_sample_count; row_idx++) {
			AggregateAsciiPrefixTargetKey key;
			if (!AggregateLoadAsciiPrefixTargetKey(source_format, source_data, row_idx, source.string_substring_length,
			                                       key)) {
				return false;
			}
			for (idx_t sample_idx = 0; sample_idx < sample_key_count; sample_idx++) {
				if (sample_keys[sample_idx] == key) {
					has_sample_repeat = true;
					break;
				}
			}
			if (has_sample_repeat) {
				break;
			}
			sample_keys[sample_key_count++] = key;
		}
		if (!has_sample_repeat) {
			return false;
		}

		state.hashes.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::SetSize(state.hashes, count_t(count));
		auto hashes = FlatVector::GetDataMutable<hash_t>(state.hashes);
		state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::SetSize(state.addresses, count_t(count));
		auto selected_addresses = FlatVector::GetDataMutable<uintptr_t>(state.addresses);

		unordered_map<AggregateAsciiPrefixTargetKey, data_ptr_t, AggregateAsciiPrefixTargetKeyHash> target_cache;
		target_cache.reserve(AggregateEstimateRepeatedKeyCacheSize(count, ascii_sample_count, sample_key_count));

		auto probe_start = AggregateTraceStart(recorder);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			AggregateAsciiPrefixTargetKey key;
			if (!AggregateLoadAsciiPrefixTargetKey(source_format, source_data, row_idx, source.string_substring_length,
			                                       key)) {
				RecordAggregateTraceStage(recorder, "find_or_create_input_vector_ascii_prefix_existing.probe_miss",
				                          probe_start);
				return false;
			}
			hashes[row_idx] = key.hash;
			auto cached = target_cache.find(key);
			if (cached != target_cache.end()) {
				selected_addresses[row_idx] = reinterpret_cast<uintptr_t>(cached->second);
				continue;
			}

			auto salt = ht_entry_t::ExtractSalt(key.hash);
			auto ht_offset = ApplyBitMask(key.hash);
			idx_t iteration_count;
			for (iteration_count = 0; iteration_count < capacity; iteration_count++) {
				auto &entry = entries[ht_offset];
				if (!entry.IsOccupied()) {
					RecordAggregateTraceStage(recorder, "find_or_create_input_vector_ascii_prefix_existing.probe_miss",
					                          probe_start);
					return false;
				}
				if (entry.GetSalt() == salt) {
					auto row_location = entry.GetPointer();
					if (cast_pointer_to_uint64(row_location) != ht_entry_t::POINTER_MASK &&
					    AggregateStoredAsciiPrefixKeyMatches(row_location, *layout_ptr, key)) {
						selected_addresses[row_idx] = reinterpret_cast<uintptr_t>(row_location);
						target_cache.emplace(key, row_location);
						break;
					}
				}
				SaltIncrementAndWrap(ht_offset, salt, bitmask);
			}
			if (iteration_count == capacity) {
				throw InternalException("Maximum input-vector ASCII-prefix find-existing iteration count reached in "
				                        "GroupedAggregateHashTable");
			}
		}
		RecordAggregateTraceStage(recorder, "find_or_create_input_vector_ascii_prefix_existing.probe", probe_start);
		if (enable_hll) {
			auto hll_start = AggregateTraceStart(recorder);
			hll.Update(state.hashes);
			RecordAggregateTraceStage(recorder, "find_or_create_input_vector_ascii_prefix_existing.hll", hll_start);
		}
		auto target_start = AggregateTraceStart(recorder);
		targets.InputOrder().Set(selected_addresses, nullptr, count);
		RecordAggregateTraceStage(recorder, "find_or_create_input_vector_ascii_prefix_existing.emit_targets",
		                          target_start);
		sink_count += count;
		return true;
	};
	if (try_find_existing_ascii_prefix_targets()) {
		return true;
	}
	auto load_key = [&](idx_t row_idx, AggregateStringPrefixTargetKey &key) {
		const auto source_idx = source_format.sel->get_index(row_idx);
		if (!source_format.validity.RowIsValid(source_idx)) {
			key.is_null = true;
			key.value = string_t();
			return true;
		}
		key.is_null = false;
		key.value = SubstringPrefixUnicode(source_data[source_idx], source.string_substring_length);
		return true;
	};

	const auto sample_count = MinValue<idx_t>(count, 64);
	std::array<AggregateStringPrefixTargetKey, 64> sample_keys;
	idx_t sample_key_count = 0;
	bool has_sample_repeat = false;
	for (idx_t row_idx = 0; row_idx < sample_count; row_idx++) {
		AggregateStringPrefixTargetKey key;
		if (!load_key(row_idx, key)) {
			return false;
		}
		for (idx_t sample_idx = 0; sample_idx < sample_key_count; sample_idx++) {
			if (sample_keys[sample_idx] == key) {
				has_sample_repeat = true;
				break;
			}
		}
		if (has_sample_repeat) {
			break;
		}
		sample_keys[sample_key_count++] = key;
	}
	if (!has_sample_repeat) {
		return false;
	}

	state.hashes.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(state.hashes, count_t(count));
	auto hashes = FlatVector::GetDataMutable<hash_t>(state.hashes);

	if (Count() + count > capacity || Count() + count > ResizeThreshold()) {
		auto resize_start = AggregateTraceStart(recorder);
		Verify();
		ResizeForAdditionalGroups(count);
		RecordAggregateTraceStage(recorder, "find_or_create_input_vector_string_prefix.resize", resize_start);
	}

	state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(state.addresses, count_t(count));
	auto selected_addresses = FlatVector::GetDataMutable<uintptr_t>(state.addresses);
	auto ht_offsets = FlatVector::GetDataMutable<uint64_t>(state.ht_offsets);
	auto duplicate_targets = FlatVector::GetDataMutable<hash_t>(state.hash_salts);

	unordered_map<AggregateStringPrefixTargetKey, AggregateStringPrefixTargetMatch, AggregateStringPrefixTargetKeyHash>
	    target_cache;
	target_cache.reserve(AggregateEstimateRepeatedKeyCacheSize(count, sample_count, sample_key_count));
	idx_t new_group_count = 0;
	idx_t duplicate_count = 0;
	auto clear_marked_entries = [&]() {
		for (idx_t marked_idx = 0; marked_idx < new_group_count; marked_idx++) {
			entries[ht_offsets[marked_idx]] = ht_entry_t();
		}
		new_group_count = 0;
	};
	auto emit_existing_state = [&](data_ptr_t row_location, idx_t row_idx) {
		selected_addresses[row_idx] = reinterpret_cast<uintptr_t>(row_location);
	};
	auto emit_new_duplicate = [&](idx_t row_idx, idx_t new_group_idx) {
		state.no_match_vector.set_index(duplicate_count, row_idx);
		duplicate_targets[duplicate_count] = new_group_idx;
		duplicate_count++;
	};
	auto stored_key_matches = [&](data_ptr_t row_location, const AggregateStringPrefixTargetKey &key) {
		const auto stored_is_valid = AggregateStoredGroupKeyIsValid(row_location, *layout_ptr, 0);
		if (key.is_null || !stored_is_valid) {
			return key.is_null == !stored_is_valid;
		}
		return key.value == Load<string_t>(row_location + layout_ptr->GetOffsets()[0]);
	};

	auto probe_start = AggregateTraceStart(recorder);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		AggregateStringPrefixTargetKey key;
		if (!load_key(row_idx, key)) {
			clear_marked_entries();
			RecordAggregateTraceStage(recorder, "find_or_create_input_vector_string_prefix.probe_miss", probe_start);
			return false;
		}
		const auto hash = key.is_null ? AGGREGATE_DESCRIPTOR_NULL_HASH : Hash(key.value);
		hashes[row_idx] = hash;
		auto cached = target_cache.find(key);
		if (cached != target_cache.end()) {
			auto &match = cached->second;
			if (match.new_group) {
				emit_new_duplicate(row_idx, match.new_group_idx);
			} else {
				emit_existing_state(match.row_location, row_idx);
			}
			continue;
		}

		const auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = ApplyBitMask(hash);
		idx_t iteration_count;
		for (iteration_count = 0; iteration_count < capacity; iteration_count++) {
			auto &entry = entries[ht_offset];
			if (!entry.IsOccupied()) {
				ht_offsets[new_group_count] = ht_offset;
				state.new_groups.set_index(new_group_count, row_idx);
				entry.SetSalt(salt);
				target_cache.emplace(key, AggregateStringPrefixTargetMatch {true, new_group_count, nullptr});
				new_group_count++;
				break;
			}
			if (entry.GetSalt() == salt) {
				auto row_location = entry.GetPointer();
				if (cast_pointer_to_uint64(row_location) != ht_entry_t::POINTER_MASK &&
				    stored_key_matches(row_location, key)) {
					emit_existing_state(row_location, row_idx);
					target_cache.emplace(
					    key, AggregateStringPrefixTargetMatch {false, DConstants::INVALID_INDEX, row_location});
					break;
				}
			}
			SaltIncrementAndWrap(ht_offset, salt, bitmask);
		}
		if (iteration_count == capacity) {
			clear_marked_entries();
			throw InternalException("Maximum input-vector string-prefix find-or-create iteration count reached in "
			                        "GroupedAggregateHashTable");
		}
	}
	RecordAggregateTraceStage(recorder, "find_or_create_input_vector_string_prefix.probe", probe_start);
	if (enable_hll) {
		auto hll_start = AggregateTraceStart(recorder);
		hll.Update(state.hashes);
		RecordAggregateTraceStage(recorder, "find_or_create_input_vector_string_prefix.hll", hll_start);
	}

	if (new_group_count == 0) {
		auto target_start = AggregateTraceStart(recorder);
		targets.InputOrder().Set(selected_addresses, nullptr, count);
		RecordAggregateTraceStage(recorder, "find_or_create_input_vector_string_prefix.emit_existing_targets",
		                          target_start);
		sink_count += count;
		return true;
	}

	AggregateRowPointerDescriptorSourceInfo sources;
	if (!AggregatePrepareDescriptorSourceInfo(payload_input, nullptr, group_sources, layout_ptr->GetTypes(), sources)) {
		clear_marked_entries();
		return false;
	}
	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout_ptr->GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout_ptr->GetTypes().size());
	auto append_fill_start = AggregateTraceStart(recorder);
	if (!AggregateFillCompactDescriptorGroupChunk(allocator, sources, group_sources, state.new_groups, new_group_count,
	                                              layout_ptr->GetTypes(), hashes, state.descriptor_group_chunk,
	                                              state.descriptor_group_hashes)) {
		clear_marked_entries();
		RecordAggregateTraceStage(recorder, "find_or_create_input_vector_string_prefix.append_key_fill_miss",
		                          append_fill_start);
		return false;
	}
	state.group_chunk.data[0].Reference(state.descriptor_group_chunk.data[0]);
	state.group_chunk.data[1].Reference(state.descriptor_group_hashes);
	state.group_chunk.SetChildCardinality(new_group_count);
	TupleDataCollection::ToUnifiedFormat(state.partitioned_append_state.chunk_state, state.group_chunk);
	RecordAggregateTraceStage(recorder, "find_or_create_input_vector_string_prefix.append_key_fill", append_fill_start);

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
	data->AppendUnified(*append_state, state.group_chunk, *FlatVector::IncrementalSelectionVector(), new_group_count);
	RowOperations::InitializeStates(*layout_ptr, append_state->chunk_state.row_locations,
	                                *FlatVector::IncrementalSelectionVector(), new_group_count);
	const auto row_locations = FlatVector::GetData<data_ptr_t>(append_state->chunk_state.row_locations);
	const auto &row_sel = append_state->reverse_partition_sel;
	for (idx_t new_idx = 0; new_idx < new_group_count; new_idx++) {
		const auto row_location = row_locations[row_sel.get_index_unsafe(new_idx)];
		entries[ht_offsets[new_idx]].SetPointer(row_location);
	}
	RecordAggregateTraceStage(recorder, "find_or_create_input_vector_string_prefix.append_new_groups", append_start);

	auto target_start = AggregateTraceStart(recorder);
	for (idx_t new_idx = 0; new_idx < new_group_count; new_idx++) {
		const auto row_idx = state.new_groups.get_index_unsafe(new_idx);
		const auto row_location = row_locations[row_sel.get_index_unsafe(new_idx)];
		selected_addresses[row_idx] = reinterpret_cast<uintptr_t>(row_location);
	}
	for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
		const auto row_idx = state.no_match_vector.get_index_unsafe(duplicate_idx);
		const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
		const auto row_location = entries[ht_offsets[marked_idx]].GetPointer();
		selected_addresses[row_idx] = reinterpret_cast<uintptr_t>(row_location);
	}
	targets.InputOrder().Set(selected_addresses, nullptr, count);
	RecordAggregateTraceStage(recorder, "find_or_create_input_vector_string_prefix.emit_targets", target_start);
	this->count += new_group_count;
	sink_count += count;
	return true;
}

bool GroupedAggregateHashTable::TryFindOrCreateDescriptorGroupStateTargetsDirect(
    DataChunk &payload_input, optional_ptr<Vector> row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, ExecutionGroupedAggregateStateTargetBatch &targets,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	targets.Reset();
	if (!AggregateDescriptorSourcesCanUseDirectTargets(payload_input, row_pointers, group_sources)) {
		return false;
	}
	if (count == 0) {
		return true;
	}
	if (payload_input.size() != count || skip_lookups || !entries) {
		return false;
	}

	if (!row_pointers && TryFindOrCreateInputVectorStringPrefixGroupStateTargets(payload_input, count, group_sources,
	                                                                             targets, recorder)) {
		return true;
	}

	if (TryFindOrCreateSingleInputVectorGroupStateTargetsDense(payload_input, count, group_sources, targets, recorder,
	                                                           dense_domain)) {
		return true;
	}

	if (!row_pointers && TryFindOrCreateInputVectorSingleFieldGroupStateTargetsDirect(
	                         payload_input, count, group_sources, targets, recorder)) {
		return true;
	}

	AggregateRowPointerDescriptorSourceInfo sources;
	if (!AggregatePrepareDescriptorSourceInfo(payload_input, row_pointers, group_sources, layout_ptr->GetTypes(),
	                                          sources)) {
		return false;
	}
	const auto descriptor_sources_repeat_with_row_pointer =
	    row_pointers && AggregateDescriptorSourcesRepeatWithRowPointer(group_sources);
	auto cache_targets = [&](const data_ptr_t *cache_row_pointers, idx_t cache_count,
	                         const ExecutionGroupedAggregateStateTargetBatch &cache_targets) {
		if (!descriptor_sources_repeat_with_row_pointer || !cache_row_pointers) {
			return false;
		}
		auto &input_order = cache_targets.Span(ExecutionGroupedAggregateStateTargetKind::INPUT_ORDER);
		if (!input_order.addresses || input_order.count != cache_count) {
			return false;
		}
		for (idx_t row_idx = 0; row_idx < cache_count; row_idx++) {
			const auto address_idx = input_order.address_sel ? input_order.address_sel[row_idx] : row_idx;
			row_pointer_descriptor_target_cache.Set(cache_row_pointers[row_idx], input_order.addresses[address_idx]);
		}
		return true;
	};

	auto try_cached_row_pointer_targets = [&]() {
		if (!descriptor_sources_repeat_with_row_pointer || count < 2) {
			return false;
		}
		auto cache_start = AggregateTraceStart(recorder);
		vector<uintptr_t> cached_addresses(count);
		vector<sel_t> miss_rows(count);
		SelectionVector miss_selection(miss_rows.data(), count);
		idx_t miss_count = 0;
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto address = row_pointer_descriptor_target_cache.Get(sources.row_pointers[row_idx]);
			if (address) {
				cached_addresses[row_idx] = address;
				continue;
			}
			miss_selection.set_index(miss_count++, row_idx);
		}
		if (miss_count == count) {
			return false;
		}
		if (miss_count > 0) {
			DataChunk miss_input;
			miss_input.InitializeEmpty(payload_input.GetTypes());
			miss_input.Slice(payload_input, miss_selection, miss_count);
			Vector miss_row_pointers(LogicalType::POINTER);
			AggregateSelectFlatRowPointers(sources.row_pointers, miss_selection, miss_count, miss_row_pointers);
			ExecutionGroupedAggregateStateTargetBatch miss_targets;
			if (!TryFindOrCreateDescriptorGroupStateTargetsDirect(miss_input, optional_ptr<Vector>(&miss_row_pointers),
			                                                      miss_count, group_sources, miss_targets, recorder,
			                                                      dense_domain)) {
				return false;
			}
			auto &miss_input_order = miss_targets.InputOrder();
			if (!miss_input_order.addresses || miss_input_order.count != miss_count) {
				return false;
			}
			auto miss_pointer_data = FlatVector::GetData<data_ptr_t>(miss_row_pointers);
			for (idx_t miss_idx = 0; miss_idx < miss_count; miss_idx++) {
				const auto address_idx =
				    miss_input_order.address_sel ? miss_input_order.address_sel[miss_idx] : miss_idx;
				const auto address = miss_input_order.addresses[address_idx];
				const auto row_idx = miss_selection.get_index_unsafe(miss_idx);
				cached_addresses[row_idx] = address;
				row_pointer_descriptor_target_cache.Set(miss_pointer_data[miss_idx], address);
			}
		}
		state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::SetSize(state.addresses, count_t(count));
		auto selected_addresses = FlatVector::GetDataMutable<uintptr_t>(state.addresses);
		memcpy(selected_addresses, cached_addresses.data(), count * sizeof(uintptr_t));
		targets.InputOrder().Set(selected_addresses, nullptr, count);
		sink_count += count - miss_count;
		RecordAggregateTraceStage(recorder, "find_or_create_descriptor.row_pointer_target_cache", cache_start);
		return true;
	};
	if (try_cached_row_pointer_targets()) {
		return true;
	}

	auto hash_start = AggregateTraceStart(recorder);
	if (!AggregateHashDescriptorRows(sources, group_sources, count, state.hashes, require_canonical_group_hash)) {
		RecordAggregateTraceStage(recorder, "find_or_create_descriptor.hash_miss", hash_start);
		return false;
	}
	RecordAggregateTraceStage(recorder, "find_or_create_descriptor.hash", hash_start);
	const auto hashes = FlatVector::GetData<hash_t>(state.hashes);
	const auto descriptor_sources_are_all_valid = AggregateDescriptorSourcesAreAllValid(group_sources);
	if (enable_hll) {
		auto hll_start = AggregateTraceStart(recorder);
		hll.Update(state.hashes);
		RecordAggregateTraceStage(recorder, "find_or_create_descriptor.hll", hll_start);
	}

	if (Count() + count > capacity || Count() + count > ResizeThreshold()) {
		auto resize_start = AggregateTraceStart(recorder);
		Verify();
		ResizeForAdditionalGroups(count);
		RecordAggregateTraceStage(recorder, "find_or_create_descriptor.resize", resize_start);
	}

	state.addresses.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(state.addresses, count_t(count));
	auto selected_addresses = FlatVector::GetDataMutable<uintptr_t>(state.addresses);
	auto ht_offsets = FlatVector::GetDataMutable<uint64_t>(state.ht_offsets);
	auto duplicate_targets = FlatVector::GetDataMutable<hash_t>(state.hash_salts);

	idx_t new_group_count = 0;
	idx_t duplicate_count = 0;
	auto clear_marked_entries = [&]() {
		for (idx_t marked_idx = 0; marked_idx < new_group_count; marked_idx++) {
			entries[ht_offsets[marked_idx]] = ht_entry_t();
		}
		new_group_count = 0;
	};
	auto emit_existing_state = [&](data_ptr_t row_location, idx_t row_idx) {
		selected_addresses[row_idx] = reinterpret_cast<uintptr_t>(row_location);
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
	auto descriptor_rows_match = [&](idx_t row_idx, idx_t other_row_idx) {
		if (descriptor_sources_repeat_with_row_pointer &&
		    sources.row_pointers[row_idx] == sources.row_pointers[other_row_idx]) {
			return true;
		}
		if (AggregateDescriptorSourcesRepeatBySourceIndex(sources, group_sources, row_idx, other_row_idx)) {
			return true;
		}
		return AggregateDescriptorSourceValuesMatch(sources, group_sources, row_idx, other_row_idx);
	};
	auto try_emit_repeated_match = [&](idx_t row_idx) {
		if (last_match_kind == LastMatchKind::NONE || last_row_idx == DConstants::INVALID_INDEX ||
		    hashes[row_idx] != hashes[last_row_idx] || !descriptor_rows_match(row_idx, last_row_idx)) {
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
	bool use_consecutive_reuse = descriptor_sources_repeat_with_row_pointer;
	if (!use_consecutive_reuse) {
		const auto sample_count = MinValue<idx_t>(count, 64);
		for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
			if (hashes[row_idx] == hashes[row_idx - 1] && descriptor_rows_match(row_idx, row_idx - 1)) {
				use_consecutive_reuse = true;
				break;
			}
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
						if (!descriptor_rows_match(row_idx, marked_row_idx)) {
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
				if (AggregateDescriptorSourceRowMatchesStored(sources, group_sources, *layout_ptr, row_location,
				                                              row_idx, descriptor_sources_are_all_valid)) {
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
	RecordAggregateTraceStage(recorder, "find_or_create_descriptor.probe", probe_start);

	if (new_group_count == 0) {
		auto target_start = AggregateTraceStart(recorder);
		targets.InputOrder().Set(selected_addresses, nullptr, count);
		cache_targets(sources.row_pointers, count, targets);
		RecordAggregateTraceStage(recorder, "find_or_create_descriptor.emit_existing_targets", target_start);
		sink_count += count;
		return true;
	}

	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout_ptr->GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout_ptr->GetTypes().size());
	auto append_fill_start = AggregateTraceStart(recorder);
	bool append_uses_input_order = false;
	const SelectionVector *append_selection = FlatVector::IncrementalSelectionVector();
	if (AggregateTryReferenceInputVectorDescriptorGroupChunk(payload_input, row_pointers, group_sources,
	                                                         layout_ptr->GetTypes(), state.hashes, count,
	                                                         state.group_chunk)) {
		append_uses_input_order = true;
		append_selection = &state.new_groups;
		RecordAggregateTraceStage(recorder, "find_or_create_descriptor.append_key_reference", append_fill_start);
	} else {
		if (!AggregateFillCompactDescriptorGroupChunk(allocator, sources, group_sources, state.new_groups,
		                                              new_group_count, layout_ptr->GetTypes(), hashes,
		                                              state.descriptor_group_chunk, state.descriptor_group_hashes)) {
			clear_marked_entries();
			RecordAggregateTraceStage(recorder, "find_or_create_descriptor.append_key_fill_miss", append_fill_start);
			return false;
		}
		for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
			state.group_chunk.data[group_idx].Reference(state.descriptor_group_chunk.data[group_idx]);
		}
		state.group_chunk.data[group_sources.size()].Reference(state.descriptor_group_hashes);
		state.group_chunk.SetChildCardinality(new_group_count);
		RecordAggregateTraceStage(recorder, "find_or_create_descriptor.append_key_fill", append_fill_start);
	}
	TupleDataCollection::ToUnifiedFormat(state.partitioned_append_state.chunk_state, state.group_chunk);

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
	data->AppendUnified(*append_state, state.group_chunk, *append_selection, new_group_count);
	RowOperations::InitializeStates(*layout_ptr, append_state->chunk_state.row_locations,
	                                *FlatVector::IncrementalSelectionVector(), new_group_count);
	const auto row_locations = FlatVector::GetData<data_ptr_t>(append_state->chunk_state.row_locations);
	const auto &row_sel = append_state->reverse_partition_sel;
	for (idx_t new_idx = 0; new_idx < new_group_count; new_idx++) {
		const auto row_location_idx = append_uses_input_order ? state.new_groups.get_index_unsafe(new_idx) : new_idx;
		const auto row_location = row_locations[row_sel.get_index_unsafe(row_location_idx)];
		entries[ht_offsets[new_idx]].SetPointer(row_location);
	}
	for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
		const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
		D_ASSERT(entries[ht_offsets[marked_idx]].GetPointer());
	}
	auto target_start = AggregateTraceStart(recorder);
	if (new_group_count > 0) {
		for (idx_t new_idx = 0; new_idx < new_group_count; new_idx++) {
			const auto row_idx = state.new_groups.get_index_unsafe(new_idx);
			const auto row_location_idx = append_uses_input_order ? row_idx : new_idx;
			const auto row_location = row_locations[row_sel.get_index_unsafe(row_location_idx)];
			selected_addresses[row_idx] = reinterpret_cast<uintptr_t>(row_location);
		}
	}
	if (duplicate_count > 0) {
		for (idx_t duplicate_idx = 0; duplicate_idx < duplicate_count; duplicate_idx++) {
			const auto row_idx = state.no_match_vector.get_index_unsafe(duplicate_idx);
			const auto marked_idx = UnsafeNumericCast<idx_t>(duplicate_targets[duplicate_idx]);
			const auto row_location = entries[ht_offsets[marked_idx]].GetPointer();
			selected_addresses[row_idx] = reinterpret_cast<uintptr_t>(row_location);
		}
	}
	targets.InputOrder().Set(selected_addresses, nullptr, count);
	cache_targets(sources.row_pointers, count, targets);
	RecordAggregateTraceStage(recorder, "find_or_create_descriptor.emit_targets", target_start);
	this->count += new_group_count;
	sink_count += count;
	RecordAggregateTraceStage(recorder, "find_or_create_descriptor.append_new_groups", append_start);
	return true;
}

bool GroupedAggregateHashTable::TryFindOrCreateRowPointerGroupStateTargetsDirect(
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, ExecutionGroupedAggregateStateTargetBatch &targets,
    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	return TryFindOrCreateDescriptorGroupStateTargetsDirect(payload_input, optional_ptr<Vector>(&row_pointers), count,
	                                                        group_sources, targets, recorder);
}

bool GroupedAggregateHashTable::TryFindOrCreateRowPointerGroupStateTargetsMaterialized(
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, ExecutionGroupedAggregateStateTargetBatch &targets,
    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	targets.Reset();
	if (row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	if (count == 0) {
		return true;
	}
	if (payload_input.size() != count) {
		return false;
	}

	auto fill_start = AggregateTraceStart(recorder);
	if (!AggregateFillDescriptorGroupChunk(allocator, payload_input, row_pointers, count, group_sources,
	                                       layout_ptr->GetTypes(), state.descriptor_group_chunk)) {
		RecordAggregateTraceStage(recorder, "find_or_create_descriptor_keys.fill_miss", fill_start);
		return false;
	}
	RecordAggregateTraceStage(recorder, "find_or_create_descriptor_keys.fill", fill_start);

	auto lookup_start = AggregateTraceStart(recorder);
	FindOrCreateGroupAddresses(state.descriptor_group_chunk, state.addresses, recorder);
	RecordAggregateTraceStage(recorder, "find_or_create_descriptor_keys.lookup_addresses", lookup_start);

	auto target_start = AggregateTraceStart(recorder);
	state.addresses.Flatten();
	targets.InputOrder().Set(FlatVector::GetData<uintptr_t>(state.addresses), nullptr, count);
	RecordAggregateTraceStage(recorder, "find_or_create_descriptor_keys.emit_targets", target_start);
	return true;
}

bool GroupedAggregateHashTable::TryFindOrCreateRowPointerGroupStateTargetsFast(
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, ExecutionGroupedAggregateStateTargetBatch &targets,
    optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	EnsureLookupEpoch();
	targets.Reset();
	if (row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	if (count == 0) {
		return true;
	}
	if (payload_input.size() != count ||
	    !AggregateDescriptorGroupKeySourcesSupported(group_sources, layout_ptr->GetTypes())) {
		return false;
	}
	if (skip_lookups || !entries) {
		return TryFindOrCreateRowPointerGroupStateTargetsMaterialized(payload_input, row_pointers, count, group_sources,
		                                                              targets, recorder);
	}
	switch (AggregateSelectRowPointerGroupLookupStrategy(payload_input, row_pointers, group_sources)) {
	case AggregateRowPointerGroupLookupStrategy::SINGLE_ROW_POINTER_FIELD_TARGETS:
		return TryFindOrCreateRowPointerSingleFieldGroupStateTargetsDirect(payload_input, row_pointers, count,
		                                                                   group_sources, targets, recorder);
	case AggregateRowPointerGroupLookupStrategy::DIRECT_DESCRIPTOR_TARGETS:
		return TryFindOrCreateRowPointerGroupStateTargetsDirect(payload_input, row_pointers, count, group_sources,
		                                                        targets, recorder);
	case AggregateRowPointerGroupLookupStrategy::MATERIALIZED_DESCRIPTOR_TARGETS:
		return TryFindOrCreateRowPointerGroupStateTargetsMaterialized(payload_input, row_pointers, count, group_sources,
		                                                              targets, recorder);
	}
	throw InternalException("Unhandled row-pointer grouped lookup strategy");
}

bool GroupedAggregateHashTable::TryFindOrCreateInputVectorGroupStateTargetsFast(
    DataChunk &payload_input, idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    ExecutionGroupedAggregateStateTargetBatch &targets, optional_ptr<ExecutionOperatorStageRecorder> recorder,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	targets.Reset();
	if (count == 0) {
		return true;
	}
	if (payload_input.size() != count ||
	    !AggregateDescriptorGroupKeySourcesSupported(group_sources, layout_ptr->GetTypes()) ||
	    !AggregateInputVectorDescriptorSourcesCanProbeDirect(payload_input, group_sources)) {
		return false;
	}
	if (skip_lookups || !entries) {
		return false;
	}
	return TryFindOrCreateDescriptorGroupStateTargetsDirect(payload_input, nullptr, count, group_sources, targets,
	                                                        recorder, dense_domain);
}

bool GroupedAggregateHashTable::TryUpdateSingleInputVectorGroupCountOneFast(
    DataChunk &payload_input, idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const ExecutionPrimitiveAggregateUpdateLane &lane, optional_ptr<ExecutionOperatorStageRecorder> recorder,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	if (count == 0) {
		return true;
	}
	if (payload_input.size() != count ||
	    !AggregateDescriptorGroupKeySourcesSupported(group_sources, layout_ptr->GetTypes()) ||
	    !AggregateInputVectorDescriptorSourcesCanProbeDirect(payload_input, group_sources) || skip_lookups ||
	    !entries) {
		return false;
	}
	return TryUpdateSingleInputVectorGroupCountOneDense(payload_input, count, group_sources, lane, recorder,
	                                                    dense_domain);
}

bool GroupedAggregateHashTable::TryAppendNewGroupAddressesFast(DataChunk &groups, Vector &addresses_out,
                                                               optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	return TryAppendNewGroupsFastInternal(groups, addresses_out, nullptr, nullptr, recorder);
}

bool GroupedAggregateHashTable::TryAppendNewGroupsFastInternal(
    DataChunk &groups, optional_ptr<Vector> addresses_out,
    ExecutionGroupedAggregateStateAddressUpdateFunction address_update_function, void *address_update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
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
	if (!entries || !layout_ptr->CannotHaveNull() || groups.ColumnCount() == 0) {
		RecordAggregateTraceStage(recorder, "find_new.unsupported_state", AggregateTraceStart(recorder));
		return false;
	}
	const bool append_only = skip_lookups;
	if (!AggregateFastAppendNewGroupsSupported(groups)) {
		RecordAggregateTraceStage(recorder, "find_new.unsupported_groups", AggregateTraceStart(recorder));
		return false;
	}
	const auto &layout_types = layout_ptr->GetTypes();
	const auto &layout_offsets = layout_ptr->GetOffsets();
	AggregateFastGroupSourceInfo sources;
	if (!AggregatePrepareFastGroupSourceInfo(groups, layout_types, chunk_size, sources)) {
		RecordAggregateTraceStage(recorder, "find_new.unsupported_sources", AggregateTraceStart(recorder));
		return false;
	}
	AggregateFastGroupSourceInfo dense_sources;
	const bool update_dense_cache = !append_only && AggregateTryPrepareDenseSingleFieldTargetCache(
	                                                    groups, layout_types, layout_offsets, chunk_size, Count(),
	                                                    dense_single_field_target_cache, dense_sources, dense_domain);
	const auto dense_append_proof =
	    update_dense_cache
	        ? AggregateProveDenseSingleFieldAppendKeysNew(dense_sources, chunk_size, dense_single_field_target_cache)
	        : AggregateDenseAppendProof::UNAVAILABLE;
	if (dense_append_proof == AggregateDenseAppendProof::DUPLICATE_EXISTING) {
		RecordAggregateTraceStage(recorder, "find_new.mark_empty_duplicate_existing", AggregateTraceStart(recorder));
		RecordAggregateTraceStage(recorder, "find_new.mark_empty_miss", AggregateTraceStart(recorder));
		return false;
	}
	if (dense_append_proof == AggregateDenseAppendProof::DUPLICATE_INPUT) {
		RecordAggregateTraceStage(recorder, "find_new.mark_empty_duplicate_pending", AggregateTraceStart(recorder));
		RecordAggregateTraceStage(recorder, "find_new.mark_empty_miss", AggregateTraceStart(recorder));
		return false;
	}
	const bool dense_append_is_proven_new = dense_append_proof == AggregateDenseAppendProof::PROVEN_NEW;

	if (!append_only && (Count() + chunk_size > capacity || Count() + chunk_size > ResizeThreshold())) {
		auto resize_start = AggregateTraceStart(recorder);
		Verify();
		ResizeForAdditionalGroups(chunk_size);
		RecordAggregateTraceStage(recorder, "find_new.resize", resize_start);
	}
	D_ASSERT(append_only || capacity - Count() >= chunk_size);

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
	state.group_chunk.data[groups.ColumnCount()].Reference(state.hashes);

	const auto hashes = state.hashes.Values<hash_t>();
	auto ht_offsets = FlatVector::GetDataMutable<uint64_t>(state.ht_offsets);
	auto hash_salts = FlatVector::GetDataMutable<hash_t>(state.hash_salts);
	auto mark_start = AggregateTraceStart(recorder);
	idx_t marked_count = 0;
	auto clear_marked_entries = [&]() {
		for (idx_t marked_idx = 0; marked_idx < marked_count; marked_idx++) {
			entries[ht_offsets[marked_idx]] = ht_entry_t();
		}
		marked_count = 0;
	};
	if (!append_only) {
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
				if (!dense_append_is_proven_new && entry.GetSalt() == salt) {
					const auto row_location = entry.GetPointer();
					if (cast_pointer_to_uint64(row_location) == ht_entry_t::POINTER_MASK) {
						for (idx_t marked_idx = 0; marked_idx < marked_count; marked_idx++) {
							if (ht_offsets[marked_idx] == ht_offset &&
							    AggregateFastGroupSourceRowsMatch(sources, row_idx, marked_idx)) {
								clear_marked_entries();
								RecordAggregateTraceStage(recorder, "find_new.mark_empty_duplicate_pending",
								                          mark_start);
								RecordAggregateTraceStage(recorder, "find_new.mark_empty_miss", mark_start);
								return false;
							}
						}
					} else if (AggregateFastExistingRowMatches(sources, *layout_ptr, row_location, row_idx)) {
						clear_marked_entries();
						RecordAggregateTraceStage(recorder, "find_new.mark_empty_duplicate_existing", mark_start);
						RecordAggregateTraceStage(recorder, "find_new.mark_empty_miss", mark_start);
						return false;
					}
				}
				SaltIncrementAndWrap(ht_offset, salt, bitmask);
			}
			if (iteration_count == capacity) {
				clear_marked_entries();
				throw InternalException("Maximum fast new-group iteration count reached in GroupedAggregateHashTable");
			}
		}
		RecordAggregateTraceStage(recorder, "find_new.mark_empty", mark_start);
	}
	if (enable_hll) {
		auto hll_start = AggregateTraceStart(recorder);
		hll.Update(state.hashes);
		RecordAggregateTraceStage(recorder, "find_new.hll", hll_start);
	}

	auto append_start = AggregateTraceStart(recorder);
	data_ptr_t *addresses = nullptr;
	if (addresses_out) {
		addresses_out->Flatten();
		addresses = FlatVector::GetDataMutable<data_ptr_t>(*addresses_out);
	}
	auto group_format_start = AggregateTraceStart(recorder);
	auto target = PrepareAppendTarget(state.group_chunk, chunk_size, append_only);
	RecordAggregateTraceStage(recorder, "find_new.group_format", group_format_start);
	const auto &append_selection = *FlatVector::IncrementalSelectionVector();
	// Append-only state publication consumes physical row locations directly. A
	// single-partition append preserves input order, so no reverse map is needed.
	const bool build_reverse_selection = !append_only || addresses;
	const auto fixed_width_append = target.data.TryAppendUnifiedFixedWidthSinglePartition(
	    target.state, state.group_chunk, append_selection, chunk_size, build_reverse_selection);
	const auto single_partition_append = fixed_width_append || target.data.TryAppendUnifiedSinglePartition(
	                                                               target.state, state.group_chunk, append_selection,
	                                                               chunk_size, build_reverse_selection);
	if (!single_partition_append) {
		target.data.AppendUnified(target.state, state.group_chunk, append_selection, chunk_size);
	}
	const auto update_mode = address_update_function && !layout_ptr->HasDestructor()
	                             ? ExecutionGroupedAggregateStateAddressUpdateMode::INITIALIZE_AND_UPDATE
	                             : ExecutionGroupedAggregateStateAddressUpdateMode::UPDATE_INITIALIZED;
	if (update_mode == ExecutionGroupedAggregateStateAddressUpdateMode::UPDATE_INITIALIZED) {
		RowOperations::InitializeStates(*layout_ptr, target.state.chunk_state.row_locations, append_selection,
		                                chunk_size);
	}
	const auto row_locations = FlatVector::GetData<data_ptr_t>(target.state.chunk_state.row_locations);
	const auto &row_sel = target.state.reverse_partition_sel;
	if (!append_only || addresses) {
		for (idx_t row_idx = 0; row_idx < chunk_size; row_idx++) {
			const auto row_location = row_locations[row_sel.get_index_unsafe(row_idx)];
			if (!append_only) {
				entries[ht_offsets[row_idx]].SetPointer(row_location);
			}
			if (addresses) {
				addresses[row_idx] = row_location;
			}
		}
	}
	if (update_dense_cache) {
		if (!AggregatePopulateDenseSingleFieldTargetCache(dense_sources, row_locations, row_sel, chunk_size,
		                                                  dense_single_field_target_cache)) {
			dense_single_field_target_cache.Disable();
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
		const auto address_sel = single_partition_append ? nullptr : row_sel.data();
		address_update_function(state_addresses, address_sel, chunk_size, update_mode, address_update_state);
		RecordAggregateTraceStage(recorder, "find_new.state_address_update", update_start);
	}
	return true;
}

bool GroupedAggregateHashTable::TryAppendNewGroupsWithStateAddressesFast(
    DataChunk &groups, ExecutionGroupedAggregateStateAddressUpdateFunction update_function, void *update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	if (!update_function) {
		return false;
	}
	return TryAppendNewGroupsFastInternal(groups, nullptr, update_function, update_state, recorder, dense_domain);
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

void GroupedAggregateHashTable::Combine(TupleDataCollection &other_data, optional_ptr<atomic<double>> progress,
                                        optional_ptr<TupleDataRowLocationRemap> state_remap) {
	// Combined groups are unknown to the dense target cache; a later dense append
	// consulting a cached range that lacks them would recreate an existing group.
	// Phase ordering makes that unreachable today; disabling makes it impossible.
	dense_single_field_target_cache.Disable();
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
		if (state_remap) {
			state_remap->Remap(fm_state.scan_state.chunk_state.row_locations, *FlatVector::IncrementalSelectionVector(),
			                   fm_state.group_addresses, fm_state.groups.size());
		}
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
	dense_single_field_target_cache.Reset();
	row_pointer_descriptor_target_cache.Reset();
	Destroy();
	aggregate_allocator->Reset();
	stored_allocators.clear();

	const auto reuse_partitioned_data = partitioned_data && RadixPartitioning::RadixBitsOfPowerOfTwo(
	                                                            partitioned_data->PartitionCount()) == radix_bits_p;
	const auto reuse_unpartitioned_data =
	    radix_bits_p > 0 && unpartitioned_data &&
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
	if (radix_bits >= UNPARTITIONED_RADIX_BITS_THRESHOLD || reuse_unpartitioned_data) {
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
	skip_lookups_require_final_combine = false;
	epochs_require_final_combine = false;
	proven_unique_append_key_type = PhysicalType::INVALID;
	proven_unique_append_has_last_key = false;
	proven_unique_append_last_signed_key = 0;
	proven_unique_append_last_unsigned_key = 0;
	proven_unique_append_ranges.clear();
	proven_unique_append_ranges_coalesced = false;
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
