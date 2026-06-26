#include "duckdb/execution/radix_partitioned_hashtable.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/enums/debug_verification_mode.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

#include <chrono>

namespace duckdb {

static std::chrono::steady_clock::time_point RadixTraceStart(optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	return recorder ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
}

static void RecordRadixTraceStage(optional_ptr<ExecutionOperatorStageRecorder> recorder, const string &stage,
                                  std::chrono::steady_clock::time_point start) {
	if (!recorder) {
		return;
	}
	auto end = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
	recorder->RecordStageRuntime(stage, elapsed);
}

static bool RadixLoadPrimitiveInteger(const_data_ptr_t data, PhysicalType type, idx_t row_idx, int64_t &result) {
	switch (type) {
	case PhysicalType::INT8:
		result = Load<int8_t>(data + row_idx * sizeof(int8_t));
		return true;
	case PhysicalType::INT16:
		result = Load<int16_t>(data + row_idx * sizeof(int16_t));
		return true;
	case PhysicalType::INT32:
		result = Load<int32_t>(data + row_idx * sizeof(int32_t));
		return true;
	case PhysicalType::INT64:
		result = Load<int64_t>(data + row_idx * sizeof(int64_t));
		return true;
	case PhysicalType::UINT8:
		result = NumericCast<int64_t>(Load<uint8_t>(data + row_idx * sizeof(uint8_t)));
		return true;
	case PhysicalType::UINT16:
		result = NumericCast<int64_t>(Load<uint16_t>(data + row_idx * sizeof(uint16_t)));
		return true;
	case PhysicalType::UINT32:
		result = NumericCast<int64_t>(Load<uint32_t>(data + row_idx * sizeof(uint32_t)));
		return true;
	default:
		return false;
	}
}

static bool RadixLoadPrimitiveDouble(const_data_ptr_t data, PhysicalType type, idx_t row_idx, double &result) {
	switch (type) {
	case PhysicalType::FLOAT:
		result = Load<float>(data + row_idx * sizeof(float));
		return true;
	case PhysicalType::DOUBLE:
		result = Load<double>(data + row_idx * sizeof(double));
		return true;
	default:
		return false;
	}
}

static void RadixAccumulateHugeintInt64(hugeint_t &sum, int64_t value) {
	const auto old_lower = sum.lower;
	sum.lower += UnsafeNumericCast<uint64_t>(value);
	sum.upper += value >> 63;
	if (sum.lower < old_lower) {
		sum.upper++;
	}
}

struct RadixPrimitiveAggregatePayload {
	const_data_ptr_t data = nullptr;
	PhysicalType type = PhysicalType::INVALID;
};

static bool RadixPrimitiveAggregatePayloadSourceIsFlatAllValid(DataChunk &chunk, idx_t source_idx,
                                                               const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                               RadixPrimitiveAggregatePayload &payload) {
	if (!AggregatePrimitiveUpdateRequiresPayload(lane.kind)) {
		return true;
	}
	if (source_idx >= chunk.ColumnCount()) {
		return false;
	}
	auto &source = chunk.data[source_idx];
	if (source.GetType().InternalType() != lane.payload_type || source.GetVectorType() != VectorType::FLAT_VECTOR ||
	    !FlatVector::Validity(source).CheckAllValid(chunk.size())) {
		return false;
	}
	payload.data = FlatVector::GetData(source);
	payload.type = source.GetType().InternalType();
	return true;
}

static bool RadixPrimitiveAggregatePayloadIsFlatAllValid(DataChunk &chunk,
                                                         const ExecutionRegionAggregateInput &aggregate,
                                                         const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                         RadixPrimitiveAggregatePayload &payload) {
	if (!AggregatePrimitiveUpdateRequiresPayload(lane.kind)) {
		return true;
	}
	if (aggregate.child_indices.size() != 1) {
		return false;
	}
	return RadixPrimitiveAggregatePayloadSourceIsFlatAllValid(chunk, aggregate.child_indices[0], lane, payload);
}

static bool RadixValidatePrimitiveAggregateUpdate(const ExecutionRegionSinkInfo &sink_info,
                                                  const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                                  DataChunk &chunk, vector<RadixPrimitiveAggregatePayload> &payloads) {
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    sink_info.aggregates.size() != lanes.size() || !sink_info.aggregate_contract.grouped_state_layout_ready) {
		return false;
	}
	payloads.clear();
	payloads.resize(lanes.size());
	for (idx_t aggregate_idx = 0; aggregate_idx < sink_info.aggregates.size(); aggregate_idx++) {
		auto &aggregate = sink_info.aggregates[aggregate_idx];
		auto lane = lanes[aggregate_idx];
		if (!lane || !lane->ready || lane->aggregate_index != aggregate.aggregate_index ||
		    aggregate.aggregate_index >= sink_info.aggregate_contract.grouped_state_offsets.size() ||
		    lane->state_offset != sink_info.aggregate_contract.grouped_state_offsets[aggregate.aggregate_index] ||
		    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
		    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset) {
			return false;
		}
		if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (aggregate.child_count != 0 || AggregatePrimitiveUpdateRequiresPayload(lane->kind)) {
				return false;
			}
			continue;
		}
		if (aggregate.child_count != 1 || !aggregate.primitive_update_ready ||
		    !AggregatePrimitiveUpdateRequiresPayload(lane->kind) ||
		    !RadixPrimitiveAggregatePayloadIsFlatAllValid(chunk, aggregate, *lane, payloads[aggregate_idx])) {
			return false;
		}
		if (chunk.size() == 0) {
			continue;
		}
		if (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
		    lane->kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			int64_t ignored;
			if (!RadixLoadPrimitiveInteger(payloads[aggregate_idx].data, payloads[aggregate_idx].type, 0, ignored)) {
				return false;
			}
		} else if (lane->kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
			double ignored;
			if (!RadixLoadPrimitiveDouble(payloads[aggregate_idx].data, payloads[aggregate_idx].type, 0, ignored)) {
				return false;
			}
		} else {
			return false;
		}
	}
	return true;
}

static bool RadixValidatePrimitiveAggregateUpdateWithPayloadInput(
    const ExecutionRegionSinkInfo &sink_info, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
    DataChunk &payload_input, const vector<idx_t> &payload_source_indices,
    vector<RadixPrimitiveAggregatePayload> &payloads) {
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    sink_info.aggregates.size() != lanes.size() || sink_info.aggregates.size() != payload_source_indices.size() ||
	    !sink_info.aggregate_contract.grouped_state_layout_ready) {
		return false;
	}
	payloads.clear();
	payloads.resize(lanes.size());
	for (idx_t aggregate_idx = 0; aggregate_idx < sink_info.aggregates.size(); aggregate_idx++) {
		auto &aggregate = sink_info.aggregates[aggregate_idx];
		auto lane = lanes[aggregate_idx];
		if (!lane || !lane->ready || lane->aggregate_index != aggregate.aggregate_index ||
		    aggregate.aggregate_index >= sink_info.aggregate_contract.grouped_state_offsets.size() ||
		    lane->state_offset != sink_info.aggregate_contract.grouped_state_offsets[aggregate.aggregate_index] ||
		    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
		    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset) {
			return false;
		}
		if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (aggregate.child_count != 0 || AggregatePrimitiveUpdateRequiresPayload(lane->kind)) {
				return false;
			}
			continue;
		}
		if (aggregate.child_count != 1 || !aggregate.primitive_update_ready ||
		    !AggregatePrimitiveUpdateRequiresPayload(lane->kind) ||
		    !RadixPrimitiveAggregatePayloadSourceIsFlatAllValid(payload_input, payload_source_indices[aggregate_idx],
		                                                       *lane, payloads[aggregate_idx])) {
			return false;
		}
		if (payload_input.size() == 0) {
			continue;
		}
		if (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
		    lane->kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			int64_t ignored;
			if (!RadixLoadPrimitiveInteger(payloads[aggregate_idx].data, payloads[aggregate_idx].type, 0, ignored)) {
				return false;
			}
		} else if (lane->kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
			double ignored;
			if (!RadixLoadPrimitiveDouble(payloads[aggregate_idx].data, payloads[aggregate_idx].type, 0, ignored)) {
				return false;
			}
		} else {
			return false;
		}
	}
	return true;
}

static bool
RadixValidateStateAddressCallbackUpdate(const ExecutionRegionSinkInfo &sink_info,
                                        ExecutionGroupedAggregateStateAddressUpdateFunction update_function) {
	return update_function && sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
	       sink_info.aggregate_contract.grouped_state_layout_ready;
}

static bool RadixValidateStateAddressCallbackUpdate(
    const ExecutionRegionSinkInfo &sink_info,
    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function) {
	return update_function && sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
	       sink_info.aggregate_contract.grouped_state_layout_ready;
}

static const uintptr_t *RadixStateAddressSpan(Vector &addresses) {
	D_ASSERT(addresses.GetVectorType() == VectorType::FLAT_VECTOR);
	return FlatVector::GetData<uintptr_t>(addresses);
}

struct RadixPrimitiveGroupUpdateState {
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> *lanes = nullptr;
	const vector<RadixPrimitiveAggregatePayload> *payloads = nullptr;
};

static void RadixUpdatePrimitiveGroup(data_ptr_t state_address, idx_t row_idx, void *state_p) {
	auto &state = *reinterpret_cast<RadixPrimitiveGroupUpdateState *>(state_p);
	if (!state.lanes || !state.payloads || state.lanes->size() != state.payloads->size()) {
		throw InternalException("Primitive aggregate grouped update state is incomplete");
	}
	auto &lanes = *state.lanes;
	auto &payloads = *state.payloads;
	for (idx_t aggregate_idx = 0; aggregate_idx < lanes.size(); aggregate_idx++) {
		auto lane = lanes[aggregate_idx];
		auto state_base = state_address + lane->state_offset;
		auto value_ptr = state_base + lane->state_value_offset;
		switch (lane->kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR: {
			auto count = reinterpret_cast<int64_t *>(value_ptr);
			(*count)++;
			break;
		}
		case AggregatePrimitiveUpdateKind::SUM_INT64: {
			int64_t value;
			RadixLoadPrimitiveInteger(payloads[aggregate_idx].data, payloads[aggregate_idx].type, row_idx, value);
			auto sum = reinterpret_cast<int64_t *>(value_ptr);
			*sum += value;
			auto state_is_set = reinterpret_cast<bool *>(state_base + lane->state_is_set_offset);
			*state_is_set = true;
			break;
		}
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
			int64_t value;
			RadixLoadPrimitiveInteger(payloads[aggregate_idx].data, payloads[aggregate_idx].type, row_idx, value);
			auto sum = reinterpret_cast<hugeint_t *>(value_ptr);
			RadixAccumulateHugeintInt64(*sum, value);
			auto state_is_set = reinterpret_cast<bool *>(state_base + lane->state_is_set_offset);
			*state_is_set = true;
			break;
		}
		case AggregatePrimitiveUpdateKind::SUM_DOUBLE: {
			double value;
			RadixLoadPrimitiveDouble(payloads[aggregate_idx].data, payloads[aggregate_idx].type, row_idx, value);
			auto sum = reinterpret_cast<double *>(value_ptr);
			*sum += value;
			auto state_is_set = reinterpret_cast<bool *>(state_base + lane->state_is_set_offset);
			*state_is_set = true;
			break;
		}
		default:
			throw InternalException("Unsupported primitive aggregate fast update kind");
		}
	}
}

static void RadixUpdatePrimitiveGroupSelected(const uintptr_t *state_addresses, const sel_t *address_sel,
                                              const sel_t *row_sel, idx_t count, void *state_p) {
	if (!state_addresses) {
		throw InternalException("Primitive aggregate selected grouped update is missing state addresses");
	}
	for (idx_t idx = 0; idx < count; idx++) {
		const auto address_idx = address_sel ? address_sel[idx] : idx;
		const auto row_idx = row_sel ? row_sel[idx] : idx;
		auto state_address = reinterpret_cast<data_ptr_t>(state_addresses[address_idx]);
		RadixUpdatePrimitiveGroup(state_address, row_idx, state_p);
	}
}

RadixPartitionedHashTable::RadixPartitionedHashTable(GroupingSet &grouping_set_p, const GroupedAggregateData &op_p,
                                                     TupleDataValidityType group_validity_p)
    : grouping_set(grouping_set_p), op(op_p), group_validity(group_validity_p) {
	auto groups_count = op.GroupCount();
	for (auto group_idx : ProjectionIndex::GetIndexes(groups_count)) {
		if (grouping_set.find(group_idx) == grouping_set.end()) {
			null_groups.push_back(group_idx);
		}
	}
	if (grouping_set.empty()) {
		// Fake a single group with a constant value for aggregation without groups
		group_types.emplace_back(LogicalType::TINYINT);
	}
	for (auto &entry : grouping_set) {
		group_types.push_back(op.group_types[entry]);
	}
	SetGroupingValues();

	auto group_types_copy = group_types;
	group_types_copy.emplace_back(LogicalType::HASH);

	auto layout = make_shared_ptr<TupleDataLayout>();
	auto aggregate_objects = AggregateObject::CreateAggregateObjects(op.bindings);
	layout->Initialize(std::move(group_types_copy), std::move(aggregate_objects), group_validity);
	layout_ptr = std::move(layout);
}

void RadixPartitionedHashTable::SetGroupingValues() {
	// Compute the GROUPING values:
	// For each parameter to the GROUPING clause, we check if the hash table groups on this particular group
	// If it does, we return 0, otherwise we return 1
	// We then use bitshifts to combine these values
	auto &grouping_functions = op.GetGroupingFunctions();
	for (auto &grouping : grouping_functions) {
		int64_t grouping_value = 0;
		D_ASSERT(grouping.size() < sizeof(int64_t) * 8);
		for (idx_t i = 0; i < grouping.size(); i++) {
			if (grouping_set.find(grouping[i]) == grouping_set.end()) {
				// We don't group on this value!
				grouping_value += 1LL << (grouping.size() - (i + 1));
			}
		}
		grouping_values.push_back(Value::BIGINT(grouping_value));
	}
}

shared_ptr<TupleDataLayout> RadixPartitionedHashTable::GetLayoutPtr() const {
	return layout_ptr;
}

const TupleDataLayout &RadixPartitionedHashTable::GetLayout() const {
	return *layout_ptr;
}

unique_ptr<GroupedAggregateHashTable> RadixPartitionedHashTable::CreateHT(ClientContext &context, const idx_t capacity,
                                                                          const idx_t radix_bits) const {
	return make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), group_types, op.payload_types,
	                                            op.bindings, capacity, radix_bits, group_validity);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
enum class AggregatePartitionState : uint8_t {
	//! Can be finalized
	READY_TO_FINALIZE = 0,
	//! Finalize is in progress
	FINALIZE_IN_PROGRESS = 1,
	//! Finalized, ready to scan
	READY_TO_SCAN = 2
};

struct AggregatePartition : StateWithBlockableTasks {
	explicit AggregatePartition(unique_ptr<TupleDataCollection> data_p)
	    : state(AggregatePartitionState::READY_TO_FINALIZE), data(std::move(data_p)), progress(0) {
	}

	AggregatePartitionState state;

	unique_ptr<TupleDataCollection> data;
	atomic<double> progress;
};

class RadixHTGlobalSinkState;

struct RadixHTConfig {
public:
	explicit RadixHTConfig(RadixHTGlobalSinkState &sink);

	void Reset();
	void SetRadixBits(const idx_t &radix_bits_p);
	bool SetRadixBitsToExternal();
	idx_t GetRadixBits() const;
	idx_t GetMaximumSinkRadixBits() const;

private:
	void SetRadixBitsInternal(idx_t radix_bits_p, bool external);
	idx_t InitialSinkRadixBits() const;
	idx_t ExternalRadixBits(bool dynamic) const;
	idx_t MaximumSinkRadixBits() const;
	idx_t SinkCapacity() const;

private:
	//! The global sink state
	RadixHTGlobalSinkState &sink;

public:
	//! Width of tuples
	const idx_t row_width;
	//! Capacity of HTs during the Sink
	const idx_t sink_capacity;

private:
	//! Sink radix bits to initialize with
	static constexpr idx_t MAXIMUM_INITIAL_SINK_RADIX_BITS = 4;
	//! Maximum Sink radix bits (independent of threads)
	static constexpr idx_t MAXIMUM_FINAL_SINK_RADIX_BITS = 8;

	//! Current thread-global sink radix bits
	atomic<idx_t> sink_radix_bits;
	//! Maximum Sink radix bits (set based on number of threads, if not external)
	const idx_t maximum_sink_radix_bits;

	//! Thresholds at which we reduce the sink radix bits
	//! This needed to reduce cache misses when we have very wide rows
	static constexpr idx_t ROW_WIDTH_THRESHOLD_ONE = 32;
	static constexpr idx_t ROW_WIDTH_THRESHOLD_TWO = 64;

public:
	//! If we have this many or less threads, we grow the HT, otherwise we abandon
	static constexpr idx_t GROW_STRATEGY_THREAD_THRESHOLD = 2;
	//! If we fill this many blocks per partition, we trigger a repartition
	static constexpr double BLOCK_FILL_FACTOR = 0.5;
	//! By how many bits to repartition if a repartition is triggered
	static constexpr idx_t REPARTITION_RADIX_BITS = 2;
};

class RadixHTGlobalSinkState : public GlobalSinkState {
public:
	RadixHTGlobalSinkState(ClientContext &context, const RadixPartitionedHashTable &radix_ht);

	//! Destroys aggregate states (if multi-scan)
	~RadixHTGlobalSinkState() override;
	void Destroy();

public:
	idx_t GetThreadLimit() const {
		return temporary_memory_state->GetReservation() / number_of_threads / 10 * 8;
	}

public:
	ClientContext &context;
	//! Temporary memory state for managing this hash table's memory usage
	unique_ptr<TemporaryMemoryState> temporary_memory_state;
	atomic<idx_t> minimum_reservation;

	//! Whether we've called Finalize
	bool finalized;
	//! Whether we are doing an external aggregation
	atomic<bool> external;
	//! Threads that have called Sink
	atomic<idx_t> active_threads;
	//! Number of threads (from TaskScheduler)
	const idx_t number_of_threads;
	//! Memory limit (from BufferManager)
	const idx_t memory_limit;
	//! Block size (from BufferManager)
	const idx_t block_alloc_size;
	//! If any thread has called combine
	atomic<bool> any_combined;
	//! If any thread has called ht.Abandon() during Sink (meaning uncombined_data may have duplicates)
	atomic<bool> any_abandoned;

	//! The radix HT
	const RadixPartitionedHashTable &radix_ht;
	//! Config for partitioning
	RadixHTConfig config;

	//! Uncombined partitioned data that will be put into the AggregatePartitions
	unique_ptr<PartitionedTupleData> uncombined_data;
	//! Allocators used during the Sink/Finalize
	vector<shared_ptr<ArenaAllocator>> stored_allocators;
	idx_t stored_allocators_size;

	//! Partitions that are finalized during GetData
	vector<unique_ptr<AggregatePartition>> partitions;
	//! For keeping track of progress
	atomic<idx_t> finalize_done;

	//! Pin properties when scanning
	TupleDataPinProperties scan_pin_properties;
	//! Total count before combining
	idx_t count_before_combining;
	//! Maximum partition size if all unique
	idx_t max_partition_size;
};

RadixHTGlobalSinkState::RadixHTGlobalSinkState(ClientContext &context_p, const RadixPartitionedHashTable &radix_ht_p)
    : context(context_p), temporary_memory_state(TemporaryMemoryManager::Get(context).Register(context)),
      finalized(false), external(false), active_threads(0),
      number_of_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())),
      memory_limit(BufferManager::GetBufferManager(context).GetOperatorMemoryLimit()),
      block_alloc_size(BufferManager::GetBufferManager(context).GetBlockAllocSize()), any_combined(false),
      any_abandoned(false), radix_ht(radix_ht_p), config(*this), stored_allocators_size(0), finalize_done(0),
      scan_pin_properties(TupleDataPinProperties::DESTROY_AFTER_DONE), count_before_combining(0),
      max_partition_size(0) {
	// Compute minimum reservation
	auto tuples_per_block = block_alloc_size / radix_ht.GetLayout().GetRowWidth();
	idx_t ht_count =
	    LossyNumericCast<idx_t>(static_cast<double>(config.sink_capacity) / GroupedAggregateHashTable::LOAD_FACTOR);
	auto num_partitions = RadixPartitioning::NumberOfPartitions(config.GetMaximumSinkRadixBits());
	auto count_per_partition = ht_count / num_partitions;
	auto blocks_per_partition = (count_per_partition + tuples_per_block) / tuples_per_block;
	if (!radix_ht.GetLayout().AllConstant()) {
		blocks_per_partition += 1;
	}
	auto ht_size = num_partitions * blocks_per_partition * block_alloc_size + config.sink_capacity * sizeof(ht_entry_t);

	// This really is the minimum reservation that we can do
	auto num_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
	minimum_reservation = num_threads * ht_size;

	temporary_memory_state->SetMinimumReservation(minimum_reservation);
	temporary_memory_state->SetRemainingSizeAndUpdateReservation(context, minimum_reservation);
}

RadixHTGlobalSinkState::~RadixHTGlobalSinkState() {
	Destroy();
}

// LCOV_EXCL_START
void RadixHTGlobalSinkState::Destroy() {
	if (scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE || count_before_combining == 0 ||
	    partitions.empty()) {
		// Already destroyed / empty
		return;
	}

	TupleDataLayout layout = partitions[0]->data->GetLayout().Copy();
	if (!layout.HasDestructor()) {
		return; // No destructors, exit
	}

	// There are aggregates with destructors: Call the destructor for each of the aggregates
	const annotated_lock_guard<annotated_mutex> guard {lock};
	RowOperationsState row_state(*stored_allocators.back());
	for (auto &partition : partitions) {
		auto &data_collection = *partition->data;
		if (data_collection.Count() == 0) {
			continue;
		}
		TupleDataChunkIterator iterator(data_collection, TupleDataPinProperties::DESTROY_AFTER_DONE, false);
		auto &row_locations = iterator.GetChunkState().row_locations;
		do {
			RowOperations::DestroyStates(row_state, layout, row_locations);
		} while (iterator.Next());
		data_collection.Reset();
	}
}
// LCOV_EXCL_STOP

RadixHTConfig::RadixHTConfig(RadixHTGlobalSinkState &sink_p)
    : sink(sink_p), row_width(sink.radix_ht.GetLayout().GetRowWidth()), sink_capacity(SinkCapacity()),
      sink_radix_bits(InitialSinkRadixBits()), maximum_sink_radix_bits(MaximumSinkRadixBits()) {
}

void RadixHTConfig::Reset() {
	sink_radix_bits = InitialSinkRadixBits();
}

void RadixHTConfig::SetRadixBits(const idx_t &radix_bits_p) {
	const auto max_bits = MinValue(maximum_sink_radix_bits, ExternalRadixBits(true));
	SetRadixBitsInternal(MinValue(radix_bits_p, max_bits), false);
}

bool RadixHTConfig::SetRadixBitsToExternal() {
	SetRadixBitsInternal(ExternalRadixBits(true), true);
	return sink.external;
}

idx_t RadixHTConfig::GetRadixBits() const {
	return sink_radix_bits;
}

idx_t RadixHTConfig::GetMaximumSinkRadixBits() const {
	return maximum_sink_radix_bits;
}

void RadixHTConfig::SetRadixBitsInternal(const idx_t radix_bits_p, bool external) {
	if (sink_radix_bits > radix_bits_p || sink.any_combined) {
		return;
	}

	const annotated_lock_guard<annotated_mutex> guard {sink.lock};
	if (sink_radix_bits > radix_bits_p || sink.any_combined) {
		return;
	}

	if (external) {
		const auto partition_multiplier = RadixPartitioning::NumberOfPartitions(radix_bits_p) /
		                                  RadixPartitioning::NumberOfPartitions(sink_radix_bits);
		sink.minimum_reservation = sink.minimum_reservation * partition_multiplier;
		sink.external = true;
	}

	sink_radix_bits = radix_bits_p;
}

idx_t RadixHTConfig::InitialSinkRadixBits() const {
	return MinValue(RadixPartitioning::RadixBitsOfPowerOfTwo(NextPowerOfTwo(sink.number_of_threads)),
	                MAXIMUM_INITIAL_SINK_RADIX_BITS);
}

idx_t RadixHTConfig::ExternalRadixBits(const bool dynamic) const {
	// Going to many partitions is great for reducing memory usage during the GetData phase
	// However, we can't go to, e.g., 256 partitions when we have 8 threads and 200 MiB of memory
	// Because we'll have too many pages in memory to do the partitioning in the first place

	// Assume we can fill half of RAM with pages, and pessimistically assume 4 pages per partition
	const auto memory_limit = dynamic ? sink.temporary_memory_state->GetReservation() : sink.memory_limit / 2;
	const auto max_partitions = memory_limit / sink.number_of_threads / sink.block_alloc_size / 4;

	// Compute number of bits, rounded down, at least as much as initial bits
	const auto bits = MaxValue(RadixPartitioning::RadixBits(max_partitions) - 1, InitialSinkRadixBits());

	// Avoid returning 0 or underflowed bits
	if (max_partitions == 0 || bits == 0) {
		return 1;
	}

	// Capped by global maximum
	return MinValue(bits, MAXIMUM_FINAL_SINK_RADIX_BITS);
}

idx_t RadixHTConfig::MaximumSinkRadixBits() const {
	if (sink.number_of_threads <= GROW_STRATEGY_THREAD_THRESHOLD) {
		return InitialSinkRadixBits(); // Don't repartition unless we go external
	}
	// If rows are very wide we have to reduce the number of partitions, otherwise cache misses get out of hand
	idx_t bits = DConstants::INVALID_INDEX;
	if (row_width >= ROW_WIDTH_THRESHOLD_TWO) {
		bits = MAXIMUM_FINAL_SINK_RADIX_BITS - 2;
	} else if (row_width >= ROW_WIDTH_THRESHOLD_ONE) {
		bits = MAXIMUM_FINAL_SINK_RADIX_BITS - 1;
	} else {
		bits = MAXIMUM_FINAL_SINK_RADIX_BITS;
	}
	// Capped by external radix bits
	return MinValue(bits, ExternalRadixBits(false));
}

idx_t RadixHTConfig::SinkCapacity() const {
	if (sink.number_of_threads <= GROW_STRATEGY_THREAD_THRESHOLD) {
		// Grow strategy, start off a bit bigger
		return 262144;
	}
	// Start off tiny, we can adapt with DecideAdaptation later
	return 32768;
}

class RadixHTLocalSinkState : public LocalSinkState {
public:
	RadixHTLocalSinkState(ClientContext &context, const RadixPartitionedHashTable &radix_ht);
	void ResetForReuse(const RadixPartitionedHashTable &radix_ht, RadixHTGlobalSinkState &gstate);

public:
	//! Thread-local HT that is re-used after abandoning
	unique_ptr<GroupedAggregateHashTable> ht;
	//! Chunk with group columns
	DataChunk group_chunk;
	//! Scratch addresses for existing-group fast updates
	Vector existing_group_addresses;
	//! Scratch primitive aggregate payload metadata for direct fast updates
	vector<RadixPrimitiveAggregatePayload> primitive_payloads;

	//! After seeing this many tuples, we decide whether to adapt our strategy
	//! This also serves as the maximum HT sink capacity
	static constexpr idx_t ADAPTIVITY_THRESHOLD = 1048576;
	//! Whether we have decided to adapt our strategy
	bool adapted;
	//! Whether this local state has already registered itself as active for the current iteration
	bool registered;
	//! Sink capacity for this thread
	idx_t local_sink_capacity;

	//! Data that is abandoned ends up here (only if we're doing external aggregation)
	unique_ptr<PartitionedTupleData> abandoned_data;
};

RadixHTLocalSinkState::RadixHTLocalSinkState(ClientContext &, const RadixPartitionedHashTable &radix_ht)
    : existing_group_addresses(LogicalType::POINTER), adapted(false), registered(false),
      local_sink_capacity(DConstants::INVALID_INDEX) {
	// If there are no groups we create a fake group so everything has the same group
	group_chunk.InitializeEmpty(radix_ht.group_types);
	if (radix_ht.grouping_set.empty()) {
		group_chunk.data[0].Reference(Value::TINYINT(42), count_t(STANDARD_VECTOR_SIZE));
	}
}

void RadixHTLocalSinkState::ResetForReuse(const RadixPartitionedHashTable &radix_ht, RadixHTGlobalSinkState &gstate) {
	group_chunk.Reset();
	if (radix_ht.grouping_set.empty()) {
		group_chunk.data[0].Reference(Value::TINYINT(42), count_t(STANDARD_VECTOR_SIZE));
	}
	registered = false;
	abandoned_data.reset();
	if (!ht) {
		adapted = false;
		local_sink_capacity = DConstants::INVALID_INDEX;
		return;
	}

	local_sink_capacity = MaxValue(gstate.config.sink_capacity, ht->Capacity());
	ht->ResetForNewIteration(local_sink_capacity, gstate.config.GetRadixBits());
	if (gstate.number_of_threads > RadixHTConfig::GROW_STRATEGY_THREAD_THRESHOLD) {
		ht->EnableHLL(true);
		adapted = false;
	} else {
		adapted = true;
	}
}

unique_ptr<GlobalSinkState> RadixPartitionedHashTable::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RadixHTGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> RadixPartitionedHashTable::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<RadixHTLocalSinkState>(context.client, *this);
}

void RadixPartitionedHashTable::ResetGlobalSinkState(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	gstate.Destroy();
	gstate.temporary_memory_state->SetMinimumReservation(gstate.minimum_reservation);
	gstate.temporary_memory_state->SetRemainingSizeAndUpdateReservation(context, gstate.minimum_reservation);
	gstate.finalized = false;
	gstate.external = false;
	gstate.active_threads = 0;
	gstate.any_combined = false;
	gstate.any_abandoned = false;
	gstate.config.Reset();
	gstate.uncombined_data.reset();
	gstate.stored_allocators.clear();
	gstate.stored_allocators_size = 0;
	gstate.partitions.clear();
	gstate.finalize_done = 0;
	gstate.scan_pin_properties = TupleDataPinProperties::DESTROY_AFTER_DONE;
	gstate.count_before_combining = 0;
	gstate.max_partition_size = 0;
}

void RadixPartitionedHashTable::ResetLocalSinkState(ExecutionContext &context, GlobalSinkState &gstate_p,
                                                    LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	auto &lstate = lstate_p.Cast<RadixHTLocalSinkState>();
	lstate.ResetForReuse(*this, gstate);
}

void RadixPartitionedHashTable::PopulateGroupChunk(DataChunk &group_chunk, DataChunk &input_chunk) const {
	idx_t chunk_index = 0;
	// Populate the group_chunk
	for (auto &group_idx : grouping_set) {
		// Retrieve the expression containing the index in the input chunk
		auto &group = op.groups[group_idx];
		D_ASSERT(group->GetExpressionType() == ExpressionType::BOUND_REF);
		auto &bound_ref_expr = group->Cast<BoundReferenceExpression>();
		// Reference from input_chunk[group.index] -> group_chunk[chunk_index]
		group_chunk.data[chunk_index++].Reference(input_chunk.data[bound_ref_expr.Index()]);
	}
	group_chunk.SetChildCardinality(input_chunk.size());
	// the fake group for empty grouping_set was created with v_size=STANDARD_VECTOR_SIZE - resize to match
	if (grouping_set.empty()) {
		FlatVector::SetSize(group_chunk.data[0], count_t(input_chunk.size()));
	}
	group_chunk.Verify();
}

void DecideAdaptation(RadixHTGlobalSinkState &gstate, RadixHTLocalSinkState &lstate) {
	//! If the number of unique values is greater than this percentage, we skip lookups altogether
	static constexpr double SKIP_LOOKUP_UNIQUE_PERCENTAGE_THRESHOLD = 0.95;
	//! If the deduplication rate could be increased by this number, we increase our sink capacity
	static constexpr double CAPACITY_INCREASE_DEDUPLICATION_RATE_THRESHOLD = 2.0;

	if (gstate.external) {
		return; // Shouldn't adapt after this flag has been set
	}

	auto &ht = *lstate.ht;
	const auto sink_count = ht.GetSinkCount();
	D_ASSERT(sink_count >= RadixHTLocalSinkState::ADAPTIVITY_THRESHOLD);

	// Deduplicated count (affected by HT size)
	const auto deduplicated_count = ht.GetMaterializedCount();
	// Estimated unique count (unaffected by HT size)
	const auto hll_count = MinValue(ht.GetHLLUpperBound(), deduplicated_count);

	// Compute actual deduplicated percentage and potential deduplicated count (estimated by hll)
	const auto deduplicated_percentage = static_cast<double>(deduplicated_count) / static_cast<double>(sink_count);
	const auto hll_percentage = static_cast<double>(hll_count) / static_cast<double>(sink_count);
	if (hll_percentage > SKIP_LOOKUP_UNIQUE_PERCENTAGE_THRESHOLD) {
		// Almost everything is unique, skip lookups, just append, defer deduplication to GetData phase
		ht.SkipLookups();
		return;
	}

	const auto potential_increase_rate = deduplicated_percentage / hll_percentage;
	if (potential_increase_rate > CAPACITY_INCREASE_DEDUPLICATION_RATE_THRESHOLD) {
		// We could be deduplicating a lot better, increase HT capacity
		D_ASSERT(IsPowerOfTwo(RadixHTLocalSinkState::ADAPTIVITY_THRESHOLD));
		const auto new_capacity = MinValue(GroupedAggregateHashTable::GetCapacityForCount(hll_count),
		                                   RadixHTLocalSinkState::ADAPTIVITY_THRESHOLD);
		lstate.local_sink_capacity = MaxValue(gstate.config.sink_capacity, new_capacity);
		ht.Abandon();
		ht.Resize(lstate.local_sink_capacity);
	}
}

void MaybeRepartition(ClientContext &context, RadixHTGlobalSinkState &gstate, RadixHTLocalSinkState &lstate,
                      const bool combine) {
	auto &config = gstate.config;
	auto &ht = *lstate.ht;

	// Check if we're approaching the memory limit
	auto &temporary_memory_state = *gstate.temporary_memory_state;
	const auto aggregate_allocator_size = ht.GetAggregateAllocator()->AllocationSize();
	const auto total_size =
	    aggregate_allocator_size + ht.GetPartitionedData().SizeInBytes() + ht.Capacity() * sizeof(ht_entry_t);
	if (total_size > gstate.GetThreadLimit()) {
		// We're over the thread memory limit
		if (!gstate.external) {
			// We haven't yet triggered out-of-core behavior, but maybe we don't have to, grab the lock and check again
			const annotated_lock_guard<annotated_mutex> guard {gstate.lock};
			if (total_size > gstate.GetThreadLimit()) {
				// Out-of-core would be triggered below, update minimum reservation and try to increase the reservation
				temporary_memory_state.SetMinimumReservation(aggregate_allocator_size * gstate.number_of_threads +
				                                             gstate.minimum_reservation);
				auto remaining_size =
				    MaxValue<idx_t>(gstate.number_of_threads * total_size, temporary_memory_state.GetRemainingSize());
				temporary_memory_state.SetRemainingSizeAndUpdateReservation(context, 2 * remaining_size);
			}
		}
	}

	if (total_size > gstate.GetThreadLimit()) {
		if (gstate.config.SetRadixBitsToExternal()) {
			// We're approaching the memory limit, unpin the data
			if (!lstate.abandoned_data) {
				lstate.abandoned_data = make_uniq<RadixPartitionedTupleData>(
				    BufferManager::GetBufferManager(context), gstate.radix_ht.GetLayoutPtr(), MemoryTag::HASH_TABLE,
				    config.GetRadixBits(), gstate.radix_ht.GetLayout().ColumnCount() - 1);
			}
			ht.SetRadixBits(gstate.config.GetRadixBits());
			ht.AcquirePartitionedData()->Repartition(context, *lstate.abandoned_data);
		}
	}

	// We can go external when there are few threads, but we shouldn't repartition here
	if (!combine && gstate.number_of_threads <= RadixHTConfig::GROW_STRATEGY_THREAD_THRESHOLD) {
		return;
	}

	const auto partition_count = ht.GetPartitionedData().PartitionCount();
	const auto current_radix_bits = RadixPartitioning::RadixBitsOfPowerOfTwo(partition_count);
	D_ASSERT(current_radix_bits <= config.GetRadixBits());

	const auto row_size_per_partition =
	    ht.GetMaterializedCount() * ht.GetPartitionedData().GetLayout().GetRowWidth() / partition_count;
	if (row_size_per_partition >
	    LossyNumericCast<idx_t>(config.BLOCK_FILL_FACTOR * static_cast<double>(gstate.block_alloc_size))) {
		// We crossed our block filling threshold, try to increment radix bits
		config.SetRadixBits(current_radix_bits + config.REPARTITION_RADIX_BITS);
	}

	const auto global_radix_bits = config.GetRadixBits();
	if (current_radix_bits == global_radix_bits) {
		return; // We're already on the right number of radix bits
	}

	// We're out-of-sync with the global radix bits, repartition
	ht.SetRadixBits(global_radix_bits);
	ht.Repartition();
}

static GroupedAggregateHashTable &PrepareRadixHTSinkState(ExecutionContext &context,
                                                          const RadixPartitionedHashTable &radix_ht,
                                                          OperatorSinkInput &input) {
	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	if (!lstate.ht) {
		lstate.local_sink_capacity = gstate.config.sink_capacity;
		lstate.ht = radix_ht.CreateHT(context.client, lstate.local_sink_capacity, gstate.config.GetRadixBits());
		if (gstate.number_of_threads > RadixHTConfig::GROW_STRATEGY_THREAD_THRESHOLD) {
			// Not using grow strategy, so we enable the HLL to potentially adapt later
			lstate.ht->EnableHLL(true);
		} else {
			// Using grow strategy, so won't ever adapt
			lstate.adapted = true;
		}
	}
	if (!lstate.registered) {
		gstate.active_threads++;
		lstate.registered = true;
	}
	return *lstate.ht;
}

static void FinishRadixHTSinkState(ClientContext &context, RadixHTGlobalSinkState &gstate,
                                   RadixHTLocalSinkState &lstate) {
	auto &ht = *lstate.ht;

	// Decide whether we should adapt our strategy to the data
	if (!lstate.adapted && lstate.ht->GetSinkCount() >= RadixHTLocalSinkState::ADAPTIVITY_THRESHOLD) {
		DecideAdaptation(gstate, lstate);
		ht.EnableHLL(false); // Can be disabled now (costs 5-10% performance in worst case, single column distinct)
		lstate.adapted = true;
	}

	if (ht.Count() + STANDARD_VECTOR_SIZE < GroupedAggregateHashTable::ResizeThreshold(lstate.local_sink_capacity)) {
		return; // We can fit another chunk
	}

	if (gstate.number_of_threads > RadixHTConfig::GROW_STRATEGY_THREAD_THRESHOLD || gstate.external) {
		// 'Reset' the HT without taking its data, we can just keep appending to the same collection
		// This only works because we never resize the HT
		// We don't do this when running with 1 or 2 threads, it only makes sense when there's many threads
		ht.Abandon();
		gstate.any_abandoned = true;
	}

	// Check if we need to repartition
	const auto radix_bits_before = ht.GetRadixBits();
	MaybeRepartition(context, gstate, lstate, false);
	const auto repartitioned = radix_bits_before != ht.GetRadixBits();

	if (repartitioned && ht.Count() != 0) {
		// We repartitioned, but we didn't clear the pointer table / reset the count because we're on 1 or 2 threads
		ht.Abandon();
		if (gstate.external) {
			ht.Resize(lstate.local_sink_capacity);
		}
	}

}

void RadixPartitionedHashTable::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
                                     DataChunk &payload_input, const unsafe_vector<idx_t> &filter) const {
	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto &ht = PrepareRadixHTSinkState(context, *this, input);

	auto &group_chunk = lstate.group_chunk;
	PopulateGroupChunk(group_chunk, chunk);

	ht.AddChunk(group_chunk, payload_input, filter);
	FinishRadixHTSinkState(context.client, gstate, lstate);
}

void RadixPartitionedHashTable::ResolveStateAddresses(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSinkInput &input, Vector &addresses_out,
                                                      optional_ptr<ExecutionOperatorStageRecorder> recorder) const {
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto prepare_start = RadixTraceStart(recorder);
	auto &ht = PrepareRadixHTSinkState(context, *this, input);
	RecordRadixTraceStage(recorder, "prepare_sink_state", prepare_start);

	auto &group_chunk = lstate.group_chunk;
	auto populate_start = RadixTraceStart(recorder);
	PopulateGroupChunk(group_chunk, chunk);
	RecordRadixTraceStage(recorder, "populate_group_chunk", populate_start);

	ht.FindOrCreateGroupAddresses(group_chunk, addresses_out, recorder);
}

bool RadixPartitionedHashTable::TryUpdateNewPrimitiveGroups(
    ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, bool finish) const {
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto &payloads = lstate.primitive_payloads;
	if (!RadixValidatePrimitiveAggregateUpdate(sink_info, lanes, chunk, payloads)) {
		return false;
	}

	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto prepare_start = RadixTraceStart(recorder);
	auto &ht = PrepareRadixHTSinkState(context, *this, input);
	RecordRadixTraceStage(recorder, "direct_new.prepare_sink_state", prepare_start);

	auto &group_chunk = lstate.group_chunk;
	auto populate_start = RadixTraceStart(recorder);
	PopulateGroupChunk(group_chunk, chunk);
	RecordRadixTraceStage(recorder, "direct_new.populate_group_chunk", populate_start);

	RadixPrimitiveGroupUpdateState update_state;
	update_state.lanes = &lanes;
	update_state.payloads = &payloads;
	auto append_start = RadixTraceStart(recorder);
	if (!ht.TryFindOrCreateGroupsUpdateFast(group_chunk, RadixUpdatePrimitiveGroup, &update_state, recorder)) {
		RecordRadixTraceStage(recorder, "direct_new.append_miss", append_start);
		return false;
	}
	RecordRadixTraceStage(recorder, "direct_new.append", append_start);
	auto update_marker_start = RadixTraceStart(recorder);
	RecordRadixTraceStage(recorder, "direct_new.primitive_update", update_marker_start);

	if (finish) {
		auto finish_start = RadixTraceStart(recorder);
		FinishRadixHTSinkState(context.client, gstate, lstate);
		RecordRadixTraceStage(recorder, "direct_new.finish_sink_state", finish_start);
	}
	return true;
}

bool RadixPartitionedHashTable::TryUpdateNewPrimitiveGroupsWithPayloadInput(
    ExecutionContext &context, DataChunk &groups, DataChunk &payload_input,
    const vector<idx_t> &payload_source_indices, OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, bool finish, optional_ptr<Vector> precomputed_hashes) const {
	if (groups.size() != payload_input.size()) {
		return false;
	}
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto &payloads = lstate.primitive_payloads;
	if (!RadixValidatePrimitiveAggregateUpdateWithPayloadInput(sink_info, lanes, payload_input,
	                                                           payload_source_indices, payloads)) {
		return false;
	}

	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto prepare_start = RadixTraceStart(recorder);
	auto &ht = PrepareRadixHTSinkState(context, *this, input);
	RecordRadixTraceStage(recorder, "direct_new_split_payload.prepare_sink_state", prepare_start);

	RadixPrimitiveGroupUpdateState update_state;
	update_state.lanes = &lanes;
	update_state.payloads = &payloads;

	auto existing_start = RadixTraceStart(recorder);
	if (ht.TryFindExistingGroupsSelectedStateUpdateFast(groups, RadixUpdatePrimitiveGroupSelected, &update_state,
	                                                    recorder, precomputed_hashes)) {
		RecordRadixTraceStage(recorder, "direct_existing_split_payload.update", existing_start);
		auto update_marker_start = RadixTraceStart(recorder);
		RecordRadixTraceStage(recorder, "direct_existing_split_payload.primitive_update", update_marker_start);
		if (finish) {
			auto finish_start = RadixTraceStart(recorder);
			FinishRadixHTSinkState(context.client, gstate, lstate);
			RecordRadixTraceStage(recorder, "direct_existing_split_payload.finish_sink_state", finish_start);
		}
		return true;
	}
	RecordRadixTraceStage(recorder, "direct_existing_split_payload.miss", existing_start);

	auto append_start = RadixTraceStart(recorder);
	if (!ht.TryFindOrCreateGroupsUpdateFast(groups, RadixUpdatePrimitiveGroup, &update_state, recorder,
	                                        precomputed_hashes)) {
		RecordRadixTraceStage(recorder, "direct_new_split_payload.append_miss", append_start);
		return false;
	}
	RecordRadixTraceStage(recorder, "direct_new_split_payload.append", append_start);
	auto update_marker_start = RadixTraceStart(recorder);
	RecordRadixTraceStage(recorder, "direct_new_split_payload.primitive_update", update_marker_start);

	if (finish) {
		auto finish_start = RadixTraceStart(recorder);
		FinishRadixHTSinkState(context.client, gstate, lstate);
		RecordRadixTraceStage(recorder, "direct_new_split_payload.finish_sink_state", finish_start);
	}
	return true;
}

bool RadixPartitionedHashTable::TryAppendNewPrimitiveGroups(
    ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, bool finish) const {
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto &payloads = lstate.primitive_payloads;
	if (!RadixValidatePrimitiveAggregateUpdate(sink_info, lanes, chunk, payloads)) {
		return false;
	}

	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto prepare_start = RadixTraceStart(recorder);
	auto &ht = PrepareRadixHTSinkState(context, *this, input);
	RecordRadixTraceStage(recorder, "direct_append_new.prepare_sink_state", prepare_start);

	auto &group_chunk = lstate.group_chunk;
	auto populate_start = RadixTraceStart(recorder);
	PopulateGroupChunk(group_chunk, chunk);
	RecordRadixTraceStage(recorder, "direct_append_new.populate_group_chunk", populate_start);

	RadixPrimitiveGroupUpdateState update_state;
	update_state.lanes = &lanes;
	update_state.payloads = &payloads;
	auto append_start = RadixTraceStart(recorder);
	if (!ht.TryAppendNewGroupsUpdateFast(group_chunk, RadixUpdatePrimitiveGroup, &update_state, recorder)) {
		RecordRadixTraceStage(recorder, "direct_append_new.append_miss", append_start);
		return false;
	}
	RecordRadixTraceStage(recorder, "direct_append_new.append", append_start);
	auto update_marker_start = RadixTraceStart(recorder);
	RecordRadixTraceStage(recorder, "direct_append_new.primitive_update", update_marker_start);

	if (finish) {
		auto finish_start = RadixTraceStart(recorder);
		FinishRadixHTSinkState(context.client, gstate, lstate);
		RecordRadixTraceStage(recorder, "direct_append_new.finish_sink_state", finish_start);
	}
	return true;
}

bool RadixPartitionedHashTable::TryUpdateExistingGroupsWithStateAddresses(
    ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
    ExecutionGroupedAggregateStateAddressUpdateFunction update_function, void *update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, bool finish) const {
	if (!RadixValidateStateAddressCallbackUpdate(sink_info, update_function)) {
		return false;
	}

	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto prepare_start = RadixTraceStart(recorder);
	auto &ht = PrepareRadixHTSinkState(context, *this, input);
	RecordRadixTraceStage(recorder, "direct_existing_state_address.prepare_sink_state", prepare_start);

	auto &group_chunk = lstate.group_chunk;
	auto populate_start = RadixTraceStart(recorder);
	PopulateGroupChunk(group_chunk, chunk);
	RecordRadixTraceStage(recorder, "direct_existing_state_address.populate_group_chunk", populate_start);

	auto lookup_start = RadixTraceStart(recorder);
	if (!ht.TryFindExistingGroupAddresses(group_chunk, lstate.existing_group_addresses, recorder)) {
		RecordRadixTraceStage(recorder, "direct_existing_state_address.lookup_miss", lookup_start);
		return false;
	}
	RecordRadixTraceStage(recorder, "direct_existing_state_address.lookup", lookup_start);

	auto update_start = RadixTraceStart(recorder);
	update_function(RadixStateAddressSpan(lstate.existing_group_addresses), nullptr, chunk.size(), update_state);
	RecordRadixTraceStage(recorder, "direct_existing_state_address.update", update_start);

	if (finish) {
		auto finish_start = RadixTraceStart(recorder);
		FinishRadixHTSinkState(context.client, gstate, lstate);
		RecordRadixTraceStage(recorder, "direct_existing_state_address.finish_sink_state", finish_start);
	}
	return true;
}

bool RadixPartitionedHashTable::TryUpdateExistingGroupsWithSelectedStateAddresses(
    ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, bool finish) const {
	if (!RadixValidateStateAddressCallbackUpdate(sink_info, update_function)) {
		return false;
	}

	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto prepare_start = RadixTraceStart(recorder);
	auto &ht = PrepareRadixHTSinkState(context, *this, input);
	RecordRadixTraceStage(recorder, "direct_existing_selected_state.prepare_sink_state", prepare_start);

	auto &group_chunk = lstate.group_chunk;
	auto populate_start = RadixTraceStart(recorder);
	PopulateGroupChunk(group_chunk, chunk);
	RecordRadixTraceStage(recorder, "direct_existing_selected_state.populate_group_chunk", populate_start);

	auto lookup_start = RadixTraceStart(recorder);
	if (!ht.TryFindExistingGroupsSelectedStateUpdateFast(group_chunk, update_function, update_state, recorder)) {
		RecordRadixTraceStage(recorder, "direct_existing_selected_state.lookup_miss", lookup_start);
		return false;
	}
	RecordRadixTraceStage(recorder, "direct_existing_selected_state.lookup_update", lookup_start);

	if (finish) {
		auto finish_start = RadixTraceStart(recorder);
		FinishRadixHTSinkState(context.client, gstate, lstate);
		RecordRadixTraceStage(recorder, "direct_existing_selected_state.finish_sink_state", finish_start);
	}
	return true;
}

bool RadixPartitionedHashTable::TryUpdateNewGroupsWithStateAddresses(
    ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
    ExecutionGroupedAggregateStateAddressUpdateFunction update_function, void *update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, bool finish) const {
	if (!RadixValidateStateAddressCallbackUpdate(sink_info, update_function)) {
		return false;
	}

	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto prepare_start = RadixTraceStart(recorder);
	auto &ht = PrepareRadixHTSinkState(context, *this, input);
	RecordRadixTraceStage(recorder, "direct_new_state_address.prepare_sink_state", prepare_start);

	auto &group_chunk = lstate.group_chunk;
	auto populate_start = RadixTraceStart(recorder);
	PopulateGroupChunk(group_chunk, chunk);
	RecordRadixTraceStage(recorder, "direct_new_state_address.populate_group_chunk", populate_start);

	auto append_start = RadixTraceStart(recorder);
	if (!ht.TryFindOrCreateGroupAddressesFast(group_chunk, lstate.existing_group_addresses, recorder)) {
		RecordRadixTraceStage(recorder, "direct_new_state_address.append_miss", append_start);
		return false;
	}
	RecordRadixTraceStage(recorder, "direct_new_state_address.append", append_start);

	auto update_start = RadixTraceStart(recorder);
	update_function(RadixStateAddressSpan(lstate.existing_group_addresses), nullptr, chunk.size(), update_state);
	RecordRadixTraceStage(recorder, "direct_new_state_address.update", update_start);

	if (finish) {
		auto finish_start = RadixTraceStart(recorder);
		FinishRadixHTSinkState(context.client, gstate, lstate);
		RecordRadixTraceStage(recorder, "direct_new_state_address.finish_sink_state", finish_start);
	}
	return true;
}

bool RadixPartitionedHashTable::TryUpdateNewGroupsWithSelectedStateAddresses(
    ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, bool finish, optional_ptr<Vector> precomputed_hashes) const {
	if (!RadixValidateStateAddressCallbackUpdate(sink_info, update_function)) {
		return false;
	}

	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto prepare_start = RadixTraceStart(recorder);
	auto &ht = PrepareRadixHTSinkState(context, *this, input);
	RecordRadixTraceStage(recorder, "direct_new_selected_state.prepare_sink_state", prepare_start);

	auto &group_chunk = lstate.group_chunk;
	auto populate_start = RadixTraceStart(recorder);
	PopulateGroupChunk(group_chunk, chunk);
	RecordRadixTraceStage(recorder, "direct_new_selected_state.populate_group_chunk", populate_start);

	auto append_start = RadixTraceStart(recorder);
	if (!ht.TryFindOrCreateGroupsSelectedStateUpdateFast(group_chunk, update_function, update_state, recorder,
	                                                     precomputed_hashes)) {
		RecordRadixTraceStage(recorder, "direct_new_selected_state.append_miss", append_start);
		return false;
	}
	RecordRadixTraceStage(recorder, "direct_new_selected_state.append_update", append_start);

	if (finish) {
		auto finish_start = RadixTraceStart(recorder);
		FinishRadixHTSinkState(context.client, gstate, lstate);
		RecordRadixTraceStage(recorder, "direct_new_selected_state.finish_sink_state", finish_start);
	}
	return true;
}

bool RadixPartitionedHashTable::TryUpdateNewGroupsWithRowPointerKeys(
    ExecutionContext &context, DataChunk &payload_input, Vector &row_pointers, idx_t count, OperatorSinkInput &input,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const ExecutionRegionSinkInfo &sink_info,
    ExecutionGroupedAggregateStateSelectedAddressUpdateFunction update_function, void *update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, bool finish) const {
	if (!RadixValidateStateAddressCallbackUpdate(sink_info, update_function)) {
		return false;
	}
	if (payload_input.size() != count) {
		return false;
	}

	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto prepare_start = RadixTraceStart(recorder);
	auto &ht = PrepareRadixHTSinkState(context, *this, input);
	RecordRadixTraceStage(recorder, "direct_row_pointer_grouped_lookup.prepare_sink_state", prepare_start);

	auto lookup_start = RadixTraceStart(recorder);
	if (!ht.TryFindOrCreateGroupsRowPointerSelectedStateUpdateFast(payload_input, row_pointers, count, group_sources,
	                                                               update_function, update_state, recorder)) {
		RecordRadixTraceStage(recorder, "direct_row_pointer_grouped_lookup.lookup_miss", lookup_start);
		return false;
	}
	RecordRadixTraceStage(recorder, "direct_row_pointer_grouped_lookup.lookup_update", lookup_start);

	if (finish) {
		auto finish_start = RadixTraceStart(recorder);
		FinishRadixHTSinkState(context.client, gstate, lstate);
		RecordRadixTraceStage(recorder, "direct_row_pointer_grouped_lookup.finish_sink_state", finish_start);
	}
	return true;
}

bool RadixPartitionedHashTable::TryAppendNewGroupsWithStateAddresses(
    ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
    ExecutionGroupedAggregateStateAddressUpdateFunction update_function, void *update_state,
    optional_ptr<ExecutionOperatorStageRecorder> recorder, bool finish) const {
	if (!RadixValidateStateAddressCallbackUpdate(sink_info, update_function)) {
		return false;
	}

	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto prepare_start = RadixTraceStart(recorder);
	auto &ht = PrepareRadixHTSinkState(context, *this, input);
	RecordRadixTraceStage(recorder, "direct_append_new_state_address.prepare_sink_state", prepare_start);

	auto &group_chunk = lstate.group_chunk;
	auto populate_start = RadixTraceStart(recorder);
	PopulateGroupChunk(group_chunk, chunk);
	RecordRadixTraceStage(recorder, "direct_append_new_state_address.populate_group_chunk", populate_start);

	auto append_start = RadixTraceStart(recorder);
	if (!ht.TryAppendNewGroupsWithStateAddressesFast(group_chunk, update_function, update_state, recorder)) {
		RecordRadixTraceStage(recorder, "direct_append_new_state_address.append_miss", append_start);
		return false;
	}
	RecordRadixTraceStage(recorder, "direct_append_new_state_address.append", append_start);

	if (finish) {
		auto finish_start = RadixTraceStart(recorder);
		FinishRadixHTSinkState(context.client, gstate, lstate);
		RecordRadixTraceStage(recorder, "direct_append_new_state_address.finish_sink_state", finish_start);
	}
	return true;
}

bool RadixPartitionedHashTable::TryResolveNewGroupAddresses(
    ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input, const ExecutionRegionSinkInfo &sink_info,
    Vector &addresses_out, optional_ptr<ExecutionOperatorStageRecorder> recorder, bool finish) const {
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    sink_info.aggregate_contract.native_grouped_state_contract.status !=
	        ExecutionRegionStateContractStatus::READY) {
		return false;
	}

	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto prepare_start = RadixTraceStart(recorder);
	auto &ht = PrepareRadixHTSinkState(context, *this, input);
	RecordRadixTraceStage(recorder, "direct_new_address.prepare_sink_state", prepare_start);

	auto &group_chunk = lstate.group_chunk;
	auto populate_start = RadixTraceStart(recorder);
	PopulateGroupChunk(group_chunk, chunk);
	RecordRadixTraceStage(recorder, "direct_new_address.populate_group_chunk", populate_start);

	auto append_start = RadixTraceStart(recorder);
	if (!ht.TryFindOrCreateGroupAddressesFast(group_chunk, addresses_out, recorder)) {
		RecordRadixTraceStage(recorder, "direct_new_address.append_miss", append_start);
		return false;
	}
	RecordRadixTraceStage(recorder, "direct_new_address.append", append_start);

	if (finish) {
		auto finish_start = RadixTraceStart(recorder);
		FinishRadixHTSinkState(context.client, gstate, lstate);
		RecordRadixTraceStage(recorder, "direct_new_address.finish_sink_state", finish_start);
	}
	return true;
}

bool RadixPartitionedHashTable::GetExecutionHashAggregateLookupLayout(
    ExecutionHashAggregateLookupLayout &layout) const {
	if (!layout_ptr) {
		layout = ExecutionHashAggregateLookupLayout();
		layout.blocker = "hash-aggregate-layout-missing";
		return false;
	}
	return ExecutionBuildHashAggregateLookupLayout(*layout_ptr, layout);
}

void RadixPartitionedHashTable::FinishStateUpdates(ExecutionContext &context, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	FinishRadixHTSinkState(context.client, gstate, lstate);
}

void RadixPartitionedHashTable::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                        LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	auto &lstate = lstate_p.Cast<RadixHTLocalSinkState>();
	if (!lstate.ht) {
		return;
	}

	// Set any_combined, then check one last time whether we need to repartition
	gstate.any_combined = true;
	MaybeRepartition(context.client, gstate, lstate, true);

	auto &ht = *lstate.ht;
	auto lstate_data = ht.AcquirePartitionedData();
	if (lstate.abandoned_data) {
		D_ASSERT(gstate.external);
		D_ASSERT(lstate.abandoned_data->PartitionCount() == lstate.ht->GetPartitionedData().PartitionCount());
		D_ASSERT(lstate.abandoned_data->PartitionCount() ==
		         RadixPartitioning::NumberOfPartitions(gstate.config.GetRadixBits()));
		lstate.abandoned_data->Combine(*lstate_data);
	} else {
		lstate.abandoned_data = std::move(lstate_data);
	}

	auto aggregate_allocator = ht.GetAggregateAllocator();

	const annotated_lock_guard<annotated_mutex> guard {gstate.lock};
	D_ASSERT(!gstate.finalized);
	if (gstate.uncombined_data) {
		gstate.uncombined_data->Combine(*lstate.abandoned_data);
	} else {
		gstate.uncombined_data = std::move(lstate.abandoned_data);
	}
	gstate.stored_allocators.emplace_back(std::move(aggregate_allocator));
	gstate.stored_allocators_size += gstate.stored_allocators.back()->AllocationSize();
}

void RadixPartitionedHashTable::Finalize(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	const annotated_lock_guard<annotated_mutex> guard {gstate.lock};
	D_ASSERT(!gstate.finalized);

	if (gstate.uncombined_data) {
		auto &uncombined_data = *gstate.uncombined_data;
		gstate.count_before_combining = uncombined_data.Count();

		// If true there is no need to combine, it was all done by a single thread in a single HT.
		// This is the case when only one thread contributed data and the HT never overflowed its
		// capacity (which would have caused Abandon() to be called, creating duplicates).
		const auto single_ht = !gstate.external && gstate.active_threads == 1 && !gstate.any_abandoned;

		auto &uncombined_partition_data = uncombined_data.GetPartitions();
		const auto n_partitions = uncombined_partition_data.size();
		gstate.partitions.reserve(n_partitions);
		for (idx_t i = 0; i < n_partitions; i++) {
			auto &partition = uncombined_partition_data[i];
			auto partition_size =
			    partition->SizeInBytes() +
			    GroupedAggregateHashTable::GetCapacityForCount(partition->Count()) * sizeof(ht_entry_t);
			gstate.max_partition_size = MaxValue(gstate.max_partition_size, partition_size);

			gstate.partitions.emplace_back(make_uniq<AggregatePartition>(std::move(partition)));
			if (single_ht) {
				gstate.finalize_done++;
				gstate.partitions.back()->progress = 1;
				gstate.partitions.back()->state = AggregatePartitionState::READY_TO_SCAN;
			}
		}
	} else {
		gstate.count_before_combining = 0;
	}

	// Minimum of combining one partition at a time
	gstate.temporary_memory_state->SetMinimumReservation(gstate.stored_allocators_size + gstate.max_partition_size);
	// Set size to 0 until the scan actually starts
	gstate.temporary_memory_state->SetZero();
	gstate.finalized = true;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
idx_t RadixPartitionedHashTable::MaxThreads(GlobalSinkState &sink_p) const {
	auto &sink = sink_p.Cast<RadixHTGlobalSinkState>();
	if (sink.partitions.empty()) {
		return 0;
	}

	const auto max_threads = MinValue<idx_t>(
	    NumericCast<idx_t>(TaskScheduler::GetScheduler(sink.context).NumberOfThreads()), sink.partitions.size());
	sink.temporary_memory_state->SetRemainingSizeAndUpdateReservation(
	    sink.context, sink.stored_allocators_size + max_threads * sink.max_partition_size);

	// we cannot spill aggregate state memory
	const auto usable_memory = sink.temporary_memory_state->GetReservation() > sink.stored_allocators_size
	                               ? sink.temporary_memory_state->GetReservation() - sink.stored_allocators_size
	                               : 0;
	// This many partitions will fit given our reservation (at least 1))
	const auto partitions_fit = MaxValue<idx_t>(usable_memory / sink.max_partition_size, 1);

	// Minimum of the two
	return MinValue<idx_t>(partitions_fit, max_threads);
}

void RadixPartitionedHashTable::SetMultiScan(GlobalSinkState &sink_p) {
	auto &sink = sink_p.Cast<RadixHTGlobalSinkState>();
	sink.scan_pin_properties = TupleDataPinProperties::UNPIN_AFTER_DONE;
}

enum class RadixHTSourceTaskType : uint8_t { NO_TASK, FINALIZE, SCAN };

class RadixHTLocalSourceState;

class RadixHTGlobalSourceState : public GlobalSourceState {
public:
	RadixHTGlobalSourceState(ClientContext &context, const RadixPartitionedHashTable &radix_ht);

	//! Assigns a task to a local source state
	SourceResultType AssignTask(RadixHTGlobalSinkState &sink, RadixHTLocalSourceState &lstate,
	                            InterruptState &interrupt_state);

public:
	//! The client context
	ClientContext &context;
	//! For synchronizing the source phase
	atomic<bool> finished;

	//! Column ids for scanning
	vector<column_t> column_ids;

	//! For synchronizing tasks
	atomic<idx_t> task_idx;
	atomic<idx_t> task_done;
};

enum class RadixHTScanStatus : uint8_t { INIT, IN_PROGRESS, DONE };

class RadixHTLocalSourceState : public LocalSourceState {
public:
	explicit RadixHTLocalSourceState(ExecutionContext &context, const RadixPartitionedHashTable &radix_ht);
	void ResetForReuse();

public:
	//! Do the work this thread has been assigned
	void ExecuteTask(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate, DataChunk &chunk);
	//! Whether this thread has finished the work it has been assigned
	bool TaskFinished();

private:
	//! Execute the finalize or scan task
	void Finalize(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate);
	void Scan(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate, DataChunk &chunk);

public:
	//! Current task and index
	RadixHTSourceTaskType task;
	idx_t task_idx;

	//! Thread-local HT that is re-used to Finalize
	unique_ptr<GroupedAggregateHashTable> ht;
	//! Current status of a Scan
	RadixHTScanStatus scan_status;

private:
	//! Allocator and layout for finalizing state
	TupleDataLayout layout;
	ArenaAllocator aggregate_allocator;
	RowOperationsState row_state;

	//! State and chunk for scanning
	TupleDataScanState scan_state;
	DataChunk scan_chunk;
};

unique_ptr<GlobalSourceState> RadixPartitionedHashTable::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<RadixHTGlobalSourceState>(context, *this);
}

unique_ptr<LocalSourceState> RadixPartitionedHashTable::GetLocalSourceState(ExecutionContext &context) const {
	return make_uniq<RadixHTLocalSourceState>(context, *this);
}

void RadixPartitionedHashTable::ResetGlobalSourceState(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSourceState>();
	gstate.finished = false;
	gstate.task_idx = 0;
	gstate.task_done = 0;
}

RadixHTGlobalSourceState::RadixHTGlobalSourceState(ClientContext &context_p, const RadixPartitionedHashTable &radix_ht)
    : context(context_p), finished(false), task_idx(0), task_done(0) {
	for (column_t column_id = 0; column_id < radix_ht.group_types.size(); column_id++) {
		column_ids.push_back(column_id);
	}
}

SourceResultType RadixHTGlobalSourceState::AssignTask(RadixHTGlobalSinkState &sink, RadixHTLocalSourceState &lstate,
                                                      InterruptState &interrupt_state) {
	// First, try to get a partition index
	lstate.task_idx = task_idx++;
	if (finished || lstate.task_idx >= sink.partitions.size()) {
		lstate.ht.reset();
		return SourceResultType::FINISHED;
	}

	// We got a partition index
	auto &partition = *sink.partitions[lstate.task_idx];
	const annotated_lock_guard<annotated_mutex> partition_guard {partition.lock};
	switch (partition.state) {
	case AggregatePartitionState::READY_TO_FINALIZE:
		partition.state = AggregatePartitionState::FINALIZE_IN_PROGRESS;
		lstate.task = RadixHTSourceTaskType::FINALIZE;
		return SourceResultType::HAVE_MORE_OUTPUT;
	case AggregatePartitionState::FINALIZE_IN_PROGRESS:
		lstate.task = RadixHTSourceTaskType::SCAN;
		lstate.scan_status = RadixHTScanStatus::INIT;
		return partition.BlockSource(interrupt_state);
	case AggregatePartitionState::READY_TO_SCAN:
		lstate.task = RadixHTSourceTaskType::SCAN;
		lstate.scan_status = RadixHTScanStatus::INIT;
		return SourceResultType::HAVE_MORE_OUTPUT;
	default:
		throw InternalException("Unexpected AggregatePartitionState in RadixHTLocalSourceState::Finalize!");
	}
}

RadixHTLocalSourceState::RadixHTLocalSourceState(ExecutionContext &context, const RadixPartitionedHashTable &radix_ht)
    : layout(radix_ht.GetLayout().Copy()), aggregate_allocator(BufferAllocator::Get(context.client)),
      row_state(aggregate_allocator) {
	auto &allocator = BufferAllocator::Get(context.client);
	auto scan_chunk_types = radix_ht.group_types;
	for (auto &aggr_type : radix_ht.op.aggregate_return_types) {
		scan_chunk_types.push_back(aggr_type);
	}
	scan_chunk.Initialize(allocator, scan_chunk_types);
	ResetForReuse();
}

void RadixHTLocalSourceState::ResetForReuse() {
	task = RadixHTSourceTaskType::NO_TASK;
	task_idx = DConstants::INVALID_INDEX;
	ht.reset();
	scan_status = RadixHTScanStatus::DONE;
	aggregate_allocator.Reset();
	row_state.addresses.reset();
	scan_state.Reset();
	scan_chunk.Reset();
}

void RadixPartitionedHashTable::ResetLocalSourceState(ExecutionContext &context, LocalSourceState &lstate_p) const {
	auto &lstate = lstate_p.Cast<RadixHTLocalSourceState>();
	lstate.ResetForReuse();
}

void RadixHTLocalSourceState::ExecuteTask(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate,
                                          DataChunk &chunk) {
	D_ASSERT(task != RadixHTSourceTaskType::NO_TASK);
	switch (task) {
	case RadixHTSourceTaskType::FINALIZE:
		Finalize(sink, gstate);
		break;
	case RadixHTSourceTaskType::SCAN:
		Scan(sink, gstate, chunk);
		break;
	default:
		throw InternalException("Unexpected RadixHTSourceTaskType in ExecuteTask!");
	}
}

void RadixHTLocalSourceState::Finalize(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate) {
	D_ASSERT(task == RadixHTSourceTaskType::FINALIZE);
	D_ASSERT(scan_status != RadixHTScanStatus::IN_PROGRESS);
	auto &partition = *sink.partitions[task_idx];

	if (!ht) {
		// This capacity would always be sufficient for all data
		const auto capacity = GroupedAggregateHashTable::GetCapacityForCount(partition.data->Count());

		// However, we will limit the initial capacity so we don't do a huge over-allocation
		const auto n_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(gstate.context).NumberOfThreads());
		const auto memory_limit = BufferManager::GetBufferManager(gstate.context).GetMaxMemory();
		const idx_t thread_limit = LossyNumericCast<idx_t>(0.6 * double(memory_limit) / double(n_threads));

		const idx_t size_per_entry = partition.data->SizeInBytes() / MaxValue<idx_t>(partition.data->Count(), 1) +
		                             idx_t(GroupedAggregateHashTable::LOAD_FACTOR * sizeof(ht_entry_t));
		// but not lower than the initial capacity
		const auto capacity_limit =
		    MaxValue(NextPowerOfTwo(thread_limit / size_per_entry), GroupedAggregateHashTable::InitialCapacity());

		ht = sink.radix_ht.CreateHT(gstate.context, MinValue<idx_t>(capacity, capacity_limit), 0);
	} else {
		ht->Abandon();
	}

	// Now combine the uncombined data using this thread's HT
	ht->Combine(*partition.data, &partition.progress);
	partition.progress = 1;

	// Move the combined data back to the partition
	partition.data = make_uniq<TupleDataCollection>(BufferManager::GetBufferManager(gstate.context),
	                                                sink.radix_ht.GetLayoutPtr(), MemoryTag::HASH_TABLE);
	partition.data->Combine(*ht->AcquirePartitionedData()->GetPartitions()[0]);

	// Update thread-global state
	const annotated_lock_guard<annotated_mutex> guard {sink.lock};
	sink.stored_allocators.emplace_back(ht->GetAggregateAllocator());
	if (task_idx == sink.partitions.size()) {
		ht.reset();
	}
	const auto finalizes_done = ++sink.finalize_done;
	D_ASSERT(finalizes_done <= sink.partitions.size());
	if (finalizes_done == sink.partitions.size()) {
		// All finalizes are done, set remaining size to 0
		sink.temporary_memory_state->SetZero();
	}

	// Update partition state
	const annotated_lock_guard<annotated_mutex> partition_guard {partition.lock};
	partition.state = AggregatePartitionState::READY_TO_SCAN;
	partition.UnblockTasks();

	// This thread will scan the partition
	task = RadixHTSourceTaskType::SCAN;
	scan_status = RadixHTScanStatus::INIT;
}

void RadixHTLocalSourceState::Scan(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate, DataChunk &chunk) {
	D_ASSERT(task == RadixHTSourceTaskType::SCAN);
	D_ASSERT(scan_status != RadixHTScanStatus::DONE);

	auto &partition = *sink.partitions[task_idx];
	D_ASSERT(partition.state == AggregatePartitionState::READY_TO_SCAN);
	auto &data_collection = *partition.data;

	if (scan_status == RadixHTScanStatus::INIT) {
		data_collection.InitializeScan(scan_state, gstate.column_ids, sink.scan_pin_properties);
		scan_status = RadixHTScanStatus::IN_PROGRESS;
	}

	if (!data_collection.Scan(scan_state, scan_chunk)) {
		if (sink.scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE) {
			data_collection.Reset();
		}
		scan_status = RadixHTScanStatus::DONE;
		const annotated_lock_guard<annotated_mutex> guard {sink.lock};
		if (++gstate.task_done == sink.partitions.size()) {
			gstate.finished = true;
		}
		return;
	}

	const auto group_cols = layout.ColumnCount() - 1;
	RowOperations::FinalizeStates(row_state, layout, scan_state.chunk_state.row_locations, scan_chunk, group_cols);

	if (sink.scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE && layout.HasDestructor()) {
		RowOperations::DestroyStates(row_state, layout, scan_state.chunk_state.row_locations);
	}

	auto &radix_ht = sink.radix_ht;
	idx_t chunk_index = 0;
	for (auto &entry : radix_ht.grouping_set) {
		chunk.data[entry].Reference(scan_chunk.data[chunk_index++]);
	}
	for (auto null_group : radix_ht.null_groups) {
		ConstantVector::SetNull(chunk.data[null_group], count_t(scan_chunk.size()));
	}
	D_ASSERT(radix_ht.grouping_set.size() + radix_ht.null_groups.size() == radix_ht.op.GroupCount());
	for (idx_t col_idx = 0; col_idx < radix_ht.op.aggregates.size(); col_idx++) {
		chunk.data[radix_ht.op.GroupCount() + col_idx].Reference(
		    scan_chunk.data[radix_ht.group_types.size() + col_idx]);
	}
	D_ASSERT(radix_ht.op.grouping_functions.size() == radix_ht.grouping_values.size());
	for (idx_t i = 0; i < radix_ht.op.grouping_functions.size(); i++) {
		chunk.data[radix_ht.op.GroupCount() + radix_ht.op.aggregates.size() + i].Reference(radix_ht.grouping_values[i],
		                                                                                   count_t(scan_chunk.size()));
	}
	D_ASSERT(chunk.size() != 0);
}

bool RadixHTLocalSourceState::TaskFinished() {
	switch (task) {
	case RadixHTSourceTaskType::FINALIZE:
		return true;
	case RadixHTSourceTaskType::SCAN:
		return scan_status == RadixHTScanStatus::DONE;
	default:
		D_ASSERT(task == RadixHTSourceTaskType::NO_TASK);
		return true;
	}
}

SourceResultType RadixPartitionedHashTable::GetData(ExecutionContext &context, DataChunk &chunk,
                                                    GlobalSinkState &sink_p, OperatorSourceInput &input) const {
	auto &sink = sink_p.Cast<RadixHTGlobalSinkState>();
	D_ASSERT(sink.finalized);

	auto &gstate = input.global_state.Cast<RadixHTGlobalSourceState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSourceState>();
	D_ASSERT(sink.scan_pin_properties == TupleDataPinProperties::UNPIN_AFTER_DONE ||
	         sink.scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE);

	if (gstate.finished) {
		return SourceResultType::FINISHED;
	}

	if (sink.count_before_combining == 0) {
		if (grouping_set.empty()) {
			ExecutionOperatorStageTimer timer(input.stage_recorder,
			                                  "source_contract.hash_aggregate_state_scan.empty_group_finalize");
			// Special case hack to sort out aggregating from empty intermediates for aggregations without groups
			D_ASSERT(chunk.ColumnCount() == null_groups.size() + op.aggregates.size() + op.grouping_functions.size());
			// For each column in the aggregates, set to initial state
			chunk.SetChildCardinality(1);
			for (auto null_group : null_groups) {
				ConstantVector::SetNull(chunk.data[null_group], count_t(1));
			}
			ArenaAllocator allocator(BufferAllocator::Get(context.client));
			for (idx_t i = 0; i < op.aggregates.size(); i++) {
				D_ASSERT(op.aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
				auto &aggr = op.aggregates[i]->Cast<BoundAggregateExpression>();
				auto aggr_state = make_unsafe_uniq_array_uninitialized<data_t>(
				    aggr.Function().GetStateSizeCallback()(aggr.Function()));
				aggr.Function().GetStateInitCallback()(aggr.Function(), aggr_state.get());

				AggregateInputData aggr_input_data(aggr, allocator);
				Vector state_vector(Value::POINTER(CastPointerToValue(aggr_state.get())), count_t(1));
				auto &agg_result = chunk.data[null_groups.size() + i];
				aggr.Function().GetStateFinalizeCallback()(state_vector, aggr_input_data, agg_result, 1, 0);
				FlatVector::SetSize(agg_result, count_t(1));
				if (aggr.Function().HasStateDestructorCallback()) {
					aggr.Function().GetStateDestructorCallback()(state_vector, aggr_input_data, 1);
				}
			}
			// Place the grouping values (all the groups of the grouping_set condensed into a single value)
			// Behind the null groups + aggregates
			for (idx_t i = 0; i < op.grouping_functions.size(); i++) {
				chunk.data[null_groups.size() + op.aggregates.size() + i].Reference(grouping_values[i], count_t(1));
			}
		}
		gstate.finished = true;
		return SourceResultType::FINISHED;
	}

	while (!gstate.finished && chunk.size() == 0) {
		if (lstate.TaskFinished()) {
			SourceResultType res;
			{
				ExecutionOperatorStageTimer timer(input.stage_recorder,
				                                  "source_contract.hash_aggregate_state_scan.assign_task");
				res = gstate.AssignTask(sink, lstate, input.interrupt_state);
			}
			if (res != SourceResultType::HAVE_MORE_OUTPUT) {
				D_ASSERT(res == SourceResultType::FINISHED || res == SourceResultType::BLOCKED);
				return res;
			}
		}
		{
			ExecutionOperatorStageTimer timer(input.stage_recorder,
			                                  "source_contract.hash_aggregate_state_scan.execute_task");
			lstate.ExecuteTask(sink, gstate, chunk);
		}
	}

	if (chunk.size() != 0) {
		return SourceResultType::HAVE_MORE_OUTPUT;
	} else {
		return SourceResultType::FINISHED;
	}
}

ProgressData RadixPartitionedHashTable::GetProgress(ClientContext &, GlobalSinkState &sink_p,
                                                    GlobalSourceState &gstate_p) const {
	auto &sink = sink_p.Cast<RadixHTGlobalSinkState>();
	auto &gstate = gstate_p.Cast<RadixHTGlobalSourceState>();

	// Get partition combine progress, weigh it 2x
	ProgressData progress;
	for (auto &partition : sink.partitions) {
		progress.done += 2.0 * partition->progress;
	}

	// Get scan progress, weigh it 1x
	progress.done += 1.0 * double(gstate.task_done);

	// Divide by 3x for the weights, and the number of partitions to get a value between 0 and 1 again
	progress.total += 3.0 * double(sink.partitions.size());

	return progress;
}

} // namespace duckdb
