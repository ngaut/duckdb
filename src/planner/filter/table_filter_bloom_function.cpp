//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_bloom_function.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/filter/table_filter_function_helpers.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

static constexpr idx_t MAX_NUM_SECTORS = (1ULL << 26);
static constexpr idx_t MIN_NUM_BITS_PER_KEY = 12;
static constexpr idx_t MIN_NUM_BITS = 512;
static constexpr idx_t LOG_SECTOR_SIZE = 6; // a sector is 64 bits, log2(64) = 6

void BloomFilter::Initialize(ClientContext &context_p, idx_t number_of_rows) {
	D_ASSERT(!initialized);
	BufferManager &buffer_manager = BufferManager::GetBufferManager(context_p);

	const idx_t min_bits = MaxValue(MIN_NUM_BITS, number_of_rows * MIN_NUM_BITS_PER_KEY);
	num_sectors = MinValue(NextPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS);
	bitmask = num_sectors - 1;

	buf_ = buffer_manager.GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint64_t));
	// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
	bf = reinterpret_cast<uint64_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
	std::fill_n(bf, num_sectors, 0);

	initialized = true;
	finalized = false;
}

void BloomFilter::Merge(const BloomFilter &other) {
	D_ASSERT(initialized);
	D_ASSERT(other.initialized);
	D_ASSERT(!finalized);
	D_ASSERT(!other.finalized);
	D_ASSERT(num_sectors == other.num_sectors);
	D_ASSERT(bitmask == other.bitmask);
	for (idx_t i = 0; i < num_sectors; i++) {
		bf[i] |= other.bf[i];
	}
}

void BloomFilter::Finalize() {
	D_ASSERT(initialized);
	finalized = true;
}

void BloomFilter::Reset() {
	buf_.Reset();
	num_sectors = 0;
	bitmask = 0;
	initialized = false;
	finalized = false;
	bf = nullptr;
}

void BloomFilter::InsertHashes(const Vector &hashes_v) const {
	if (hashes_v.GetVectorType() == VectorType::FLAT_VECTOR && FlatVector::Validity(hashes_v).CannotHaveNull()) {
		const auto hashes = FlatVector::GetData<hash_t>(hashes_v);
		for (idx_t i = 0; i < hashes_v.size(); i++) {
			InsertOne(hashes[i]);
		}
		return;
	}
	for (auto hash : hashes_v.Values<uint64_t>()) {
		InsertOne(hash.GetValue());
	}
}

inline void BloomFilter::InsertOne(const hash_t hash) const {
	D_ASSERT(initialized);
	D_ASSERT(!finalized);
	const uint64_t bf_offset = hash & bitmask;
	const uint64_t mask = BloomFilter::GetMask(hash);
	atomic<uint64_t> &slot = *reinterpret_cast<atomic<uint64_t> *>(&bf[bf_offset]);

	slot.fetch_or(mask, std::memory_order_relaxed);
}

BloomFilterFunctionData::BloomFilterFunctionData(optional_ptr<BloomFilter> filter_p, bool filters_null_values_p,
                                                 const string &key_column_name_p, const LogicalType &key_type_p)
    : filter(filter_p), filters_null_values(filters_null_values_p), key_column_name(key_column_name_p),
      key_type(key_type_p) {
}

unique_ptr<FunctionData> BloomFilterFunctionData::Copy() const {
	return make_uniq<BloomFilterFunctionData>(filter, filters_null_values, key_column_name, key_type);
}

bool BloomFilterFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<BloomFilterFunctionData>();
	return filter.get() == other.filter.get() && filters_null_values == other.filters_null_values &&
	       key_column_name == other.key_column_name && key_type == other.key_type;
}

static idx_t SelectBloomFilter(Vector &input, const BloomFilterFunctionData &func_data, SelectionVector &result_sel,
                               idx_t count) {
	D_ASSERT(func_data.filter);
	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(input_data);

	auto select_typed = [&](auto typed_data) {
		using T = typename std::remove_cv<typename std::remove_pointer<decltype(typed_data)>::type>::type;
		idx_t result_count = 0;
		if (input_data.validity.CannotHaveNull()) {
			for (idx_t i = 0; i < count; i++) {
				const auto input_idx = input_data.sel->get_index(i);
				if (func_data.filter->LookupOne(Hash<T>(typed_data[input_idx]))) {
					result_sel.set_index(result_count++, i);
				}
			}
			return result_count;
		}
		for (idx_t i = 0; i < count; i++) {
			const auto input_idx = input_data.sel->get_index(i);
			if (!input_data.validity.RowIsValidUnsafe(input_idx)) {
				if (!func_data.filters_null_values) {
					result_sel.set_index(result_count++, i);
				}
				continue;
			}
			if (func_data.filter->LookupOne(Hash<T>(typed_data[input_idx]))) {
				result_sel.set_index(result_count++, i);
			}
		}
		return result_count;
	};

	switch (input.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return select_typed(UnifiedVectorFormat::GetData<int8_t>(input_data));
	case PhysicalType::INT16:
		return select_typed(UnifiedVectorFormat::GetData<int16_t>(input_data));
	case PhysicalType::INT32:
		return select_typed(UnifiedVectorFormat::GetData<int32_t>(input_data));
	case PhysicalType::INT64:
		return select_typed(UnifiedVectorFormat::GetData<int64_t>(input_data));
	case PhysicalType::UINT8:
		return select_typed(UnifiedVectorFormat::GetData<uint8_t>(input_data));
	case PhysicalType::UINT16:
		return select_typed(UnifiedVectorFormat::GetData<uint16_t>(input_data));
	case PhysicalType::UINT32:
		return select_typed(UnifiedVectorFormat::GetData<uint32_t>(input_data));
	case PhysicalType::UINT64:
		return select_typed(UnifiedVectorFormat::GetData<uint64_t>(input_data));
	case PhysicalType::INT128:
		return select_typed(UnifiedVectorFormat::GetData<hugeint_t>(input_data));
	case PhysicalType::UINT128:
		return select_typed(UnifiedVectorFormat::GetData<uhugeint_t>(input_data));
	case PhysicalType::FLOAT:
		return select_typed(UnifiedVectorFormat::GetData<float>(input_data));
	case PhysicalType::DOUBLE:
		return select_typed(UnifiedVectorFormat::GetData<double>(input_data));
	case PhysicalType::INTERVAL:
		return select_typed(UnifiedVectorFormat::GetData<interval_t>(input_data));
	case PhysicalType::VARCHAR:
		return select_typed(UnifiedVectorFormat::GetData<string_t>(input_data));
	default:
		break;
	}

	Vector hashes(LogicalType::HASH, count);
	VectorOperations::Hash(input, hashes, count);
	hashes.Flatten();
	const auto hash_data = FlatVector::GetData<hash_t>(hashes);

	idx_t result_count = 0;
	if (input_data.validity.CannotHaveNull()) {
		for (idx_t i = 0; i < count; i++) {
			if (func_data.filter->LookupOne(hash_data[i])) {
				result_sel.set_index(result_count++, i);
			}
		}
		return result_count;
	}

	for (idx_t i = 0; i < count; i++) {
		const auto input_idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValidUnsafe(input_idx)) {
			if (!func_data.filters_null_values) {
				result_sel.set_index(result_count++, i);
			}
			continue;
		}
		if (func_data.filter->LookupOne(hash_data[i])) {
			result_sel.set_index(result_count++, i);
		}
	}
	return result_count;
}

static idx_t BloomFilterSelect(DataChunk &args, ExpressionState &state, optional_ptr<const SelectionVector> sel,
                               optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.BindInfo()->Cast<BloomFilterFunctionData>();

	auto count = args.size();
	if (!func_data.filter) {
		return SetAllTrueSelection(count, sel, true_sel, false_sel);
	}

	SelectionVector temp_true(count);
	auto result_true_sel = (!true_sel || (sel && true_sel.get() == sel.get())) ? &temp_true : true_sel.get();
	auto approved_count = SelectBloomFilter(args.data[0], func_data, *result_true_sel, count);
	return TranslateSelection(count, sel, *result_true_sel, approved_count, true_sel, false_sel);
}

template <class T>
static FilterPropagateResult TemplatedBloomFilterPrune(const BloomFilter &bf, const BaseStatistics &stats) {
	if (!NumericStats::HasMinMax(stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	const auto min = NumericStats::GetMin<T>(stats);
	const auto max = NumericStats::GetMax<T>(stats);
	if (min > max) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	T range_typed;
	idx_t range;
	if (!TrySubtractOperator::Operation(max, min, range_typed) || !TryCast::Operation(range_typed, range) ||
	    range >= DEFAULT_STANDARD_VECTOR_SIZE) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	T val = min;
	idx_t hits = 0;
	for (idx_t i = 0; i <= range; i++) {
		hits += bf.LookupOne(Hash(val));
		val += i < range;
	}

	if (hits == 0) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (hits == range + 1) {
		return FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

ScalarFunction BloomFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, nullptr, TableFilterFunctions::Bind);
	func.SetSelectCallback(BloomFilterSelect);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	func.SetFilterPruneCallback(BloomFilterScalarFun::FilterPrune);
	func.SetSerializeCallback(TableFilterFunctionSerialize);
	func.SetDeserializeCallback(TableFilterFunctionDeserialize);
	return func;
}

string BloomFilterScalarFun::ToString(const string &column_name, const string &key_column_name) {
	return column_name + " IN BF(" + key_column_name + ")";
}

FilterPropagateResult BloomFilterScalarFun::FilterPrune(const FunctionStatisticsPruneInput &input) {
	if (!input.bind_data) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &data = input.bind_data->Cast<BloomFilterFunctionData>();
	if (!data.filter || !data.filter->IsInitialized()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	switch (data.key_type.InternalType()) {
	case PhysicalType::UINT8:
		return TemplatedBloomFilterPrune<uint8_t>(*data.filter, input.stats);
	case PhysicalType::UINT16:
		return TemplatedBloomFilterPrune<uint16_t>(*data.filter, input.stats);
	case PhysicalType::UINT32:
		return TemplatedBloomFilterPrune<uint32_t>(*data.filter, input.stats);
	case PhysicalType::UINT64:
		return TemplatedBloomFilterPrune<uint64_t>(*data.filter, input.stats);
	case PhysicalType::UINT128:
		return TemplatedBloomFilterPrune<uhugeint_t>(*data.filter, input.stats);
	case PhysicalType::INT8:
		return TemplatedBloomFilterPrune<int8_t>(*data.filter, input.stats);
	case PhysicalType::INT16:
		return TemplatedBloomFilterPrune<int16_t>(*data.filter, input.stats);
	case PhysicalType::INT32:
		return TemplatedBloomFilterPrune<int32_t>(*data.filter, input.stats);
	case PhysicalType::INT64:
		return TemplatedBloomFilterPrune<int64_t>(*data.filter, input.stats);
	case PhysicalType::INT128:
		return TemplatedBloomFilterPrune<hugeint_t>(*data.filter, input.stats);
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

ScalarFunction TableFilterBloomFilterFun::GetFunction() {
	return BloomFilterScalarFun::GetFunction(LogicalType::ANY);
}

} // namespace duckdb
