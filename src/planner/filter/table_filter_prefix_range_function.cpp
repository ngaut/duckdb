//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_prefix_range_function.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/filter/table_filter_function_helpers.hpp"

#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/planner/table_filter_state.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

AllocatedData AllocateBitmap(ClientContext &context, const idx_t word_count, uint64_t *&bitmap_begin) {
	const idx_t size = word_count * sizeof(uint64_t);
	BufferManager &buffer_manager = BufferManager::GetBufferManager(context);
	auto buffer = buffer_manager.GetBufferAllocator().Allocate(64ULL + size);
	bitmap_begin = reinterpret_cast<uint64_t *>((64ULL + reinterpret_cast<uint64_t>(buffer.get())) & ~63ULL);
	std::fill_n(bitmap_begin, word_count, 0);
	return buffer;
}

struct PrefixRangeBitmapBuildState : public PrefixRangeFilter::BuildState {
	explicit PrefixRangeBitmapBuildState(AllocatedData data_p, uint64_t *bitmap_p)
	    : data(std::move(data_p)), bitmap(bitmap_p) {
	}

	AllocatedData data;
	uint64_t *bitmap;
};

static constexpr idx_t PREFIX_RANGE_MAX_PREFIX_LENGTH = 20;
static constexpr uint64_t PREFIX_RANGE_CAP_BITS = 1ULL << PREFIX_RANGE_MAX_PREFIX_LENGTH;
static constexpr uint64_t PREFIX_RANGE_MAX_EXACT_BITS = 1ULL << 23;
static constexpr uint64_t PREFIX_RANGE_MAX_EXACT_BITS_PER_BUILD_ROW = 256;

struct PrefixRangeBitmapShape {
	idx_t shift;
	idx_t bucket_count;
};

static PrefixRangeBitmapShape ComputePrefixRangeBitmapShape(uint64_t span, idx_t number_of_rows,
                                                            bool allow_adaptive_exact_bitmap) {
	const auto exact_span_budget =
	    number_of_rows > NumericLimits<uint64_t>::Maximum() / PREFIX_RANGE_MAX_EXACT_BITS_PER_BUILD_ROW
	        ? NumericLimits<uint64_t>::Maximum()
	        : static_cast<uint64_t>(number_of_rows) * PREFIX_RANGE_MAX_EXACT_BITS_PER_BUILD_ROW;
	const bool adaptive_exact =
	    allow_adaptive_exact_bitmap && span < PREFIX_RANGE_MAX_EXACT_BITS && span <= exact_span_budget;

	idx_t shift = 0;
	if (span >= PREFIX_RANGE_CAP_BITS && !adaptive_exact) {
		const auto quotient = span >> PREFIX_RANGE_MAX_PREFIX_LENGTH;
		shift = quotient <= 1 ? 0 : 64 - CountZeros<uint64_t>::Leading(quotient - 1);
	}
	return {shift, UnsafeNumericCast<idx_t>((span >> shift) + 1)};
}

template <typename U>
class PrefixRangeBitmap {
public:
	void Initialize(ClientContext &context, U min_p, U span_p, idx_t number_of_rows, bool allow_adaptive_exact_bitmap) {
		min = min_p;
		span = span_p;
		build_row_count = number_of_rows;
		const auto shape = ComputePrefixRangeBitmapShape(UnsafeNumericCast<uint64_t>(span), number_of_rows,
		                                                 allow_adaptive_exact_bitmap);
		shift = shape.shift;
		const auto buckets = shape.bucket_count;
		word_count = buckets == 0 ? 1 : (buckets + 63) >> WORD_SHIFT;

		buf_ = AllocateBitmap(context, word_count, bitmap);

		// Only mark initialized as true when local bitmaps are merged.
		initialized = false;
	}

	unique_ptr<PrefixRangeBitmapBuildState> InitializeBuildState(ClientContext &context) const {
		D_ASSERT(bitmap);
		uint64_t *state_bitmap;
		auto state_data = AllocateBitmap(context, word_count, state_bitmap);
		return make_uniq<PrefixRangeBitmapBuildState>(std::move(state_data), state_bitmap);
	}

	template <typename T, typename CONVERTER>
	void InsertKeys(Vector &keys, uint64_t *state_bitmap) const {
		for (const auto &entry : keys.template ValidValues<T>()) {
			const U y = CONVERTER::Convert(entry.GetValue()) - min;
			// All keys are in-range by construction, so the range check can be omitted here.
			const U idx = y >> shift;
			state_bitmap[idx >> WORD_SHIFT] |= 1ULL << (idx & WORD_MASK);
		}
	}

	void MergeBuildState(PrefixRangeBitmapBuildState &state) {
		for (idx_t word_idx = 0; word_idx < word_count; word_idx++) {
			bitmap[word_idx] |= state.bitmap[word_idx];
		}
		initialized = true;
	}

	template <typename T, typename CONVERTER>
	inline bool LookupOne(const Value &value) const {
		if (value.IsNull()) {
			return false;
		}

		return LookupValue<T, CONVERTER>(value.GetValueUnsafe<T>());
	}

	template <typename T, typename CONVERTER>
	inline bool LookupValue(const T &value) const {
		const U comparable = CONVERTER::Convert(value);
		const U y = comparable - min;
		const U bit_idx = y >> shift;
		const uint8_t in_range = y <= span;
		const uint32_t word_idx = (bit_idx >> WORD_SHIFT) & (0U - in_range);
		const uint8_t bit = (bitmap[word_idx] >> (bit_idx & WORD_MASK)) & 1ULL;
		return bit & in_range;
	}

	template <typename T, typename CONVERTER>
	idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const {
		idx_t found_count = 0;
		for (const auto &entry : keys.template ValidValues<T>()) {
			const U comparable = CONVERTER::Convert(entry.GetValue());
			const U y = comparable - min;
			const U bit_idx = y >> shift;
			const uint8_t in_range = y <= span;
			const uint32_t word_idx = (bit_idx >> WORD_SHIFT) & (0U - in_range);
			const uint8_t bit = (bitmap[word_idx] >> (bit_idx & WORD_MASK)) & 1ULL;

			if (bit & in_range) {
				result_sel.set_index(found_count++, entry.GetIndex());
			}
		}
		return found_count;
	}

	template <typename T, typename CONVERTER>
	idx_t LookupKeys(Vector &keys, const SelectionVector &sel, SelectionVector &result_sel, idx_t count) const {
		UnifiedVectorFormat key_data;
		keys.ToUnifiedFormat(key_data);
		auto values = UnifiedVectorFormat::GetData<const T>(key_data);
		idx_t found_count = 0;
		for (idx_t i = 0; i < count; i++) {
			const auto row_idx = sel.get_index(i);
			const auto vector_idx = key_data.sel->get_index(row_idx);
			if (!key_data.validity.RowIsValid(vector_idx)) {
				continue;
			}
			const U comparable = CONVERTER::Convert(values[vector_idx]);
			const U y = comparable - min;
			const U bit_idx = y >> shift;
			const uint8_t in_range = y <= span;
			const uint32_t word_idx = (bit_idx >> WORD_SHIFT) & (0U - in_range);
			const uint8_t bit = (bitmap[word_idx] >> (bit_idx & WORD_MASK)) & 1ULL;

			if (bit & in_range) {
				result_sel.set_index(found_count++, row_idx);
			}
		}
		return found_count;
	}

	FilterPropagateResult LookupRange(U lower_bound, U upper_bound) const {
		const U lb_y = lower_bound - min;
		const U lb_bit_idx = lb_y >> shift;
		const auto lb_word_idx = lb_bit_idx >> WORD_SHIFT;

		const U ub_y = upper_bound - min;
		const U ub_bit_idx = ub_y >> shift;
		const auto ub_word_idx = ub_bit_idx >> WORD_SHIFT;

		const idx_t lb_bit_off = UnsafeNumericCast<idx_t>(lb_bit_idx & UnsafeNumericCast<U>(WORD_MASK));
		const idx_t ub_bit_off = UnsafeNumericCast<idx_t>(ub_bit_idx & UnsafeNumericCast<U>(WORD_MASK));

		// TODO: Count the amount of 1's in the range, compare to a threshold, and make a decision if we want to use the
		// per-row filter for this row group.
		if (lb_word_idx == ub_word_idx) {
			const auto range_mask = ((~0ULL << lb_bit_off) & (~0ULL >> (WORD_MASK - ub_bit_off)));
			if (bitmap[lb_word_idx] & range_mask) {
				return FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}

		const auto lb_word_mask = (~0ULL << lb_bit_off);
		if (bitmap[lb_word_idx] & lb_word_mask) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}

		for (idx_t i = UnsafeNumericCast<idx_t>(lb_word_idx) + 1; i < UnsafeNumericCast<idx_t>(ub_word_idx); i++) {
			if (bitmap[i]) {
				return FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
		}

		const auto ub_word_mask = ~0ULL >> (WORD_MASK - ub_bit_off);
		if (bitmap[ub_word_idx] & ub_word_mask) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}

		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	bool IsInitialized() const {
		return initialized;
	}

	U Min() const {
		return min;
	}

	U Span() const {
		return span;
	}

	idx_t Shift() const {
		return shift;
	}

	const uint64_t *BitmapData() const {
		return bitmap;
	}

	idx_t DistinctCountUpperBound() const {
		if (!initialized) {
			return 0;
		}
		idx_t occupied_buckets = 0;
		for (idx_t word_idx = 0; word_idx < word_count; word_idx++) {
			auto word = bitmap[word_idx];
			while (word) {
				word &= word - 1;
				occupied_buckets++;
			}
		}
		if (shift >= sizeof(idx_t) * 8) {
			return std::numeric_limits<idx_t>::max();
		}
		const auto bucket_width = idx_t(1) << shift;
		if (occupied_buckets > std::numeric_limits<idx_t>::max() / bucket_width) {
			return std::numeric_limits<idx_t>::max();
		}
		return occupied_buckets * bucket_width;
	}

	idx_t EstimateDistinctCountInRange(U lower_bound, U upper_bound) const {
		if (!initialized || lower_bound > upper_bound) {
			return 0;
		}
		const auto bitmap_max = min + span;
		const auto adjusted_lower = MaxValue<U>(lower_bound, min);
		const auto adjusted_upper = MinValue<U>(upper_bound, bitmap_max);
		if (adjusted_lower > adjusted_upper ||
		    LookupRange(adjusted_lower, adjusted_upper) == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
			return 0;
		}
		const auto distinct_upper_bound = MinValue(DistinctCountUpperBound(), build_row_count);
		if (distinct_upper_bound == 0) {
			return 0;
		}
		const auto total_width = static_cast<long double>(span) + 1;
		const auto overlap_width = static_cast<long double>(adjusted_upper - adjusted_lower) + 1;
		const auto estimate =
		    static_cast<idx_t>(std::ceil(static_cast<long double>(distinct_upper_bound) * overlap_width / total_width));
		return MinValue(distinct_upper_bound, MaxValue<idx_t>(estimate, 1));
	}

private:
	static constexpr idx_t WORD_SHIFT = 6;
	static constexpr idx_t WORD_MASK = 63;

	bool initialized = false;
	U min;
	U span;
	idx_t shift;
	idx_t word_count;
	idx_t build_row_count;
	AllocatedData buf_;
	uint64_t *bitmap;
};

template <typename T>
struct NumericConverter {
	using comparable_type = typename MakeUnsigned<T>::type;

	static inline comparable_type Convert(T value) {
		// Overflow is explicitly allowed for unsigned to signed cast
		return static_cast<comparable_type>(value);
	}
};

struct StringPrefixConverter {
	static inline uint32_t Convert(const string_t &value) {
		return value.GetPrefixIntegerComparable();
	}
};

uint32_t StringMinComparable(const Value &value) {
	return StringPrefixConverter::Convert(value.GetValueUnsafe<string_t>());
}

uint32_t StringMaxComparable(const Value &value) {
	const auto max_string = value.GetValueUnsafe<string_t>();
	if (max_string.GetSize() >= string_t::PREFIX_BYTES) {
		return max_string.GetPrefixIntegerComparable();
	}

	// Pad string prefix with 0xFF to keep correctness if max is truncated at \0 char, e.g., ab\0c -> ab
	array<char, string_t::PREFIX_BYTES> padded_prefix;
	padded_prefix.fill(char(0xFF));
	for (idx_t i = 0; i < max_string.GetSize(); i++) {
		padded_prefix[i] = max_string.GetData()[i];
	}
	return string_t(padded_prefix.data(), string_t::PREFIX_BYTES).GetPrefixIntegerComparable();
}

template <typename T>
class NumericPrefixRangeFilter : public PrefixRangeFilter {
private:
	using Comparable = typename MakeUnsigned<T>::type;

public:
	void Initialize(ClientContext &context, idx_t number_of_rows, Value min_val, Value max_val) override {
		D_ASSERT(min_val <= max_val);
		D_ASSERT(number_of_rows > 0);
		const auto min = NumericConverter<T>::Convert(min_val.GetValueUnsafe<T>());
		const auto max = NumericConverter<T>::Convert(max_val.GetValueUnsafe<T>());
		bitmap.Initialize(context, min, max - min, number_of_rows, true);
	}

	unique_ptr<BuildState> InitializeBuildState(ClientContext &context) const override {
		return bitmap.InitializeBuildState(context);
	}

	void InsertKeys(Vector &keys, BuildState &state) const override {
		auto &bitmap_state = state.Cast<PrefixRangeBitmapBuildState>();
		bitmap.template InsertKeys<T, NumericConverter<T>>(keys, bitmap_state.bitmap);
	}

	void MergeBuildState(BuildState &state) override {
		bitmap.MergeBuildState(state.Cast<PrefixRangeBitmapBuildState>());
	}

	idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const override {
		if (keys.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return bitmap.template LookupOne<T, NumericConverter<T>>(keys.GetValue(0)) ? count : 0;
		}
		return bitmap.template LookupKeys<T, NumericConverter<T>>(keys, result_sel, count);
	}

	idx_t LookupKeys(Vector &keys, const SelectionVector &sel, SelectionVector &result_sel,
	                 idx_t count) const override {
		if (keys.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (!bitmap.template LookupOne<T, NumericConverter<T>>(keys.GetValue(0))) {
				return 0;
			}
			for (idx_t i = 0; i < count; i++) {
				result_sel.set_index(i, sel.get_index(i));
			}
			return count;
		}
		return bitmap.template LookupKeys<T, NumericConverter<T>>(keys, sel, result_sel, count);
	}

	bool GetSignedLookupData(PrefixRangeLookupData &result) const override {
		if (!bitmap.IsInitialized()) {
			return false;
		}
		result.min = static_cast<uint64_t>(bitmap.Min());
		result.span = static_cast<uint64_t>(bitmap.Span());
		result.shift = bitmap.Shift();
		result.bitmap = bitmap.BitmapData();
		return true;
	}

	idx_t DistinctCountUpperBound() const override {
		return bitmap.DistinctCountUpperBound();
	}

	idx_t EstimateDistinctCountInRange(const Value &lower_bound, const Value &upper_bound) const override {
		if (lower_bound.IsNull() || upper_bound.IsNull() || lower_bound.type().InternalType() != GetTypeId<T>() ||
		    upper_bound.type().InternalType() != GetTypeId<T>()) {
			return DistinctCountUpperBound();
		}
		const auto lower_comparable = NumericConverter<T>::Convert(lower_bound.GetValueUnsafe<T>());
		const auto upper_comparable = NumericConverter<T>::Convert(upper_bound.GetValueUnsafe<T>());
		// Signed domains that cross zero wrap in their unsigned comparable
		// representation. Keep the conservative global estimate for that case.
		if (lower_comparable > upper_comparable) {
			return DistinctCountUpperBound();
		}
		return bitmap.EstimateDistinctCountInRange(lower_comparable, upper_comparable);
	}

	FilterPropagateResult LookupRange(const Value &lower_bound, const Value &upper_bound) const override {
		const auto lb = lower_bound.GetValueUnsafe<T>();
		const auto ub = upper_bound.GetValueUnsafe<T>();

		const auto bitmap_min = static_cast<T>(bitmap.Min());
		const auto bitmap_max = static_cast<T>(bitmap.Min() + bitmap.Span());
		if (ub < bitmap_min || lb > bitmap_max) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}

		const auto adjusted_lb = NumericConverter<T>::Convert(MaxValue<T>(lb, bitmap_min));
		const auto adjusted_ub = NumericConverter<T>::Convert(MinValue<T>(ub, bitmap_max));
		return bitmap.LookupRange(adjusted_lb, adjusted_ub);
	}

	bool IsInitialized() const override {
		return bitmap.IsInitialized();
	}

private:
	PrefixRangeBitmap<Comparable> bitmap;
};

class StringPrefixRangeFilter : public PrefixRangeFilter {
public:
	void Initialize(ClientContext &context, idx_t number_of_rows, Value min_val, Value max_val) override {
		D_ASSERT(min_val <= max_val);
		D_ASSERT(number_of_rows > 0);
		const auto min = StringPrefixConverter::Convert(min_val.GetValueUnsafe<string_t>());
		const auto max = StringPrefixConverter::Convert(max_val.GetValueUnsafe<string_t>());
		D_ASSERT(min <= max);
		bitmap.Initialize(context, min, max - min, number_of_rows, false);
	}

	unique_ptr<BuildState> InitializeBuildState(ClientContext &context) const override {
		return bitmap.InitializeBuildState(context);
	}

	void InsertKeys(Vector &keys, BuildState &state) const override {
		auto &bitmap_state = state.Cast<PrefixRangeBitmapBuildState>();
		bitmap.template InsertKeys<string_t, StringPrefixConverter>(keys, bitmap_state.bitmap);
	}

	void MergeBuildState(BuildState &state) override {
		bitmap.MergeBuildState(state.Cast<PrefixRangeBitmapBuildState>());
	}

	idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const override {
		if (keys.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return bitmap.template LookupOne<string_t, StringPrefixConverter>(keys.GetValue(0)) ? count : 0;
		}
		return bitmap.template LookupKeys<string_t, StringPrefixConverter>(keys, result_sel, count);
	}

	idx_t LookupKeys(Vector &keys, const SelectionVector &sel, SelectionVector &result_sel,
	                 idx_t count) const override {
		if (keys.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (!bitmap.template LookupOne<string_t, StringPrefixConverter>(keys.GetValue(0))) {
				return 0;
			}
			for (idx_t i = 0; i < count; i++) {
				result_sel.set_index(i, sel.get_index(i));
			}
			return count;
		}
		return bitmap.template LookupKeys<string_t, StringPrefixConverter>(keys, sel, result_sel, count);
	}

	bool GetSignedLookupData(PrefixRangeLookupData &result) const override {
		return false;
	}

	idx_t DistinctCountUpperBound() const override {
		return bitmap.DistinctCountUpperBound();
	}

	FilterPropagateResult LookupRange(const Value &lower_bound, const Value &upper_bound) const override {
		auto lower_bound_comparable = StringMinComparable(lower_bound);
		auto upper_bound_comparable = StringMaxComparable(upper_bound);
		if (lower_bound_comparable > upper_bound_comparable) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}

		const auto bitmap_min = bitmap.Min();
		const auto bitmap_max = bitmap.Min() + bitmap.Span();
		if (upper_bound_comparable < bitmap_min || lower_bound_comparable > bitmap_max) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}

		lower_bound_comparable = MaxValue<uint32_t>(lower_bound_comparable, bitmap_min);
		upper_bound_comparable = MinValue<uint32_t>(upper_bound_comparable, bitmap_max);
		return bitmap.LookupRange(lower_bound_comparable, upper_bound_comparable);
	}

	bool IsInitialized() const override {
		return bitmap.IsInitialized();
	}

private:
	PrefixRangeBitmap<uint32_t> bitmap;
};

template <typename T>
bool ComputeSpan(const Value &lower_bound, const Value &upper_bound, uhugeint_t &result) {
	T lb_value = lower_bound.GetValueUnsafe<T>();
	T ub_value = upper_bound.GetValueUnsafe<T>();
	T res;
	if (TrySubtractOperator::Operation(ub_value, lb_value, res)) {
		result = Uhugeint::Convert(res);
		return true;
	} else {
		return false;
	}
}

bool ComputeStringPrefixSpan(const Value &lower_bound, const Value &upper_bound, uhugeint_t &result) {
#ifdef DUCKDB_DEBUG_NO_INLINE
	return false;
#else
	auto lb_value = lower_bound.GetValueUnsafe<string_t>().GetPrefixIntegerComparable();
	auto ub_value = upper_bound.GetValueUnsafe<string_t>().GetPrefixIntegerComparable();
	uint32_t res;
	if (TrySubtractOperator::Operation(ub_value, lb_value, res)) {
		result = Uhugeint::Convert(res);
		return true;
	} else {
		return false;
	}
#endif
}

unique_ptr<PrefixRangeFilter> PrefixRangeFilter::CreatePrefixRangeFilter(const LogicalType &key_type) {
	switch (key_type.InternalType()) {
	case PhysicalType::UINT8:
		return make_uniq<NumericPrefixRangeFilter<uint8_t>>();
	case PhysicalType::UINT16:
		return make_uniq<NumericPrefixRangeFilter<uint16_t>>();
	case PhysicalType::UINT32:
		return make_uniq<NumericPrefixRangeFilter<uint32_t>>();
	case PhysicalType::UINT64:
		return make_uniq<NumericPrefixRangeFilter<uint64_t>>();
	case PhysicalType::INT8:
		return make_uniq<NumericPrefixRangeFilter<int8_t>>();
	case PhysicalType::INT16:
		return make_uniq<NumericPrefixRangeFilter<int16_t>>();
	case PhysicalType::INT32:
		return make_uniq<NumericPrefixRangeFilter<int32_t>>();
	case PhysicalType::INT64:
		return make_uniq<NumericPrefixRangeFilter<int64_t>>();
	case PhysicalType::VARCHAR:
#ifdef DUCKDB_DEBUG_NO_INLINE
		throw NotImplementedException("Prefix range filter is not implemented for type %s", key_type.ToString());
#else
		return make_uniq<StringPrefixRangeFilter>();
#endif
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	default:
		throw NotImplementedException("Prefix range filter is not implemented for type %s", key_type.ToString());
	}
}

bool PrefixRangeFilter::TryComputeSpan(const Value &lower_bound, const Value &upper_bound, uhugeint_t &result) {
	if (lower_bound.type().InternalType() != upper_bound.type().InternalType()) {
		return false;
	}

	switch (lower_bound.type().InternalType()) {
	case PhysicalType::UINT8:
		return ComputeSpan<uint8_t>(lower_bound, upper_bound, result);
	case PhysicalType::UINT16:
		return ComputeSpan<uint16_t>(lower_bound, upper_bound, result);
	case PhysicalType::UINT32:
		return ComputeSpan<uint32_t>(lower_bound, upper_bound, result);
	case PhysicalType::UINT64:
		return ComputeSpan<uint64_t>(lower_bound, upper_bound, result);
	case PhysicalType::INT8:
		return ComputeSpan<int8_t>(lower_bound, upper_bound, result);
	case PhysicalType::INT16:
		return ComputeSpan<int16_t>(lower_bound, upper_bound, result);
	case PhysicalType::INT32:
		return ComputeSpan<int32_t>(lower_bound, upper_bound, result);
	case PhysicalType::INT64:
		return ComputeSpan<int64_t>(lower_bound, upper_bound, result);
	case PhysicalType::VARCHAR:
		return ComputeStringPrefixSpan(lower_bound, upper_bound, result);
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	default:
		return false;
	}
}

bool PrefixRangeFilter::TryEstimateMaxBucketDensity(idx_t number_of_rows, const Value &lower_bound,
                                                    const Value &upper_bound, double &result) {
	if (number_of_rows == 0) {
		return false;
	}
	uhugeint_t wide_span;
	if (!TryComputeSpan(lower_bound, upper_bound, wide_span) || wide_span == 0) {
		return false;
	}
	uint64_t span;
	if (!Uhugeint::TryCast(wide_span, span)) {
		return false;
	}
	const bool allow_adaptive_exact_bitmap = lower_bound.type().InternalType() != PhysicalType::VARCHAR;
	const auto shape = ComputePrefixRangeBitmapShape(span, number_of_rows, allow_adaptive_exact_bitmap);
	if (shape.bucket_count <= 1) {
		return false;
	}

	// Min and max already occupy the endpoints. Model the remaining build
	// cardinality over the remaining prefix intervals. This is conservative in
	// the presence of duplicates, and exactly classifies evenly spaced domains
	// at the adaptive filter's selectivity boundary.
	const auto bucket_intervals = shape.bucket_count - 1;
	const auto build_intervals = MinValue(number_of_rows - 1, bucket_intervals);
	result = static_cast<double>(build_intervals) / static_cast<double>(bucket_intervals);
	return true;
}

bool PrefixRangeFilter::SupportedType(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
		return true;
	case PhysicalType::VARCHAR:
#ifdef DUCKDB_DEBUG_NO_INLINE
		return false;
#else
		return true;
#endif
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	default:
		return false;
	}
}

PrefixRangeFunctionData::PrefixRangeFunctionData(optional_ptr<PrefixRangeFilter> filter_p,
                                                 const string &key_column_name_p, const LogicalType &key_type_p,
                                                 shared_ptr<ExecutionRuntimeFilterIdentity> runtime_filter_identity_p)
    : filter(filter_p), key_column_name(key_column_name_p), key_type(key_type_p),
      runtime_filter_identity(std::move(runtime_filter_identity_p)) {
}

unique_ptr<FunctionData> PrefixRangeFunctionData::Copy() const {
	return make_uniq<PrefixRangeFunctionData>(filter, key_column_name, key_type, runtime_filter_identity);
}

bool PrefixRangeFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<PrefixRangeFunctionData>();
	return filter.get() == other.filter.get() && key_column_name == other.key_column_name &&
	       key_type == other.key_type && runtime_filter_identity == other.runtime_filter_identity;
}

static idx_t SelectPrefixRange(Vector &input, const PrefixRangeFunctionData &func_data, SelectionVector &result_sel,
                               idx_t count) {
	D_ASSERT(func_data.filter);
	return func_data.filter->LookupKeys(input, result_sel, count);
}

static idx_t PrefixRangeSelect(DataChunk &args, ExpressionState &state, optional_ptr<const SelectionVector> sel,
                               optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.BindInfo()->Cast<PrefixRangeFunctionData>();

	auto count = args.size();
	if (!func_data.filter || !func_data.filter->IsInitialized()) {
		return SetAllTrueSelection(count, sel, true_sel, false_sel);
	}
	SelectionVector temp_true(count);
	auto result_true_sel = (!true_sel || (sel && true_sel.get() == sel.get())) ? &temp_true : true_sel.get();
	auto approved_count = SelectPrefixRange(args.data[0], func_data, *result_true_sel, count);
	return TranslateSelection(count, sel, *result_true_sel, approved_count, true_sel, false_sel);
}

ScalarFunction PrefixRangeScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, nullptr, TableFilterFunctions::Bind);
	func.SetSelectCallback(PrefixRangeSelect);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	func.SetFilterPruneCallback(PrefixRangeScalarFun::FilterPrune);
	func.SetSerializeCallback(TableFilterFunctionSerialize);
	func.SetDeserializeCallback(TableFilterFunctionDeserialize);
	return func;
}

string PrefixRangeScalarFun::ToString(const string &column_name, const string &key_column_name) {
	return column_name + " IN PRF(" + key_column_name + ")";
}

FilterPropagateResult PrefixRangeScalarFun::FilterPrune(const FunctionStatisticsPruneInput &input) {
	if (!input.bind_data) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &data = input.bind_data->Cast<PrefixRangeFunctionData>();
	if (!data.filter || !data.filter->IsInitialized()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	switch (input.stats.GetStatsType()) {
	case StatisticsType::NUMERIC_STATS: {
		if (!NumericStats::HasMinMax(input.stats)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		const auto min = NumericStats::Min(input.stats);
		const auto max = NumericStats::Max(input.stats);
		if (min > max) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		return data.filter->LookupRange(min, max);
	}
	case StatisticsType::STRING_STATS: {
		if (!StringStats::HasMinMax(input.stats)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		// String stats may contain raw parquet bytes that are not valid UTF-8. Reconstruct them as BLOBs so the
		// prefix-range comparable logic can inspect the raw bytes without value-construction validation.
		return data.filter->LookupRange(Value::BLOB_RAW(StringStats::Min(input.stats)),
		                                Value::BLOB_RAW(StringStats::Max(input.stats)));
	}
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

ScalarFunction TableFilterPrefixRangeFun::GetFunction() {
	return PrefixRangeScalarFun::GetFunction(LogicalType::ANY);
}

} // namespace duckdb
