//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/hash_bloom_filter.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/scalar/tablefilter_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {

class BaseStatistics;
class Expression;
class PrefixRangeFilter;
struct DynamicFilterData;
struct ExecutionRuntimeFilterIdentity;

enum class SelectivityOptionalFilterType : uint8_t { MIN_MAX = 0, BF = 1, PRF = 3 };

void GetThresholdAndVectorsToCheck(SelectivityOptionalFilterType type, float &selectivity_threshold,
                                   idx_t &n_vectors_to_check);

unique_ptr<Expression> CreateOptionalFilterExpression(unique_ptr<Expression> child_expr,
                                                      const LogicalType &target_type);
unique_ptr<Expression> CreateSelectivityOptionalFilterExpression(unique_ptr<Expression> child_expr,
                                                                 const LogicalType &target_type,
                                                                 float selectivity_threshold, idx_t n_vectors_to_check);
unique_ptr<Expression> CreateDynamicFilterExpression(shared_ptr<DynamicFilterData> filter_data,
                                                     const LogicalType &target_type);
//! Variant that evaluates the dynamic filter on top of a custom input expression (e.g. a cast chain over
//! the raw scan column). The input must return `target_type`. When `input` is nullptr, a
//! BoundReferenceExpression(0) placeholder in `target_type` is used instead.
unique_ptr<Expression> CreateDynamicFilterExpression(shared_ptr<DynamicFilterData> filter_data,
                                                     const LogicalType &target_type, unique_ptr<Expression> input);

//! A closed range over a signed integral physical type. The bounds are always inside the domain of the physical type
//! the range was computed for, so consumers can narrow them to that type without wrapping.
struct SignedNumericRangeFilterData {
	bool empty = false;
	bool has_lower = false;
	bool has_upper = false;
	int64_t lower = 0;
	int64_t upper = 0;

	//! A non-empty range as an offset window: v is inside when uint64_t(int64_t(v)) - OffsetBase() <= OffsetSpan(),
	//! one compare and no branch on which bounds exist
	uint64_t OffsetBase() const {
		return has_lower ? static_cast<uint64_t>(lower) : uint64_t(1) << 63;
	}
	uint64_t OffsetSpan() const {
		return (has_upper ? static_cast<uint64_t>(upper) : (uint64_t(1) << 63) - 1) - OffsetBase();
	}
};

bool IsSignedNumericRangePhysicalType(PhysicalType physical_type);
bool TryGetSignedNumericRange(const Expression &expr, const LogicalType &target_type,
                              SignedNumericRangeFilterData &range);
//! Cached variant over the expression the filter state was initialized with.
bool TryGetSignedNumericRange(ExpressionFilterState &state, const LogicalType &target_type,
                              SignedNumericRangeFilterData &range);

//! Bind function that prevents user access to internal tablefilter functions
struct TableFilterFunctions {
	static unique_ptr<FunctionData> Bind(BindScalarFunctionInput &input);
	static bool IsTableFilterFunction(const Identifier &name);
	static bool IsTableFilterFunction(const BoundScalarFunction &function) {
		return IsTableFilterFunction(function.GetName());
	}
};

//! Runtime bloom-filter state used by join pushdown and internal tablefilter functions.
//! A filter is first built (Initialize, then InsertHashes) and afterwards sealed. Only a sealed filter
//! describes a complete key set, so only a sealed filter may be looked up or handed to a scan.
class BloomFilter {
public:
	BloomFilter() = default;
	void Initialize(ClientContext &context_p, idx_t number_of_rows);

	//! VectorOperations::Hash writes at the source row positions, so the hashes to insert are the ones sel
	//! points at rather than the first count entries
	void InsertHashes(const Vector &hashes_v, const SelectionVector &sel, idx_t count);
	//! Inserts every hash the vector carries
	void InsertHashes(const Vector &hashes_v);
	idx_t LookupHashes(const Vector &hashes_v, SelectionVector &result_sel, idx_t count) const;
	//! result_sel contains local positions into sel rather than source row ids.
	idx_t LookupHashes(const Vector &hashes_v, const SelectionVector &sel, SelectionVector &result_sel,
	                   idx_t count) const;

	void InsertOne(hash_t hash);
	//! A sealed filter is immutable, so lookups read the sectors without atomics.
	inline bool LookupOne(hash_t hash) const {
		D_ASSERT(initialized);
		D_ASSERT(sealed);
		const auto bf_offset = HashBloomFilterSector(hash, bitmask);
		const auto mask = BloomFilter::GetMask(hash);
		return (bf[bf_offset] & mask) == mask;
	}
	//! Folds the filter down to the smallest size that still meets the target false positive rate, releases the
	//! memory that folding freed up, and marks the filter complete.
	void Seal(ClientContext &context);
	//! Whether a lookup batch of count probes is large enough, over a filter large enough, to prefetch sectors
	bool ShouldPrefetch(idx_t count) const;

	static inline uint64_t GetMask(hash_t hash) {
		return HashBloomFilterMask(hash);
	}

	bool IsInitialized() const {
		return initialized;
	}
	bool IsSealed() const {
		return sealed;
	}
	//! Whether the sealed filter still rejects enough for pushing it into a scan to pay off
	bool IsUseful() const {
		D_ASSERT(sealed);
		return useful;
	}
	bool IsPublished() const {
		return published;
	}
	//! Marks the filter as handed to a probe-side consumer: it must stay immutable afterwards
	void MarkPublished() {
		published = true;
	}

	const uint64_t *Data() const {
		return bf;
	}

	uint64_t Bitmask() const {
		return bitmask;
	}

	static idx_t GetNumberOfSectors(idx_t number_of_rows);

private:
	//! Halves the filter until only new_num_sectors sectors remain. Bit-exact with a filter that was built at
	//! the smaller size from the start, because the sector index is the low bits of the hash and the bits set
	//! within a sector are taken from the high bits.
	void Fold(idx_t new_num_sectors);
	//! Fraction of bits that are set, over the sectors currently in use
	double Density() const;

private:
	idx_t num_sectors;
	//! Sector capacity of buf_, so an unpublished filter can be reused across recursive iterations
	idx_t allocated_sectors = 0;
	uint64_t bitmask; // num_sectors - 1 -> used to get the sector offset

	bool initialized = false;
	bool sealed = false;
	bool useful = false;
	bool published = false;
	AllocatedData buf_;
	uint64_t *bf;
};

//! FunctionData for bloom filter internal function
struct BloomFilterFunctionData : public FunctionData {
	BloomFilterFunctionData(shared_ptr<const BloomFilter> filter_p, bool filters_null_values_p,
	                        const string &key_column_name_p, const LogicalType &key_type_p);

	//! Selectivity policy is owned by the selectivity-optional wrapper; the primitive only implements matching.
	//! Published filters are shared and immutable: the hash table may drop its own reference at any time
	shared_ptr<const BloomFilter> filter;
	bool filters_null_values;
	string key_column_name;
	LogicalType key_type;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//! Runtime prefix-range filter state used by join pushdown and internal tablefilter functions.
struct PrefixRangeLookupData {
	uint64_t min = 0;
	uint64_t span = 0;
	idx_t shift = 0;
	const uint64_t *bitmap = nullptr;
};

class PrefixRangeFilter {
public:
	struct BuildState {
		virtual ~BuildState() = default;
		template <class TARGET>

		TARGET &Cast() {
			DynamicCastCheck<TARGET>(this);
			return reinterpret_cast<TARGET &>(*this);
		}
		template <class TARGET>
		const TARGET &Cast() const {
			DynamicCastCheck<TARGET>(this);
			return reinterpret_cast<const TARGET &>(*this);
		}
	};

	virtual ~PrefixRangeFilter() = default;
	virtual void Initialize(ClientContext &context, idx_t number_of_rows, Value min, Value max, idx_t max_bits) = 0;
	virtual unique_ptr<BuildState> InitializeBuildState(ClientContext &context) const = 0;
	virtual void InsertKeys(Vector &keys, BuildState &state) const = 0;
	virtual void InsertKeysParallel(Vector &keys, BuildState &state) const = 0;
	virtual void MergeBuildState(BuildState &state) = 0;
	virtual idx_t GetBuildStateSize() const = 0;
	virtual idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const = 0;
	//! result_sel contains local positions into sel rather than source row ids.
	virtual idx_t LookupKeys(Vector &keys, const SelectionVector &sel, SelectionVector &result_sel,
	                         idx_t count) const = 0;
	//! Exposes the bitmap of an integral filter (NULL-free lookup over the unsigned comparable domain).
	virtual bool GetSignedLookupData(PrefixRangeLookupData &result) const = 0;
	virtual idx_t DistinctCountUpperBound() const = 0;
	//! Estimate how many distinct filter keys can overlap a scan range. This is
	//! planning evidence only: lookup remains the semantic authority.
	virtual idx_t EstimateDistinctCountInRange(const Value &lower_bound, const Value &upper_bound) const {
		return DistinctCountUpperBound();
	}
	virtual FilterPropagateResult LookupRange(const Value &lower_bound, const Value &upper_bound) const = 0;
	virtual bool IsInitialized() const = 0;
	static bool SupportedType(const LogicalType &type);
	static unique_ptr<PrefixRangeFilter> CreatePrefixRangeFilter(const LogicalType &key_type);
	static bool TryComputeSpan(const Value &lower_bound, const Value &upper_bound, uhugeint_t &result);
};

//! FunctionData for prefix range internal function
struct PrefixRangeFunctionData : public FunctionData {
	PrefixRangeFunctionData(optional_ptr<PrefixRangeFilter> filter_p, bool filters_null_values_p,
	                        const string &key_column_name_p, const LogicalType &key_type_p,
	                        shared_ptr<ExecutionRuntimeFilterIdentity> runtime_filter_identity_p = nullptr);

	//! Selectivity policy is owned by the selectivity-optional wrapper; the primitive only implements matching.
	optional_ptr<PrefixRangeFilter> filter;
	bool filters_null_values;
	string key_column_name;
	LogicalType key_type;
	//! Execution-region identity of the build side that publishes this filter (JIT export ABI).
	shared_ptr<ExecutionRuntimeFilterIdentity> runtime_filter_identity;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//! Runtime dynamic-filter state shared by internal tablefilter functions.
struct DynamicFilterData {
public:
	DynamicFilterData(ExpressionType comparison_type, Value constant);

	mutable annotated_shared_mutex lock;
	ExpressionType comparison_type;
	Value constant DUCKDB_GUARDED_BY(lock);
	atomic<bool> initialized = {false};

	unique_ptr<Expression> ToExpression(const Expression &column) const;

	void SetValue(Value val);
	void Reset();
	static bool CompareValue(ExpressionType comparison_type, const Value &constant, const Value &value);
	static FilterPropagateResult CheckStatistics(const BaseStatistics &stats, ExpressionType comparison_type,
	                                             const Value &constant);
};

//! FunctionData for dynamic filter internal function
struct DynamicFilterFunctionData : public FunctionData {
	explicit DynamicFilterFunctionData(shared_ptr<DynamicFilterData> filter_data_p);

	shared_ptr<DynamicFilterData> filter_data;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//! FunctionData for optional filter internal function (always returns TRUE, used for statistics only)
struct OptionalFilterFunctionData : public FunctionData {
	//! The child filter expression, used for CheckStatistics via FilterPruneCallback
	unique_ptr<Expression> child_filter_expr;

	explicit OptionalFilterFunctionData(unique_ptr<Expression> child_filter_expr_p);

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//! FunctionData for selectivity-optional filter internal function
struct SelectivityOptionalFilterFunctionData : public FunctionData {
	//! The child filter expression, executed only while it remains selective enough
	unique_ptr<Expression> child_filter_expr;
	float selectivity_threshold;
	idx_t n_vectors_to_check;

	SelectivityOptionalFilterFunctionData(unique_ptr<Expression> child_filter_expr_p, float selectivity_threshold_p,
	                                      idx_t n_vectors_to_check_p);

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

//! Factory for bloom filter internal function
struct BloomFilterScalarFun : public TableFilterBloomFilterFun {
	using TableFilterBloomFilterFun::GetFunction;
	static constexpr const char *NAME = TableFilterBloomFilterFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &column_name, const string &key_column_name);
};

//! Factory for prefix range internal function
struct PrefixRangeScalarFun : public TableFilterPrefixRangeFun {
	using TableFilterPrefixRangeFun::GetFunction;
	static constexpr const char *NAME = TableFilterPrefixRangeFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &column_name, const string &key_column_name);
};

//! Factory for dynamic filter internal function
struct DynamicFilterScalarFun : public TableFilterDynamicFun {
	using TableFilterDynamicFun::GetFunction;
	static constexpr const char *NAME = TableFilterDynamicFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &column_name, bool has_filter_data);
};

//! Factory for optional filter internal function (always returns TRUE)
struct OptionalFilterScalarFun : public TableFilterOptionalFun {
	using TableFilterOptionalFun::GetFunction;
	static constexpr const char *NAME = TableFilterOptionalFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &child_filter_string);
};

//! Factory for selectivity-optional filter internal function
struct SelectivityOptionalFilterScalarFun : public TableFilterSelectivityOptionalFun {
	using TableFilterSelectivityOptionalFun::GetFunction;
	static constexpr const char *NAME = TableFilterSelectivityOptionalFun::Name;
	static ScalarFunction GetFunction(const LogicalType &input_type);
	static FilterPropagateResult FilterPrune(const FunctionStatisticsPruneInput &input);
	static string ToString(const string &child_filter_string);
};

} // namespace duckdb
