#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"

#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/execution/execution_hash_join_runtime.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/planner/joinside.hpp"

namespace duckdb {

PerfectHashJoinExecutor::PerfectHashJoinExecutor(const PhysicalHashJoin &join_p, JoinHashTable &ht_p)
    : join(join_p), ht(ht_p) {
}

const LogicalType &PerfectHashJoinExecutor::GetKeyType() const {
	return ht.equality_types[0];
}

//===--------------------------------------------------------------------===//
// Initialize
//===--------------------------------------------------------------------===//
bool ExtractNumericValue(const Value &val, hugeint_t &result) {
	if (!val.type().IsIntegral()) {
		switch (val.type().InternalType()) {
		case PhysicalType::INT8:
			result = Hugeint::Convert(val.GetValueUnsafe<int8_t>());
			break;
		case PhysicalType::INT16:
			result = Hugeint::Convert(val.GetValueUnsafe<int16_t>());
			break;
		case PhysicalType::INT32:
			result = Hugeint::Convert(val.GetValueUnsafe<int32_t>());
			break;
		case PhysicalType::INT64:
			result = Hugeint::Convert(val.GetValueUnsafe<int64_t>());
			break;
		case PhysicalType::INT128:
			result = val.GetValueUnsafe<hugeint_t>();
			break;
		case PhysicalType::UINT8:
			result = Hugeint::Convert(val.GetValueUnsafe<uint8_t>());
			break;
		case PhysicalType::UINT16:
			result = Hugeint::Convert(val.GetValueUnsafe<uint16_t>());
			break;
		case PhysicalType::UINT32:
			result = Hugeint::Convert(val.GetValueUnsafe<uint32_t>());
			break;
		case PhysicalType::UINT64:
			result = Hugeint::Convert(val.GetValueUnsafe<uint64_t>());
			break;
		case PhysicalType::UINT128: {
			const auto uhugeint_val = val.GetValueUnsafe<uhugeint_t>();
			if (uhugeint_val > NumericCast<uhugeint_t>(NumericLimits<hugeint_t>::Maximum())) {
				return false;
			}
			result.lower = uhugeint_val.lower;
			result.upper = NumericCast<int64_t>(uhugeint_val.upper);
			break;
		}
		default:
			return false;
		}
	} else {
		auto cast = val.DefaultTryCastAs(LogicalType::HUGEINT);
		if (!cast) {
			return false;
		}
		result = cast->GetValue<hugeint_t>();
	}
	return true;
}

bool PerfectHashJoinExecutor::CanDoPerfectHashJoin(const PhysicalHashJoin &op, const Value &min, const Value &max) {
	// TODO: Add support for residual predicates
	if (op.predicate) {
		return false;
	}

	// We only do this optimization for inner joins with one integer equality condition
	const auto key_type = op.conditions[0].GetLHS().GetReturnType();
	if (op.join_type != JoinType::INNER || op.conditions.size() != 1 ||
	    op.conditions[0].GetComparisonType() != ExpressionType::COMPARE_EQUAL ||
	    !TypeIsInteger(key_type.InternalType())) {
		return false;
	}
	// Physical perfect hashing supports the complete integral key domain. The execution layout carries the
	// corresponding full-width bounds so native backends can preserve the same contract without truncation.
	// We bail out if there are nested types on the RHS
	for (auto &type : op.children[1].get().GetTypes()) {
		switch (type.InternalType()) {
		case PhysicalType::STRUCT:
		case PhysicalType::LIST:
		case PhysicalType::ARRAY:
			return false;
		default:
			break;
		}
	}

	// Recompute the range on every call. Planning-time statistics are sufficient
	// to decide whether collecting build bounds is worthwhile, but prepared
	// physical plans survive DML. Finalization must therefore replace the
	// preliminary range with bounds from the current build input.
	perfect_join_statistics.is_build_small = false;
	perfect_join_statistics.build_min = min;
	perfect_join_statistics.build_max = max;
	static constexpr idx_t DEFAULT_MAX_BUILD_SIZE = 1048576;
	static constexpr idx_t EXTENDED_MAX_BUILD_SIZE = 2097152;
	if (key_type.InternalType() == PhysicalType::UINT128) {
		const auto min_value = perfect_join_statistics.build_min.GetValueUnsafe<uhugeint_t>();
		const auto max_value = perfect_join_statistics.build_max.GetValueUnsafe<uhugeint_t>();
		if (Uhugeint::LessThan(max_value, min_value)) {
			return false;
		}
		auto build_range = max_value;
		if (!Uhugeint::TrySubtractInPlace(build_range, min_value) ||
		    build_range > Uhugeint::Convert(EXTENDED_MAX_BUILD_SIZE)) {
			return false;
		}
		perfect_join_statistics.build_range = NumericCast<idx_t>(build_range);
		if (ht.Count() > perfect_join_statistics.build_range + 1 ||
		    (perfect_join_statistics.build_range > DEFAULT_MAX_BUILD_SIZE &&
		     ht.Count() < (perfect_join_statistics.build_range + 2) / 2)) {
			return false;
		}
		perfect_join_statistics.is_build_small = true;
		return true;
	}
	hugeint_t min_value, max_value;
	if (!ExtractNumericValue(perfect_join_statistics.build_min, min_value) ||
	    !ExtractNumericValue(perfect_join_statistics.build_max, max_value)) {
		return false;
	}
	if (max_value < min_value) {
		return false; // Empty table
	}

	hugeint_t build_range;
	if (!TrySubtractOperator::Operation(max_value, min_value, build_range)) {
		return false;
	}

	// Keep the established one-million-key range for regular joins, and allow a
	// dense extended range when direct lookup removes enough hash-probe work to
	// justify the additional RHS dictionary materialization.
	if (build_range > Hugeint::Convert(EXTENDED_MAX_BUILD_SIZE)) {
		return false;
	}
	perfect_join_statistics.build_range = NumericCast<idx_t>(build_range);

	// If count is larger than range (duplicates), we bail out
	if (ht.Count() > perfect_join_statistics.build_range + 1) {
		return false;
	}
	if (perfect_join_statistics.build_range > DEFAULT_MAX_BUILD_SIZE &&
	    ht.Count() < (perfect_join_statistics.build_range + 2) / 2) {
		return false;
	}

	perfect_join_statistics.is_build_small = true;
	return true;
}

//===--------------------------------------------------------------------===//
// Build
//===--------------------------------------------------------------------===//
void PerfectHashJoinExecutor::InitializePerfectHashTable() {
	const auto build_size = perfect_join_statistics.build_range + 1;
	perfect_hash_table.clear();
	for (const auto &type : join.rhs_output_columns.col_types) {
		// PHJ keeps each entry alive for the operator's lifetime and wraps it in every emitted chunk
		perfect_hash_table.emplace_back(DictionaryVector::CreateReusableGlobalDictionary(type, build_size));
		// Fill tasks clear the bits of NULL values concurrently, so the mask must exist before they start
		FlatVector::ValidityMutable(perfect_hash_table.back()->data).SetAllValid(build_size);
	}
	bitmap_build_idx.Initialize(build_size);
	bitmap_build_idx.SetAllInvalid(build_size);
	unique_keys = 0;
	has_duplicate_key = false;
}

template <typename T>
bool PerfectHashJoinExecutor::TemplatedClaimBuildSlots(const Vector &keys, idx_t count, SelectionVector &slot_sel,
                                                       SelectionVector &row_sel, idx_t &claimed) {
	const auto min_value = perfect_join_statistics.build_min.GetValueUnsafe<T>();
	const auto max_value = perfect_join_statistics.build_max.GetValueUnsafe<T>();
	const auto entries = FlatVector::GetData<T>(keys);
	const auto occupied = bitmap_build_idx.GetData();
	claimed = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto key = entries[i];
		if (key < min_value || max_value < key) {
			continue;
		}
		const auto slot = UnsafeNumericCast<idx_t>(key - min_value);
		const auto bit = validity_t(1) << (slot % ValidityMask::BITS_PER_VALUE);
		auto &word = *reinterpret_cast<atomic<validity_t> *>(&occupied[slot / ValidityMask::BITS_PER_VALUE]);
		if (word.fetch_or(bit, std::memory_order_relaxed) & bit) {
			return false;
		}
		slot_sel.set_index(claimed, slot);
		row_sel.set_index(claimed++, i);
	}
	return true;
}

bool PerfectHashJoinExecutor::ClaimBuildSlots(const Vector &keys, idx_t count, SelectionVector &slot_sel,
                                              SelectionVector &row_sel, idx_t &claimed) {
	switch (keys.GetType().InternalType()) {
	case PhysicalType::INT8:
		return TemplatedClaimBuildSlots<int8_t>(keys, count, slot_sel, row_sel, claimed);
	case PhysicalType::INT16:
		return TemplatedClaimBuildSlots<int16_t>(keys, count, slot_sel, row_sel, claimed);
	case PhysicalType::INT32:
		return TemplatedClaimBuildSlots<int32_t>(keys, count, slot_sel, row_sel, claimed);
	case PhysicalType::INT64:
		return TemplatedClaimBuildSlots<int64_t>(keys, count, slot_sel, row_sel, claimed);
	case PhysicalType::INT128:
		return TemplatedClaimBuildSlots<hugeint_t>(keys, count, slot_sel, row_sel, claimed);
	case PhysicalType::UINT8:
		return TemplatedClaimBuildSlots<uint8_t>(keys, count, slot_sel, row_sel, claimed);
	case PhysicalType::UINT16:
		return TemplatedClaimBuildSlots<uint16_t>(keys, count, slot_sel, row_sel, claimed);
	case PhysicalType::UINT32:
		return TemplatedClaimBuildSlots<uint32_t>(keys, count, slot_sel, row_sel, claimed);
	case PhysicalType::UINT64:
		return TemplatedClaimBuildSlots<uint64_t>(keys, count, slot_sel, row_sel, claimed);
	case PhysicalType::UINT128:
		return TemplatedClaimBuildSlots<uhugeint_t>(keys, count, slot_sel, row_sel, claimed);
	default:
		throw NotImplementedException("Type not supported for perfect hash join");
	}
}

template <idx_t WIDTH>
struct PerfectHashPayloadValue {
	uint8_t bytes[WIDTH];
};

template <class T>
static void ScatterPerfectHashPayloadValues(const Vector &source, const SelectionVector &slot_sel, idx_t count,
                                            Vector &target) {
	const auto source_data = reinterpret_cast<const T *>(FlatVector::GetData(source));
	const auto target_data = reinterpret_cast<T *>(FlatVector::GetDataMutable(target));
	for (idx_t i = 0; i < count; i++) {
		target_data[slot_sel.get_index(i)] = source_data[i];
	}
}

//! Moves a chunk-local gathered payload column into its claimed table slots. Every slot has exactly one writer, but
//! NULL bits of neighbouring slots share validity words with other fill tasks.
static void ScatterPerfectHashPayload(const Vector &source, const SelectionVector &slot_sel, idx_t count,
                                      Vector &target) {
	switch (GetTypeIdSize(source.GetType().InternalType())) {
	case 1:
		ScatterPerfectHashPayloadValues<uint8_t>(source, slot_sel, count, target);
		break;
	case 2:
		ScatterPerfectHashPayloadValues<uint16_t>(source, slot_sel, count, target);
		break;
	case 4:
		ScatterPerfectHashPayloadValues<uint32_t>(source, slot_sel, count, target);
		break;
	case 8:
		ScatterPerfectHashPayloadValues<uint64_t>(source, slot_sel, count, target);
		break;
	case 16:
		ScatterPerfectHashPayloadValues<PerfectHashPayloadValue<16>>(source, slot_sel, count, target);
		break;
	default:
		throw InternalException("Unsupported perfect hash join payload width");
	}
	const auto &source_validity = FlatVector::Validity(source);
	if (source_validity.CannotHaveNull()) {
		return;
	}
	const auto target_validity = FlatVector::ValidityMutable(target).GetData();
	for (idx_t i = 0; i < count; i++) {
		if (source_validity.RowIsValid(i)) {
			continue;
		}
		const auto slot = slot_sel.get_index(i);
		auto &word = *reinterpret_cast<atomic<validity_t> *>(&target_validity[slot / ValidityMask::BITS_PER_VALUE]);
		word.fetch_and(~(validity_t(1) << (slot % ValidityMask::BITS_PER_VALUE)), std::memory_order_relaxed);
	}
}

void PerfectHashJoinExecutor::FillPerfectHashTable(idx_t chunk_begin, idx_t chunk_end) {
	if (chunk_begin == chunk_end || has_duplicate_key.load(std::memory_order_relaxed)) {
		return;
	}
	auto &data_collection = ht.GetDataCollection();
	Vector keys(GetKeyType());
	vector<unique_ptr<Vector>> payload;
	for (const auto &type : join.rhs_output_columns.col_types) {
		payload.push_back(make_uniq<Vector>(type));
	}
	SelectionVector slot_sel(STANDARD_VECTOR_SIZE);
	SelectionVector row_sel(STANDARD_VECTOR_SIZE);
	const auto &incremental = *FlatVector::IncrementalSelectionVector();
	idx_t claimed_keys = 0;
	TupleDataChunkIterator iterator(data_collection, TupleDataPinProperties::KEEP_EVERYTHING_PINNED, chunk_begin,
	                                chunk_end, false);
	do {
		const auto count = iterator.GetCurrentChunkCount();
		if (count == 0) {
			continue;
		}
		auto &row_locations = iterator.GetChunkState().row_locations;
		data_collection.Gather(row_locations, incremental, count, 0, keys, incremental, nullptr);
		idx_t claimed;
		if (!ClaimBuildSlots(keys, count, slot_sel, row_sel, claimed)) {
			has_duplicate_key = true;
			return;
		}
		claimed_keys += claimed;
		for (idx_t i = 0; i < payload.size(); i++) {
			auto &column = *payload[i];
			FlatVector::ValidityMutable(column).Reset();
			data_collection.Gather(row_locations, row_sel, claimed, ht.output_columns[i], column, incremental, nullptr);
			ScatterPerfectHashPayload(column, slot_sel, claimed, perfect_hash_table[i]->data);
		}
	} while (iterator.Next());
	unique_keys += claimed_keys;
}

bool PerfectHashJoinExecutor::FinalizePerfectHashTable() {
	if (has_duplicate_key) {
		return false;
	}
	ht.GetDataCollection().VerifyEverythingPinned();
	const auto build_size = perfect_join_statistics.build_range + 1;
	if (unique_keys == build_size && !ht.has_filtered_null) {
		perfect_join_statistics.is_build_dense = true;
		bitmap_build_idx.Reset(build_size); // All valid
	}
	for (auto &entry : perfect_hash_table) {
		auto &validity = FlatVector::ValidityMutable(entry->data);
		// A column without NULL values keeps the representation that proves it to consumers
		if (validity.CheckAllValid(build_size)) {
			validity.Reset(build_size);
		}
		// Empty slots are NULL, so that the emitted dictionary vectors make sense
		validity.Combine(bitmap_build_idx, build_size);
	}
	return PublishExecutionPerfectHashJoinMembershipLayout();
}

static bool PerfectHashJoinValueBits(const Value &value, PhysicalType physical_type, uint64_t &bits) {
	if (value.IsNull()) {
		return false;
	}
	switch (physical_type) {
	case PhysicalType::BOOL:
		bits = value.GetValueUnsafe<bool>() ? 1 : 0;
		return true;
	case PhysicalType::INT8:
		bits = static_cast<uint64_t>(value.GetValueUnsafe<int8_t>());
		return true;
	case PhysicalType::INT16:
		bits = static_cast<uint64_t>(value.GetValueUnsafe<int16_t>());
		return true;
	case PhysicalType::INT32:
		bits = static_cast<uint64_t>(value.GetValueUnsafe<int32_t>());
		return true;
	case PhysicalType::INT64:
		bits = static_cast<uint64_t>(value.GetValueUnsafe<int64_t>());
		return true;
	case PhysicalType::UINT8:
		bits = static_cast<uint64_t>(value.GetValueUnsafe<uint8_t>());
		return true;
	case PhysicalType::UINT16:
		bits = static_cast<uint64_t>(value.GetValueUnsafe<uint16_t>());
		return true;
	case PhysicalType::UINT32:
		bits = static_cast<uint64_t>(value.GetValueUnsafe<uint32_t>());
		return true;
	case PhysicalType::UINT64:
		bits = value.GetValueUnsafe<uint64_t>();
		return true;
	default:
		return false;
	}
}

bool PerfectHashJoinExecutor::PublishExecutionPerfectHashJoinMembershipLayout() {
	auto &layout = membership_layout;
	layout = ExecutionPerfectHashJoinMembershipLayout();
	layout.key_physical_type = GetKeyType().InternalType();
	if (perfect_join_statistics.build_min.IsNull() || perfect_join_statistics.build_max.IsNull()) {
		return false;
	}
	if (layout.key_physical_type == PhysicalType::INT128) {
		layout.build_min_128 = perfect_join_statistics.build_min.GetValueUnsafe<hugeint_t>();
		layout.build_max_128 = perfect_join_statistics.build_max.GetValueUnsafe<hugeint_t>();
	} else if (layout.key_physical_type == PhysicalType::UINT128) {
		layout.build_min_u128 = perfect_join_statistics.build_min.GetValueUnsafe<uhugeint_t>();
		layout.build_max_u128 = perfect_join_statistics.build_max.GetValueUnsafe<uhugeint_t>();
	} else if (!PerfectHashJoinValueBits(perfect_join_statistics.build_min, layout.key_physical_type,
	                                     layout.build_min) ||
	           !PerfectHashJoinValueBits(perfect_join_statistics.build_max, layout.key_physical_type,
	                                     layout.build_max)) {
		return false;
	}
	layout.is_build_dense = perfect_join_statistics.is_build_dense;
	layout.build_range = perfect_join_statistics.build_range;
	layout.build_capacity = perfect_join_statistics.build_range + 1;
	layout.build_unique_count = unique_keys;
	layout.build_validity = bitmap_build_idx.GetData();
	layout.build_validity_word_count = ValidityMask::EntryCount(layout.build_capacity);
	layout.ready = true;
	return true;
}

bool PerfectHashJoinExecutor::GetExecutionPerfectHashJoinTableLayout(
    ExecutionPerfectHashJoinTableLayout &layout) const {
	layout = ExecutionPerfectHashJoinTableLayout();
	layout.key_type = GetKeyType();
	if (!membership_layout.ready) {
		layout.blocker = "perfect-hash-join-native-layout-not-published";
		return false;
	}
	if (perfect_hash_table.size() != join.rhs_output_columns.col_types.size()) {
		layout.blocker = "perfect-hash-join-native-layout-rhs-shape-mismatch";
		return false;
	}
	// The published membership layout is ready, so copying it last makes the table layout ready only on success.
	static_cast<ExecutionPerfectHashJoinMembershipLayout &>(layout) = membership_layout;
	layout.runtime_filter_identity = ht.GetRuntimeFilterIdentity();
	layout.rhs_output_column_count = perfect_hash_table.size();
	layout.rhs_output_types = join.rhs_output_columns.col_types;
	layout.rhs_dictionary_buffers = perfect_hash_table;
	return true;
}

//===--------------------------------------------------------------------===//
// Probe
//===--------------------------------------------------------------------===//
class PerfectHashJoinState : public OperatorState {
public:
	PerfectHashJoinState(ClientContext &context, const PhysicalHashJoin &join)
	    : probe_executor(context), dictionary_gather_state(join.rhs_output_columns.col_types.size()) {
		join_keys.Initialize(Allocator::Get(context), join.condition_types);
		for (auto &cond : join.conditions) {
			probe_executor.AddExpression(cond.GetLHS());
		}
		build_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
		probe_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
		seq_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
	}

	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		dictionary_gather_state.Reset();
	}

	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	SelectionVector build_sel_vec;
	SelectionVector probe_sel_vec;
	SelectionVector seq_sel_vec;
	DictionaryOwnershipCache dictionary_gather_state;
};

unique_ptr<OperatorState> PerfectHashJoinExecutor::GetOperatorState(ExecutionContext &context) {
	auto state = make_uniq<PerfectHashJoinState>(context.client, join);
	return std::move(state);
}

OperatorResultType PerfectHashJoinExecutor::ProbePerfectHashTable(ExecutionContext &context, DataChunk &input,
                                                                  DataChunk &lhs_output_columns, DataChunk &result,
                                                                  OperatorState &state_p) {
	auto &state = state_p.Cast<PerfectHashJoinState>();
	// keeps track of how many probe keys have a match
	idx_t probe_sel_count = 0;

	// fetch the join keys from the chunk
	state.join_keys.Reset();
	state.probe_executor.Execute(input, state.join_keys);
	// select the keys that are in the min-max range
	const auto &keys_vec = state.join_keys.data[0];
	auto keys_count = state.join_keys.size();
	// todo: add check for fast pass when probe is part of build domain
	FillSelectionVectorSwitchProbe(keys_vec, keys_count, state.probe_sel_vec, probe_sel_count, &state.build_sel_vec);

	// If build is dense and probe is in build's domain, just reference probe
	if (perfect_join_statistics.is_build_dense && keys_count == probe_sel_count) {
		result.Reference(lhs_output_columns);
	} else {
		// otherwise, filter it out the values that do not match
		result.Slice(lhs_output_columns, state.probe_sel_vec, probe_sel_count, 0);
	}
	// on the build side, we need to fetch the data and build dictionary vectors with the sel_vec
	for (idx_t i = 0; i < join.rhs_output_columns.col_types.size(); i++) {
		auto &result_vector = result.data[lhs_output_columns.ColumnCount() + i];
		D_ASSERT(result_vector.GetType() == ht.layout_ptr->GetTypes()[ht.output_columns[i]]);
		result_vector.Dictionary(state.dictionary_gather_state.GetEntry(i, perfect_hash_table[i]), state.build_sel_vec,
		                         probe_sel_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

void PerfectHashJoinExecutor::FillSelectionVectorSwitchProbe(const Vector &source, const idx_t &count,
                                                             SelectionVector &probe_sel_vec, idx_t &probe_sel_count,
                                                             optional_ptr<SelectionVector> build_sel_vec) const {
	if (build_sel_vec) {
		FillSelectionVectorSwitchProbe<true>(source, count, probe_sel_vec, probe_sel_count, build_sel_vec.get());
	} else {
		FillSelectionVectorSwitchProbe<false>(source, count, probe_sel_vec, probe_sel_count, nullptr);
	}
}

template <bool BUILD_SEL_VEC>
void PerfectHashJoinExecutor::FillSelectionVectorSwitchProbe(const Vector &source, const idx_t &count,
                                                             SelectionVector &probe_sel_vec, idx_t &probe_sel_count,
                                                             SelectionVector *build_sel_vec) const {
	D_ASSERT(BUILD_SEL_VEC == static_cast<bool>(build_sel_vec));
	switch (source.GetType().InternalType()) {
	case PhysicalType::INT8:
		TemplatedFillSelectionVectorProbe<int8_t, BUILD_SEL_VEC>(source, count, probe_sel_vec, probe_sel_count,
		                                                         build_sel_vec);
		break;
	case PhysicalType::INT16:
		TemplatedFillSelectionVectorProbe<int16_t, BUILD_SEL_VEC>(source, count, probe_sel_vec, probe_sel_count,
		                                                          build_sel_vec);
		break;
	case PhysicalType::INT32:
		TemplatedFillSelectionVectorProbe<int32_t, BUILD_SEL_VEC>(source, count, probe_sel_vec, probe_sel_count,
		                                                          build_sel_vec);
		break;
	case PhysicalType::INT64:
		TemplatedFillSelectionVectorProbe<int64_t, BUILD_SEL_VEC>(source, count, probe_sel_vec, probe_sel_count,
		                                                          build_sel_vec);
		break;
	case PhysicalType::INT128:
		TemplatedFillSelectionVectorProbe<hugeint_t, BUILD_SEL_VEC>(source, count, probe_sel_vec, probe_sel_count,
		                                                            build_sel_vec);
		break;
	case PhysicalType::UINT8:
		TemplatedFillSelectionVectorProbe<uint8_t, BUILD_SEL_VEC>(source, count, probe_sel_vec, probe_sel_count,
		                                                          build_sel_vec);
		break;
	case PhysicalType::UINT16:
		TemplatedFillSelectionVectorProbe<uint16_t, BUILD_SEL_VEC>(source, count, probe_sel_vec, probe_sel_count,
		                                                           build_sel_vec);
		break;
	case PhysicalType::UINT32:
		TemplatedFillSelectionVectorProbe<uint32_t, BUILD_SEL_VEC>(source, count, probe_sel_vec, probe_sel_count,
		                                                           build_sel_vec);
		break;
	case PhysicalType::UINT64:
		TemplatedFillSelectionVectorProbe<uint64_t, BUILD_SEL_VEC>(source, count, probe_sel_vec, probe_sel_count,
		                                                           build_sel_vec);
		break;
	case PhysicalType::UINT128:
		TemplatedFillSelectionVectorProbe<uhugeint_t, BUILD_SEL_VEC>(source, count, probe_sel_vec, probe_sel_count,
		                                                             build_sel_vec);
		break;
	default:
		throw NotImplementedException("Type not supported");
	}
}

template <typename T, bool BUILD_SEL_VEC>
void PerfectHashJoinExecutor::TemplatedFillSelectionVectorProbe(const Vector &source, const idx_t &count,
                                                                SelectionVector &probe_sel_vec, idx_t &probe_sel_count,
                                                                SelectionVector *build_sel_vec) const {
	D_ASSERT(probe_sel_count == 0);
	const auto min_value = perfect_join_statistics.build_min.GetValueUnsafe<T>();
	const auto max_value = perfect_join_statistics.build_max.GetValueUnsafe<T>();

	auto entries = source.Values<T>();
	// build selection vector for non-dense build
	for (idx_t i = 0; i < count; ++i) {
		auto entry = entries[i];
		if (!entry.IsValid()) {
			continue;
		}
		const auto &input_value = entry.GetValue();
		// add index to selection vector if value in the range
		if (min_value <= input_value && input_value <= max_value) {
			// subtract min value to get the idx
			const auto idx = UnsafeNumericCast<idx_t>(input_value - min_value);
			// position check for matches in the build
			if (bitmap_build_idx.RowIsValid(idx)) {
				if (BUILD_SEL_VEC) {
					build_sel_vec->set_index(probe_sel_count, idx);
				}
				probe_sel_vec.set_index(probe_sel_count++, i);
			}
		}
	}
}

} // namespace duckdb
