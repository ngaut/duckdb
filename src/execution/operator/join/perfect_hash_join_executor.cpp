#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"

#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/execution/execution_hash_join_runtime.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"

namespace duckdb {

PerfectHashJoinExecutor::PerfectHashJoinExecutor(const PhysicalHashJoin &join_p, JoinHashTable &ht_p)
    : join(join_p), ht(ht_p), runtime_filter_identity(make_shared_ptr<ExecutionRuntimeFilterIdentity>()) {
}

const LogicalType &PerfectHashJoinExecutor::GetKeyType() const {
	return ht.equality_types[0];
}

//===--------------------------------------------------------------------===//
// Initialize
//===--------------------------------------------------------------------===//
bool ExtractNumericValue(Value val, hugeint_t &result) {
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
		if (!val.DefaultTryCastAs(LogicalType::HUGEINT)) {
			return false;
		}
		result = val.GetValue<hugeint_t>();
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
	if (perfect_join_statistics.is_build_small) {
		return true; // Already true based on static statistics
	}

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

	// And when the build range is smaller than the threshold
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
bool PerfectHashJoinExecutor::BuildPerfectHashTable() {
	// First, allocate memory for each build column
	const auto build_size = perfect_join_statistics.build_range + 1;
	for (const auto &type : join.rhs_output_columns.col_types) {
		perfect_hash_table.emplace_back(DictionaryVector::CreateReusableDictionary(type, build_size));
	}

	// and for duplicate_checking
	bitmap_build_idx.Initialize(build_size);
	bitmap_build_idx.SetAllInvalid(build_size);
	const auto build_word_count = ValidityMask::EntryCount(build_size);
	bitmap_build_non_empty_words.Initialize(build_word_count);
	bitmap_build_non_empty_words.SetAllInvalid(build_word_count);

	// Now fill columns with build data
	return FullScanHashTable();
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

bool PerfectHashJoinExecutor::PublishExecutionPerfectHashJoinFilterLayout() {
	auto &layout = execution_filter_layout;
	layout = ExecutionPerfectHashJoinFilterLayout();
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
	layout.build_validity_non_empty_words = bitmap_build_non_empty_words.GetData();
	layout.build_validity_word_count = ValidityMask::EntryCount(layout.build_capacity);
	layout.ready = true;
	return true;
}

optional_ptr<const ExecutionPerfectHashJoinFilterLayout>
PerfectHashJoinExecutor::GetExecutionPerfectHashJoinFilterLayout() const {
	if (!execution_filter_layout.ready) {
		return nullptr;
	}
	return execution_filter_layout;
}

bool PerfectHashJoinExecutor::GetExecutionPerfectHashJoinTableLayout(
    ExecutionPerfectHashJoinTableLayout &layout) const {
	layout = ExecutionPerfectHashJoinTableLayout();
	layout.key_type = GetKeyType();
	auto filter_layout = GetExecutionPerfectHashJoinFilterLayout();
	if (!filter_layout) {
		layout.blocker = "perfect-hash-join-native-layout-not-published";
		return false;
	}
	layout.key_physical_type = filter_layout->key_physical_type;
	layout.is_build_dense = filter_layout->is_build_dense;
	layout.build_range = filter_layout->build_range;
	layout.build_capacity = filter_layout->build_capacity;
	layout.build_unique_count = filter_layout->build_unique_count;
	layout.build_min = filter_layout->build_min;
	layout.build_max = filter_layout->build_max;
	layout.build_min_128 = filter_layout->build_min_128;
	layout.build_max_128 = filter_layout->build_max_128;
	layout.build_min_u128 = filter_layout->build_min_u128;
	layout.build_max_u128 = filter_layout->build_max_u128;
	layout.build_validity = filter_layout->build_validity;
	layout.build_validity_non_empty_words = filter_layout->build_validity_non_empty_words;
	layout.build_validity_word_count = filter_layout->build_validity_word_count;
	layout.runtime_filter_identity = runtime_filter_identity;
	layout.rhs_output_column_count = perfect_hash_table.size();
	layout.rhs_output_types = join.rhs_output_columns.col_types;
	layout.rhs_dictionary_buffers = perfect_hash_table;
	if (layout.rhs_output_column_count != layout.rhs_output_types.size()) {
		layout.blocker = "perfect-hash-join-native-layout-rhs-shape-mismatch";
		return false;
	}
	layout.ready = true;
	layout.blocker.clear();
	return true;
}

bool PerfectHashJoinExecutor::FullScanHashTable() {
	auto &data_collection = ht.GetDataCollection();

	// TODO: In a parallel finalize: One should exclusively lock and each thread should do one part of the code below.
	Vector tuples_addresses(LogicalType::POINTER, ht.Count()); // allocate space for all the tuples
	Vector build_vector(GetKeyType(), ht.Count());
	auto key_count = ht.ScanKeyColumn(tuples_addresses, build_vector, 0);

	// Now fill the selection vector using the build keys and create a sequential vector
	// TODO: add check for fast pass when probe is part of build domain
	SelectionVector sel_build(key_count + 1);
	SelectionVector sel_tuples(key_count + 1);
	bool success = FillSelectionVectorSwitchBuild(build_vector, sel_build, sel_tuples, key_count);

	// early out
	if (!success) {
		return false;
	}

	const auto build_size = perfect_join_statistics.build_range + 1;
	if (unique_keys == build_size && !ht.has_filtered_null) {
		perfect_join_statistics.is_build_dense = true;
		bitmap_build_idx.Reset(build_size); // All valid
		bitmap_build_non_empty_words.Reset(ValidityMask::EntryCount(build_size));
	}
	key_count = unique_keys; // do not consider keys out of the range

	// Full scan the remaining build columns and fill the perfect hash table
	for (idx_t i = 0; i < join.rhs_output_columns.col_types.size(); i++) {
		auto &vector = perfect_hash_table[i]->data;
		const auto output_col_idx = ht.output_columns[i];
		D_ASSERT(vector.GetType() == ht.layout_ptr->GetTypes()[output_col_idx]);
		auto &col_mask = FlatVector::ValidityMutable(vector);
		col_mask.Reset(build_size);
		data_collection.Gather(tuples_addresses, sel_tuples, key_count, output_col_idx, vector, sel_build, nullptr);
		// This ensures the empty entries are set to NULL, so that the emitted dictionary vectors make sense
		col_mask.Combine(bitmap_build_idx, build_size);
	}

	if (!PublishExecutionPerfectHashJoinFilterLayout()) {
		return false;
	}

	return true;
}

bool PerfectHashJoinExecutor::FillSelectionVectorSwitchBuild(const Vector &source, SelectionVector &sel_vec,
                                                             SelectionVector &seq_sel_vec, idx_t count) {
	switch (source.GetType().InternalType()) {
	case PhysicalType::INT8:
		return TemplatedFillSelectionVectorBuild<int8_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT16:
		return TemplatedFillSelectionVectorBuild<int16_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT32:
		return TemplatedFillSelectionVectorBuild<int32_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT64:
		return TemplatedFillSelectionVectorBuild<int64_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT128:
		return TemplatedFillSelectionVectorBuild<hugeint_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT8:
		return TemplatedFillSelectionVectorBuild<uint8_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT16:
		return TemplatedFillSelectionVectorBuild<uint16_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT32:
		return TemplatedFillSelectionVectorBuild<uint32_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT64:
		return TemplatedFillSelectionVectorBuild<uint64_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT128:
		return TemplatedFillSelectionVectorBuild<uhugeint_t>(source, sel_vec, seq_sel_vec, count);
	default:
		throw NotImplementedException("Type not supported for perfect hash join");
	}
}

template <typename T>
bool PerfectHashJoinExecutor::TemplatedFillSelectionVectorBuild(const Vector &source, SelectionVector &sel_vec,
                                                                SelectionVector &seq_sel_vec, idx_t count) {
	if (perfect_join_statistics.build_min.IsNull() || perfect_join_statistics.build_max.IsNull()) {
		return false;
	}
	auto min_value = perfect_join_statistics.build_min.GetValueUnsafe<T>();
	auto max_value = perfect_join_statistics.build_max.GetValueUnsafe<T>();
	auto entries = source.Values<T>();
	// generate the selection vector
	for (idx_t i = 0, sel_idx = 0; i < count; ++i) {
		auto input_value = entries.GetValueUnsafe(i);
		// add index to selection vector if value in the range
		if (min_value <= input_value && input_value <= max_value) {
			auto idx = UnsafeNumericCast<idx_t>(input_value - min_value); // subtract min value to get the idx position
			sel_vec.set_index(sel_idx, idx);
			if (bitmap_build_idx.RowIsValidUnsafe(idx)) {
				return false;
			} else {
				bitmap_build_idx.SetValidUnsafe(idx);
				bitmap_build_non_empty_words.SetValidUnsafe(idx / ValidityMask::BITS_PER_VALUE);
				unique_keys++;
			}
			seq_sel_vec.set_index(sel_idx++, i);
		}
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Probe
//===--------------------------------------------------------------------===//
class PerfectHashJoinState : public OperatorState {
public:
	PerfectHashJoinState(ClientContext &context, const PhysicalHashJoin &join) : probe_executor(context) {
		join_keys.Initialize(Allocator::Get(context), join.condition_types);
		for (auto &cond : join.conditions) {
			probe_executor.AddExpression(cond.GetLHS());
		}
		build_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
		probe_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
		seq_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
	}

	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	SelectionVector build_sel_vec;
	SelectionVector probe_sel_vec;
	SelectionVector seq_sel_vec;
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
		result_vector.Dictionary(perfect_hash_table[i], state.build_sel_vec, probe_sel_count);
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

idx_t PerfectHashJoinExecutor::FilterSelection(const UnifiedVectorFormat &source, const LogicalType &source_type,
                                               optional_ptr<const SelectionVector> input_sel, idx_t count,
                                               SelectionVector &result_sel) const {
	const bool has_null = !source.validity.CannotHaveNull();
	switch (source_type.InternalType()) {
	case PhysicalType::INT8:
		return has_null ? FilterSelectionTargetSwitch<int8_t, true>(source, input_sel, count, result_sel)
		                : FilterSelectionTargetSwitch<int8_t, false>(source, input_sel, count, result_sel);
	case PhysicalType::INT16:
		return has_null ? FilterSelectionTargetSwitch<int16_t, true>(source, input_sel, count, result_sel)
		                : FilterSelectionTargetSwitch<int16_t, false>(source, input_sel, count, result_sel);
	case PhysicalType::INT32:
		return has_null ? FilterSelectionTargetSwitch<int32_t, true>(source, input_sel, count, result_sel)
		                : FilterSelectionTargetSwitch<int32_t, false>(source, input_sel, count, result_sel);
	case PhysicalType::INT64:
		return has_null ? FilterSelectionTargetSwitch<int64_t, true>(source, input_sel, count, result_sel)
		                : FilterSelectionTargetSwitch<int64_t, false>(source, input_sel, count, result_sel);
	case PhysicalType::INT128:
		return has_null ? FilterSelectionTargetSwitch<hugeint_t, true>(source, input_sel, count, result_sel)
		                : FilterSelectionTargetSwitch<hugeint_t, false>(source, input_sel, count, result_sel);
	case PhysicalType::UINT8:
		return has_null ? FilterSelectionTargetSwitch<uint8_t, true>(source, input_sel, count, result_sel)
		                : FilterSelectionTargetSwitch<uint8_t, false>(source, input_sel, count, result_sel);
	case PhysicalType::UINT16:
		return has_null ? FilterSelectionTargetSwitch<uint16_t, true>(source, input_sel, count, result_sel)
		                : FilterSelectionTargetSwitch<uint16_t, false>(source, input_sel, count, result_sel);
	case PhysicalType::UINT32:
		return has_null ? FilterSelectionTargetSwitch<uint32_t, true>(source, input_sel, count, result_sel)
		                : FilterSelectionTargetSwitch<uint32_t, false>(source, input_sel, count, result_sel);
	case PhysicalType::UINT64:
		return has_null ? FilterSelectionTargetSwitch<uint64_t, true>(source, input_sel, count, result_sel)
		                : FilterSelectionTargetSwitch<uint64_t, false>(source, input_sel, count, result_sel);
	case PhysicalType::UINT128:
		return has_null ? FilterSelectionTargetSwitch<uhugeint_t, true>(source, input_sel, count, result_sel)
		                : FilterSelectionTargetSwitch<uhugeint_t, false>(source, input_sel, count, result_sel);
	default:
		throw NotImplementedException("Type not supported");
	}
}

template <typename SOURCE, bool HAS_NULL>
idx_t PerfectHashJoinExecutor::FilterSelectionTargetSwitch(const UnifiedVectorFormat &source,
                                                           optional_ptr<const SelectionVector> input_sel, idx_t count,
                                                           SelectionVector &result_sel) const {
	switch (GetKeyType().InternalType()) {
	case PhysicalType::INT8:
		return TemplatedFilterSelection<SOURCE, int8_t, HAS_NULL>(source, input_sel, count, result_sel);
	case PhysicalType::INT16:
		return TemplatedFilterSelection<SOURCE, int16_t, HAS_NULL>(source, input_sel, count, result_sel);
	case PhysicalType::INT32:
		return TemplatedFilterSelection<SOURCE, int32_t, HAS_NULL>(source, input_sel, count, result_sel);
	case PhysicalType::INT64:
		return TemplatedFilterSelection<SOURCE, int64_t, HAS_NULL>(source, input_sel, count, result_sel);
	case PhysicalType::INT128:
		return TemplatedFilterSelection<SOURCE, hugeint_t, HAS_NULL>(source, input_sel, count, result_sel);
	case PhysicalType::UINT8:
		return TemplatedFilterSelection<SOURCE, uint8_t, HAS_NULL>(source, input_sel, count, result_sel);
	case PhysicalType::UINT16:
		return TemplatedFilterSelection<SOURCE, uint16_t, HAS_NULL>(source, input_sel, count, result_sel);
	case PhysicalType::UINT32:
		return TemplatedFilterSelection<SOURCE, uint32_t, HAS_NULL>(source, input_sel, count, result_sel);
	case PhysicalType::UINT64:
		return TemplatedFilterSelection<SOURCE, uint64_t, HAS_NULL>(source, input_sel, count, result_sel);
	case PhysicalType::UINT128:
		return TemplatedFilterSelection<SOURCE, uhugeint_t, HAS_NULL>(source, input_sel, count, result_sel);
	default:
		throw NotImplementedException("Type not supported");
	}
}

template <typename SOURCE, typename TARGET, bool HAS_NULL>
idx_t PerfectHashJoinExecutor::TemplatedFilterSelection(const UnifiedVectorFormat &source,
                                                        optional_ptr<const SelectionVector> input_sel, idx_t count,
                                                        SelectionVector &result_sel) const {
	if constexpr (std::is_same<SOURCE, TARGET>::value && !HAS_NULL) {
		return perfect_join_statistics.is_build_dense
		           ? TemplatedFilterSelectionLayoutSwitch<SOURCE, TARGET, HAS_NULL, true>(source, input_sel, count,
		                                                                                  result_sel)
		           : TemplatedFilterSelectionLayoutSwitch<SOURCE, TARGET, HAS_NULL, false>(source, input_sel, count,
		                                                                                   result_sel);
	} else {
		const auto min_value = perfect_join_statistics.build_min.GetValueUnsafe<TARGET>();
		const auto max_value = perfect_join_statistics.build_max.GetValueUnsafe<TARGET>();
		const auto data = UnifiedVectorFormat::GetData<const SOURCE>(source);
		const auto input_selection = input_sel ? input_sel->data() : nullptr;
		const auto source_selection = source.sel->data();
		idx_t result_count = 0;
		for (idx_t i = 0; i < count; i++) {
			const auto row_idx = input_selection ? input_selection[i] : i;
			const auto source_idx = source_selection ? source_selection[row_idx] : row_idx;
			if (HAS_NULL && !source.validity.RowIsValidUnsafe(source_idx)) {
				continue;
			}
			TARGET input_value;
			if constexpr (std::is_same<SOURCE, TARGET>::value) {
				input_value = data[source_idx];
			} else if (!TryCast::Operation(data[source_idx], input_value)) {
				continue;
			}
			if (input_value >= min_value && input_value <= max_value &&
			    (perfect_join_statistics.is_build_dense ||
			     bitmap_build_idx.RowIsValidUnsafe(UnsafeNumericCast<idx_t>(input_value - min_value)))) {
				result_sel.set_index(result_count++, row_idx);
			}
		}
		return result_count;
	}
}

template <typename SOURCE, typename TARGET, bool HAS_NULL, bool BUILD_DENSE>
idx_t PerfectHashJoinExecutor::TemplatedFilterSelectionLayoutSwitch(const UnifiedVectorFormat &source,
                                                                    optional_ptr<const SelectionVector> input_sel,
                                                                    idx_t count, SelectionVector &result_sel) const {
	const auto input_selection = input_sel ? input_sel->data() : nullptr;
	const auto source_selection = source.sel->data();
	if (input_selection) {
		return source_selection ? TemplatedFilterSelectionLoop<SOURCE, TARGET, HAS_NULL, BUILD_DENSE, true, true>(
		                              source, input_selection, source_selection, count, result_sel)
		                        : TemplatedFilterSelectionLoop<SOURCE, TARGET, HAS_NULL, BUILD_DENSE, true, false>(
		                              source, input_selection, source_selection, count, result_sel);
	}
	return source_selection ? TemplatedFilterSelectionLoop<SOURCE, TARGET, HAS_NULL, BUILD_DENSE, false, true>(
	                              source, input_selection, source_selection, count, result_sel)
	                        : TemplatedFilterSelectionLoop<SOURCE, TARGET, HAS_NULL, BUILD_DENSE, false, false>(
	                              source, input_selection, source_selection, count, result_sel);
}

template <typename SOURCE, typename TARGET, bool HAS_NULL, bool BUILD_DENSE, bool INPUT_SELECTED, bool SOURCE_SELECTED>
idx_t PerfectHashJoinExecutor::TemplatedFilterSelectionLoop(const UnifiedVectorFormat &source,
                                                            const sel_t *input_selection, const sel_t *source_selection,
                                                            idx_t count, SelectionVector &result_sel) const {
	const auto min_value = perfect_join_statistics.build_min.GetValueUnsafe<TARGET>();
	const auto max_value = perfect_join_statistics.build_max.GetValueUnsafe<TARGET>();
	const auto data = UnifiedVectorFormat::GetData<const SOURCE>(source);
	auto result_selection = result_sel.data();
	idx_t result_count = 0;
	idx_t i = 0;
	if constexpr (std::is_same<SOURCE, TARGET>::value && !HAS_NULL && !BUILD_DENSE) {
		auto row_index = [&](idx_t input_idx) {
			return UnsafeNumericCast<sel_t>(INPUT_SELECTED ? input_selection[input_idx] : input_idx);
		};
		auto matches = [&](sel_t row_idx) {
			const auto source_idx = SOURCE_SELECTED ? source_selection[row_idx] : row_idx;
			const auto input_value = data[source_idx];
			return input_value >= min_value && input_value <= max_value &&
			       bitmap_build_idx.RowIsValidUnsafe(UnsafeNumericCast<idx_t>(input_value - min_value));
		};
		for (; i + 4 <= count; i += 4) {
			const auto row0 = row_index(i);
			const auto row1 = row_index(i + 1);
			const auto row2 = row_index(i + 2);
			const auto row3 = row_index(i + 3);
			const auto match0 = matches(row0);
			const auto match1 = matches(row1);
			const auto match2 = matches(row2);
			const auto match3 = matches(row3);
			D_ASSERT(result_count + 4 <= result_sel.Capacity());
			if (match0) {
				result_selection[result_count++] = row0;
			}
			if (match1) {
				result_selection[result_count++] = row1;
			}
			if (match2) {
				result_selection[result_count++] = row2;
			}
			if (match3) {
				result_selection[result_count++] = row3;
			}
		}
	}
	for (; i < count; i++) {
		const auto row_idx = INPUT_SELECTED ? input_selection[i] : i;
		const auto source_idx = SOURCE_SELECTED ? source_selection[row_idx] : row_idx;
		if (HAS_NULL && !source.validity.RowIsValidUnsafe(source_idx)) {
			continue;
		}
		TARGET input_value;
		if constexpr (std::is_same<SOURCE, TARGET>::value) {
			input_value = data[source_idx];
		} else if (!TryCast::Operation(data[source_idx], input_value)) {
			continue;
		}
		if (input_value >= min_value && input_value <= max_value &&
		    (BUILD_DENSE || bitmap_build_idx.RowIsValidUnsafe(UnsafeNumericCast<idx_t>(input_value - min_value)))) {
			D_ASSERT(result_count < result_sel.Capacity());
			result_selection[result_count++] = UnsafeNumericCast<sel_t>(row_idx);
		}
	}
	return result_count;
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
