//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_function_runtime.hpp"
#include "sljit_native_types.hpp"

#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

const_data_ptr_t NativeIntegerSourceData(UnifiedVectorFormat &format, SljitNativeIntegerKind kind);
data_ptr_t NativeIntegerResultData(Vector &result, SljitNativeIntegerKind kind);
const_data_ptr_t NativeSignedIntegerSourceData(UnifiedVectorFormat &format, SljitNativeSignedIntegerWidth width);
data_ptr_t NativeSignedIntegerResultData(Vector &result, SljitNativeSignedIntegerWidth width);
const_data_ptr_t NativeUnsignedIntegerSourceData(UnifiedVectorFormat &format, SljitNativeUnsignedIntegerWidth width);
data_ptr_t NativeUnsignedIntegerResultData(Vector &result, SljitNativeUnsignedIntegerWidth width);

struct SljitNativePredicateSourceAdapter {
	void Reset();
	void Prepare(DataChunk *input, const vector<idx_t> &input_source_indices);
	const_data_ptr_t *DataArray();
	const sel_t **SelectionArray();
	const validity_t **ValidityArray();
	idx_t SourceCount() const;
	bool SourcesAllValid() const;

private:
	vector<UnifiedVectorFormat> formats;
	vector<const_data_ptr_t> source_data;
	vector<const sel_t *> source_sel;
	vector<const validity_t *> source_validity;
	bool sources_all_valid = false;
};

template <class ADAPTER_SCRATCH>
static SljitNativePredicateInput SljitPrepareNativePredicateInput(ADAPTER_SCRATCH &adapter_scratch, DataChunk &input,
                                                                  const vector<idx_t> &source_indices,
                                                                  const SelectionVector *execute_sel, idx_t count,
                                                                  data_ptr_t result_data, validity_t *result_validity,
                                                                  sel_t *true_sel, sel_t *false_sel) {
	auto &predicate_sources = adapter_scratch.predicate_sources;
	predicate_sources.Prepare(&input, source_indices);

	SljitNativePredicateInput native_input;
	native_input.source_data = predicate_sources.DataArray();
	native_input.source_sel = predicate_sources.SelectionArray();
	native_input.source_validity = predicate_sources.ValidityArray();
	native_input.sources_all_valid = predicate_sources.SourcesAllValid();
	native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
	native_input.result_data = result_data;
	native_input.result_validity = result_validity;
	native_input.true_sel = true_sel;
	native_input.false_sel = false_sel;
	native_input.selected_count = 0;
	native_input.count = count;
	return native_input;
}

} // namespace duckdb
