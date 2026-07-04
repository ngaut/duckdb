//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_runtime_source.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

namespace duckdb {

const sel_t *SljitNormalizedSourceSelectionData(const UnifiedVectorFormat &format);
bool SljitNormalizedSourceAllValid(const UnifiedVectorFormat &format, const sel_t *sel, idx_t count);
bool SljitNormalizedSourceAllValid(const UnifiedVectorFormat &format, const sel_t *source_sel,
                                   const SelectionVector *execute_sel, idx_t count);
bool SljitSourceVectorRowsAllValid(Vector &source, idx_t count, const SelectionVector *sel = nullptr);
const validity_t *SljitNormalizedSourceValidityData(const UnifiedVectorFormat &format, const sel_t *sel, idx_t count);
const validity_t *SljitNormalizedSourceValidityData(const UnifiedVectorFormat &format, const sel_t *source_sel,
                                                    const SelectionVector *execute_sel, idx_t count);
bool SljitUnifiedFormatHasIdentitySelection(const UnifiedVectorFormat &format);
SljitNativeIntegerKind SljitPerfectHashGroupIntegerKind(const LogicalType &type);

class SljitSourceVectorScratch {
public:
	void Resize(idx_t source_count);
	idx_t Count() const;

	UnifiedVectorFormat &PrepareFormat(DataChunk &input, idx_t input_index, idx_t source_idx,
	                                   const char *out_of_range_error);
	bool PrepareFlatSource(DataChunk &input, idx_t input_index, idx_t source_idx, const_data_ptr_t source_data,
	                       idx_t count, const char *out_of_range_error, bool source_known_not_null = false,
	                       const SelectionVector *execute_sel = nullptr);
	bool PrepareDictionarySource(DataChunk &input, idx_t input_index, idx_t source_idx, const_data_ptr_t source_data,
	                             idx_t count, const char *out_of_range_error, bool source_known_not_null = false,
	                             const SelectionVector *execute_sel = nullptr);
	bool PrepareConstantSource(DataChunk &input, idx_t input_index, idx_t source_idx, const_data_ptr_t source_data,
	                           idx_t count, const char *out_of_range_error, bool source_known_not_null = false);
	void SetData(idx_t source_idx, const_data_ptr_t source_data);
	void FinishSource(idx_t source_idx, const SelectionVector *execute_sel, idx_t count,
	                  bool source_known_not_null = false);
	void PrepareTypedExpressionSource(DataChunk &input, idx_t input_index, idx_t source_idx,
	                                  const SelectionVector *execute_sel, idx_t count, const char *out_of_range_error,
	                                  bool source_known_not_null = false);
	void PrepareIntegerSource(DataChunk &input, idx_t input_index, idx_t source_idx,
	                          SljitNativeIntegerKind integer_kind, const SelectionVector *execute_sel, idx_t count,
	                          const char *out_of_range_error, bool source_known_not_null = false);
	void PrepareValiditySource(DataChunk &input, idx_t input_index, idx_t source_idx,
	                           const SelectionVector *execute_sel, idx_t count, const char *out_of_range_error,
	                           bool source_known_not_null = false);

	const_data_ptr_t *DataArray();
	const sel_t **SelectionArray();
	const sel_t **SelectionArrayOrNull();
	const validity_t **ValidityArray();
	const validity_t **ValidityArrayOrNull();

	const sel_t *CanonicalizeCommonSelection(SljitSourceVectorScratch &other);
	const sel_t *CanonicalizeCommonSourceSelection();
	bool HasCommonSelection() const;
	bool AllSelectionsPresent() const;
	bool HasAnySelection() const;
	bool AllValid() const;
	bool SourceCanHaveNull() const;
	bool FlatNoSelection(const SelectionVector *execute_sel) const;
	bool FlatAllValid(const SelectionVector *execute_sel) const;
	bool FlatNoSelection(const sel_t *native_execute_sel, const sel_t *source_common_sel = nullptr) const;
	bool FlatAllValid(const sel_t *native_execute_sel, const sel_t *source_common_sel = nullptr) const;

private:
	void CheckSourceIndex(idx_t source_idx) const;

	vector<UnifiedVectorFormat> formats;
	vector<const_data_ptr_t> data;
	vector<const sel_t *> selections;
	vector<const validity_t *> validity;
};

} // namespace duckdb
