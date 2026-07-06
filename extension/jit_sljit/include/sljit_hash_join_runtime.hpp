//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_plan.hpp"

#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

namespace duckdb {

class DataChunk;
class SelectionVector;
class Vector;
class ExecutionRegionRuntime;

enum class ExecutionHashJoinProbeLayoutKind : uint8_t;
struct ExecutionHashJoinBuildBinding;
struct ExecutionHashJoinProbeBinding;
struct ExecutionHashJoinTableLayout;
struct ExecutionPerfectHashJoinTableLayout;
struct SljitHashJoinProbeDrainState;

enum class SljitHashJoinProbeInputKind { GENERIC, FLAT_ALL_VALID, SELECTED_ALL_VALID };

enum class SljitHashJoinMarkSelectionMode : uint8_t { NONE, MATCHES, NON_MATCHES };

static inline bool SljitHashJoinEmitsMarkSelection(SljitHashJoinMarkSelectionMode mode) {
	return mode != SljitHashJoinMarkSelectionMode::NONE;
}

struct SljitRegularHashJoinProbeInputShape {
	bool source_selection_present = false;
	bool source_common_selection_present = false;
	bool source_validity_present = false;
	bool rhs_keys_all_valid = false;

	SljitHashJoinProbeInputKind PathKind() const {
		if (!rhs_keys_all_valid || source_validity_present) {
			return SljitHashJoinProbeInputKind::GENERIC;
		}
		if (!source_selection_present) {
			return SljitHashJoinProbeInputKind::FLAT_ALL_VALID;
		}
		if (source_common_selection_present) {
			return SljitHashJoinProbeInputKind::SELECTED_ALL_VALID;
		}
		return SljitHashJoinProbeInputKind::GENERIC;
	}
};

struct SljitPreparedRegularHashJoinProbeInput {
	SljitNativeRegularHashJoinProbeInput native_input;
	SljitHashJoinProbeInputKind input_kind;
};

struct SljitPreparedPerfectHashJoinProbeInput {
	UnifiedVectorFormat source_format;
	SljitNativePerfectHashJoinProbeInput native_input;
};

const_data_ptr_t NativeHashJoinKeySourceData(UnifiedVectorFormat &format, SljitNativeHashJoinKeyKind kind);

SljitHashJoinProbeLayoutKind SljitHashJoinTableLayoutKind(const ExecutionHashJoinTableLayout &layout);
bool SljitHashJoinCanUseAllValidChainInput(const SljitNativeRegularHashJoinProbeInput &input);
const char *SljitHashJoinProbeLayoutName(ExecutionHashJoinProbeLayoutKind kind);
void SljitMarkHashJoinBuildMatchesAfterResidual(const SljitNativeHashJoinProbePlan &plan, Vector &row_pointers,
                                                idx_t count);

const SljitNativeHashJoinProbeKeyPlan &
SljitValidatePerfectHashJoinProbeExecutionLayout(const SljitNativeHashJoinProbePlan &plan,
                                                 const ExecutionHashJoinProbeBinding &probe, DataChunk &input);
void SljitPreparePerfectHashJoinProbeInput(const SljitNativeHashJoinProbeKeyPlan &key,
                                           const ExecutionPerfectHashJoinTableLayout &layout, DataChunk &input,
                                           SelectionVector &match_selection, SelectionVector &build_selection,
                                           SljitHashJoinProbeDrainState &state, bool allow_unchecked_int64_to_int32,
                                           SljitPreparedPerfectHashJoinProbeInput &result);
SljitHashJoinProbeLayoutKind
SljitValidateRegularHashJoinProbeExecutionLayout(const SljitNativeHashJoinProbePlan &plan,
                                                 const ExecutionHashJoinProbeBinding &probe);

const char *SljitGeneratedRegularHashJoinProbeStage();
const char *SljitGeneratedRegularHashJoinProbeStage(bool uses_bloom_filter);
const char *SljitGeneratedRegularHashJoinProbeStage(bool uses_bloom_filter,
                                                    SljitHashJoinMarkSelectionMode mark_selection_mode);
const char *SljitGeneratedPerfectHashJoinProbeStage();

SinkResultType SljitExecuteNativeHashJoinBuildUpdate(ExecutionRegionRuntime &runtime, idx_t op_idx,
                                                     SljitNativeRegionOpKind op_kind,
                                                     ExecutionHashJoinBuildBinding &build, DataChunk &input,
                                                     DataChunk &source_chunk, Vector &hash_values,
                                                     SelectionVector &build_sel);

} // namespace duckdb
