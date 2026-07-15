//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_perfect_hash_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_descriptor.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_region_plan.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/execution/execution_region_ir.hpp"

namespace duckdb {

constexpr idx_t SLJIT_LOCAL_PERFECT_HASH_MAX_GROUPS = 16;
constexpr idx_t SLJIT_DEFERRED_PERFECT_HASH_FLAG_MAX_GROUPS = 1024;
constexpr sljit_sw SLJIT_STRING_T_SHIFT = 4;

#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 8
constexpr bool SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG = true;
constexpr sljit_s32 SLJIT_PERFECT_HASH_STATE_REG = SLJIT_S7;
constexpr sljit_s32 SLJIT_PERFECT_HASH_REDUCTION_STATE_REG = SLJIT_S7;
constexpr sljit_s32 SLJIT_PERFECT_HASH_SAVED_REG_COUNT = 8;
#else
constexpr bool SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG = false;
constexpr sljit_s32 SLJIT_PERFECT_HASH_STATE_REG = SLJIT_S4;
constexpr sljit_s32 SLJIT_PERFECT_HASH_REDUCTION_STATE_REG = SLJIT_S4;
constexpr sljit_s32 SLJIT_PERFECT_HASH_SAVED_REG_COUNT = 7;
#endif

#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
constexpr bool SLJIT_HAS_PERFECT_HASH_GROUP_DATA_REGS = true;
constexpr sljit_s32 SLJIT_PERFECT_HASH_GROUP_DATA_SAVED_REG_COUNT = 10;
#else
constexpr bool SLJIT_HAS_PERFECT_HASH_GROUP_DATA_REGS = false;
constexpr sljit_s32 SLJIT_PERFECT_HASH_GROUP_DATA_SAVED_REG_COUNT = SLJIT_PERFECT_HASH_SAVED_REG_COUNT;
#endif

struct SljitDensePerfectHashAggregateReductionLane {
	sljit_sw lower_offset = -1;
	sljit_sw upper_offset = -1;
	sljit_sw saw_offset = -1;
	sljit_sw count_offset = -1;
	bool value_always_seen = false;
	bool batch_lower_never_overflows = false;
};

struct SljitDensePerfectHashAggregateReductionPlan {
	idx_t group_count = 0;
	idx_t count_seen_lane = DConstants::INVALID_INDEX;
	sljit_sw state_rows_offset = -1;
	sljit_sw state_row_shift = 0;
	sljit_sw group_seen_offset = -1;
	vector<SljitDensePerfectHashAggregateReductionLane> lanes;

	bool Ready() const {
		return group_count > 0;
	}
};

struct SljitDeferredPerfectHashFlagPlan {
	bool enabled = false;
	idx_t group_count = 0;
	sljit_sw group_seen_offset = -1;
};

struct SljitPerfectHashGroupPlan {
	SljitNativeRegionExpressionKind expression_kind = SljitNativeRegionExpressionKind::REFERENCE;
	SljitNativeIntegerKind integer_kind = SljitNativeIntegerKind::INT64;
	SljitNativeSignedIntegerWidth integer_source_width = SljitNativeSignedIntegerWidth::INT32;
	unique_ptr<ExecutionExpressionIR> expression_tree;
	vector<idx_t> expression_tree_source_indices;
	idx_t source_index = DConstants::INVALID_INDEX;
	idx_t string_compress_target_size = 0;
	int64_t integer_source_minimum = 0;
	int64_t minimum = 0;
	idx_t shift = 0;
};

enum class SljitFusedAggregateGroupIndexMode : uint8_t { LOGICAL, SELECTED_NULLABLE, SELECTED_PRESENT };

bool TryBuildSljitPerfectHashGroupPlans(const vector<ExecutionRegionGroupInput> &groups,
                                        const vector<SljitNativeRegionExpressionPlan> &group_expressions,
                                        const ExecutionRegionAggregateContract &contract,
                                        vector<SljitPerfectHashGroupPlan> &result,
                                        bool allow_typed_expression_tree = false);
bool SljitCanPrecomputePerfectHashStringGroupOffset(const vector<SljitPerfectHashGroupPlan> &groups);
vector<SljitTypedExpressionTreeDataPointerHoist>
BuildSljitPerfectHashSourceDataPointerHoists(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                             idx_t max_hoists = 2, bool include_fast_validity_reg = false);
vector<SljitTypedExpressionTreeDataPointerHoist>
BuildSljitPerfectHashSpareFastSourceDataPointerHoists(const vector<SljitNativeRegionExpressionPlan> &payloads);
void EmitSljitPerfectHashSetOutputGroup(struct sljit_compiler *compiler, sljit_s32 group_index_reg);
void EmitSljitPerfectHashStatePointer(struct sljit_compiler *compiler, sljit_s32 group_index_reg, sljit_s32 target_reg);
void EmitLoadFusedAggregateGroupSourceIndex(
    struct sljit_compiler *compiler, idx_t group_idx, sljit_s32 target_reg,
    SljitFusedAggregateGroupIndexMode mode = SljitFusedAggregateGroupIndexMode::SELECTED_NULLABLE,
    sljit_s32 group_sel_array_base_reg = 0);
struct sljit_jump *EmitFusedAggregateJumpIfGroupValidityNull(struct sljit_compiler *compiler, idx_t group_idx,
                                                             sljit_s32 index_reg);
void EmitLoadFusedAggregateGroupData(struct sljit_compiler *compiler, idx_t group_idx,
                                     const SljitPerfectHashGroupPlan &group, sljit_s32 index_reg, sljit_s32 target_reg,
                                     bool use_hoisted_group_data, sljit_s32 group_data_reg,
                                     bool use_precomputed_string_offset = false,
                                     sljit_s32 group_data_array_base_reg = 0,
                                     bool fuse_nonempty_string_compress_bias = false);
sljit_s32 SljitPerfectHashGroupDataPointerReg(idx_t group_idx);
sljit_s32 SljitPerfectHashSourceDataPointerReg(idx_t hoist_idx, bool include_fast_validity_reg);

vector<bool> BuildSljitAggregatePayloadNotNull(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                               const vector<ExecutionRegionAggregateInput> &aggregates,
                                               const vector<bool> &source_not_null);
bool TryBuildSljitDensePerfectHashAggregateReductionPlan(const vector<ExecutionRegionAggregateInput> &aggregates,
                                                         const ExecutionRegionAggregateContract &contract,
                                                         const vector<bool> &payloads_not_null,
                                                         const vector<bool> &batch_lower_never_overflows,
                                                         sljit_sw &local_size,
                                                         SljitDensePerfectHashAggregateReductionPlan &result);
bool TryBuildSljitDeferredPerfectHashFlagPlan(const vector<ExecutionRegionAggregateInput> &aggregates,
                                              const ExecutionRegionAggregateContract &contract, sljit_sw &local_size,
                                              SljitDeferredPerfectHashFlagPlan &result);
vector<bool> BuildSljitDensePerfectHashLowerNeverOverflows(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                           const vector<ExecutionRegionAggregateInput> &aggregates,
                                                           const vector<Value> &source_min_values,
                                                           const vector<Value> &source_max_values);
void EmitZeroSljitDensePerfectHashAggregateReduction(struct sljit_compiler *compiler,
                                                     const SljitDensePerfectHashAggregateReductionPlan &plan);
void EmitZeroSljitDeferredPerfectHashFlagArray(struct sljit_compiler *compiler,
                                               const SljitDeferredPerfectHashFlagPlan &plan);
void EmitMarkSljitDensePerfectHashGroupSeen(struct sljit_compiler *compiler,
                                            const SljitDensePerfectHashAggregateReductionPlan &plan,
                                            sljit_s32 group_index_reg);
void EmitSljitPerfectHashReductionStatePointer(struct sljit_compiler *compiler,
                                               const SljitDensePerfectHashAggregateReductionPlan &plan,
                                               sljit_s32 reduction_index_reg, sljit_s32 state_pointer_reg);
void EmitMarkSljitDeferredPerfectHashGroupSeen(struct sljit_compiler *compiler,
                                               const SljitDeferredPerfectHashFlagPlan &plan, sljit_s32 group_index_reg);
void EmitSljitDensePerfectHashIncrementCount(struct sljit_compiler *compiler,
                                             const SljitDensePerfectHashAggregateReductionLane &lane,
                                             sljit_s32 state_pointer_reg);
void EmitSljitDensePerfectHashAccumulate(struct sljit_compiler *compiler,
                                         const SljitDensePerfectHashAggregateReductionLane &lane,
                                         AggregatePrimitiveUpdateKind kind, sljit_s32 state_pointer_reg,
                                         sljit_s32 value_reg);
void EmitSljitDensePerfectHashAggregateReductionCommit(
    struct sljit_compiler *compiler, const SljitDensePerfectHashAggregateReductionPlan &reduction_plan,
    const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
    const ExecutionRegionAggregateContract &contract);
void EmitSljitDeferredPerfectHashFlagsCommit(struct sljit_compiler *compiler,
                                             const SljitDeferredPerfectHashFlagPlan &deferred_plan,
                                             const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
                                             const ExecutionRegionAggregateContract &contract);

} // namespace duckdb
