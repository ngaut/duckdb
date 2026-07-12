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
constexpr idx_t SLJIT_EAGER_ZERO_SPARSE_LOCAL_MAX_GROUPS = 64;
constexpr idx_t SLJIT_SPARSE_LOCAL_PERFECT_HASH_MAX_GROUPS = 1024;
constexpr idx_t SLJIT_DEFERRED_PERFECT_HASH_FLAG_MAX_GROUPS = 1024;
constexpr sljit_sw SLJIT_STRING_T_SHIFT = 4;

#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 8
constexpr bool SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG = true;
constexpr sljit_s32 SLJIT_PERFECT_HASH_STATE_REG = SLJIT_S7;
constexpr sljit_s32 SLJIT_PERFECT_HASH_GROUP_INDEX_REG = SLJIT_S7;
constexpr sljit_s32 SLJIT_PERFECT_HASH_SAVED_REG_COUNT = 8;
#else
constexpr bool SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG = false;
constexpr sljit_s32 SLJIT_PERFECT_HASH_STATE_REG = SLJIT_S4;
constexpr sljit_s32 SLJIT_PERFECT_HASH_GROUP_INDEX_REG = SLJIT_S4;
constexpr sljit_s32 SLJIT_PERFECT_HASH_SAVED_REG_COUNT = 7;
#endif

#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
constexpr bool SLJIT_HAS_PERFECT_HASH_GROUP_DATA_REGS = true;
constexpr sljit_s32 SLJIT_PERFECT_HASH_GROUP_DATA_SAVED_REG_COUNT = 10;
#else
constexpr bool SLJIT_HAS_PERFECT_HASH_GROUP_DATA_REGS = false;
constexpr sljit_s32 SLJIT_PERFECT_HASH_GROUP_DATA_SAVED_REG_COUNT = SLJIT_PERFECT_HASH_SAVED_REG_COUNT;
#endif

#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
constexpr bool SLJIT_HAS_SPARSE_LOCAL_RUN_CACHE_REGS = true;
constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_LOWER0_REG = SLJIT_S6;
constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_LOWER1_REG = SLJIT_S8;
constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_LOWER2_REG = SLJIT_S9;
constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_SAVED_REG_COUNT = 10;
#else
constexpr bool SLJIT_HAS_SPARSE_LOCAL_RUN_CACHE_REGS = false;
constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_LOWER0_REG = SLJIT_R0;
constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_LOWER1_REG = SLJIT_R0;
constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_LOWER2_REG = SLJIT_R0;
constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_SAVED_REG_COUNT = SLJIT_PERFECT_HASH_SAVED_REG_COUNT;
#endif

struct SljitLocalPerfectHashAggregateLane {
	sljit_sw lower_offset = -1;
	sljit_sw upper_offset = -1;
	sljit_sw saw_offset = -1;
	sljit_sw count_offset = -1;
	bool value_always_seen = false;
	bool local_lower_never_overflows = false;
};

struct SljitLocalPerfectHashAggregatePlan {
	bool enabled = false;
	bool sparse = false;
	bool sparse_eager_zero = false;
	bool group_seen_is_byte = false;
	idx_t group_count = 0;
	idx_t count_seen_lane = DConstants::INVALID_INDEX;
	sljit_sw group_seen_offset = -1;
	sljit_sw active_groups_offset = -1;
	sljit_sw active_count_offset = -1;
	sljit_sw group_payload_offset = -1;
	sljit_sw group_payload_stride = 0;
	vector<SljitLocalPerfectHashAggregateLane> lanes;
};

struct SljitSparseLocalRunCachedLane {
	idx_t payload_idx = DConstants::INVALID_INDEX;
	sljit_s32 lower_reg = 0;
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
                                     sljit_s32 group_data_array_base_reg = 0);
sljit_s32 SljitPerfectHashGroupDataPointerReg(idx_t group_idx);
sljit_s32 SljitPerfectHashSourceDataPointerReg(idx_t hoist_idx, bool include_fast_validity_reg);

vector<bool> BuildSljitAggregatePayloadNotNull(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                               const vector<ExecutionRegionAggregateInput> &aggregates,
                                               const vector<bool> &source_not_null);
bool TryBuildSljitLocalPerfectHashAggregatePlan(const vector<ExecutionRegionAggregateInput> &aggregates,
                                                const ExecutionRegionAggregateContract &contract,
                                                const vector<bool> &payloads_not_null, sljit_sw &local_size,
                                                SljitLocalPerfectHashAggregatePlan &result);
bool TryBuildSljitDeferredPerfectHashFlagPlan(const vector<ExecutionRegionAggregateInput> &aggregates,
                                              const ExecutionRegionAggregateContract &contract, sljit_sw &local_size,
                                              SljitDeferredPerfectHashFlagPlan &result);
void AnnotateSljitLocalPerfectHashAggregatePlan(SljitLocalPerfectHashAggregatePlan &plan,
                                                const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                const vector<ExecutionRegionAggregateInput> &aggregates,
                                                const vector<Value> &source_min_values,
                                                const vector<Value> &source_max_values);
bool SljitSparseLocalUsesCountSeen(const SljitLocalPerfectHashAggregatePlan &plan);
idx_t CountSljitSparseLocalRunCacheableLanes(const SljitLocalPerfectHashAggregatePlan &plan,
                                             const vector<ExecutionRegionAggregateInput> &aggregates);
vector<SljitSparseLocalRunCachedLane>
BuildSljitSparseLocalRunCachedLanes(const SljitLocalPerfectHashAggregatePlan &plan,
                                    const vector<ExecutionRegionAggregateInput> &aggregates,
                                    const vector<sljit_s32> &lower_regs);
const SljitSparseLocalRunCachedLane *
FindSljitSparseLocalRunCachedLane(const vector<SljitSparseLocalRunCachedLane> &cached_lanes, idx_t payload_idx);

void EmitZeroSljitLocalPerfectHashAggregateArrays(struct sljit_compiler *compiler,
                                                  const SljitLocalPerfectHashAggregatePlan &plan);
void EmitZeroSljitDeferredPerfectHashFlagArray(struct sljit_compiler *compiler,
                                               const SljitDeferredPerfectHashFlagPlan &plan);
void EmitMarkSljitLocalPerfectHashGroupSeen(struct sljit_compiler *compiler,
                                            const SljitLocalPerfectHashAggregatePlan &plan, sljit_s32 group_index_reg,
                                            sljit_s32 group_pointer_reg, bool mark_payloads_seen = false,
                                            bool increment_count_seen = true);
void EmitMarkSljitDeferredPerfectHashGroupSeen(struct sljit_compiler *compiler,
                                               const SljitDeferredPerfectHashFlagPlan &plan, sljit_s32 group_index_reg);
void EmitSljitLocalPerfectHashIncrementCount(struct sljit_compiler *compiler,
                                             const SljitLocalPerfectHashAggregateLane &lane, sljit_s32 group_index_reg);
void EmitSljitLocalPerfectHashAccumulate(struct sljit_compiler *compiler,
                                         const SljitLocalPerfectHashAggregateLane &lane,
                                         AggregatePrimitiveUpdateKind kind, sljit_s32 group_index_reg,
                                         sljit_s32 value_reg);
void EmitSljitSparseLocalPerfectHashIncrementCount(struct sljit_compiler *compiler,
                                                   const SljitLocalPerfectHashAggregateLane &lane,
                                                   sljit_s32 group_pointer_reg);
void EmitSljitSparseLocalPerfectHashAccumulate(struct sljit_compiler *compiler,
                                               const SljitLocalPerfectHashAggregateLane &lane,
                                               AggregatePrimitiveUpdateKind kind, sljit_s32 group_pointer_reg,
                                               sljit_s32 value_reg, bool store_saw = true);
void EmitSljitSparseLocalRunCacheFlush(struct sljit_compiler *compiler, const SljitLocalPerfectHashAggregatePlan &plan,
                                       const vector<SljitSparseLocalRunCachedLane> &cached_lanes,
                                       sljit_s32 group_pointer_reg, sljit_sw cached_group_offset,
                                       sljit_sw cached_position_offset, bool explicit_count,
                                       sljit_s32 current_index_reg);
void EmitSljitSparseLocalRunCacheLoadCurrent(struct sljit_compiler *compiler,
                                             const SljitLocalPerfectHashAggregatePlan &plan,
                                             const vector<SljitSparseLocalRunCachedLane> &cached_lanes,
                                             sljit_s32 group_pointer_reg, sljit_sw cached_position_offset,
                                             bool explicit_count, sljit_s32 current_index_reg);
void EmitSljitSparseLocalRunCacheAccumulate(struct sljit_compiler *compiler,
                                            const SljitLocalPerfectHashAggregateLane &lane,
                                            AggregatePrimitiveUpdateKind kind, sljit_s32 lower_reg,
                                            sljit_s32 group_pointer_reg, sljit_s32 value_reg);
void EmitSljitLocalPerfectHashCommit(struct sljit_compiler *compiler,
                                     const SljitLocalPerfectHashAggregatePlan &local_plan,
                                     const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
                                     const ExecutionRegionAggregateContract &contract,
                                     bool local_payloads_known_seen = false);
void EmitSljitDeferredPerfectHashFlagsCommit(struct sljit_compiler *compiler,
                                             const SljitDeferredPerfectHashFlagPlan &deferred_plan,
                                             const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
                                             const ExecutionRegionAggregateContract &contract);

} // namespace duckdb
