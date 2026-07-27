//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_regular_setup_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_key_codegen.hpp"
#include "sljit_hash_join_probe_loop_codegen.hpp"
#include "sljit_join_probe_codegen.hpp"
#include "sljit_platform.hpp"

#include "duckdb/common/numeric_utils.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

static constexpr sljit_s32 SLJIT_REGULAR_HASH_JOIN_FIXED_SAVED_REG_COUNT = 5;

struct SljitRegularHashJoinProbeRegisters {
	bool assume_all_keys_valid = false;
	sljit_s32 saved_reg_count = SLJIT_REGULAR_HASH_JOIN_FIXED_SAVED_REG_COUNT;
	sljit_s32 pointer_mask_reg = 0;
	sljit_s32 bitmask_reg = 0;
	sljit_s32 hash_multiplier_reg = 0;
	sljit_s32 common_source_index_reg = 0;
	sljit_s32 source_index_reg = SLJIT_R1;
	vector<sljit_s32> source_data_regs;
	sljit_s32 aux_next_ptrs_reg = 0;
};

static inline bool PrepareRegularHashJoinProbeRegisters(const SljitNativeHashJoinProbePlan &plan,
                                                        const SljitHashJoinProbeCodegenConfig &config,
                                                        SljitRegularHashJoinProbeRegisters &registers, string &error) {
	auto &keys = plan.keys;
	SljitSavedRegisterAllocator register_allocator(5, SLJIT_REGULAR_HASH_JOIN_FIXED_SAVED_REG_COUNT);
	if (!register_allocator.Valid()) {
		error = "not enough addressable saved registers for regular hash join probe";
		return false;
	}
	auto allocate_saved_register = [&]() {
		auto reg = register_allocator.Allocate();
		registers.saved_reg_count = register_allocator.SavedRegisterCount();
		return reg;
	};
	registers.assume_all_keys_valid = config.AssumesAllKeysValid();
	registers.saved_reg_count = SLJIT_REGULAR_HASH_JOIN_FIXED_SAVED_REG_COUNT;
	registers.source_data_regs.assign(keys.size(), 0);

	// The common selected index must survive hashing and collision checks. Reserve
	// it before optional invariant hoists so smaller register files reload masks
	// instead of rejecting an otherwise supported probe.
	if (config.AssumesCommonSelectionAllValid()) {
		registers.common_source_index_reg = allocate_saved_register();
		if (registers.common_source_index_reg == 0) {
			error = "not enough saved registers for selected all-valid hash join probe";
			return false;
		}
	}
	registers.pointer_mask_reg = allocate_saved_register();
	registers.bitmask_reg = allocate_saved_register();
	registers.hash_multiplier_reg = allocate_saved_register();
	if (registers.assume_all_keys_valid) {
		for (idx_t key_idx = 0; key_idx < keys.size(); key_idx++) {
			auto reg = allocate_saved_register();
			if (reg == 0) {
				break;
			}
			registers.source_data_regs[key_idx] = reg;
		}
	}
	if (config.PreloadsAuxNextPointers()) {
		registers.aux_next_ptrs_reg = allocate_saved_register();
	}
	registers.source_index_reg =
	    config.AssumesFlatAllValid()
	        ? SLJIT_S1
	        : (registers.common_source_index_reg != 0 ? registers.common_source_index_reg : SLJIT_R1);
	return true;
}

static inline void EmitEnterRegularHashJoinProbe(struct sljit_compiler *compiler,
                                                 const SljitRegularHashJoinProbeRegisters &registers) {
	const auto local_size = registers.assume_all_keys_valid ? 0 : SLJIT_HASH_JOIN_PROBE_LOCAL_SIZE;
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, registers.saved_reg_count, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, input_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, entries));
	if (registers.pointer_mask_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, registers.pointer_mask_reg, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeRegularHashJoinProbeInput, pointer_mask));
	}
	if (registers.bitmask_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, registers.bitmask_reg, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeRegularHashJoinProbeInput, bitmask));
	}
	if (registers.hash_multiplier_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, registers.hash_multiplier_reg, 0, SLJIT_IMM,
		               DuckDBMurmurHashMultiplierImmediate());
	}
	for (idx_t key_idx = 0; key_idx < registers.source_data_regs.size(); key_idx++) {
		auto reg = registers.source_data_regs[key_idx];
		if (reg == 0) {
			continue;
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, reg, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeRegularHashJoinProbeInput, source_data));
		sljit_emit_op1(compiler, SLJIT_MOV_P, reg, 0, SLJIT_MEM1(reg),
		               NumericCast<sljit_sw>(key_idx * sizeof(const_data_ptr_t)));
	}
	if (registers.aux_next_ptrs_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, registers.aux_next_ptrs_reg, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeRegularHashJoinProbeInput, aux_next_ptrs));
	}
}

} // namespace duckdb
