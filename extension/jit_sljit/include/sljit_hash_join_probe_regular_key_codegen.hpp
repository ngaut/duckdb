//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_regular_key_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_key_codegen.hpp"
#include "sljit_hash_join_probe_loop_codegen.hpp"
#include "sljit_hash_join_probe_regular_codegen_util.hpp"
#include "sljit_hash_join_probe_regular_input_codegen.hpp"
#include "sljit_hash_join_probe_regular_setup_codegen.hpp"
#include "sljit_join_probe_codegen.hpp"

#include "duckdb/common/numeric_utils.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

struct SljitRegularHashJoinProbeKeyJumps {
	vector<struct sljit_jump *> equality_key_mismatches;
	vector<struct sljit_jump *> predicate_key_mismatches;
};

static inline vector<struct sljit_jump *>
EmitRegularHashJoinProbeHash(struct sljit_compiler *compiler, const SljitNativeHashJoinProbePlan &plan,
                             const SljitHashJoinProbeCodegenConfig &config,
                             const SljitRegularHashJoinProbeRegisters &registers) {
	auto &keys = plan.keys;
	const auto equality_key_count = plan.equality_key_count;
	const auto hash_multiplier_reg = registers.hash_multiplier_reg;
	const auto common_source_index_reg = registers.common_source_index_reg;
	const auto source_index_reg = registers.source_index_reg;
	const auto &source_data_regs = registers.source_data_regs;
	const bool assume_all_keys_valid = registers.assume_all_keys_valid;

	if (config.AssumesCommonSelectionAllValid()) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeRegularHashJoinProbeInput, source_sel));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), 0);
		sljit_emit_op1(compiler, SLJIT_MOV_U32, common_source_index_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	}

	vector<struct sljit_jump *> source_is_null;
	for (idx_t key_idx = 0; key_idx < equality_key_count; key_idx++) {
		auto &key = keys[key_idx];
		if (!assume_all_keys_valid) {
			EmitLoadRegularHashJoinSourceIndex(compiler, key_idx, SLJIT_R1, SLJIT_R0);
		}
		auto source_null = EmitJumpIfRegularHashJoinSourceNull(compiler, key_idx, source_index_reg, SLJIT_R0, SLJIT_R4,
		                                                       assume_all_keys_valid);
		if (!key.null_equal || assume_all_keys_valid) {
			if (source_null) {
				source_is_null.push_back(source_null);
			}
			auto source_data_reg = EmitPrepareRegularHashJoinSourceData(compiler, key_idx, SLJIT_R0, source_data_regs);
			EmitHashJoinKeyHashFromSourceData(compiler, key_idx, key.key_kind, SLJIT_R2, source_data_reg,
			                                  source_index_reg, SLJIT_R4, hash_multiplier_reg);
		} else {
			auto source_data_reg = EmitPrepareRegularHashJoinSourceData(compiler, key_idx, SLJIT_R0, source_data_regs);
			EmitHashJoinKeyHashFromSourceData(compiler, key_idx, key.key_kind, SLJIT_R2, source_data_reg,
			                                  source_index_reg, SLJIT_R4, hash_multiplier_reg);
			auto source_hash_ready = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(source_null, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, DuckDBNullHashImmediate());
			sljit_set_label(source_hash_ready, sljit_emit_label(compiler));
		}
		if (key_idx == 0) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_R2, 0);
		} else {
			EmitDuckDBCombineHashScalar(compiler, SLJIT_R3, SLJIT_R2, SLJIT_R4, hash_multiplier_reg);
		}
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_R3, 0);
	sljit_emit_op2(compiler, SLJIT_OR, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_S5, 0);
	EmitApplyHashJoinBitmask(compiler, SLJIT_R1, SLJIT_R4, SLJIT_REGULAR_HASH_JOIN_BITMASK_REG_AVAILABLE);
	return source_is_null;
}

static inline struct sljit_jump *
EmitRegularHashJoinSaltMismatch(struct sljit_compiler *compiler, const SljitHashJoinProbeCodegenConfig &config) {
	if (!config.HasRuntimeLayout() && !config.UsesSalt()) {
		return nullptr;
	}

	struct sljit_jump *skip_salt = nullptr;
	if (config.HasRuntimeLayout()) {
		EmitLoadHashJoinProbeLayoutKind(compiler, SLJIT_R4);
		auto no_chain_salt =
		    sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_IMM,
		                   SljitHashJoinProbeLayoutImmediate(SljitHashJoinProbeLayoutKind::NO_CHAIN_SALT));
		auto chain_salt =
		    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_R4, 0, SLJIT_IMM,
		                   SljitHashJoinProbeLayoutImmediate(SljitHashJoinProbeLayoutKind::CHAIN_SALT_DIRECT));
		skip_salt = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto salt_layout = sljit_emit_label(compiler);
		sljit_set_label(no_chain_salt, salt_layout);
		sljit_set_label(chain_salt, salt_layout);
	}

	sljit_emit_op2(compiler, SLJIT_OR, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_S5, 0);
	auto salt_mismatch = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R3, 0);
	if (skip_salt) {
		sljit_set_label(skip_salt, sljit_emit_label(compiler));
	}
	return salt_mismatch;
}

static inline SljitRegularHashJoinProbeKeyJumps
EmitRegularHashJoinKeyChecks(struct sljit_compiler *compiler, const SljitNativeHashJoinProbePlan &plan,
                             const SljitRegularHashJoinProbeRegisters &registers) {
	auto &keys = plan.keys;
	const auto equality_key_count = plan.equality_key_count;
	const auto source_index_reg = registers.source_index_reg;
	const auto &source_data_regs = registers.source_data_regs;
	const bool assume_all_keys_valid = registers.assume_all_keys_valid;

	SljitRegularHashJoinProbeKeyJumps jumps;
	for (idx_t key_idx = 0; key_idx < keys.size(); key_idx++) {
		auto &key = keys[key_idx];
		if (!assume_all_keys_valid) {
			EmitLoadRegularHashJoinSourceIndex(compiler, key_idx, SLJIT_R1, SLJIT_R4);
		}
		if (key_idx >= equality_key_count) {
			auto source_null = EmitJumpIfRegularHashJoinSourceNull(compiler, key_idx, source_index_reg, SLJIT_R2,
			                                                       SLJIT_R4, assume_all_keys_valid);
			if (source_null) {
				jumps.predicate_key_mismatches.push_back(source_null);
			}
			auto rhs_null =
			    EmitJumpIfHashJoinRhsKeyNull(compiler, key_idx, SLJIT_R0, SLJIT_R2, SLJIT_R4, assume_all_keys_valid);
			if (rhs_null) {
				jumps.predicate_key_mismatches.push_back(rhs_null);
			}
			auto source_data_reg = EmitPrepareRegularHashJoinSourceData(compiler, key_idx, SLJIT_R4, source_data_regs);
			EmitLoadHashJoinSourceKey(compiler, key_idx, key.key_kind, SLJIT_R2, source_data_reg, source_index_reg, 0,
			                          SLJIT_R3, false);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(key.key_layout_offset));
			EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R4, SLJIT_R4, SLJIT_IMM, 0);
			jumps.predicate_key_mismatches.push_back(
			    sljit_emit_cmp(compiler, SljitHashJoinPredicateMismatchComparison(key.comparison_type, key.key_kind),
			                   SLJIT_R2, 0, SLJIT_R4, 0));
		} else if (key.null_equal && !assume_all_keys_valid) {
			auto source_null = EmitJumpIfRegularHashJoinSourceNull(compiler, key_idx, SLJIT_R1, SLJIT_R2, SLJIT_R4);
			jumps.equality_key_mismatches.push_back(
			    EmitJumpIfHashJoinRhsKeyNull(compiler, key_idx, SLJIT_R0, SLJIT_R2, SLJIT_R4));
			EmitHashJoinEqualityKeyMismatch(compiler, key_idx, key.key_kind,
			                                NumericCast<sljit_sw>(key.key_layout_offset),
			                                jumps.equality_key_mismatches, source_data_regs, source_index_reg);
			auto key_done = sljit_emit_jump(compiler, SLJIT_JUMP);

			sljit_set_label(source_null, sljit_emit_label(compiler));
			auto rhs_null_match = EmitJumpIfHashJoinRhsKeyNull(compiler, key_idx, SLJIT_R0, SLJIT_R2, SLJIT_R4);
			jumps.equality_key_mismatches.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			auto nulls_match = sljit_emit_label(compiler);
			sljit_set_label(rhs_null_match, nulls_match);
			sljit_set_label(key_done, nulls_match);
		} else {
			auto rhs_null =
			    EmitJumpIfHashJoinRhsKeyNull(compiler, key_idx, SLJIT_R0, SLJIT_R2, SLJIT_R4, assume_all_keys_valid);
			if (rhs_null) {
				jumps.equality_key_mismatches.push_back(rhs_null);
			}
			EmitHashJoinEqualityKeyMismatch(compiler, key_idx, key.key_kind,
			                                NumericCast<sljit_sw>(key.key_layout_offset),
			                                jumps.equality_key_mismatches, source_data_regs, source_index_reg);
		}
	}
	return jumps;
}

} // namespace duckdb
