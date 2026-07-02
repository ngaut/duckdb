#include "sljit_native_flat_floating_stats_codegen.hpp"

#include "sljit_native_flat_double_codegen_helpers.hpp"

#include "duckdb/common/helper.hpp"

namespace duckdb {

sljit_s32 SljitFlatFloatingProjectionStatsMinRegister(idx_t projection_idx) {
	return SLJIT_FR(NumericCast<sljit_s32>(4 + projection_idx * 2));
}

sljit_s32 SljitFlatFloatingProjectionStatsMaxRegister(idx_t projection_idx) {
	return SLJIT_FR(NumericCast<sljit_s32>(5 + projection_idx * 2));
}

static sljit_s32 NativeFloatingCompare(sljit_s32 compare_type, bool single_precision) {
	return single_precision ? compare_type | SLJIT_32 : compare_type;
}

void EmitSljitFlatFloatingStatsInit(struct sljit_compiler *compiler, sljit_s32 move_op, sljit_s32 value_reg,
                                    sljit_s32 min_reg, sljit_s32 max_reg) {
	sljit_emit_fop1(compiler, move_op, min_reg, 0, value_reg, 0);
	sljit_emit_fop1(compiler, move_op, max_reg, 0, value_reg, 0);
}

void EmitSljitFlatFloatingStatsUpdate(struct sljit_compiler *compiler, sljit_s32 move_op, bool single_precision,
                                      sljit_s32 value_reg, sljit_s32 min_reg, sljit_s32 max_reg) {
	auto value_is_nan_for_min =
	    sljit_emit_fcmp(compiler, NativeFloatingCompare(SLJIT_UNORDERED, single_precision), value_reg, 0, value_reg, 0);
	auto min_is_nan =
	    sljit_emit_fcmp(compiler, NativeFloatingCompare(SLJIT_UNORDERED, single_precision), min_reg, 0, min_reg, 0);
	auto value_less_than_min = sljit_emit_fcmp(compiler, NativeFloatingCompare(SLJIT_ORDERED_LESS, single_precision),
	                                           value_reg, 0, min_reg, 0);
	auto min_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto set_min = sljit_emit_label(compiler);
	sljit_set_label(min_is_nan, set_min);
	sljit_set_label(value_less_than_min, set_min);
	sljit_emit_fop1(compiler, move_op, min_reg, 0, value_reg, 0);
	auto after_min = sljit_emit_label(compiler);
	sljit_set_label(value_is_nan_for_min, after_min);
	sljit_set_label(min_done, after_min);

	auto value_is_nan_for_max =
	    sljit_emit_fcmp(compiler, NativeFloatingCompare(SLJIT_UNORDERED, single_precision), value_reg, 0, value_reg, 0);
	auto value_greater_than_max = sljit_emit_fcmp(
	    compiler, NativeFloatingCompare(SLJIT_ORDERED_GREATER, single_precision), value_reg, 0, max_reg, 0);
	auto max_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto set_max = sljit_emit_label(compiler);
	sljit_set_label(value_is_nan_for_max, set_max);
	sljit_set_label(value_greater_than_max, set_max);
	sljit_emit_fop1(compiler, move_op, max_reg, 0, value_reg, 0);
	auto after_max = sljit_emit_label(compiler);
	sljit_set_label(max_done, after_max);
}

void EmitSljitStoreFlatFloatingStats(struct sljit_compiler *compiler, idx_t projection_index, bool single_precision,
                                     sljit_s32 min_reg, sljit_s32 max_reg, sljit_s32 stats_min_base,
                                     sljit_s32 stats_max_base) {
	auto move_op = NativeDirectFloatingMoveOp(single_precision);
	auto fmem_align = NativeDirectFloatingMemoryAlignment(single_precision);
	auto constant_width = NativeDirectFloatingDataWidth(single_precision);
	auto stats_offset = NumericCast<sljit_sw>(projection_index * constant_width);
	sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, min_reg, SLJIT_MEM1(stats_min_base),
	                stats_offset);
	sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, max_reg, SLJIT_MEM1(stats_max_base),
	                stats_offset);
}

} // namespace duckdb
