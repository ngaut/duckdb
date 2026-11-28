//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_kernel_frame.cpp
//
//===----------------------------------------------------------------------===//

#include "sljit_kernel_frame.hpp"

#include "sljit_platform.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

SljitKernelFrame::SljitKernelFrame(sljit_s32 fixed_scratch_count, sljit_s32 fixed_saved_count,
                                   bool loop_calls_helpers_p)
    : scratch_count(fixed_scratch_count), saved_count(fixed_saved_count), loop_calls_helpers(loop_calls_helpers_p) {
	if (!GetSljitTargetCapabilities().registers.SupportsLayout(scratch_count, saved_count)) {
		throw InternalException("SLJIT kernel frame cannot enter with its fixed registers");
	}
}

void SljitKernelFrame::Donate(sljit_s32 reg) {
	donated.push_back(reg);
}

sljit_s32 SljitKernelFrame::AllocateRegister() {
	if (!donated.empty()) {
		auto reg = donated.front();
		donated.erase(donated.begin());
		return reg;
	}
	const auto &registers = GetSljitTargetCapabilities().registers;
	if (!loop_calls_helpers && registers.SupportsLayout(scratch_count + 1, saved_count)) {
		return SLJIT_R(scratch_count++);
	}
	if (registers.SupportsLayout(scratch_count, saved_count + 1)) {
		return SljitSavedRegisterAt(saved_count++);
	}
	return 0;
}

SljitOperand SljitKernelFrame::AllocateCarried() {
	if (auto reg = AllocateRegister()) {
		return SljitOperand::Register(reg);
	}
	const auto offset = local_size;
	local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	return {SLJIT_MEM1(SLJIT_SP), offset};
}

void SljitKernelFrame::ReserveLocal(sljit_sw size) {
	if (local_size != 0) {
		throw InternalException("SLJIT kernel frame reserves its kernel's stack area before its own slots");
	}
	local_size = size;
}

SljitOperand SljitKernelFrame::AllocateInvariant(SljitOperand home) {
	auto reg = AllocateRegister();
	if (!reg) {
		return home;
	}
	invariants.emplace_back(SljitOperand::Register(reg), home);
	return SljitOperand::Register(reg);
}

void SljitKernelFrame::EmitLoadInvariants(struct sljit_compiler *compiler) const {
	for (auto &invariant : invariants) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, invariant.first.operand, 0, invariant.second.operand,
		               invariant.second.operand_w);
	}
}

void SljitKernelFrame::EmitEnter(struct sljit_compiler *compiler, sljit_s32 arg_types) const {
	sljit_emit_enter(compiler, 0, arg_types, scratch_count, saved_count, local_size);
}

sljit_s32 SljitKernelFrameRegister(struct sljit_compiler *compiler, const SljitOperand &operand, sljit_s32 fallback) {
	if (!(operand.operand & SLJIT_MEM)) {
		return operand.operand;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, fallback, 0, operand.operand, operand.operand_w);
	return fallback;
}

} // namespace duckdb
