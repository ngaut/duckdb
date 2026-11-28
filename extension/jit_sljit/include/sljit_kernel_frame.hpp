//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_kernel_frame.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_codegen_internal.hpp"

#include "duckdb/common/vector.hpp"

namespace duckdb {

//! A generated kernel's registers and stack slots. The kernel fixes the registers its loop protocol names (its
//! argument, row index, scratch temporaries), then asks for each value its loop keeps, hottest first: a value carried
//! across rows (a group key, an accumulator) or one read in the loop and never written (an output pointer). Each gets
//! the next register the target can still enter with, else stays in its home: a stack slot for a carried value, the
//! memory it is loaded from for an invariant. Every sljit operation takes either as an operand; a memory base needs a
//! register (SljitKernelFrameRegister). A kernel whose loop calls helpers takes saved registers only, which survive
//! the calls; any other takes scratch registers first
class SljitKernelFrame {
public:
	SljitKernelFrame(sljit_s32 fixed_scratch_count, sljit_s32 fixed_saved_count, bool loop_calls_helpers = false);

	//! A value carried across rows, starting in a stack slot when no register is left
	SljitOperand AllocateCarried();
	//! A value the loop reads from home, which EmitLoadInvariants loads into its register when it has one
	SljitOperand AllocateInvariant(SljitOperand home);
	//! Loads every invariant that has a register from its home; the homes must be addressable at the call
	void EmitLoadInvariants(struct sljit_compiler *compiler) const;
	//! Enters the kernel with the frame's registers and stack
	void EmitEnter(struct sljit_compiler *compiler, sljit_s32 arg_types) const;

	sljit_s32 ScratchCount() const {
		return scratch_count;
	}
	sljit_s32 SavedCount() const {
		return saved_count;
	}
	sljit_sw LocalSize() const {
		return local_size;
	}

private:
	sljit_s32 NextRegister();

private:
	sljit_s32 scratch_count;
	sljit_s32 saved_count;
	bool loop_calls_helpers;
	sljit_sw local_size = 0;
	vector<std::pair<SljitOperand, SljitOperand>> invariants;
};

//! The operand's register, or fallback after loading the operand into it
sljit_s32 SljitKernelFrameRegister(struct sljit_compiler *compiler, const SljitOperand &operand, sljit_s32 fallback);

} // namespace duckdb
