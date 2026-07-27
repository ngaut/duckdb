//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_platform.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljitLir.h"

#include <cstdint>

namespace duckdb {

enum class SljitTargetArchitecture : uint8_t {
	UNKNOWN,
	X86_32,
	X86_64,
	ARM_32,
	ARM_64,
	PPC,
	MIPS,
	RISCV,
	S390X,
	LOONGARCH
};

struct SljitRegisterFile {
	sljit_s32 register_count = 0;
	sljit_s32 saved_register_count = 0;
	sljit_s32 addressable_saved_register_count = 0;

	bool CanEnter(sljit_s32 scratch_count, sljit_s32 requested_saved_register_count) const;
	bool HasAddressableSavedRegisters(sljit_s32 requested_saved_register_count) const;
	bool SupportsLayout(sljit_s32 scratch_count, sljit_s32 requested_saved_register_count) const;
	sljit_s32 MaxAddressableSavedRegisters(sljit_s32 scratch_count) const;
};

struct SljitTargetCapabilities {
	SljitTargetArchitecture architecture = SljitTargetArchitecture::UNKNOWN;
	SljitRegisterFile registers;
	sljit_s32 machine_word_bytes = 0;
	bool platform_available = false;
	bool simd_available = false;

	bool BackendAvailable() const;
	bool IsArm64() const;
	bool IsX86_64() const;
	bool Has64BitMachineWord() const;
	bool SupportsPrimitiveRunRegisterABI() const;
};

class SljitSavedRegisterAllocator {
public:
	SljitSavedRegisterAllocator(sljit_s32 scratch_register_count, sljit_s32 fixed_saved_register_count);

	bool Valid() const;
	sljit_s32 Allocate();
	sljit_s32 SavedRegisterCount() const;

private:
	sljit_s32 scratch_register_count;
	sljit_s32 saved_register_count;
	bool valid;
};

const SljitTargetCapabilities &GetSljitTargetCapabilities();
sljit_s32 SljitSavedRegisterAt(sljit_s32 index);
bool SljitPlatformAvailable();

} // namespace duckdb
