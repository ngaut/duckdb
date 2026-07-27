#include "sljit_platform.hpp"

namespace duckdb {

bool SljitRegisterFile::CanEnter(sljit_s32 scratch_count, sljit_s32 requested_saved_register_count) const {
	return scratch_count >= 0 && requested_saved_register_count >= 0 &&
	       requested_saved_register_count <= saved_register_count &&
	       scratch_count + requested_saved_register_count <= register_count;
}

bool SljitRegisterFile::HasAddressableSavedRegisters(sljit_s32 requested_saved_register_count) const {
	return requested_saved_register_count >= 0 && requested_saved_register_count <= addressable_saved_register_count;
}

bool SljitRegisterFile::SupportsLayout(sljit_s32 scratch_count, sljit_s32 requested_saved_register_count) const {
	return CanEnter(scratch_count, requested_saved_register_count) &&
	       HasAddressableSavedRegisters(requested_saved_register_count);
}

sljit_s32 SljitRegisterFile::MaxAddressableSavedRegisters(sljit_s32 scratch_count) const {
	if (scratch_count < 0 || scratch_count > register_count) {
		return 0;
	}
	const auto enter_limit = register_count - scratch_count;
	const auto physical_limit = saved_register_count < addressable_saved_register_count
	                                ? saved_register_count
	                                : addressable_saved_register_count;
	return enter_limit < physical_limit ? enter_limit : physical_limit;
}

bool SljitTargetCapabilities::BackendAvailable() const {
	return platform_available && Has64BitMachineWord() && registers.SupportsLayout(5, 6);
}

bool SljitTargetCapabilities::IsArm64() const {
	return architecture == SljitTargetArchitecture::ARM_64;
}

bool SljitTargetCapabilities::IsX86_64() const {
	return architecture == SljitTargetArchitecture::X86_64;
}

bool SljitTargetCapabilities::Has64BitMachineWord() const {
	return machine_word_bytes >= static_cast<sljit_s32>(sizeof(int64_t));
}

bool SljitTargetCapabilities::SupportsPrimitiveRunRegisterABI() const {
	return BackendAvailable() && Has64BitMachineWord() && registers.SupportsLayout(7, 6);
}

SljitSavedRegisterAllocator::SljitSavedRegisterAllocator(sljit_s32 scratch_register_count_p,
                                                         sljit_s32 fixed_saved_register_count)
    : scratch_register_count(scratch_register_count_p), saved_register_count(fixed_saved_register_count), valid(false) {
	const auto &capabilities = GetSljitTargetCapabilities();
	valid = capabilities.BackendAvailable() &&
	        capabilities.registers.SupportsLayout(scratch_register_count_p, fixed_saved_register_count);
}

bool SljitSavedRegisterAllocator::Valid() const {
	return valid;
}

sljit_s32 SljitSavedRegisterAllocator::Allocate() {
	if (!valid ||
	    !GetSljitTargetCapabilities().registers.SupportsLayout(scratch_register_count, saved_register_count + 1)) {
		return 0;
	}
	return SljitSavedRegisterAt(saved_register_count++);
}

sljit_s32 SljitSavedRegisterAllocator::SavedRegisterCount() const {
	return saved_register_count;
}

static SljitTargetArchitecture SljitArchitecture() {
#if defined(SLJIT_CONFIG_X86_32) && SLJIT_CONFIG_X86_32
	return SljitTargetArchitecture::X86_32;
#elif defined(SLJIT_CONFIG_X86_64) && SLJIT_CONFIG_X86_64
	return SljitTargetArchitecture::X86_64;
#elif defined(SLJIT_CONFIG_ARM_32) && SLJIT_CONFIG_ARM_32
	return SljitTargetArchitecture::ARM_32;
#elif defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
	return SljitTargetArchitecture::ARM_64;
#elif defined(SLJIT_CONFIG_PPC) && SLJIT_CONFIG_PPC
	return SljitTargetArchitecture::PPC;
#elif defined(SLJIT_CONFIG_MIPS) && SLJIT_CONFIG_MIPS
	return SljitTargetArchitecture::MIPS;
#elif defined(SLJIT_CONFIG_RISCV) && SLJIT_CONFIG_RISCV
	return SljitTargetArchitecture::RISCV;
#elif defined(SLJIT_CONFIG_S390X) && SLJIT_CONFIG_S390X
	return SljitTargetArchitecture::S390X;
#elif defined(SLJIT_CONFIG_LOONGARCH) && SLJIT_CONFIG_LOONGARCH
	return SljitTargetArchitecture::LOONGARCH;
#else
	return SljitTargetArchitecture::UNKNOWN;
#endif
}

static SljitTargetCapabilities DetectSljitTargetCapabilities() {
	SljitTargetCapabilities result;
	result.architecture = SljitArchitecture();
	result.machine_word_bytes = static_cast<sljit_s32>(sizeof(sljit_sw));
	result.registers.register_count = SLJIT_NUMBER_OF_REGISTERS;
	result.registers.saved_register_count = SLJIT_NUMBER_OF_SAVED_REGISTERS;
	result.platform_available = sljit_get_platform_name() != nullptr;
	if (!result.platform_available) {
		return result;
	}
	for (sljit_s32 saved_index = 0; saved_index < result.registers.saved_register_count; saved_index++) {
		if (sljit_get_register_index(SLJIT_GP_REGISTER, SLJIT_S(saved_index)) < 0) {
			break;
		}
		result.registers.addressable_saved_register_count++;
	}
	result.simd_available = sljit_has_cpu_feature(SLJIT_HAS_SIMD);
	return result;
}

const SljitTargetCapabilities &GetSljitTargetCapabilities() {
	static const auto result = DetectSljitTargetCapabilities();
	return result;
}

sljit_s32 SljitSavedRegisterAt(sljit_s32 index) {
	if (index < 0 || !GetSljitTargetCapabilities().registers.HasAddressableSavedRegisters(index + 1)) {
		return 0;
	}
	return SLJIT_S(index);
}

bool SljitPlatformAvailable() {
	return GetSljitTargetCapabilities().BackendAvailable();
}

} // namespace duckdb
