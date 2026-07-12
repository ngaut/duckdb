//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_payload_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_value_abi.hpp"
#include "sljit_region_plan.hpp"

namespace duckdb {

enum class SljitAggregateTypedPayloadKind : uint8_t { INVALID, INT32, INT64, DECIMAL64 };

struct SljitAggregatePayloadDescriptor {
	idx_t aggregate_index = DConstants::INVALID_INDEX;
	AggregatePrimitiveUpdateKind primitive_kind = AggregatePrimitiveUpdateKind::NONE;
	PhysicalType input_type = PhysicalType::INVALID;
	SljitAggregatePayloadValueABI value_abi = SljitAggregatePayloadValueABI::INVALID;
	SljitAggregateTypedPayloadKind typed_payload_kind = SljitAggregateTypedPayloadKind::INVALID;
	idx_t state_size = 0;
	idx_t state_value_offset = 0;
	idx_t state_is_set_offset = 0;
	bool has_payload = false;

	bool IsMachineWord() const {
		return value_abi == SljitAggregatePayloadValueABI::SIGNED_MACHINE_WORD;
	}

	bool IsDoubleWord() const {
		return value_abi == SljitAggregatePayloadValueABI::SIGNED_DOUBLE_WORD;
	}

	bool TryGetTypedIntegerKind(SljitNativeIntegerKind &result) const {
		switch (typed_payload_kind) {
		case SljitAggregateTypedPayloadKind::INT32:
			result = SljitNativeIntegerKind::INT32;
			return true;
		case SljitAggregateTypedPayloadKind::INT64:
			result = SljitNativeIntegerKind::INT64;
			return true;
		case SljitAggregateTypedPayloadKind::DECIMAL64:
			result = SljitNativeIntegerKind::DECIMAL64;
			return true;
		default:
			return false;
		}
	}
};

static inline bool SljitTryBindAggregatePayloadDescriptor(const SljitNativeRegionExpressionPlan &payload,
                                                          const ExecutionRegionAggregateInput &aggregate,
                                                          SljitAggregatePayloadDescriptor &descriptor) {
	descriptor = SljitAggregatePayloadDescriptor();
	if (!aggregate.primitive_update_ready ||
	    !AggregatePrimitiveUpdateKindIsSupported(aggregate.primitive_update_kind)) {
		return false;
	}
	descriptor.aggregate_index = aggregate.aggregate_index;
	descriptor.primitive_kind = aggregate.primitive_update_kind;
	descriptor.state_size = aggregate.primitive_update_state_size;
	descriptor.state_value_offset = aggregate.primitive_update_state_value_offset;
	descriptor.state_is_set_offset = aggregate.primitive_update_state_is_set_offset;
	descriptor.has_payload = AggregatePrimitiveUpdateRequiresPayload(descriptor.primitive_kind);
	if (!descriptor.has_payload) {
		return descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR && aggregate.child_count == 0 &&
		       aggregate.child_types.empty();
	}
	if (aggregate.child_count != 1 || aggregate.child_types.size() != 1 ||
	    payload.return_type.InternalType() != aggregate.child_types[0].InternalType()) {
		return false;
	}
	descriptor.input_type = aggregate.child_types[0].InternalType();
	switch (descriptor.input_type) {
	case PhysicalType::INT32:
		descriptor.typed_payload_kind = SljitAggregateTypedPayloadKind::INT32;
		break;
	case PhysicalType::INT64:
		descriptor.typed_payload_kind = aggregate.child_types[0].id() == LogicalTypeId::DECIMAL
		                                    ? SljitAggregateTypedPayloadKind::DECIMAL64
		                                    : SljitAggregateTypedPayloadKind::INT64;
		break;
	default:
		break;
	}
	if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
		descriptor.value_abi = SljitAggregatePayloadValueABI::VALIDITY_ONLY;
		return true;
	}
	if (aggregate.primitive_update_input_type != descriptor.input_type) {
		return false;
	}
	descriptor.value_abi = SljitGetAggregatePayloadValueABI(descriptor.input_type);
	if (descriptor.value_abi == SljitAggregatePayloadValueABI::INVALID) {
		return false;
	}
	switch (descriptor.primitive_kind) {
	case AggregatePrimitiveUpdateKind::SUM_INT64:
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		return descriptor.value_abi == SljitAggregatePayloadValueABI::SIGNED_MACHINE_WORD ||
		       descriptor.value_abi == SljitAggregatePayloadValueABI::SIGNED_DOUBLE_WORD;
	case AggregatePrimitiveUpdateKind::SUM_DOUBLE:
		return descriptor.value_abi == SljitAggregatePayloadValueABI::DOUBLE;
	default:
		return false;
	}
}

} // namespace duckdb
