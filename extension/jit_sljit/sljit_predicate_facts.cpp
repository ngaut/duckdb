#include "sljit_predicate_codegen_helpers.hpp"

namespace duckdb {

bool SljitPredicateDoubleCompareUsesHelper(const SljitNativePredicate &predicate) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT:
		return NativeDoubleSourceUsesHelper(predicate.double_source_kind);
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
		return NativeDoubleSourceUsesHelper(predicate.double_source_kind) ||
		       NativeDoubleSourceUsesHelper(predicate.double_right_source_kind);
	case SljitNativePredicateKind::NOT:
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		return predicate.child && SljitPredicateDoubleCompareUsesHelper(*predicate.child);
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (child && SljitPredicateDoubleCompareUsesHelper(*child)) {
				return true;
			}
		}
		return false;
	default:
		return false;
	}
}

bool SljitPredicateUsesSourceValidity(const SljitNativePredicate &predicate, const vector<bool> &source_not_null) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		return false;
	case SljitNativePredicateKind::NOT:
		return predicate.child && SljitPredicateUsesSourceValidity(*predicate.child, source_not_null);
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (child && SljitPredicateUsesSourceValidity(*child, source_not_null)) {
				return true;
			}
		}
		return false;
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		for (auto source_index : predicate.guard_source_indices) {
			if (PredicateSourceCanHaveNull(source_index, source_not_null)) {
				return true;
			}
		}
		return predicate.child && SljitPredicateUsesSourceValidity(*predicate.child, source_not_null);
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
		return PredicateSourceCanHaveNull(predicate.source_index, source_not_null) ||
		       PredicateSourceCanHaveNull(predicate.right_source_index, source_not_null);
	default:
		return PredicateSourceCanHaveNull(predicate.source_index, source_not_null);
	}
}

} // namespace duckdb
