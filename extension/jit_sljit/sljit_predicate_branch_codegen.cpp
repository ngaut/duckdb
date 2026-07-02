//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_branch_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_predicate_codegen_helpers.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

SljitPredicateBranches EmitSljitPredicateBranches(struct sljit_compiler *compiler,
                                                  const SljitNativePredicate &predicate,
                                                  vector<shared_ptr<void>> &owned_data,
                                                  const vector<bool> &source_not_null, bool sources_all_valid) {
	SljitPredicateBranches result;
	if (TryEmitSljitStructuralPredicateBranches(compiler, predicate, owned_data, source_not_null, sources_all_valid,
	                                            result)) {
		return result;
	}
	if (TryEmitSljitNumericPredicateBranches(compiler, predicate, source_not_null, sources_all_valid, result)) {
		return result;
	}
	if (TryEmitSljitStringPredicateBranches(compiler, predicate, owned_data, source_not_null, sources_all_valid,
	                                        result)) {
		return result;
	}
	throw InternalException("Unknown SLJIT native predicate kind");
}

} // namespace duckdb
