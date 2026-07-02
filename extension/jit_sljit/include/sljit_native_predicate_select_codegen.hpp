#pragma once

#include "sljit_codegen_internal.hpp"

#include "sljitLir.h"

namespace duckdb {

void EmitSljitNativeSelectLoadResultIndex(struct sljit_compiler *compiler);
void EmitSljitNativeSelectLoadResultAndSourceIndex(struct sljit_compiler *compiler);
void EmitSljitNativeSelectStoreTrue(struct sljit_compiler *compiler);
void EmitSljitNativeSelectStoreFalse(struct sljit_compiler *compiler);
void EmitSljitNativeSelectStoreResult(struct sljit_compiler *compiler, bool selected);
void EmitSljitNativeSelectInitLoop(struct sljit_compiler *compiler);
void EmitSljitNativeSelectFinishLoop(struct sljit_compiler *compiler);

template <class EMIT_ROW>
static inline void EmitSljitNativeSelectRowLoop(struct sljit_compiler *compiler, EMIT_ROW &&emit_row) {
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	emit_row(loop);
	EmitNextSljitNativeVectorLoop(compiler, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
	EmitSljitNativeSelectFinishLoop(compiler);
}

} // namespace duckdb
