# JIT Microbenchmarks

These benchmarks measure specific SLJIT generated-body shapes. They are not
broad JIT benchmarks, and a body speedup is not by itself proof that a whole
native-source region should be admitted while the source is still a DuckDB-owned
source boundary.

The admitted-shape templates are self-checking. Their `result_query` verifies
the SQL answer and the JIT event stream after the measured query. The templates
call `duckdb_jit_clear_events()` during initialization and cleanup so setup
events and previous hot runs cannot mask or satisfy the measured-query shape
proof:

- `off` must produce no compiled region event;
- `auto` must keep source-boundary native-source regions non-fused, even when a
  body shape has a non-negative admission score;
- `force` must compile only native fused native-source or full-pipeline regions
  that have executable code.

This keeps benchmark results tied to the region shape they claim to measure.

Run the manifest-backed speed proof:

```sh
python3 benchmark/micro/jit/verify_micro_jit_inventory.py
python3 benchmark/micro/jit/micro_jit_benchmark.py --out-dir /tmp/duckdb_jit_micro_benchmark
python3 benchmark/micro/jit/verify_micro_jit_benchmark.py /tmp/duckdb_jit_micro_benchmark
```

The inventory verifier checks that benchmark families match backend shape
constants, proof strings, thresholds, templates, and file names. Diagnostic
benchmark families may have `off` and `force` files, but no `auto` file until
they have their own measured whole-region admission proof.

The speed verifier reads benchmark-runner timings and checks that the reported
speedup and `faster_than_off` flags match the raw medians. `auto` is retained as
a policy observation and should not be interpreted as whole-region admission
proof for source-boundary sources.

Run the diagnostic timing suite for shapes that remain deliberately unadmitted:

```sh
python3 benchmark/micro/jit/micro_jit_diagnostic_benchmark.py --out-dir /tmp/duckdb_jit_micro_diagnostic
python3 benchmark/micro/jit/verify_micro_jit_diagnostic_benchmark.py /tmp/duckdb_jit_micro_diagnostic
```

The diagnostic verifier covers projection-only, filter-only, generic
filter/projection, and full-pipeline decimal projection/ungrouped-SUM shapes at
threshold and large sizes. Threshold diagnostic shapes must stay below the
admission margin; if one crosses it, the verifier forces the shape to be
promoted with a real auto-admission proof or explicitly reclassified.

Run the diagnostic cardinality sweep when deciding whether a rejected shape is
merely too small at the threshold or fundamentally not profitable:

```sh
python3 benchmark/micro/jit/micro_jit_diagnostic_sweep.py --out-dir /tmp/duckdb_jit_micro_sweep
python3 benchmark/micro/jit/verify_micro_jit_diagnostic_sweep.py /tmp/duckdb_jit_micro_sweep
```

The sweep emits generated self-checking benchmark files, `summary.csv`, and
`family_summary.csv`. The family summary records the first tested cardinality
that reaches the admission margin, if any.

Run the full-pipeline selectivity sweep when deciding whether a q06-style force
signal is explained by scan-filter selectivity:

```sh
python3 benchmark/micro/jit/micro_jit_full_pipeline_selectivity_sweep.py \
  --out-dir /tmp/duckdb_jit_micro_full_pipeline_selectivity
python3 benchmark/micro/jit/verify_micro_jit_full_pipeline_selectivity_sweep.py \
  /tmp/duckdb_jit_micro_full_pipeline_selectivity
```

The selectivity sweep emits generated self-checking benchmark files,
`summary.csv`, and `selectivity_summary.csv`. It keeps row count fixed and
varies the surviving rows from the native table-scan filter. This separates
shape profitability from source-filter selectivity.

The manifest-backed trace companion records the same admitted shapes through the
JIT event/runtime/counter path:

```sh
python3 benchmark/micro/jit/micro_jit_trace.py --out-dir /tmp/duckdb_jit_micro_trace
python3 benchmark/micro/jit/verify_micro_jit_trace.py /tmp/duckdb_jit_micro_trace
```

This trace verifies admission proof strings, native code generation, generated
IR, skipped non-fused boundary candidates, kernel runtime rows, and counters for
`off`, `auto`, and `force`. It is not the speed proof: `EXPLAIN ANALYZE` and
`jit_trace_runtime=true` intentionally add diagnostic overhead. Use
`benchmark_runner` for speed measurements and the trace verifier for
shape/runtime honesty.

## Native Filter/Projection

`native_filter_projection_*.benchmark` measures the SLJIT generic native
filter/projection region:

- `sljit:full-pipeline:filter-projection`
- one generated integer filter
- one generated integer projection
- native table-scan native-source execution plus normal aggregate boundary
- single-threaded execution

The template materializes a table source intentionally. DuckDB-owned range
sources are source boundaries and are not valid proof for generated body-only
execution.

Run the threshold proof:

```sh
build/release/benchmark/benchmark_runner --disable-timeout \
  benchmark/micro/jit/native_filter_projection_threshold_off.benchmark
build/release/benchmark/benchmark_runner --disable-timeout \
  benchmark/micro/jit/native_filter_projection_threshold_auto.benchmark
build/release/benchmark/benchmark_runner --disable-timeout \
  benchmark/micro/jit/native_filter_projection_threshold_force.benchmark
```

Run the large-shape proof:

```sh
build/release/benchmark/benchmark_runner --disable-timeout benchmark/micro/jit/native_filter_projection_off.benchmark
build/release/benchmark/benchmark_runner --disable-timeout benchmark/micro/jit/native_filter_projection_auto.benchmark
build/release/benchmark/benchmark_runner --disable-timeout benchmark/micro/jit/native_filter_projection_force.benchmark
```

The exact values are machine-dependent. Do not maintain benchmark timing tables
by hand in this README; `micro_jit_benchmark.py` writes the current repeated-run
timings and medians to `summary.csv`. The invariant is that `force` must compile
only native fused fragments and must leave source-boundary native-source regions
non-fused, the same as `auto`.

## Native Projection Chain

`native_projection_chain_*.benchmark` measures the SLJIT projection-chain
generated body:

- `sljit:full-pipeline:projection-chain`
- adjacent generated integer projections that backend lowering composes into
  one generated projection
- normal DuckDB scan and aggregate boundaries
- single-threaded execution

Run the threshold proof:

```sh
build/release/benchmark/benchmark_runner --disable-timeout \
  benchmark/micro/jit/native_projection_chain_threshold_off.benchmark
build/release/benchmark/benchmark_runner --disable-timeout \
  benchmark/micro/jit/native_projection_chain_threshold_auto.benchmark
build/release/benchmark/benchmark_runner --disable-timeout \
  benchmark/micro/jit/native_projection_chain_threshold_force.benchmark
```

Run the large-shape proof:

```sh
build/release/benchmark/benchmark_runner --disable-timeout benchmark/micro/jit/native_projection_chain_off.benchmark
build/release/benchmark/benchmark_runner --disable-timeout benchmark/micro/jit/native_projection_chain_auto.benchmark
build/release/benchmark/benchmark_runner --disable-timeout benchmark/micro/jit/native_projection_chain_force.benchmark
```

## Native Projection

`native_projection_*.benchmark` measures a diagnostic SLJIT native-source shape:

- `sljit:full-pipeline:projection`
- one generated integer projection followed by a native typed reference projection
- normal DuckDB scan and aggregate boundaries
- single-threaded execution

This shape is not auto-admitted yet. The diagnostic cardinality sweep is the
place to study whether larger native-source projection kernels become
profitable, but promotion requires a stable threshold benchmark proof and a
backend admission rule.

Run the threshold diagnostic:

```sh
build/release/benchmark/benchmark_runner --disable-timeout \
  benchmark/micro/jit/native_projection_threshold_off.benchmark
build/release/benchmark/benchmark_runner --disable-timeout \
  benchmark/micro/jit/native_projection_threshold_force.benchmark
```

Run the large-shape diagnostic:

```sh
build/release/benchmark/benchmark_runner --disable-timeout benchmark/micro/jit/native_projection_off.benchmark
build/release/benchmark/benchmark_runner --disable-timeout benchmark/micro/jit/native_projection_force.benchmark
```

Projection-only, filter-only, and generic multi-op regions have separate
benchmark files but no `auto` admission rule unless their own threshold
measurement proves a win. Backend lowering assigns deterministic native-source
shape keys such as `sljit:full-pipeline:projection`,
`sljit:full-pipeline:filter`, and
`sljit:full-pipeline:filter-projection-projection` so skipped regions remain attributable.
Native table-scan native-source filter/projection is treated the same way:
`sljit:full-pipeline:filter-projection` is a capability/diagnostic key, not an
`auto` key, until a repeated production benchmark proves the whole region wins.

## Native Full-Pipeline Decimal Projection Ungrouped SUM

`native_full_pipeline_decimal_projection_ungrouped_sum_*.benchmark` measures a
diagnostic full-pipeline scan/filter/projection/aggregate shape:

- `sljit:full-pipeline:filter-projection-ungrouped-aggregate-update`
- native DuckDB table-scan source with generated ownership of one pushed integer comparison filter
- one generated DECIMAL64 projection
- one generated native ungrouped aggregate SUM update
- single-threaded execution

The template is self-checking: `force` must compile exactly one native fused
full-pipeline region for the expected shape, and `off` must compile nothing.
This shape is deliberately diagnostic, not admitted. Repeated benchmark-runner
measurements currently show force slower than off at both the threshold and
large sizes. The full-pipeline selectivity sweep also shows force slower from
1% through 75% surviving rows. Auto admission must remain absent until a new
measured proof contradicts that result.

Run the threshold diagnostic:

```sh
build/release/benchmark/benchmark_runner --disable-timeout \
  benchmark/micro/jit/native_full_pipeline_decimal_projection_ungrouped_sum_threshold_off.benchmark
build/release/benchmark/benchmark_runner --disable-timeout \
  benchmark/micro/jit/native_full_pipeline_decimal_projection_ungrouped_sum_threshold_force.benchmark
```

Run the large-shape diagnostic:

```sh
build/release/benchmark/benchmark_runner --disable-timeout \
  benchmark/micro/jit/native_full_pipeline_decimal_projection_ungrouped_sum_off.benchmark
build/release/benchmark/benchmark_runner --disable-timeout \
  benchmark/micro/jit/native_full_pipeline_decimal_projection_ungrouped_sum_force.benchmark
```
