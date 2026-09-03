# ADR-0032: Require algorithmically equivalent evidence before adopting native kernels

- **Status:** accepted
- **Date:** 2026-09-03 (retrospective; decided 2026-08-17)
- **Spec:** §16, §18
- **Historical evidence:** `2026-08-17-rust-ffm-kernel-frontier.md`

## Context

Rust/FFM has been proposed as a route to better machine code. Existing experiments range from modest wins to clear
losses, and several comparisons mixed language, algorithm, layout, prefetch, output allocation, or library provenance.
The FFM boundary can erase a kernel win through gathering or copying.

## Decision

Native code remains an evidence-driven optional physical implementation, not an architectural direction or production
dependency. Compare algorithmically equivalent Java, Vector API, and Rust kernels over the same buffers and semantics;
include transition, copying, safepoint, build provenance, and end-to-end SQL effects. Admit only broad, reproducible
wins large enough to justify deployment complexity.

## Consequences

- Large page/vector kernels remain valid experiments.
- Short monomorphic probes with good Java layouts are negative controls rather than assumed candidates.
- Native binaries require CPU dispatch, packaging, security, observability, and reproducible build design before use.
- A native implementation cannot claim a compiler win when it changes the algorithm or layout.

## Alternatives considered

**Rewrite hot operators in Rust.** Broad and unsupported by existing measurements.

**Reject native code categorically.** Existing decode/compression evidence does not justify that conclusion either.

**Compare only isolated kernel throughput.** Misses FFM transition, data movement, and critical-path effects.
