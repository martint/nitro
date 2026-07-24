# Metadata-derived scalar invocation experiment

Date: 2026-07-24

All runs use JDK 26 (`/opt/java/openjdk`), one unpinned JMH thread, a 12 GB
heap, transparent huge pages, 10x1s warmup, 5x1s measurement, steady-state
allocation, and the same eight-event `perfnorm` bundle:
instructions, cycles, L1D misses/loads, dTLB misses/loads, branch misses, and
branches. TPC-H and TPC-DS use SF10 Parquet; ClickBench uses the established
Hits dataset. The harnesses and query shapes were unchanged.

The experiment asked whether ordinary scalar projection lowering could be
derived from carrier/null/failure/determinism metadata plus an opaque method
handle, avoiding a provider-authored projection program.

## Result

The activation was rejected. No production function, registry, evaluator, or
projection-compiler change from this experiment remains in the source tree.

The initial constructor-field method-handle shape regressed q01 by 21.10% in
duration and 23.34% in instructions. Hidden-class constant call sites repaired
q01 and q43, but q39 remained placement-sensitive. Restricting activation to
five I64 functions regressed the three-fork q39 comparison by 6.08% duration,
2.07% instructions, 6.06% cycles, 8.98% L1D misses, 31.42% dTLB misses, and
8.58% branch misses.

Replacing the synthetic helper call directly with `invokedynamic` still
regressed three-fork q39 versus the first exact-parent reverse control:

| Metric | Candidate delta |
|---|---:|
| duration | +5.25% |
| allocation | -0.39% |
| instructions | +1.49% |
| cycles | +4.89% |
| L1D misses | +7.55% |
| L1D loads | +1.83% |
| dTLB misses | +15.37% |
| dTLB loads | +20.20% |
| branch misses | +6.10% |
| branches | +1.34% |

Even after restoring all production builtins, the enlarged inactive compiler
plumbing and then the registry-only metadata slice remained correlated with
broad q39 regressions against a final reverse-parent run. The registry-only
comparison was +13.35% duration, +3.08% instructions, +11.49% cycles, +14.14%
L1D misses, +24.16% dTLB misses, and +12.77% branch misses. That sensitivity is
not an acceptable basis for promotion.

## Architectural conclusion

Carrier, null, failure, determinism, and invocation metadata are sufficient to
construct a correct generic scalar call, but they do not reveal the value
operation represented by an opaque method handle. A provider-independent
engine therefore cannot derive direct `+`, comparison, or UTF semantics from
that metadata alone. Provider lowering remains an optional dynamic capability
until a classloader-neutral code-generation protocol can express executable
semantics without a closed engine vocabulary and without perturbing the hot
shape. UTF is not a special semantic exception; only its variable-width
physical access may need representation-aware generated code.

The published performance board is unchanged.
