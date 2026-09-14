# ADR-0139: Batch independent long join probes

- **Status:** accepted
- **Date:** 2026-09-14
- **Spec:** §13, §16, §18
- **Related:** ADR-0018, ADR-0027, ADR-0036, ADR-0105, rejected ADR-0115/0119

## Context

Current TPC-H q09/q13/q21 profiles retain substantial exact join-table lookup cost. Narrowing dispatch to make a
scalar lookup inline did not qualify in ADR-0115, and replacing the SIMD tag reduction failed in ADR-0119. Neither
experiment overlapped independent table accesses. ADR-0105 established this distinction for the distinct table.

## Decision and correlation

Stage independent initial tag probes, then load their candidate keys in a separate pass, then resolve exact matches
and collisions. Preserve existing hashing, grouped-table representation, load factor, exact equality and duplicate
row ordering. Empty or collided candidates never authorize a false match; the existing scalar lookup resolves them.
No join semantics, planner, scheduler, type/function registry or filter admission changes.

Scratch belongs to the constructed per-probe view and injected allocator pool, never the immutable shared build.
Its retained-byte accounting and release must cover standalone and prepared-build consumers. Input key staging must
respect selected positions, nulls and complete-build membership before probing. Small/non-grouped paths keep their
existing exact lookup until evidence justifies broader batching. There is no feature-enabling flag.

This conforms to immutable prepared builds, explicit scratch ownership and physical batch specialization. It is not
a revival of the rejected tag-mask or inlining-only candidates. First test the table mechanism independently; wire it
into execution and accept it only if complete correctness and warmed SQL breadth qualification show a useful gain.

## Required validation

Cover zero/full-width keys, absent keys, tag collisions, displaced groups, duplicates, unallocated tables, table growth
before probing and exact selected output order. Verify scratch bounds/reuse/release and independent concurrent probe
views. Compare component and SQL CPU/allocation with frozen controls; preserve all native execution and result gates.
The accepted admission and completed qualification evidence are recorded below.

## Initial measurements and bounded admission

Two-fork warmed component measurements rotate through one million probe keys. At one/four million table slots,
all-hit batched lookup costs 0.392/0.307 of scalar CPU; zero-hit lookup instead costs 1.230/1.519. Small tables lose
even at high hit rates. These are table-kernel results, not SQL speedups.

The accepted constructed policy requires at least 1,048,576 table slots and 4,096 selected probe rows.
Two successive bounded 32-position samples with at least 75% matches admit batching; a sample below 50% exits.
Samples between the thresholds preserve the active strategy but break the admission streak. Every eligible batch
is observed, so distribution shifts are not hidden by a one-time startup decision. Direct/sequence lookups remain
unchanged. The gate, snapshot counters, scratch accounting and null/selection behavior are covered together.
The admission is qualified against paired SQL and adverse component workloads; it is not a claim of universal gain.

## Focused SQL and correctness qualification

The complete Nitro suite passes 2,189 tests with no failures or errors and 394 skips. Tests exercise exact selected
order, inaccessible null values, pool reuse, independent prepared probes, concurrent readers of one immutable build,
admission hysteresis and actual use by a large build. Snapshots report the latest admission reason separately from
the next eligible batch's strategy: a small batch retains learned state without being counted as batched execution.

Eight-query local TPC-H parent/candidate/parent qualification found a repeatable q13 CPU reduction. A longer
40-warmup/15-measurement repeat measured 3.237/2.926/3.247 CPU-seconds; remote paired execution measured
10.757/9.529/10.708 seconds. These are separate configurations, not one combined sample. Local q12 and q21 longer
guards overlap their controls; q09/q21 have no established gain. Component gains are not substituted for SQL results.

The remote four-suite functional inventory passes all 409 SQL members at the existing 32 GiB heap. A local 12 GiB
ClickBench q23 exhaustion reproduces with the unchanged parent and is not attributed to this join change.
The combined character-allocation fix and final diagnostic snapshot passed the exact-runtime gate and the unchanged
warmed 182-point board. q13 CPU is 9.620 seconds versus the preceding Nitro board's 10.874 and Cork's 10.338,
with separated ranges. The board contains 173 CPU wins and nine remaining losses, including q09 and q21; batching
is not claimed to fix those queries. See `2026-09-14-char-join-complete-board.md` for the full scope and limitations.

## Rejected medium-table admission refinement

An outcome diagnostic finds large flat batches with 100% sampled matches at a 262,144-slot grouped table in q09.
At that observed capacity, an independent two-fork, 8,192-row component matrix measures batched/scalar ratios
1.557 at zero hits, 0.767 at half hits, and 0.412 at all hits. A separate refinement lowered only the constructed
capacity floor to 262,144 slots; the 4,096-row floor and high-hit hysteresis remain unchanged. Smaller tables,
direct indexes and scalar sparse-domain layouts are unchanged. This is physical admission, not query recognition.

The 40-warmup/15-measurement q09 bracket measured CPU 5.710/5.639/5.712 seconds. A longer 80-warmup/60-measurement
bracket measured 5.608/5.582/5.695 seconds: ranges overlap, and the gain against the first control shrinks below 0.5%.
Latency and allocation provide no compensating clear benefit. This is insufficient evidence to broaden admission
after staging and SQL overhead; it is not proof that the table kernel fails at that capacity. Restore the original
floor and remove the dedicated refinement test. The frozen board and combined mixed-egress gate use that original
floor. Evidence is retained in both `small-join-admission-20260914` and `small-join-admission-long-20260914`.
Evidence: `2026-09-14-join-probe-admission-frontier.md` and the linked component/outcome captures.
