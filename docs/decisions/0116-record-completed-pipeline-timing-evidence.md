# ADR-0116: Record completed pipeline timing evidence

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §10, §18
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context

Warmed q20 retains a latency gap larger than its CPU gap. CPU profiles show similar compression/decompression
work, while wall profiles contain many idle/service threads with no direct query dependency attribution. Completed
QueryInfo already retains task and pipeline start/end timestamps and cumulative CPU/blocked statistics. EXPLAIN
does not expose those timing envelopes in a convenient structured form.

## Decision

Persist that existing evidence per measured point member and repetition beside EXPLAIN, after all measurements
and profiling have stopped. Record query/stage/task/pipeline identities, upstream stages, operator plan identities,
epoch-millisecond timing envelopes, driver count, and cumulative nanosecond CPU/scheduled/input/output-blocked
times. Preserve missing timestamps as null, never zero. No scheduling, planning, engine, heap or warmup policy changes.

A pipeline envelope spans its first driver start to last driver end. It is not a continuous CPU interval. Blocked and
scheduled totals can overlap across drivers; do not add them to reconstruct query latency. Stage dependencies and
the physical plan remain necessary to interpret possible critical paths. Existing reports and the frozen remote
board remain unchanged; the new export is a diagnostic addition, not retroactive evidence for older runs.

## Validation

The benchmark-harness suite passes 16 tests, including JSON identity, units and missing timestamps. A local q20
pair with 100 warmups and nine measurements per engine passes exact-result and native-island checks and exports
360 unique run/task/pipeline rows per engine (40 pipelines in every measured run). Evidence is retained in
`pipeline-timing-harness-final-tests-20260913.log` and the `tpcds-q20-pipeline-timings-{nitro,cork}-20260913`
result directories under the notes evidence root. Pipeline timing envelopes identified source input-blocked time
as an investigation target; they do not by themselves identify which future caused the wait.
