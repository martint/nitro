# Three-engine board (2026-07-03) — Nitro interpreter-default

All three engines re-run **same-day** on this machine, `taskset -c 0`, single-thread.
Nitro/Trino: JMH `@Fork1 wi3 i5 w1s r2s`. Velox: `run_query_verbose` (CB/TPC-DS) /
`bm_min_iters=3` (TPC-H), `--num_drivers=1 --num_io_threads=1`, TPC-DS `--cache_gb=12`.
Ratio > 1 = Nitro slower than that engine.

**Why same-day:** the reused 2026-06-21 Velox data was *stale*, not merely cross-day — e.g.
same-day Velox TPC-H q7 = 2910ms vs the reused 1530ms, because this session's Velox harness fix
(match Trino's plan: build full 15M orders, not the unfair nation-pushdown 1.2M) makes the fair
number slower. Reusing it would have shown a fake ~2x N/V improvement. Everything here is same-day
with the current harnesses.

**Nitro config:** interpreter-default (fused projection compiler flipped to opt-in this session);
plus the full monomorphic null-free scalar-kernel rollout + substr dict-peel + harness fairness fix.

## Geomeans of per-query ratios

| suite | #N∩T | N/T geo | N/T med | #N∩V | N/V geo | N/V med |
|---|---|---|---|---|---|---|
| TPC-H | 21 | 0.75x | 0.76x | 21 | 1.54x | 1.42x |
| TPC-DS | 99 | 0.27x | 0.29x | 98 | 1.51x | 1.48x |
| ClickBench | 36 | 0.65x | 0.75x | 32 | 0.89x | 0.94x |

- **vs Trino:** Nitro is faster on all three suites (1.3x TPC-H, 3.7x TPC-DS, 1.5x ClickBench).
- **vs Velox:** Nitro ~1.5x slower on TPC-H/TPC-DS, ~1.1x faster on ClickBench.

## Nitro SLOWER than Trino (>1.10x) — the only regressions

  TPC-H q10: nitro=3255 trino=2938 (1.11x)   [marginal]

## Top Velox laggards (N/V > 2.0) — the operator-level frontier

  TPC-DS q88: nitro=1322 velox=182  (7.26x)   [DF/skip-decode + native scan]
  TPC-DS q9:  nitro=3803 velox=735  (5.17x)
  TPC-DS q83: nitro=137  velox=30   (4.57x)
  TPC-DS q64: nitro=5213 velox=1290 (4.04x)   [3-key join probe]
  TPC-DS q81: nitro=507  velox=133  (3.81x)
  TPC-DS q23: nitro=7682 velox=2330 (3.30x)
  TPC-DS q85: nitro=955  velox=301  (3.17x)
  TPC-DS q10: nitro=664  velox=220  (3.02x)
  TPC-H  q15: nitro=1934 velox=644  (3.00x)
  TPC-DS q45: nitro=343  velox=115  (2.98x)
  TPC-DS q69: nitro=660  velox=222  (2.97x)
  TPC-DS q75: nitro=3262 velox=1120 (2.91x)
  TPC-H  q16: nitro=721  velox=249  (2.90x)
  TPC-DS q76: nitro=1043 velox=364  (2.87x)
  TPC-DS q15: nitro=270  velox=97   (2.78x)

The Velox gaps are operator-level (scan/DF pruning, join-probe, grouping) — the expression-eval
workstream this session doesn't touch them. q88's 7.26x is Velox's native dynamic-filter pruning +
skip-decode; the join-heavy ones (q64/q9/q23) are the probe cache-miss/prefetch wall.
