# ClickBench q33 grouping representation screen

This screen tests whether Nitro's general adaptive multi-long grouping path can
reduce q33's peak memory relative to the default full-width pair packed-identity
representation. Runs use a 12 GiB heap, five warmups, exact-result validation,
thread-allocation measurement, operator CPU metrics, and sampled peak memory.

## Result

| Nitro representation | p50 wall ms | p50 CPU ms | allocation MiB | sampled query peak MiB |
|---|---:|---:|---:|---:|
| Default packed identity (full board) | 8,428.4 | 17,220 | 30,585.1 | 5,921.9 |
| Adaptive pair (`nitro.group.fullWidthPairPackedIdentity=false`) | 9,072.9 | 16,310 | 38,780.3 | 8,651.9 |

The adaptive alternative is rejected. It raises wall time by 7.6%, allocation
by 26.8%, and sampled peak memory by 46.1%. It also drives repeated major GCs
near the 12 GiB heap ceiling. The apparent 5.3% CPU reduction does not offset
those structural resource regressions and is not a matched interleaved A/B.

The default diagnostic confirms q33's `I64Vector + I32Vector` key selects the
deferred full-width pair packed-identity path. The default remains the correct
general representation. No production code was changed.

A second screen capped generic grouped-state headroom at 150% above one million
groups. It was also rejected: the warmed result was effectively unchanged at
8,494.7 ms wall, 17,230 CPU-ms, 30,564.9 MiB allocated, and 5,921.9 MiB peak.
A capacity-transition diagnostic showed why: the registered aggregation units
already select exact logical capacity (`selected == needed`) and use incremental
storage, bypassing generic vector headroom. The candidate and its diagnostic
instrumentation were reverted.

Artifacts:

- `representation.log`: one-warmup representation diagnostic for the default.
- `adaptive-pair.log`: five-warmup, three-measure adaptive-alternative screen.
- `../20260807-q33-state-growth/capped-150.log`: five-warmup capped-state screen.
- `../20260807-q33-state-growth/growth-diagnostic.log`: capacity-transition evidence.
