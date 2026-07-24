# Explicit grouping code-generation resources

This focused architecture control validates moving the generated grouping-table,
dictionary-hash, mixed-composite, and dictionary-record-equality caches from
process-global static state into the engine-owned
`OperatorCodeGenerationResources`. It does not replace the published
165-query board.

## Method

- JDK `/opt/java/openjdk` (26)
- Nitro operator harnesses, unpinned, one JMH thread
- 12 GiB heap with transparent huge pages
- candidate first, then exact parent `98c33f35` from a sparse jj workspace
- short screen: one fork, 5 × 1 s warmup, 5 × 1 s measurement
- escalated q39 control: three forks, 10 × 1 s warmup, 5 × 1 s measurement
- allocation and all eight publication hardware counters captured together

Positive deltas mean the candidate used more than the reverse-order parent.

| Control | Duration | Allocation | Instructions | Cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | Branch misses | Branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-DS q17, short | -0.37% | -0.02% | -0.35% | -1.27% | -1.23% | -0.78% | -3.39% | -2.75% | -0.74% | -0.89% |
| TPC-DS q39, short | +13.23% | -0.61% | +2.23% | +13.25% | +12.71% | +1.21% | +33.53% | +15.58% | +2.67% | +2.52% |
| TPC-DS q39, 10-warmup/3-fork | -7.65% | +0.00% | -2.27% | -6.84% | -8.58% | -1.58% | -7.22% | +3.91% | -3.13% | -2.18% |

The first q39 fork failed the counter gate and was escalated rather than
accepted. The longer control reversed the apparent regression: duration and
ordinary work improved, allocation was unchanged, and every captured counter
except dTLB loads improved. Q39 remains visibly bimodal across iterations, so
the isolated +3.91% dTLB-load movement is treated as placement sensitivity, not
as an architectural win. The q17 guard is neutral to favorable. There is no
stable measured regression from this slice.

The q39 disagreement reinforces the separate architecture warning: the current
hot grouping pipeline remains too sensitive to harmless source/layout changes.
Future cleanup should reduce that sensitivity; it must not rely on a favorable
placement outcome.

Each JSON file is machine-readable; the matching log contains the complete
environment and raw trial output.
