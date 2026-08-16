# Bounded borrowed binary remote output

Remote output already borrowed dense fixed-width Nitro arrays for its
synchronous destination-appender call. This experiment extends that lifetime
contract to dense flat binary vectors, avoiding the temporary binary-block copy.
Sparse, CHAR-trimming, dictionary, and RLE paths retain owned representations.

The first installed candidate incorrectly wrapped the vector's complete retained
byte capacity. It reduced q35 CPU from about 23.0 to 20.5 seconds but increased
p50 wall from about 4.48 to 8.10 seconds and reduced query parallel utilization
from about 5.1 to 2.65 CPUs. Those results are in `q35-candidate-installed.log`
and `q35-candidate-repeat.log`. The earlier `q35-candidate.log` did not contain
the candidate artifact and is retained only as invalid-run evidence.

The corrected candidate limits the borrowed Slice to the logical final offset.
Its five-measurement q35 run reports 4.656 seconds p50 wall and 21.945 mean
CPU-seconds. In the allocation-enabled adjacent pair, candidate/control report:

| Metric | Candidate | Control | Ratio |
|---|---:|---:|---:|
| p50 wall | 5,470.3 ms | 6,110.0 ms | 0.895x |
| mean CPU | 21,630.7 ms | 22,691.0 ms | 0.953x |
| p50 allocation | 37,741.6 MiB | 41,535.2 MiB | 0.909x |
| p50 query peak | 3,674.7 MiB | 3,712.3 MiB | 0.990x |

Fresh-process wall remains GC-sensitive, but the allocation and CPU reductions
directly match removal of one binary materialization. TPC-DS q22 measures
4,820.3 ms wall, 10,888 mean CPU-ms, and 12,624.4 MiB allocation. Q23a measures
5,918.5 ms, 29,008 mean CPU-ms, and 53,935.9 MiB. The complete 43-query
real-Parquet ClickBench correctness run passes exact comparison with all native
source attempts admitted. No JFR, heap dump, or Kata artifact was created.
