# Word-at-a-time Page validity bitmap

The q23a profile attributed 2.95% of Java execution samples to individual
`Bitmap.set` calls while adapting Nitro null streams to Trino blocks. The
accepted boundary change constructs each 64-bit validity word directly and
allocates the bitmap lazily only after observing a null. Page/Block semantics,
dictionary/RLE preservation, and ownership are unchanged.

The focused page-processor suite passes 76 tests, including a new sparse
selection crossing positions 63/64 and 127/128. With five warmups and three
interleaved measurements on SF10 Parquet, q23a measured:

| Engine | Wall p50 | CPU p50 | Allocation p50 |
| --- | ---: | ---: | ---: |
| Trino | 6,011.711 ms | 28,197 ms | 72,395.565 MiB |
| Nitro | 5,652.418 ms | 28,013 ms | 51,424.036 MiB |

Nitro/Trino is 0.940 wall and 0.993 CPU. The preceding clean Nitro control was
5,679 ms / 28,385 CPU-ms, so the CPU improvement is about 1.3%; allocation is
unchanged apart from run noise. Raw evidence is in `q23a.log` and
`q23a-paired.log`.

Two native-handoff experiments were rejected before this change. Dense native
normalization measured 1.322x wall / 1.452x CPU against Trino and allocated
72.4 GiB. Composing nested dictionary IDs without copying values measured
7,227 ms / 34,532 CPU-ms. The Page boundary is therefore still providing
valuable compact, depth-1 dictionary semantics; eliminating it requires a
bounded encoded transport with an adaptive-size signal equivalent to the Page
representation, not dense vector copying or unbounded upstream retention.
