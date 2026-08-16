# Bounded initial dictionary-transfer experiment

TPC-DS q23a was used to test whether adaptive partial aggregation should retain a bounded dictionary value tree while
copying only a dense result-owned ID mapping. Runs used SF10 Parquet input, JDK 26, a 12 GiB JVM, an 8 GiB query
limit, five complete warmups, three alternating measurements, exact result comparison, allocation accounting, and
peak-memory sampling. No JFR, heap dump, or Kata artifact was created.

The candidate did not justify its ownership complexity:

| configuration | Nitro wall p50 | Nitro CPU p50 | allocation p50 | query peak p50 |
| --- | ---: | ---: | ---: | ---: |
| bounded transfer | 5,786.125 ms | 27,619 ms | 48,046.6 MiB | 3,634.1 MiB |
| transfer disabled | 6,016.724 ms | 27,371 ms | 48,086.1 MiB | 2,825.6 MiB |

The wall movement is not accompanied by a CPU or allocation improvement and the candidate raises sampled query peak
memory by 808.5 MiB (28.6%). Its within-run Nitro/Trino CPU ratio is 0.983x versus 0.973x for the disabled control.
The candidate and its policy were therefore removed.

An activation diagnostic confirmed that q23a's description key reaches the initial-row builder as a
`DictionaryVector` with about 10,000 row IDs and roughly 0.6 MiB of retained values, so the default 1 MiB/1x bounds
admit it. The aggregation boundary's zero dictionary-egress counter was misleading: the retained dictionary is
consumed inside adaptive aggregation rather than crossing that Trino boundary. Extending the prototype to
allocator-owned ID mappings did not change the result. This rules out a per-batch bounded ownership transfer as the
remaining q23 solution; a reusable portable dictionary identity or optimizer-proven functional-dependency rewrite is
still required.

Primary artifacts are `q23a-candidate.log`, `q23a-disabled-control.log`, and the activation diagnostics
`q23a-transfer-shapes.log` / `q23a-transfer-shapes-all-keys.log`.
