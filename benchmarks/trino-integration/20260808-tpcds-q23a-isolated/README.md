# TPC-DS q23a isolated memory control

Clean accepted Nitro `23e32d77` and Trino `75bf4c27`; SF10 Parquet, 12 GiB JVM,
8 GiB query limit, and separate fresh JVMs with five warmups and five
measurements. The earlier paired diagnostic used the test defaults of three
warmups and ten measurements and is retained as `paired.xml`.

In separate JVMs Nitro measured 3,784.588 ms mean wall / 18,443.8 CPU-ms
versus Trino's 3,610.501 / 19,267.4 (1.048x / 0.957x). Median thread
allocation was 24,168.172 / 36,461.057 MiB (0.663x). Median sampled query peak
was 2,161.994 / 1,605.451 MiB (1.347x), and Nitro's median process-heap peak
was 11,090.790 MiB versus 9,820.638 MiB.

The isolated CPU regression does not reproduce, but the wall loss and higher
retained query memory do. Nitro's lower total allocation arrives in bursts
that drive G1 to the heap ceiling. No JFR, heap dump, or Kata artifact was
created.

## Provider-owned DATE flat-key storage

Trino DATE's complete epoch-day domain fits a signed 32-bit value, so its type
binding now publishes the same four-byte canonical flat-key capability as
INTEGER. Operators remain unaware of DATE and consume only the provider-owned
logical-domain contract.

On q23a each high-cardinality final aggregation fell from 633.6 to 607.2 MiB.
Against the clean separate-JVM control, median query peak fell from 2,162.0 to
2,060.6 MiB (4.7%), allocation from 24,168.2 to 23,777.6 MiB, while mean wall
and CPU were neutral-to-better at 3,763.3 / 18,417.6 ms versus 3,784.6 /
18,443.8 ms.

An adjacent five-warmup/five-measurement q23b A/B was stronger: candidate vs
parent was 3,423.9 / 16,062.2 ms versus 3,698.4 / 17,690.2 ms wall/CPU
(0.926x / 0.908x), 20,928 / 23,056 MiB allocation (0.908x), and 1,404.9 /
2,046.0 MiB median query peak (0.687x). Q22 was also screened, but its SQL has
no logical DATE grouping key; identical allocation and boundary counts show
that its unrelated fresh-JVM timing movement was a non-activating control.

The Trino Nitro cohort passes 238 tests. The complete Nitro suite passes 1,680
tests with zero failures and 567 skipped. No JFR, heap dump, or Kata artifact
was created.
