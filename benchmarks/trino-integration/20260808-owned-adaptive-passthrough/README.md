# Owned adaptive pass-through experiment

TPC-DS q23a was measured with five warmups and five measurements in a 12 GiB JVM.
The first three logs are non-activating controls: `trino-main` had been compiled but
not installed, so `testing/trino-tests` resolved the previous installed snapshot.

After installing both Nitro and `trino-main`, `q23a-activated-candidate.log` proved
that the ownership path activated.  The large partial aggregation nodes' encoded-key
copy time fell from roughly 1.3--1.6 seconds per node to zero.  The end-to-end result
was nevertheless negative:

| configuration | wall p50 (ms) | CPU p50 (ms) | allocation p50 (MiB) | peak p50 (MiB) |
|---|---:|---:|---:|---:|
| accepted compact-record baseline | 5806.072 | 27147.6 | 47100.115 | 2527.487 |
| activated owned sparse forwarding | 5695.572 | 28380.0 | 47331.328 | 2314.846 |

Native-exchange retained bytes increased from about 53.4 GiB to 57.8 GiB.  Preserving
sparse physical positions avoided the local key copy but retained and transported
oversized upstream vectors, shifting rather than removing work.  The experiment was
rejected.  The next narrower target is dense dictionary-ID compaction that preserves
encoded key values without retaining sparse physical batches.

That narrower alternative was also tested.  Low-cardinality admission did not match
q23's join-produced dictionaries and remained neutral noise (5752.017 ms wall,
27513 ms CPU).  Flattening nested dictionaries did not change admission.  Broadening
admission to dense, high-cardinality dictionaries activated but regressed q23 to
5988.371 ms wall, 28335 ms CPU, and 48977.753 MiB allocated: copying the large base
plus compact IDs costs more than materializing the selected values.  These variants
were rejected as well.  The accepted implementation therefore remains unchanged.
