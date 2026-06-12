# TPC-H results after the scan-path fixes (sf10, 2026-06-12)

Compiled re-sweep after the staged-filter and scan-path work (same-column
conjunct fusion, entry-mask evaluation over dictionary pages, and the direct
all-numeric parquet page source); the operator-engine columns repeat the
after-laggard-fixes sweep (those engines are unchanged).

| q | compiled | nitro | trino | nitro/compiled | trino/compiled |
|---|---------:|------:|------:|---------------:|---------------:|
| 01 | 1487 | 6721 | 2288 | 4.52x | 1.54x |
| 02 | 391 | 723 | 692 | 1.85x | 1.77x |
| 03 | 1679 | 3603 | 2423 | 2.15x | 1.44x |
| 04 | 1464 | 2627 | 1910 | 1.79x | 1.30x |
| 05 | 1849 | 4699 | 2943 | 2.54x | 1.59x |
| 06 | 835 | 1429 | 850 | 1.71x | 1.02x |
| 07 | 2011 | 6253 | 6033 | 3.11x | 3.00x |
| 08 | 2174 | 4861 | 2815 | 2.24x | 1.29x |
| 09 | 2874 | 6938 | 5419 | 2.41x | 1.89x |
| 10 | 2499 | 4012 | 2542 | 1.61x | 1.02x |
| 11 | 272 | 573 | 536 | 2.10x | 1.97x |
| 12 | 1057 | 1260 | 1284 | 1.19x | 1.21x |
| 13 | 2830 | 4600 | 5225 | 1.63x | 1.85x |
| 14 | 1335 | 3191 | 1511 | 2.39x | 1.13x |
| 15 | 1694 | 2730 | 1818 | 1.61x | 1.07x |
| 16 | 285 | 787 | 589 | 2.76x | 2.06x |
| 17 | 3128 | 4719 | 3734 | 1.51x | 1.19x |
| 18 | 3323 | 4612 | 3485 | 1.39x | 1.05x |
| 19 | 1495 | 2862 | 2155 | 1.91x | 1.44x |
| 20 | 1924 | 2439 | 2506 | 1.27x | 1.30x |
| 21 | 13040 | 20107 | 16212 | 1.54x | 1.24x |
| 22 | 498 | 640 | 866 | 1.28x | 1.74x |

Geomean speedup: 1.92x vs the Nitro operator harness, 1.45x vs the Trino
twin. EVERY query is now at or ahead of both operator engines -- the former
scan-bound stragglers close at q06 1.02x / q10 1.02x / q15 1.07x vs Trino,
measured in isolation (interleaved sweeps inflate heavy neighbors).
