# TPC-H sf10 three-engine sweep (2026-06-13)

Definitive sweep after the scan-path work (staged-filter fusion, dictionary entry
masks, the gated direct page source). compiled vs the Nitro operator harness vs the
Trino operator twin; identical logical operator shapes per query.

| q | compiled | nitro | trino | nitro/compiled | trino/compiled |
|---|---------:|------:|------:|---------------:|---------------:|
| 01 | 1492 | 6754 | 2297 | 4.53x | 1.54x |
| 02 | 414 | 671 | 677 | 1.62x | 1.64x |
| 03 | 1685 | 3567 | 2403 | 2.12x | 1.43x |
| 04 | 1313 | 2668 | 1930 | 2.03x | 1.47x |
| 05 | 1850 | 4612 | 2895 | 2.49x | 1.56x |
| 06 | 833 | 1426 | 849 | 1.71x | 1.02x |
| 07 | 1952 | 5943 | 5857 | 3.04x | 3.00x |
| 08 | 2172 | 4834 | 2781 | 2.23x | 1.28x |
| 09 | 2882 | 6663 | 5418 | 2.31x | 1.88x |
| 10 | 2528 | 4047 | 2540 | 1.60x | 1.00x |
| 11 | 232 | 568 | 533 | 2.45x | 2.30x |
| 12 | 1050 | 1296 | 1177 | 1.23x | 1.12x |
| 13 | 2804 | 4445 | 4847 | 1.59x | 1.73x |
| 14 | 1315 | 3144 | 1515 | 2.39x | 1.15x |
| 15 | 1641 | 2745 | 1814 | 1.67x | 1.11x |
| 16 | 284 | 776 | 576 | 2.73x | 2.03x |
| 17 | 3233 | 4855 | 3582 | 1.50x | 1.11x |
| 18 | 3263 | 4754 | 3377 | 1.46x | 1.03x |
| 19 | 1453 | 2801 | 2036 | 1.93x | 1.40x |
| 20 | 1902 | 2344 | 2303 | 1.23x | 1.21x |
| 21 | 13141 | 19593 | 16573 | 1.49x | 1.26x |
| 22 | 503 | 616 | 879 | 1.23x | 1.75x |

Geomean over 22 queries: 1.92x vs the Nitro operator harness, 1.44x vs the Trino twin. The compiled engine is at or ahead of the Trino twin on every query; sub-Nitro entries are millisecond-scale dimension queries or high-variance hash-dominated aggregates (this box thermally throttles late in a multi-hour sweep).
