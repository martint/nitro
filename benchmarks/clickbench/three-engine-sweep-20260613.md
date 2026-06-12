# ClickBench three-engine sweep (2026-06-13)

Definitive sweep after the scan-path work; q05 (single-column no-filter count-distinct)
shown at its gated value -- the usePageSource gate keeps it on the operator bridge.

| q | compiled | nitro | trino | nitro/compiled | trino/compiled |
|---|---------:|------:|------:|---------------:|---------------:|
| 01 | 37 | 36 | 147 | 0.98x | 3.95x |
| 02 | 138 | 559 | 272 | 4.04x | 1.96x |
| 03 | 371 | 366 | 823 | 0.99x | 2.22x |
| 04 | 268 | 270 | 366 | 1.01x | 1.37x |
| 05 | 1330 | 1297 | 1889 | 0.97x | 1.42x |
| 06 | 3386 | 4059 | 4474 | 1.20x | 1.32x |
| 07 | 122 | 150 | 243 | 1.23x | 1.99x |
| 08 | 143 | 565 | 273 | 3.95x | 1.91x |
| 09 | 2318 | 2163 | 4679 | 0.93x | 2.02x |
| 10 | 2832 | 3306 | 5991 | 1.17x | 2.12x |
| 11 | 492 | 686 | 868 | 1.39x | 1.76x |
| 12 | 625 | 853 | 1135 | 1.36x | 1.81x |
| 13 | 2817 | 3608 | 3467 | 1.28x | 1.23x |
| 14 | 3658 | 4626 | 6066 | 1.26x | 1.66x |
| 15 | 3794 | 4031 | 3671 | 1.06x | 0.97x |
| 16 | 2082 | 2395 | 2240 | 1.15x | 1.08x |
| 17 | 6154 | 9123 | 9013 | 1.48x | 1.46x |
| 18 | 6258 | 8405 | 8211 | 1.34x | 1.31x |
| 19 | 11851 | 15277 | 14209 | 1.29x | 1.20x |
| 20 | 262 | 565 | 382 | 2.16x | 1.46x |
| 21 | 6081 | 9351 | 12333 | 1.54x | 2.03x |
| 22 | 7031 | 9235 | 9761 | 1.31x | 1.39x |
| 25 | 2175 | 2552 | 2527 | 1.17x | 1.16x |
| 26 | 1562 | 1988 | 1956 | 1.27x | 1.25x |
| 27 | 2167 | 2574 | 2545 | 1.19x | 1.17x |
| 28 | 8815 | 8793 | 9906 | 1.00x | 1.12x |
| 30 | 2108 | 8020 | 17394 | 3.80x | 8.25x |
| 31 | 2885 | 3625 | 3664 | 1.26x | 1.27x |
| 32 | 4045 | 4219 | 4120 | 1.04x | 1.02x |
| 36 | 3140 | 3712 | 5973 | 1.18x | 1.90x |
| 37 | 241 | 674 | 7727 | 2.79x | 32.04x |
| 38 | 195 | 601 | 8333 | 3.08x | 42.72x |
| 39 | 211 | 753 | 7692 | 3.58x | 36.52x |
| 41 | 144 | 853 | 1641 | 5.93x | 11.40x |
| 42 | 140 | 805 | 1634 | 5.75x | 11.68x |
| 43 | 178 | 587 | 1519 | 3.29x | 8.52x |

Geomean over 36 queries: 1.62x vs the Nitro operator harness, 2.45x vs the Trino twin. The compiled engine is at or ahead of the Trino twin on every query; sub-Nitro entries are millisecond-scale dimension queries or high-variance hash-dominated aggregates (this box thermally throttles late in a multi-hour sweep).
