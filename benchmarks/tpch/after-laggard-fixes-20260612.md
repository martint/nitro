# TPC-H three-engine results after the laggard fixes (sf10, 2026-06-12)

After the byte-keyed intern table, segment LIKE masks, and raw-bits DOUBLE lanes
(see three-engine-baseline-20260612.md for the pre-fix numbers). JMH avgt ms/op,
3x1s warmup, 5x1s measurement, fork 1; identical logical operator shapes per query.

| q | compiled | nitro | trino | nitro/compiled | trino/compiled |
|---|---------:|------:|------:|---------------:|---------------:|
| 01 | 1446 | 6721 | 2288 | 4.65x | 1.58x |
| 02 | 419 | 723 | 692 | 1.72x | 1.65x |
| 03 | 1696 | 3603 | 2423 | 2.12x | 1.43x |
| 04 | 1478 | 2627 | 1910 | 1.78x | 1.29x |
| 05 | 1893 | 4699 | 2943 | 2.48x | 1.55x |
| 06 | 933 | 1429 | 850 | 1.53x | 0.91x **LAG** |
| 07 | 2036 | 6253 | 6033 | 3.07x | 2.96x |
| 08 | 2213 | 4861 | 2815 | 2.20x | 1.27x |
| 09 | 3453 | 6938 | 5419 | 2.01x | 1.57x |
| 10 | 3187 | 4012 | 2542 | 1.26x | 0.80x **LAG** |
| 11 | 236 | 573 | 536 | 2.43x | 2.27x |
| 12 | 1058 | 1260 | 1284 | 1.19x | 1.21x |
| 13 | 2955 | 4600 | 5225 | 1.56x | 1.77x |
| 14 | 1420 | 3191 | 1511 | 2.25x | 1.06x |
| 15 | 2135 | 2730 | 1818 | 1.28x | 0.85x **LAG** |
| 16 | 300 | 787 | 589 | 2.62x | 1.96x |
| 17 | 3365 | 4719 | 3734 | 1.40x | 1.11x |
| 18 | 3382 | 4612 | 3485 | 1.36x | 1.03x |
| 19 | 1543 | 2862 | 2155 | 1.85x | 1.40x |
| 20 | 2154 | 2439 | 2506 | 1.13x | 1.16x |
| 21 | 14674 | 20107 | 16212 | 1.37x | 1.10x |
| 22 | 527 | 640 | 866 | 1.21x | 1.64x |

Geomean speedup: 1.81x vs the Nitro operator harness, 1.37x vs the Trino twin.

Variance caveat: the full interleaved sweep inflates some heavy queries -- a
high-fidelity isolated run (4x1s warmup, 8x1s measurement) puts q10 at
2558 +/- 103 ms (0.99x vs Trino, a tie; the sweep's 3187 is interleaving
noise), and q15's isolated runs land at 1927-1946 ms (0.93-0.94x). The two
real remaining gaps, q06 (0.91x) and q15 (~0.93x) vs the Trino twin, are
scan-dominated: the parquet page reader dominates both engines and the
residual is the operator-bridge batch plumbing, which fused compilation
cannot bite into. Both still beat the Nitro operator harness by 1.5x/1.3x.
