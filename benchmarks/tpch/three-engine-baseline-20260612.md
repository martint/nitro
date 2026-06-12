# TPC-H three-engine baseline (sf10, 2026-06-12)

First full benchmark after all 22 queries reached the compiled engine, the Nitro
operator harness, and the Trino operator twin (JMH avgt ms/op, 3x1s warmup, 5x1s
measurement, fork 1; identical logical operator shapes per query).

| q | compiled | nitro | trino | nitro/compiled | trino/compiled |
|---|---------:|------:|------:|---------------:|---------------:|
| 01 | 1579 | 6742 | 2319 | 4.27x | 1.47x |
| 02 | 424 | 680 | 686 | 1.61x | 1.62x |
| 03 | 1706 | 3617 | 2422 | 2.12x | 1.42x |
| 04 | 1494 | 2627 | 1976 | 1.76x | 1.32x |
| 05 | 1906 | 4621 | 2909 | 2.42x | 1.53x |
| 06 | 985 | 1429 | 850 | 1.45x | 0.86x **LAG** |
| 07 | 2006 | 6192 | 5961 | 3.09x | 2.97x |
| 08 | 2298 | 4803 | 2834 | 2.09x | 1.23x |
| 09 | 3194 | 6698 | 5438 | 2.10x | 1.70x |
| 10 | 3314 | 3990 | 2513 | 1.20x | 0.76x **LAG** |
| 11 | 241 | 572 | 534 | 2.37x | 2.21x |
| 12 | 1072 | 1292 | 1183 | 1.21x | 1.10x |
| 13 | 4873 | 4528 | 4899 | 0.93x | 1.01x **LAG** |
| 14 | 1621 | 3172 | 1513 | 1.96x | 0.93x **LAG** |
| 15 | 1944 | 2731 | 1815 | 1.41x | 0.93x **LAG** |
| 16 | 325 | 783 | 585 | 2.41x | 1.80x |
| 17 | 3643 | 4741 | 3535 | 1.30x | 0.97x |
| 18 | 3405 | 4505 | 3358 | 1.32x | 0.99x |
| 19 | 1526 | 2828 | 2042 | 1.85x | 1.34x |
| 20 | 2532 | 2384 | 2300 | 0.94x | 0.91x **LAG** |
| 21 | 13751 | 19991 | 16603 | 1.45x | 1.21x |
| 22 | 496 | 624 | 874 | 1.26x | 1.76x |

Laggards (compiled slower than an operator engine, outside ~5% noise):
q10 (0.76x vs Trino), q06 (0.86x vs Trino), q20 (0.91x T / 0.94x N — both),
q13 (0.93x vs Nitro), q14 (0.93x vs Trino), q15 (0.93x vs Trino).
q17 (0.97x) and q18 (0.99x) sit inside measurement noise.
