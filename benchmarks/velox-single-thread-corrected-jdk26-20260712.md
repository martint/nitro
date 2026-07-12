# JDK 26 corrected single-thread TPC-DS N/V board (2026-07-12)

Nitro was rebuilt from scratch for JDK 26 preview and benchmarked on SF10 with JMH, pinned to CPU 0. The run used one
fork, one thread, 3 x 1-second warmup iterations, and 5 x 2-second measurement iterations. The JMH fork was capped at
`-Xmx12g`; the isolated Maven daemon was capped at 2 GiB. Available system memory stayed above 32 GiB during the full
sweep.

Velox times are the corrected single-thread reference from `velox-single-thread-corrected-20260704.md`: min-of-3,
`taskset -c 0`, `--num_drivers=1`, and `--num_io_threads=1`. The Q45 and JDK changes did not alter the Velox harness or
logical operator trees, so those reference times remain the apples-to-apples comparison. Ratio above 1 means Nitro is
slower.

The full sweep initially exposed dictionary-peeling bounds bugs in q50, q62, q66, and q99. The bounds guards were folded
into the Q45 change, the four Nitro plans then executed successfully, and their board rows were rerun with the same JMH
configuration. Overall, Nitro/Velox geomean is **0.950x** across 98 matched queries; Nitro wins 58 and loses 40.

The leading three sweep laggards were rerun in isolation with 5 warmup and 8 measurement iterations. The results held:
q48 **3104.0 ms** (2.72x Velox), q54 **846.7 ms** (1.70x), and q84 **201.2 ms** (1.66x). q48 is the next frontier.

| query | Nitro ms | Velox ms | N/V |
|---|---:|---:|---:|
| q48 | 3156.360 | 1140 | 2.769 |
| q84 | 213.459 | 121 | 1.764 |
| q54 | 856.696 | 498 | 1.720 |
| q82 | 7274.225 | 4320 | 1.684 |
| q18 | 589.655 | 352 | 1.675 |
| q93 | 712.552 | 427 | 1.669 |
| q75 | 4609.630 | 2860 | 1.612 |
| q13 | 2026.361 | 1270 | 1.596 |
| q11 | 5229.605 | 3300 | 1.585 |
| q26 | 345.150 | 218 | 1.583 |
| q99 | 940.163 | 598 | 1.572 |
| q37 | 7170.197 | 4570 | 1.569 |
| q85 | 778.298 | 504 | 1.544 |
| q20 | 91.071 | 60 | 1.518 |
| q86 | 261.394 | 173 | 1.511 |
| q57 | 2961.797 | 1980 | 1.496 |
| q70 | 1479.395 | 1040 | 1.422 |
| q7 | 841.918 | 592 | 1.422 |
| q51 | 5242.836 | 3890 | 1.348 |
| q67 | 11731.275 | 8710 | 1.347 |
| q15 | 145.331 | 110 | 1.321 |
| q36 | 643.444 | 493 | 1.305 |
| q62 | 384.664 | 309 | 1.245 |
| q47 | 4962.313 | 3990 | 1.244 |
| q50 | 1497.691 | 1210 | 1.238 |
| q72 | 4193.560 | 3400 | 1.233 |
| q39 | 774.403 | 645 | 1.201 |
| q65 | 1390.036 | 1170 | 1.188 |
| q22 | 1184.838 | 1000 | 1.185 |
| q27 | 711.275 | 612 | 1.162 |
| q32 | 70.826 | 63 | 1.124 |
| q79 | 676.633 | 612 | 1.106 |
| q41 | 10.779 | 10 | 1.078 |
| q98 | 229.760 | 215 | 1.069 |
| q4 | 2069.242 | 1940 | 1.067 |
| q64 | 2668.278 | 2510 | 1.063 |
| q81 | 281.017 | 268 | 1.049 |
| q76 | 600.119 | 590 | 1.017 |
| q21 | 484.915 | 484 | 1.002 |
| q44 | 981.349 | 979 | 1.002 |
| q12 | 69.254 | 70 | 0.989 |
| q8 | 248.273 | 252 | 0.985 |
| q1 | 216.338 | 220 | 0.983 |
| q89 | 355.921 | 371 | 0.959 |
| q34 | 326.713 | 343 | 0.953 |
| q66 | 293.911 | 310 | 0.948 |
| q24 | 2271.525 | 2410 | 0.943 |
| q97 | 1541.651 | 1650 | 0.934 |
| q40 | 132.186 | 143 | 0.924 |
| q83 | 57.784 | 63 | 0.917 |
| q30 | 239.647 | 263 | 0.911 |
| q23 | 6685.538 | 7370 | 0.907 |
| q87 | 1129.283 | 1250 | 0.903 |
| q46 | 567.412 | 629 | 0.902 |
| q69 | 426.944 | 474 | 0.901 |
| q49 | 358.179 | 404 | 0.887 |
| q43 | 461.074 | 524 | 0.880 |
| q9 | 3431.980 | 3920 | 0.876 |
| q35 | 562.708 | 651 | 0.864 |
| q91 | 90.677 | 106 | 0.855 |
| q96 | 119.971 | 141 | 0.851 |
| q60 | 343.834 | 408 | 0.843 |
| q88 | 1095.274 | 1300 | 0.843 |
| q10 | 413.605 | 492 | 0.841 |
| q38 | 1040.233 | 1240 | 0.839 |
| q73 | 270.508 | 323 | 0.837 |
| q71 | 287.530 | 344 | 0.836 |
| q56 | 297.382 | 360 | 0.826 |
| q68 | 542.845 | 664 | 0.818 |
| q31 | 714.533 | 920 | 0.777 |
| q61 | 473.087 | 615 | 0.769 |
| q53 | 246.175 | 323 | 0.762 |
| q33 | 273.188 | 363 | 0.753 |
| q63 | 241.500 | 324 | 0.745 |
| q19 | 256.544 | 345 | 0.744 |
| q77 | 236.143 | 321 | 0.736 |
| q45 | 93.279 | 127 | 0.734 |
| q92 | 47.658 | 65 | 0.733 |
| q74 | 1575.728 | 2170 | 0.726 |
| q52 | 121.236 | 172 | 0.705 |
| q55 | 117.735 | 168 | 0.701 |
| q59 | 1006.623 | 1450 | 0.694 |
| q78 | 3020.991 | 4400 | 0.687 |
| q42 | 115.457 | 169 | 0.683 |
| q16 | 6016.030 | 9060 | 0.664 |
| q29 | 1042.851 | 1590 | 0.656 |
| q25 | 1036.469 | 1590 | 0.652 |
| q95 | 1600.369 | 2470 | 0.648 |
| q17 | 1033.088 | 1620 | 0.638 |
| q80 | 550.392 | 901 | 0.611 |
| q5 | 260.154 | 437 | 0.595 |
| q94 | 1439.886 | 2470 | 0.583 |
| q28 | 2586.985 | 4830 | 0.536 |
| q3 | 89.481 | 175 | 0.511 |
| q90 | 36.561 | 73 | 0.501 |
| q2 | 1323.602 | 2940 | 0.450 |
| q6 | 570.812 | 2009 | 0.284 |
| q58 | 505.185 | 1890 | 0.267 |
