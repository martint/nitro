# Compressed staged direct grouping publication

All JVM values are per operation from JDK 26 JMH with 10 warmups, five measurements, three fresh forks, `gc`, and
the same eight-event `perfnorm` bundle. Nitro and Trino are unpinned and capped at 12 GiB. Q21 uses identical fixed
12 GiB pre-touch for both JVM engines; q04/q18 use the standard expanding heap. Velox is pinned to CPU 0, uses one
driver/I/O thread, ten repeats, and a 12 GiB managed cache; perf totals below are divided by ten.

Counters are instructions / cycles / L1D misses / L1D loads / dTLB misses / dTLB loads / branch misses / branches.

| query | engine | duration ms | allocation B/op | counters |
|---|---|---:|---:|---|
| q04 | Nitro | 968.3 | 137.796M | 13.752B / 5.070B / 211.881M / 8.222B / 115.999K / 1.172M / 47.825M / 2.811B |
| q04 | Trino | 1704.2 | 3.080B | 26.499B / 9.598B / 267.544M / 12.402B / 7.136M / 20.090M / 29.982M / 5.203B |
| q04 | Velox | 2590 | 3.346B | 39.492B / 13.526B / 414.392M / 19.059B / 1.283M / 28.966M / 31.994M / 6.268B |
| q18 | Nitro | 1553.0 | 1.162B | 25.913B / 8.561B / 291.742M / 13.304B / 3.616M / 12.225M / 31.785M / 5.190B |
| q18 | Trino | 2332.2 | 5.980B | 34.216B / 13.643B / 422.848M / 17.916B / 1.559M / 19.351M / 64.210M / 6.441B |
| q18 | Velox | 2750 | 1.254B | 49.494B / 14.453B / 508.762M / 23.444B / 1.741M / 36.449M / 38.893M / 7.604B |
| q21 | Nitro | 3787.8 | 3.966B | 59.132B / 20.124B / 751.915M / 35.143B / 12.009M / 98.030M / 142.076M / 11.345B |
| q21 | Trino | 7842.7 | 20.331B | 103.115B / 48.145B / 1.747B / 53.157B / 43.816M / 191.848M / 307.303M / 19.834B |
| q21 | Velox | 8020 | 6.306B | 130.676B / 41.822B / 1.771B / 64.725B / 2.878M / 156.038M / 181.241M / 21.117B |

Primary artifacts:

- `compressed-direct-publication-3fork.json` (Nitro q04/q18 plus ordinary-heap q21 diagnostic)
- `compressed-direct-q21-pretouch-publication-3fork.json`
- `trino-q04-q18-publication-3fork.json`
- `trino-q21-pretouch-publication-3fork.json`
- `velox-q04.perf.csv`, `velox-q18.perf.csv`, `velox-q21.perf.csv`

The Velox verbose executions reported 3.346/1.254/6.306 GB cumulative managed allocation and final execution times
2.59/2.75/8.02 seconds for q04/q18/q21. Their stage row counts and physical plans match the previously qualified
Trino-SQL-derived harness shapes.
