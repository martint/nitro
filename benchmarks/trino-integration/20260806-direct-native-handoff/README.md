# Direct native handoff control — 2026-08-06

The candidate negotiates an instance-owned native batch transport when adjacent physical operator factories both
advertise Nitro exchange capabilities. It does not alter join, aggregation, or source algorithms, and retains the
existing Trino `Page` path whenever either side lacks the capability.

TPC-DS q22 was run against analyzed SF10 Parquet inputs with both engines in one 12 GiB JVM, five complete warmups,
three measurements, allocation tracking, boundary counters, and per-operator CPU metrics.

| Candidate | Wall ratio | CPU ratio | Allocation ratio | Native handoff rows/execution |
|---|---:|---:|---:|---:|
| Direct adjacent handoff | 0.831x | 1.069x | 0.625x | 26.52M |
| Final source (mandatory-partial experiment reverted) | 0.817x | 1.063x | 0.624x | 26.52M |

The analyzed fifteen-warmup control before this candidate was 1.183x CPU. Page ingress and egress each fell by
26.52 million positions per execution. The remaining Nitro item join is about 6.55 CPU-seconds versus roughly
2.87 seconds for Trino's corresponding joins, while Nitro aggregation is about 7.69 seconds versus Trino's 9.38.

The final SQL ratio now corroborates the current SQL-shaped q22 operator benchmark (1.075x CPU). The old frozen q22
operator fixture's much larger win is not comparable: it pre-aggregated by item before the join, a rewrite absent
from Trino's physical SQL plan. The integration gap is closed for this query; future q22 work belongs either in the
optimizer (to choose that rewrite) or in the native join implementation, not in Trino/Nitro adaptation.

Artifacts:

- `q22-5w3m.log` — direct handoff candidate.
- `q22-mandatory-partial-5w3m.log` — no-effect admission experiment; source change was reverted.
