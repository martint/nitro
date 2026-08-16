# TPC-DS q23a required-partial screen

This screen tested whether q23a's adaptive partial aggregation retires too early because duplicate keys arrive later
than Trino's reduction observation window. The Nitro integration temporarily required aggregation for all partial
stages while retaining the ordinary memory-triggered flushes. The change was confined to the Nitro planner adapter.

After one complete warmup, q23a was exact but regressed to 7,443.184 ms wall, 32,109 CPU-ms, 51,085.3 MiB allocated,
and 3,552.7 MiB sampled query peak. The accepted adaptive path is roughly 27.4 CPU-seconds, 48.1 GiB allocation, and
2.8 GiB sampled peak in the adjacent controlled run. Continuing to maintain the full three-key grouping state is
therefore more expensive than the identity-row key materialization it avoids. The temporary change was removed.

The experiment rules out simply extending or disabling Trino's adaptive retirement. q23 still requires a cheaper
exact representation for the redundant variable-width key, not more grouping work. The raw artifact is
`q23a-required-1w1m.log`. No JFR, heap dump, or Kata artifact was created.
