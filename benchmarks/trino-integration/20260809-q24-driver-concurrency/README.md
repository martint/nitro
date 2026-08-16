# ClickBench q24 split and concurrency attribution

All SQL controls used a fresh 12 GiB JVM, warmups before measurement, the
Nitro-native Parquet reader, one configured worker, and the deterministic q24
ordering. The ordinary benchmark configuration uses 120 MiB Parquet ranges.

| Shape | Sources | Wall p50 (ms) | CPU mean (ms) | Allocation mean (MiB) |
|---|---:|---:|---:|---:|
| production ranges, normal scheduler | 165 | 5,103 | 21,288 | 35,680 |
| whole-file sources, normal scheduler | 100 | 4,495 | 19,419 | 28,960 |
| whole-file sources, node cap of one driver per task | 100 | 10,178 | 17,347 | 28,855 |
| exact whole-file operator JMH | 100 | 16,637 | n/a | 28,219 |

The 65 extra range sources account for about 1.9 CPU-seconds and essentially
the full SQL/operator allocation gap. Normal concurrent execution accounts for
another 2.1 CPU-seconds versus the node-capped control. The node-capped SQL CPU
is within about 4% of the exact serial operator result; the wall regression
shows that globally serializing source work is not a production optimization.

`task_concurrency=1` does not serialize scan drivers. Trino's enabled
thread-per-driver scheduler uses node-owned driver limits and ignores the
session-level `max_drivers_per_task` passed to its `addTask` method. Session-only
controls therefore had no effect. The diagnostic node override was removed
after the measurement; no Trino scheduler behavior was changed.

Files:

- `q24-drivers.log`: ordinary 165-source driver/pipeline attribution.
- `q24-whole-file-splits.log`: 100 whole-file sources under the normal scheduler.
- `q24-whole-file-serial.log` and `q24-whole-file-one-driver.log`: ineffective
  session-only controls.
- `q24-whole-file-node-one-driver.log`: effective node-level one-driver control.
- `q24-one-driver.log`: earlier session-cap diagnostic on the production ranges.

No JFR, heap dump, or Kata review artifact was created.
