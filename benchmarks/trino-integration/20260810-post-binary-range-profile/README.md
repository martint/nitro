# Q30 profile after retained binary range copying

This fresh JDK 26 run used 15 warmups and ten measurements with a 12 GiB heap, 8 GiB query memory, allocation and
operator metrics, exact results, and heap-dump generation disabled. Nitro measured 271.0 ms median wall, 474.3
CPU-ms, and 1,498.8 MiB allocated. `q30.jfr` covers only the measured executions.

The accepted retained binary range copy removes `JoinBufferSupport.copyBinaryPositions` from the hot-method board.
The remaining leading Java samples are primitive Nitro-to-Trino egress (9.3%), Snappy foreign-memory session release
(9.1%), and Trino variable-width destination append (4.0%). The egress samples are partition-output work over encoded
join results; they are not evidence that Nitro Parquet publishes padded CHAR values.

Two adjacent candidates were rejected and removed. Eliminating CHAR validation violated the tested padded connector-
boundary contract. Direct raw-validity decoding passed 80 focused tests but measured 479.5 CPU-ms versus 463.1 ms in
the restored-parent control, without reducing allocation. The accepted staged Trino jar was restored after the run.

The 9.1% `SharedSession.release0` frame is Airlift native Snappy FFM downcall release rather than a Nitro lifecycle
leak. A Java-Snappy candidate passed 93 focused Parquet tests but measured 478.8 CPU-ms versus 470.7 ms for the
immediately adjacent native control, with unchanged allocation. The candidate was removed and the accepted native
Parquet jar restored.
