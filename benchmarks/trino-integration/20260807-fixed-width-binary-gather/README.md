# TPC-DS q23a fixed-width binary gather screen

A general `BinaryVector.copy(positions)` candidate used exact allocation and arithmetic offsets when the source
declared a physical fixed-width trait. It was intended to accelerate q23a's dictionary-backed 30-character
description substring without retaining upstream ownership.

After five warmups and three alternating measurements, Nitro measured 5,982.129 ms wall and 28,073 CPU-ms versus
Trino at 6,198.951 ms and 28,092 CPU-ms. The Nitro initial encoded-key phase remained 2.73--3.04 seconds, showing that
the path did not address the residual cost. `substr(..., 1, 30)` is logically bounded but its values are not
physically fixed width when source descriptions are shorter than 30 characters, so mixed dictionary bases do not
carry the fixed-width trait. The candidate and its test were removed. Raw evidence is in `q23a-candidate.log`; no
JFR, heap dump, or Kata artifact was created.
