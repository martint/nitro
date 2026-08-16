# Buffered native remote output validation

The committed Trino artifact (`dba60d1b`) was audited against the SF10 Parquet data with a 12 GiB JVM. All 22 TPC-H
queries, all 43 ClickBench queries, and all 103 TPC-DS variants produced results equal to the Trino backend. TPC-H q15
failed once in suite order because Trino returned no row from its duplicated floating-point CTE, then passed in a
fresh isolated comparison with one equal row from each engine. This is the established q15 order instability.

Every plan satisfied the existing fully-Nitro compute assertion. All non-empty native Parquet source attempts were
admitted except the documented empty-result TPC-DS q17 and q23b cases: dynamic filtering pruned one now-empty split in
each, which the audit counts as a rejected source attempt before it falls through to an empty host page source. The
same q17 count appears in the August 3 audit; q23b improved from four such attempts to one. ClickBench q10 initially
hit the harness's 1 GiB query cap and passed with q10-q43 under an 8 GiB cap inside the 12 GiB JVM.

Artifacts are the `*-audit*.log` files in this directory. No JFR or heap dump was created.
