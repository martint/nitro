# Rejected CHAR source-metadata enrichment

TPC-DS q30's post-range-copy JFR left a `CHAR` canonicalization scan in the Nitro-to-Trino adapter. A temporary
read-only Nitro Parquet audit scanned all 500,000 rows of the customer file for the eight q30 character columns:
`c_customer_id`, `c_salutation`, `c_first_name`, `c_last_name`, `c_preferred_cust_flag`, `c_birth_country`,
`c_login`, and `c_email_address`. None of the 4,000,000 physical values ended in an ASCII space. The temporary test
was removed after the audit.

A general prototype let a dynamic `TypeBinding` inspect an allocator-owned values vector once at source ingress and
attach provider-proven physical metadata. Trino's `CHAR` binding certified flat, dictionary-base, and RLE-base
binary vectors only when every physical value lacked trailing ASCII space; padded connector values retained the
existing safe scan/copy/trim behavior. Operators and the connector remained unaware of Trino type identity.

The mechanism was not retained. A 30-warmup/50-measurement Nitro-only q30 control measured:

| Build | Wall p50 | Wall mean | CPU p50 | CPU mean | Allocation p50 |
| --- | ---: | ---: | ---: | ---: | ---: |
| metadata candidate | 228.464 ms | 232.963 ms | 454 ms | 454.560 ms | 1,479.765 MiB |
| accepted parent | 225.444 ms | 232.916 ms | 450 ms | 453.240 ms | 1,496.855 MiB |

Wall is identical and candidate CPU is slightly worse. The apparent 1.1% allocation reduction has no credible
mechanism because the candidate only skips a read-only byte scan. Two shorter adjacent candidate/parent pairs also
crossed in both directions. The Trino prototype was removed, the unused Nitro SPI commit was abandoned, and the
accepted staged jars were restored. No JFR, heap dump, or Kata artifact was created.
