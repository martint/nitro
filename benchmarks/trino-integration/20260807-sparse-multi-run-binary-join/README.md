# Sparse multi-run binary join output

TPC-DS q23a exposed repeated materialization of a dictionary-backed description key after a hash join produced
multiple non-retained build runs under a sparse downstream mask. Nitro now admits one allocator-owned binary backing
per eligible build column, maps join output positions to that backing with dictionary IDs, and copies null/error side
streams independently. Admission is immutable, requires at least first-use output reuse, and is bounded to 64 MiB per
join. Full-mask output deliberately retains the existing bulk copier: a screen that enabled the dictionary there made
q22 about 4% slower without reducing allocation.

Fresh 12 GiB JVMs used five complete warmups, an 8 GiB query limit, exact SF10 Parquet results, allocation accounting,
and peak-memory sampling. q23a and q22 used five measurements; q67 used three.

| query | candidate wall / disabled | candidate CPU / disabled | allocation | query peak |
| --- | ---: | ---: | ---: | ---: |
| q23a | 5,826.750 / 5,872.488 ms | 27,959 / 28,089 ms | 48,757.847 / 49,613.606 MiB | 2,653.414 / 2,632.756 MiB |
| q22 | 4,573.603 / 4,603.965 ms | 10,727 / 10,867 ms | 11,535.199 / 11,537.698 MiB | 315.992 / 317.840 MiB |
| q67 | 4,365.132 / 4,545.793 ms | 13,703 / 14,097 ms | 23,645.941 / 23,558.516 MiB | 1,034.832 / 1,142.439 MiB |

The q23a result is modest in duration but removes about 856 MiB of allocation without retaining upstream batch
generations. q67 improves wall, CPU, and peak memory; q22 is neutral-to-positive. The artifacts in this directory are
the Surefire XML reports for the accepted candidate and disabled controls. No JFR, heap dump, or Kata artifact was
created.
