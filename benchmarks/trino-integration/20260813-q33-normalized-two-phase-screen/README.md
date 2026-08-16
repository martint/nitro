# Invalid ClickBench q33 normalized-key screen

This result must not be used. The candidate compiled and passed 57 focused tests, but the Trino test classpath resolved an older Nitro artifact from the standard Maven repository while `mvnd install` had placed the candidate in the enhanced repository's `local-installs` tier. The recorded SQL measurements therefore did not execute the candidate.

The raw log is retained only as provenance for the invalidated run. No performance conclusion can be drawn from it, and the candidate source was removed.
