# ADR-0138: Discover grouping-hash consumers inside islands

- **Status:** rejected; production candidate removed
- **Date:** 2026-09-14
- **Spec:** §12, §17–18
- **Related:** ADR-0003, ADR-0010, ADR-0036

## Context

The host negotiates an authoritative physical grouping hash between compatible partial and final aggregations.
Consumer discovery currently inspects only the root operator of each Nitro island. A final aggregation below an
island-owned TopN or projection is therefore invisible even when its source is a complete compatible exchange chain.
ClickBench q39's measured final grouping reports no authoritative-hash batches; this structural limitation is a
candidate explanation for avoidable hashing, not yet proof of an end-to-end performance gain.

## Proposal and correlation

Discover final grouping consumers throughout each island's internal plan, stopping at its declared input boundaries.
Associate each contract with the owning island and the exact aggregation node. Preserve the existing complete-chain,
provider-capability, key-layout and producer-root requirements. Do not modify the logical operators, island boundaries,
host scheduler or partition assignment. Do not infer that arbitrary internal producers can expose hidden outputs.

This fills a composition gap in the existing physical contract and conforms to indivisible Nitro islands, optimizer-
owned physical rewrites, and explicit hidden execution channels. A consumer's position inside its island is not a
semantic reason to repeat compatible producer hashing.

## Qualification

First reproduce missing consumer discovery with a final aggregation inside a larger island. Assert exact contract
ownership, hidden exchange channels, unchanged public output, idempotence and input-boundary isolation. Preserve
negative cases for incomplete chains and incompatible keys/providers. Run execution tests and warmed exact/native
SQL qualification; count authoritative-hash batches and compare allocation/CPU against frozen accepted controls.
Do not accept based solely on planner tests or profile weights. Keep this experiment separate from ADR-0137.

## Outcome

The new test fails on the unchanged optimizer and passes with internal discovery, including input-boundary isolation,
nested-island ownership and repeated discovery. Six focused tests pass. Exact/native q39 execution succeeds and its
first measured run consumes 123 authoritative-hash batches rather than zero. The structural omission is real, but enabling
the contract does not establish a worthwhile execution improvement.

Local frozen parent/candidate/parent, 15 warmups / 7 measurements, distributed tc4 / APC6 / 12 GiB:

| Query | Parent A CPU, s | Candidate CPU, s | Parent C CPU, s |
| --- | ---: | ---: | ---: |
| ClickBench q14 | 6.323 | 6.585 | 6.297 |
| ClickBench q39 | 0.731 | 0.728 | 0.728 |
| TPC-DS q22 | 13.443 | 14.209 | 14.627 |
| TPC-DS q67 | 13.126 | 13.020 | 13.544 |

q39 CPU ranges overlap both controls and the repeat has exactly the same median. q14's candidate median is 4.1–4.6%
higher; its range separates from the first control and overlaps the repeat. q22 reverses direction between drifting
controls; q67 overlaps the first control. q39 allocation rises about 4.4–4.6% in point medians, with overlapping ranges.
These results do not qualify the proposal. The extra portable hash must be produced and transmitted; consumer hash
avoidance alone is not evidence of net computation reuse. Keep the existing production admission unchanged.

Remove the experimental optimizer change and its feature tests, retaining the patch, frozen classes, test failure/
success logs, counts and paired results outside the repositories. This is not an opt-in feature. A future revisit
must establish net avoided hashing and transport cost before broadening admission. Evidence:
`~/notes/nitro/results/internal-grouping-hash-qualification-20260914` and
`~/notes/nitro/internal-grouping-hash-rejected-20260914.patch`.
