# Agent guidance for Nitro

## Architecture sources of truth

Read these before material architecture, execution, or performance work:

- `~/notes/specification-decision-record-practice.md` — project-agnostic maintenance and correlation protocol.
- `docs/SPEC.md` — the normative Nitro and host-integration contract.
- `docs/decisions/README.md` and applicable ADRs — why consequential choices were made, including proposed work.
- `~/notes/nitro/design-principles.md` — empirical performance rules and implementation lessons.
- Applicable dated notes under `~/notes/nitro/` — evidence and investigation history.

## Required architecture correlation

Before changing architecture, behavior, a physical-policy default, or a host boundary, identify the applicable spec
sections, accepted/proposed ADRs, and design principles. State whether the change conforms, fills a gap, or contradicts
or reverses them.

Contradiction and backtracking are allowed only as explicit decisions. Discuss the conflict before implementation. Add
a new ADR to amend or supersede the prior decision, update both records and the ADR index, revise the spec to state the
new current contract, and update any empirical principle invalidated by evidence. Never silently edit an accepted ADR
to make history appear consistent.

Land implementation, tests, spec, ADR, and principle changes together when they express one decision. For work spanning
Nitro and Cork, the Nitro repository owns the central spec and ADR; the paired Cork commit cites the ADR number.

Material changes include adaptive defaults/admission, ownership and lifetime, scheduler cooperation, function and type
semantics, connector and Page/Block boundaries, memory accounting, and benchmark methodology that changes the meaning
of reported results—not only public Java APIs.

## Repository practices

- This is a Jujutsu repository; use `jj`, not Git commands.
- Use JDK 26 from `/opt/java/openjdk` and `mvnd -Dmaven.gitcommitid.skip=true`.
- Preserve unrelated user changes and benchmark artifacts. Do not create JFR artifacts unless explicitly requested.
- Use semantic navigation/refactoring for Java changes when available; do not substitute brittle textual architecture
  tests for capability and behavior assertions.

## Command-output hygiene

- Redirect long or noisy output—especially Maven/mvnd builds and tests, benchmarks, profilers, JVM diagnostics, and
  large diffs—to a file rather than emitting the complete stream into the conversation.
- Preserve the command's exit status. After completion, extract only the evidence needed with targeted tools such as
  `rg`, `awk`, `sed`, or `tail`: summaries, failures, warnings, timings, and relevant counters.
- Always report the retained output-file path so the complete evidence can be inspected later. Do not suppress useful
  diagnostics with `/dev/null` merely to reduce context usage.
- Store durable investigation output in the applicable `~/notes/nitro/` campaign directory and genuinely ephemeral
  output in a uniquely named directory under `/tmp`. Keep multi-megabyte logs, benchmark results, profiles, and
  `EXPLAIN` artifacts out of the repository working copy.
- Short, bounded inspection commands may return directly when their output is already small and relevant.
