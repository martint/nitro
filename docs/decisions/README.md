# Nitro decision records

Every architectural or design decision of consequence for Nitro or its host integration is recorded here, one file
per decision, numbered in the order it is accepted.

The specification records what the engine must do. These records preserve why a choice won, what it costs, and which
viable alternatives lost. Implementation notes and benchmark narratives can provide evidence, but they are not a
substitute for a discoverable decision.

## When to write a record

Write one when a change selects one viable architecture over another; changes an SPI, ownership, execution,
scheduling, type, function, connector, or host-boundary contract; constrains something expensive to reverse;
deliberately accepts a cost or coverage limit; changes an adaptive default based on evidence; or declines a capability
a reasonable reader would expect.

Do not write one for an obvious implementation detail with no live alternative. An experiment does not become a
decision record until it changes the accepted design or default.

## Format

Files are named `NNNN-short-kebab-title.md` and contain Status and Date, links to affected specification sections and
related records, Context, Decision, Consequences, and Alternatives considered.

Use exactly four statuses:

| Status | Meaning |
|---|---|
| `accepted` | The decision is true as written. |
| `amended` | The decision remains, but a later record corrects or narrows it. |
| `superseded` | A later record replaces it. |
| `proposed` | The decision has not been accepted. |

Accepted records are immutable. A correction or reversal creates a new record. Update the old record's status and
cross-reference, the new record, and this index in the same change.

For changes spanning Nitro and a host repository, the record lives here as the single architectural authority. The
Nitro commit includes the spec/record change; paired host-integration commits cite the ADR number. An integration-only
decision receives a record here before or with the paired change set.

## Backfill

The existing design-principles and chronological reconciliation notes contain prior decisions mixed with experiments
and history. Backfill them incrementally, starting with load-bearing current contracts. Backfilled records say they
are retrospective and link durable evidence. Do not manufacture false dates or imply that reconstructed alternatives
were documented contemporaneously.

## Index

| # | Decision | Status |
|---|---|---|
| [0001](0001-record-architectural-decisions.md) | Record architectural decisions beside the Nitro specification | accepted |
| [0002](0002-one-normative-engine-specification.md) | Keep one normative specification for Nitro and its host integration | accepted |
| [0003](0003-compose-indivisible-pull-islands.md) | Compose each Nitro island as one indivisible pull graph | accepted |
| [0004](0004-keep-cooperative-pull-scheduler-neutral.md) | Keep cooperative pull independent of the host scheduler | accepted |
| [0005](0005-inject-all-execution-dependencies.md) | Inject all execution dependencies and policies at construction | accepted |
| [0006](0006-keep-logical-semantics-in-registries.md) | Keep logical types and function semantics in dynamic registries | accepted |
| [0007](0007-make-vector-ownership-explicit.md) | Make allocator ownership, borrowing, and transfer explicit | accepted |
| [0008](0008-use-first-class-masks-and-streams.md) | Make masks and VALUES/NULLS/ERRORS first-class | accepted |
| [0009](0009-read-parquet-natively-over-host-io.md) | Decode Parquet natively over connector-authoritative ranged I/O | accepted |
