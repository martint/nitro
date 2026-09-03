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

The initial historical architecture backfill was completed on 2026-09-03. It covers the current specification,
accepted cross-cutting defaults, explicit deferrals, and unresolved architectural proposals. Query-specific kernel
experiments remain in dated evidence notes unless they changed a general default or contract; applying an experiment's
result to a new general default requires a new ADR.

## Specification coverage

This map is an audit aid, not a substitute for each record's precise `Spec` field.

| Spec section | Principal decisions |
|---|---|
| §1–2 Purpose and scope | 0002, 0003, 0005, 0006, 0009, 0024, 0029, 0039, 0044 |
| §3 Vocabulary | 0003, 0007, 0008, 0012, 0023 |
| §4 Composition and modules | 0005, 0006, 0048 |
| §5 Vectors, streams, types | 0006, 0008, 0012, 0021, 0022, 0023, 0054, 0064 |
| §6 Masks and conditions | 0008, 0012, 0020, 0034, 0064 |
| §7 Evaluation IR | 0010, 0011, 0012, 0040, 0067 |
| §8 Function conventions | 0006, 0011, 0013–0016, 0037, 0038, 0040, 0049–0054, 0062, 0063, 0066, 0067 |
| §9 Operators and islands | 0003, 0010, 0017, 0024, 0044–0047 |
| §10 Cooperative execution | 0004, 0044, 0045 |
| §11 Allocation and memory | 0007, 0014, 0017–0019, 0041, 0066 |
| §12 Aggregation, windows, patterns, table functions | 0011–0013, 0015, 0016, 0022, 0036, 0045, 0046, 0055–0061, 0064 |
| §13 Joins and dynamic filters | 0006, 0012, 0018, 0025, 0035, 0036 |
| §14 Sources and Parquet | 0009, 0020–0023, 0031 |
| §15 Host boundaries | 0003, 0017, 0019, 0024, 0033, 0044, 0046 |
| §16 Adaptivity and generation | 0011, 0012, 0027, 0032, 0036, 0037, 0054–0061 |
| §17 Correctness and tests | 0026, 0029, 0043, 0047 |
| §18 Performance evidence | 0026–0028, 0032, 0041, 0042 |
| §19 Coverage and productization | 0029–0031, 0033–0035, 0042, 0046, 0047 |
| §20 Change protocol | 0001, 0002, 0039 |

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
| [0010](0010-keep-relational-rewrites-in-the-optimizer.md) | Keep relational rewrites in the optimizer | accepted |
| [0011](0011-generate-specializations-from-physical-capabilities.md) | Generate specializations from physical capabilities and layouts | accepted |
| [0012](0012-execute-over-encoded-domains.md) | Execute over encoded domains, not only encoded vectors | accepted |
| [0013](0013-use-layered-function-calling-conventions.md) | Use layered Nitro-native function calling conventions | accepted |
| [0014](0014-stream-function-results-into-owned-vectors.md) | Stream function results into allocator-owned vectors | accepted |
| [0015](0015-keep-aggregate-state-with-function-providers.md) | Keep aggregate state and intermediate semantics with function providers | accepted |
| [0016](0016-lower-function-ordering-to-physical-input.md) | Lower function-local ordering to a physical input contract | accepted |
| [0017](0017-negotiate-native-vector-exchange.md) | Negotiate native vectors and leases across exchanges | accepted |
| [0018](0018-publish-immutable-task-scoped-builds.md) | Publish immutable prepared join builds at task scope | accepted |
| [0019](0019-account-retained-state-to-the-host.md) | Account retained Nitro state to host memory reservations | accepted |
| [0020](0020-negotiate-source-output-demand.md) | Negotiate source output demand after selection | accepted |
| [0021](0021-treat-traits-as-logical-refinements.md) | Treat physical traits as refinements of registered logical types | accepted |
| [0022](0022-compose-recursive-structural-vectors.md) | Compose nested values as recursive vectors and materialize them in one pass | accepted |
| [0023](0023-put-vectors-and-allocation-in-the-connector-spi.md) | Put Nitro vectors and allocator ownership in the connector SPI | accepted |
| [0024](0024-confine-page-block-adaptation-to-boundaries.md) | Confine Page and Block adaptation to explicit host boundaries | accepted |
| [0025](0025-use-one-logical-dynamic-filter-lifecycle.md) | Use one logical dynamic-filter lifecycle with multiple physical consumers | accepted |
| [0026](0026-require-identical-plan-and-work-for-benchmarks.md) | Require identical plan shape and physical work for engine comparisons | accepted |
| [0027](0027-gate-adaptive-defaults-with-breadth-evidence.md) | Gate adaptive defaults with breadth evidence and expose their decisions | accepted |
| [0028](0028-separate-routine-and-stress-benchmarks.md) | Separate routine performance coverage from stress coverage | accepted |
| [0029](0029-make-coverage-gaps-explicit.md) | Make coverage gaps explicit and add end-to-end probes with every capability | accepted |
| [0030](0030-defer-spilling-behind-engine-arbitration.md) | Design spilling behind engine-level memory arbitration | proposed |
| [0031](0031-defer-locator-based-payload-materialization.md) | Defer locator-based payload materialization until remote reads are designed | accepted |
| [0032](0032-require-equivalent-evidence-for-native-kernels.md) | Require algorithmically equivalent evidence before adopting native kernels | accepted |
| [0033](0033-defer-destination-owned-exchange-accumulation.md) | Defer destination-owned exchange accumulation | accepted |
| [0034](0034-allow-disjoint-branches-to-share-a-destination.md) | Allow conditional branches to populate one destination only through explicit ownership | proposed |
| [0035](0035-unify-dynamic-filter-transport-later.md) | Preserve a path to subsume Nitro dynamic-filter transport under the host | proposed |
| [0036](0036-share-physical-key-contracts-across-consumers.md) | Share physical key and table contracts across grouping, joins, and distinct | accepted |
| [0037](0037-link-generated-scalar-targets-as-constants.md) | Link generated scalar targets as class constants | accepted |
| [0038](0038-keep-function-optimization-at-the-registry-boundary.md) | Keep function-specific optimization at the registry boundary | accepted |
| [0039](0039-keep-one-authoritative-production-path.md) | Keep one authoritative production path after a decision is settled | accepted |
| [0040](0040-use-one-normalized-mask-aware-expression-ir.md) | Use one normalized, mask-aware expression IR and evaluator | accepted |
| [0041](0041-target-zero-row-proportional-steady-state-allocation.md) | Target zero row-proportional allocation in steady-state streaming execution | accepted |
| [0042](0042-extend-benchmarks-with-parameterized-curves.md) | Extend macro benchmarks with attributed parameterized response curves | accepted |
| [0043](0043-test-capabilities-and-invariants-not-names.md) | Test architectural capabilities and invariants, not names | accepted |
| [0044](0044-limit-host-changes-to-nitro-execution-boundaries.md) | Limit host changes to execution components Nitro replaces and their adapters | accepted |
| [0045](0045-own-pattern-recognition-in-nitro.md) | Implement pattern recognition as an engine-owned restartable Nitro operator | accepted |
| [0046](0046-use-a-native-batch-spi-for-table-functions.md) | Use a Nitro-native batch SPI for table functions | accepted |
| [0047](0047-report-structured-island-admission.md) | Report island admission and rejection through semantic categories | accepted |
| [0048](0048-enforce-one-way-module-dependencies.md) | Enforce one-way dependencies between SPI, engine, providers, compatibility, and tests | accepted |
| [0049](0049-exclude-invalid-rows-from-fallible-scalars.md) | Exclude null and errored rows from fallible strict scalar targets | accepted |
| [0050](0050-map-declared-scalar-failures-at-the-provider-boundary.md) | Map declared scalar failures at the provider boundary | accepted |
| [0051](0051-contain-mapped-failures-during-values-evaluation.md) | Contain mapped failures during VALUES-only evaluation | accepted |
| [0052](0052-gate-scalar-adaptation-on-carrier-materialization.md) | Gate scalar adaptation on carrier materialization | accepted |
| [0053](0053-refine-failure-semantics-for-exact-bindings.md) | Refine failure semantics for exact registry bindings | accepted |
| [0054](0054-generate-fixed-width-structural-projections.md) | Generate fixed-width structural projections without carrier-specific engine types | accepted |
| [0055](0055-link-generated-aggregation-targets-as-constants.md) | Link generated aggregation targets as constants | accepted |
| [0056](0056-allow-generated-aggregation-updates-to-consume-tuples.md) | Allow generated aggregation updates to consume primitive tuples | accepted |
| [0057](0057-describe-aggregation-inputs-by-primitive-carrier.md) | Describe generated aggregation inputs by primitive carrier | accepted |
| [0058](0058-select-structural-components-in-generated-aggregation-updates.md) | Let providers select structural components for generated aggregation updates | accepted |
| [0059](0059-pass-variable-width-contributions-as-binary-regions.md) | Pass variable-width generated aggregation contributions as binary regions | accepted |
| [0060](0060-generate-aligned-encoded-domain-aggregation.md) | Generate aggregation over aligned encoded domains | accepted |
| [0061](0061-preserve-metadata-for-conditional-grouped-domains.md) | Preserve metadata for conditional grouped-domain admission | accepted |
| [0062](0062-expose-composed-type-identity-to-functions.md) | Expose composed type identity to registry-bound functions | accepted |
| [0063](0063-expose-composed-key-binding-to-stateful-functions.md) | Expose composed key binding to stateful registry functions | accepted |
| [0064](0064-count-independent-encoded-domains.md) | Count independent encoded domains under a mask | accepted |
| [0065](0065-reuse-domain-counts-after-equivalent-mapping-proof.md) | Reuse domain counts after an equivalent-mapping proof | accepted |
| [0066](0066-close-evaluator-local-function-state.md) | Close evaluator-local function state with its evaluator | accepted |
| [0067](0067-bind-higher-order-functions-as-nested-plans.md) | Bind higher-order functions as provider-owned nested plans | accepted |
