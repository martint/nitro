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

Use these statuses:

| Status | Meaning |
|---|---|
| `accepted` | The decision is true as written. |
| `amended` | The decision remains, but a later record corrects or narrows it. |
| `superseded` | A later record replaces it. |
| `proposed` | The decision has not been accepted. |
| `rejected` | The proposal was evaluated and declined; its implementation is not current. |

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
| §1–2 Purpose and scope | 0002, 0003, 0005, 0006, 0009, 0024, 0029, 0039, 0044, 0090 |
| §3 Vocabulary | 0003, 0007, 0008, 0012, 0023 |
| §4 Composition and modules | 0005, 0006, 0048 |
| §5 Vectors, streams, types | 0006, 0008, 0012, 0021, 0022, 0023, 0054, 0064, 0068, 0070, 0073, 0089, 0093 |
| §6 Masks and conditions | 0008, 0012, 0020, 0034, 0064 |
| §7 Evaluation IR | 0010, 0011, 0012, 0040, 0067 |
| §8 Function conventions | 0006, 0011, 0013–0016, 0037, 0038, 0040, 0049–0054, 0062, 0063, 0066–0068, 0070, 0071, 0073, 0075–0077, 0089, 0090, 0093 |
| §9 Operators and islands | 0003, 0010, 0017, 0024, 0044–0047, 0072, 0079–0081 |
| §10 Cooperative execution | 0004, 0044, 0045 |
| §11 Allocation and memory | 0007, 0014, 0017–0019, 0041, 0066, 0072, 0075, 0077 |
| §12 Aggregation, windows, patterns, table functions | 0011–0013, 0015, 0016, 0022, 0036, 0045, 0046, 0055–0061, 0064, 0075–0081, 0089, 0090, 0093 |
| §13 Joins and dynamic filters | 0006, 0012, 0018, 0025, 0035, 0036, 0089, 0090, 0093, 0100, 0101, 0103, 0104 |
| §14 Sources and Parquet | 0009, 0020–0023, 0027, 0031, 0068 |
| §15 Host boundaries | 0003, 0017, 0019, 0024, 0033, 0044, 0046, 0071, 0100, 0104 |
| §16 Adaptivity and generation | 0011, 0012, 0027, 0032, 0036, 0037, 0054–0061, 0068, 0071–0073, 0075, 0089, 0090, 0093 |
| §17 Correctness and tests | 0026, 0029, 0043, 0047, 0100 |
| §18 Performance evidence | 0026–0028, 0032, 0041, 0042, 0087 |
| §19 Coverage and productization | 0029–0031, 0033–0035, 0042, 0046, 0047, 0089, 0090, 0093, 0100 |
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
| [0033](0033-defer-destination-owned-exchange-accumulation.md) | Defer destination-owned exchange accumulation | superseded |
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
| [0068](0068-recover-low-cardinality-nested-parent-domains.md) | Recover low-cardinality nested parent domains | accepted |
| [0069](0069-preserve-dictionary-domains-through-structural-construction.md) | Preserve dictionary domains through structural construction | accepted |
| [0070](0070-make-unordered-placement-explicit.md) | Make unordered-value placement explicit in ordering capabilities | accepted |
| [0071](0071-bind-specialization-constants-before-generated-scalar-adaptation.md) | Bind specialization constants before generated scalar adaptation | accepted |
| [0072](0072-retain-only-qualifying-topn-ranking-groups.md) | Retain only qualifying Top-N ranking peer groups | accepted |
| [0073](0073-bind-exact-normalized-ordering-keys.md) | Bind exact normalized ordering keys through the type provider | accepted |
| [0074](0074-share-bounded-state-across-partitioned-topn-ranking.md) | Share bounded state across partitioned Top-N ranking | accepted |
| [0075](0075-bind-exact-counted-key-aggregation.md) | Bind exact counted-key aggregation through type identity | accepted |
| [0076](0076-bind-reversible-window-aggregation-state.md) | Bind reversible window aggregation state through providers | accepted |
| [0077](0077-materialize-selected-window-results-by-forward-batch-range.md) | Materialize selected capable window results by forward batch range | accepted |
| [0078](0078-run-reversible-window-ranges-in-provider-kernels.md) | Run reversible window ranges in provider kernels | accepted |
| [0079](0079-compose-synchronous-operator-result-ranges.md) | Compose synchronous operator result ranges without batch transport | accepted |
| [0080](0080-compose-provider-primitive-result-ranges.md) | Compose provider primitive result ranges | accepted |
| [0081](0081-preserve-repeated-primitive-range-multiplicity.md) | Preserve repeated primitive range multiplicity | accepted |
| [0082](0082-represent-contiguous-vector-copies-directly.md) | Represent contiguous vector copies directly | accepted |
| [0083](0083-match-generated-kernel-shapes-exactly.md) | Match generated-kernel physical shapes exactly | accepted |
| [0084](0084-prove-constant-window-bounds-across-sources.md) | Prove constant window bounds across retained sources | accepted |
| [0085](0085-preserve-rle-through-row-concatenation.md) | Preserve RLE through row concatenation | accepted |
| [0086](0086-prove-raw-key-identity-through-type-providers.md) | Prove raw key identity through type providers | amended |
| [0087](0087-reuse-one-benchmark-jvm-per-suite.md) | Reuse one benchmark JVM per suite | accepted |
| [0088](0088-measure-serial-query-bundles-as-benchmark-points.md) | Measure serial query bundles as benchmark points | accepted |
| [0089](0089-generate-fixed-width-key-kernels-from-provider-layouts.md) | Generate fixed-width key kernels from provider layouts | amended |
| [0090](0090-reject-row-wise-semantic-key-tables.md) | Reject row-wise semantic key tables | accepted |
| [0091](0091-generate-canonical-fixed-width-key-projections.md) | Generate canonical fixed-width key projections | accepted |
| [0092](0092-compose-persistent-key-layouts-in-one-generated-table.md) | Compose persistent key layouts in one generated table | accepted |
| [0093](0093-generate-recursive-product-key-layouts.md) | Generate recursive product key layouts | amended |
| [0094](0094-generate-ordered-repeated-key-layouts.md) | Generate ordered repeated key layouts | amended |
| [0095](0095-generate-unordered-repeated-key-layouts.md) | Generate unordered repeated key layouts | accepted |
| [0096](0096-normalize-mixed-flat-distinct-keys.md) | Normalize mixed flat DISTINCT keys into generated fixed-width layouts | accepted |
| [0097](0097-admit-exact-distinct-value-ids-by-observed-reuse.md) | Admit exact DISTINCT value IDs by observed reuse | accepted |
| [0098](0098-borrow-remote-output-storage-through-synchronous-destination-append.md) | Borrow remote-output storage through synchronous destination append | accepted |
| [0099](0099-admit-mutable-binary-topn-at-every-limit.md) | Admit mutable binary TopN winner storage at every positive limit | accepted |
| [0100](0100-carry-correlated-dynamic-filters-across-fragment-outputs.md) | Carry correlated dynamic filters across fragment outputs | amended |
| [0101](0101-bound-correlated-hash-domains-through-promotion.md) | Bound correlated hash domains through promotion | accepted |
| [0102](0102-measure-cumulative-jvm-allocations.md) | Measure cumulative JVM allocations across thread lifetimes | accepted |
| [0103](0103-publish-native-filter-domains-with-primitive-position-ordering.md) | Publish native filter domains with primitive position ordering | accepted |
| [0104](0104-integrate-correlated-filtering-without-an-opt-in.md) | Integrate validated filter optimizations without opt-ins | accepted |
| [0105](0105-batch-exact-scalar-table-lookups.md) | Batch exact scalar table lookups | accepted |
| [0106](0106-bound-borrowed-views-by-selected-extents.md) | Bound borrowed views by selected extents | accepted |
| [0107](0107-derive-group-null-metadata-from-stored-keys.md) | Derive group null metadata from stored keys | rejected |
| [0108](0108-pool-non-escaping-partition-remapping-scratch.md) | Pool non-escaping partition remapping scratch | accepted |
| [0109](0109-encode-constant-initial-average-counts.md) | Encode constant initial average counts | accepted |
| [0110](0110-consume-compacted-ranges-through-residual-filters.md) | Consume compacted ranges through residual filters | accepted |
| [0111](0111-honor-bound-null-metadata-in-specialized-layouts.md) | Honor bound null metadata in specialized key layouts | accepted |
| [0112](0112-probe-grouped-join-tags-with-packed-word-masks.md) | Probe grouped join tags with packed-word masks | rejected |
| [0113](0113-bound-direct-index-growth-for-capped-hash-builds.md) | Avoid provably unusable direct build state | accepted |
| [0114](0114-reduce-only-the-first-empty-join-tag.md) | Reduce only the first empty join tag | rejected |
| [0115](0115-bind-single-match-hash-dispatch-per-batch.md) | Bind single-match hash dispatch per batch | rejected |
| [0116](0116-record-completed-pipeline-timing-evidence.md) | Record completed pipeline timing evidence | accepted |
| [0117](0117-consume-host-filter-snapshots-without-restarting-waits.md) | Consume host filter snapshots without restarting waits | rejected |
| [0118](0118-count-host-visible-source-output-once.md) | Count host-visible source output once | accepted |
| [0119](0119-reevaluate-grouped-tag-kernels-on-wide-sql-probes.md) | Reevaluate grouped tag kernels on wide SQL probes | rejected |
| [0120](0120-specialize-provider-owned-primitive-literals.md) | Specialize provider-owned primitive literals | accepted |
| [0121](0121-discard-unrequested-mapped-scalar-errors.md) | Reject unrequested-error scratch removal after SQL qualification | rejected |
| [0122](0122-report-late-admission-observations.md) | Report late admission observations | accepted |
| [0123](0123-use-primitive-iteration-for-conditional-merges.md) | Reject conditional-merge traversal revisions after SQL qualification | rejected |
| [0124](0124-preserve-provider-proven-bound-call-failure-conventions.md) | Preserve provider-proven bound-call failure conventions | accepted |
| [0125](0125-specialize-scalar-loops-by-physical-input-shape.md) | Reject scalar physical-shape specialization after SQL qualification | rejected |
| [0126](0126-reuse-exact-generated-scalar-bindings.md) | Reject generated scalar binding reuse after SQL qualification | rejected |
| [0127](0127-preserve-projection-capabilities-through-partial-application.md) | Preserve projection capabilities through partial application | accepted |
| [0128](0128-clear-selected-errors-in-reused-scalar-outputs.md) | Clear selected errors in reused scalar outputs | accepted |
| [0129](0129-reject-probes-outside-completed-build-bounds.md) | Reject probes outside completed build bounds | accepted |
| [0130](0130-close-component-benchmark-allocator-domains.md) | Close component benchmark allocator domains | accepted |
| [0131](0131-qualify-primitive-evaluator-traversal-with-error-containment.md) | Qualify primitive evaluator traversal with error containment | accepted |
| [0132](0132-recycle-evaluator-scratch-after-batch-invalidation.md) | Recycle evaluator scratch after batch invalidation | accepted |
| [0133](0133-copy-primitive-vector-selections-without-boxing.md) | Copy primitive vector selections without boxing | accepted |
| [0134](0134-constrain-selected-batches-once.md) | Constrain selected batches once | accepted |
| [0135](0135-pack-value-interner-fingerprints-with-stable-ids.md) | Pack value interner fingerprints with stable IDs | rejected |
| [0136](0136-cache-canonical-character-domains-at-retained-boundaries.md) | Cache canonical character domains at retained boundaries | rejected |
| [0137](0137-reuse-byte-views-during-character-egress.md) | Reuse byte views during character egress | accepted |
| [0138](0138-discover-grouping-hash-consumers-inside-islands.md) | Discover grouping-hash consumers inside islands | rejected |
| [0139](0139-batch-independent-long-join-probes.md) | Batch independent long join probes | accepted |
| [0140](0140-borrow-compatible-constant-runs-at-egress.md) | Borrow compatible constant runs at synchronous egress | rejected |
| [0141](0141-mix-owned-and-borrowed-synchronous-egress.md) | Mix owned and borrowed synchronous egress | accepted |
| [0142](0142-preserve-dictionaries-for-non-null-grouped-ranges.md) | Preserve dictionaries for non-null grouped ranges | rejected |
| [0143](0143-release-rejected-grouped-dictionary-mappings.md) | Release rejected grouped dictionary mappings | accepted |
| [0144](0144-skip-membership-prechecks-for-high-hit-probe-batches.md) | Skip membership prechecks for high-hit probe batches | rejected |
