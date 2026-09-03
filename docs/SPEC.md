# Nitro Execution Engine Specification

This document specifies the architecture Nitro is building: its concepts, contracts, execution model, and host
integration boundaries. It describes what must be true, not the history of how those choices were made. The code is
expected to conform to this document; a disagreement is either an implementation defect or a deliberate specification
change accompanied by a decision record.

The reasons, rejected alternatives, and accepted costs live in [`decisions/`](decisions/README.md). Detailed
performance heuristics and experimental evidence remain in the Nitro design knowledge base. The older
[`architecture-target-contracts.md`](architecture-target-contracts.md) is retained as migration history while its
still-relevant material is absorbed here.

The specification is normative for Nitro itself and for Nitro execution embedded in a host engine such as Trino or
Cork. A host may provide planning, scheduling, functions, types, connectors, memory accounting, exchange, and result
transport, but those services enter Nitro only through the boundaries defined below.

---

## 1. Purpose

Nitro is a batch-oriented, vectorized execution engine for relational query plans. It is designed to reduce CPU,
allocation, and retained memory by preserving physical representations, avoiding work made unnecessary by selection,
and adapting execution to observed runtime shape.

Nitro replaces the core execution inside admitted plan regions. It does not require a replacement SQL planner,
metadata system, function catalog, connector ecosystem, scheduler, or wire protocol.

Three properties govern the design:

- **Work follows demand.** A value, stream, or column is computed only for positions and consumers that require it.
- **Physical structure remains visible.** Dictionary domains, runs, mappings, null shape, ordering, and exact statistics
  remain available until preserving them costs more than materializing them.
- **Ownership is explicit.** Every mutable buffer, borrowed view, retained vector, and reusable allocation has a
  construction-time owner and a defined release authority.

## 2. Scope and non-goals

Nitro owns physical execution inside a Nitro island, including source consumption, expression evaluation, joins,
aggregation, sorting, ranking, windows, and other supported relational operators.

Nitro does not:

- interpret SQL or choose logical plans;
- contain built-in knowledge of host function or logical type identities;
- expose Trino `Page`, `Block`, or `ConnectorPageSource` as Nitro engine abstractions;
- install process-global services, registries, pools, execution policies, or mutable configuration;
- replace the host scheduler as a condition of using Nitro;
- silently fall back to another Parquet record reader when native decoding lacks coverage; or
- split one admitted Nitro island into host operators to work around missing Nitro composition.

Unsupported behavior must fail with a diagnostic at the relevant planning or binding boundary. Coverage gaps are
features to implement, not permission to weaken the model invisibly.

## 3. Vocabulary

**Batch** — one logical set of positions and lazily available output streams. A batch has a fixed logical position
count even when only a subset is active.

**Mask** — the first-class set of active logical positions in a batch. Masks can be dense, ranged, sparse, or derived
from encoded domains. They carry selection, not values.

**Stream** — one semantic component of an expression result. The standard streams are `VALUES`, `NULLS`, and
`ERRORS`. A consumer requests only the streams it observes.

**Vector** — a physical representation of one stream. Flat, dictionary, RLE, nested, and other vector forms describe
storage and mapping, not SQL semantics.

**Physical domain** — the distinct stored positions over which an encoding maps logical positions. A dictionary's
values and an RLE value are domains; logical rows are not necessarily the unit of computation.

**Operator** — a stateful physical transformation or source participating in a pull graph. Operators exchange Nitro
batches, masks, and vectors.

**Nitro island** — a maximal supported plan region lowered to one composed Nitro pull graph, vector ownership domain,
and host-operator boundary.

**Type binding** — a registry-owned description of logical semantics and the physical vector capabilities that may
carry values of that type.

**Function binding** — a registry-resolved implementation and its calling-convention capabilities. It may provide a
batch implementation, a scalar target suitable for generated adaptation, an encoded-domain implementation, or a
specialized physical kernel.

**Allocator** — the owner of Nitro vector storage and reusable buffer pools for a defined lifetime.

**Allocator context** — an immutable attribution and policy scope within an allocator. Contexts can charge different
host memory reservations while sharing a storage and pooling domain.

**Borrow** — temporary read access that does not transfer release authority and cannot outlive the owner's lease.

**Transfer** — movement of ownership and release authority to another component.

**Host boundary** — an explicit ingress or egress at which host representations, scheduling, memory accounting,
exchange, or connector services are adapted to Nitro contracts.

## 4. Composition and dependency ownership

Every service and policy used during execution is supplied at construction. An engine instance, task, island,
operator, evaluator, function adapter, source, and allocator must not discover dependencies through static state,
system properties, thread locals, service locators, or ambient registries.

System-property and configuration-file readers may exist only as standalone composition adapters. They translate host
configuration into immutable objects before construction; execution code receives those objects directly.

Policies describe general physical decisions such as admission thresholds, retention limits, and adaptive sampling.
They are immutable and instance-owned. Query planning may choose a policy object, but an operator must not inspect a
query identity, table, column combination, function name, or benchmark to select behavior.

Registries are dynamically supplied and classloader-neutral at the Nitro boundary. A registry lookup returns SPI
capabilities or method handles whose declaring implementation remains owned by the provider classloader.

## 5. Vectors, streams, and logical types

Vectors represent physical storage. Logical meaning comes from type bindings.

The vector family includes fixed-width flat vectors, variable-width binary vectors, dictionary and RLE vectors,
arrays, maps, rows/structs, and compositional wrappers whose mappings can be resolved without flattening children.

A physical carrier does not imply a logical type. An I64 carrier may hold BIGINT, a short decimal, date, time, or a
timestamp representation. Binary storage does not imply UTF-8. The type registry supplies equality, hashing,
comparison, coercion, display, and function semantics.

Physical traits describe facts a producer can prove, such as ASCII-only bytes, valid UTF-8, sortedness, exact range,
cardinality, null shape, or stable dictionary identity. Traits refine an already-bound logical type; they never create
logical meaning from a physical vector.

`VALUES`, `NULLS`, and `ERRORS` are independently demandable streams. When companion streams share an encoded domain,
that alignment must be explicit. An encoded `VALUES` stream and row-indexed `NULLS` stream cannot be treated as one
domain merely because they have the same logical length.

## 6. Masks and conditional computation

Masks are part of every evaluation and operator contract. They are not optional side metadata attached after values
have been computed.

An operator or function receiving a mask must not observe inactive positions unless its declared physical capability
explicitly operates on a smaller encoded domain whose mapping is proven equivalent for the requested result.

Conditional expressions evaluate their condition first, derive disjoint branch masks, evaluate each branch only for
its active positions, and combine the results by logical position. An error on an inactive branch is unobservable.
Nested conditions repeat this rule recursively.

Masks can propagate toward a source to defer or skip payload decoding. A narrower downstream mask constrains only the
batch and generation from which it was derived; mask ownership cannot outlive that batch.

## 7. Evaluation IR

The evaluation IR is a function-neutral dataflow graph. It represents inputs, literals, calls, references, copies,
merges, structural field access, and mask algebra. It carries resolved type and function bindings without naming
engine-specific implementations.

Normalization lowers SQL special forms into explicit dataflow and mask relationships. The normalized graph must make
conditional branch masks, stream demand, reuse, and error propagation visible to the evaluator.

The evaluator computes only requested streams and positions, memoizes reusable results within their valid batch
generation, shares computation across outputs, preserves compatible encodings and statistics, resolves physical
strategy outside position loops, and generates or selects tight loops for registered capabilities.

Physical-plan rewrites belong to the optimizer. The evaluator may choose an implementation for the physical shape of
an already-resolved expression, but it must not change relational semantics or recognize a specific function.

## 8. Function calling conventions

Nitro defines layered batch calling conventions. The most general convention receives input streams, a mask, requested
output streams, optional proposed output vectors, and an allocator context. More convenient conventions may hide
allocation, null propagation, error handling, or encoding traversal, but they lower to the same ownership rules.

A provider can supply a general batch implementation, an encoded-domain implementation, an exact scalar target for
generated adaptation, a provider-authored physical kernel, or aggregation/window state and update capabilities.

Generated adapters are ordinary implementations of the batch convention. They may generate direct invocation bytecode
around a constant method-handle target, hoist null/error classification, traverse selected positions, and write into
allocator-owned outputs. Generation does not transfer function semantics into the evaluator.

Reference-carrier arguments are read through exact handles supplied by the logical type binding. Reference-carrier
results are appended immediately through a provider-owned result writer into Nitro vectors. Nitro does not retain an
array of host objects and does not infer logical meaning from `Slice`, `Block`, or another carrier class.

A VALUES-only request does not force NULLS or ERRORS materialization. Values beneath an unrequested semantic null are
unspecified. Requested companion streams preserve exact semantics.

## 9. Operators and islands

Nitro operators form batch-pull graphs. A downstream operator asks upstream for the next batch or required state.
Operators can narrow masks and delay borrowing payload columns until survivor positions are known.

A Nitro island is maximal and indivisible. Adjacent Nitro stages communicate only through Nitro interfaces. They are
not represented as adjacent Trino operators and do not round-trip through `Page` or `Block`.

Planning must compose an adjacent supported factory into the same graph or reject the plan with a diagnostic.
Unsupported composition is a coverage defect. Tests assert that admitted complete plans have no host-compute edge
inside an island.

Blocking operators may retain state, but retained input and output ownership must be explicit. Operators close fully
consumed batches promptly and cannot retain borrowed vectors after their lease ends.

Operators are generic over functions, logical types, arity, query shape, tables, and column combinations.
Specialization comes through general physical interfaces or generated resolved layouts.

## 10. Cooperative execution and scheduling

Nitro does not own the host scheduler. The current Cork and Trino integration retains the existing time-sharing
executor and platform-thread lifecycle.

The island root cooperates through an injected execution-control interface. Checkpoints are bounded and restart-safe:
an operator commits progress to instance-owned state before control can unwind, and re-entry cannot duplicate or lose
input. Generated kernels checkpoint between bounded chunks, not for every position.

Temporary depletion at a host-fed ingress is distinct from end-of-input. The ingress leaf preserves the Nitro graph,
allocator, and state across offered host pages; only the host's finish signal ends input.

A future thread-per-driver scheduler may park and resume the same pull stack. That is another implementation of the
same contract, not a requirement, second operator API, or part of the current integration. Nitro work must not modify
host scheduling outside the island unless separately specified.

## 11. Allocation, pooling, and memory accounting

All vector storage and retained execution state is allocated through an injected Nitro allocator. Operators do not own
unrelated private pooling domains merely because the host represents them as separate accounting units.

One island or task may share a storage owner and bounded pools across its drivers. Allocator contexts preserve separate
attribution, retention policy, and host reservation charging. The allocator is thread-safe where its declared sharing
scope requires it; borrowed mutable vectors are not implicitly safe for concurrent mutation.

The lifecycle is acquire or reuse, borrow within a lease, transfer when retention is necessary, release exactly once,
and retain only bounded policy-admitted storage. Pools report live, retained, and peak bytes. Soft references are not
an ownership protocol.

The steady-state goal is no allocation proportional to row count for streaming scan/filter/project/aggregate shapes.
Initialization, bounded resizing, state growth, output materialization, and provider-required variable-size results may
allocate, but repeated batches should reuse their working set.

## 12. Aggregation and window functions

Aggregation and window operators drive provider-supplied functions. Function state, intermediate representation, null
semantics, ordering, and final materialization belong to the registered implementation or type capabilities, not to
operator conditionals.

State is allocator-owned and may use fixed-width, segmented, recursive, dictionary-domain, or generated layouts.
Function-specific aggregation-state vector types belong with function implementations, not in the core vector
vocabulary.

Grouping is a physical key-to-group operation. It can select flat, packed, dictionary-domain, generated composite, or
other general representations based on observed shape and immutable policy. It cannot recognize aggregate functions
or SQL types. Partial aggregation considers retained work, cardinality, state size, and reduction.

## 13. Joins and dynamic filters

Join algorithms are separate from key semantics. Type bindings and registry functions provide exact hashing,
comparison, and residual predicates. Physical key layouts may be generated from resolved lanes.

Build state and prepared membership are task-owned capabilities shareable across compatible probe drivers. Join output
preserves mappings and encodings when this avoids copies and satisfies ownership.

Dynamic filtering has one logical lifecycle: build evidence becomes a published immutable constraint with exact
semantics and bounded representation. Nitro operators and connector/source capabilities may consume it. The source may
use row-group statistics, page indexes, dictionaries, bloom filters, or value-level filtering; the join retains exact
enforcement as the correctness backstop.

Host dynamic-filter plumbing may carry the same logical constraint, but Nitro must not maintain two independent
semantic authorities. Future unification may use the host transport if it preserves Nitro timing, physical-domain, and
source-capability contracts.

## 14. Sources and native Parquet execution

Connectors interact through the source and vector SPI. They receive allocator, type bindings, I/O capabilities, and
immutable source policies at construction. The engine does not know connector implementations.

The Nitro Parquet reader decodes directly into Nitro vectors, preserves useful dictionary/RLE structure, and advances
unneeded streams without decoding values. Missing types or encodings fail loudly; they do not fall back to `parquet-mr`
record materialization.

The reader uses a connector-authoritative ranged-input abstraction that preserves object-store support,
authentication, retry, accounting, and cancellation. Nitro may add coalescing, prefetch, or caching above ranged reads,
but cannot bypass those host capabilities.

Decoder selection is driven by Parquet metadata and registered type bindings. Primitive value decoders, level
decoders, and structural reconstruction are separate capabilities; nested reconstruction does not grow a switch over
SQL types. Selection skips work at the largest proven boundary while advancing every involved cursor consistently.

## 15. Host integration boundaries

The host planner identifies maximal supported regions and constructs islands. It supplies resolved types, functions,
connectors, memory accounting, execution control, exchanges, and output adaptation.

Host `Page`/`Block` adaptation is permitted only at explicit ingress or egress where native Nitro transport is not yet
available. Adaptation preserves laziness, encoding, companion-stream semantics, logical length, and ownership wherever
the host representation permits it.

The host can charge separate memory reservations through allocator contexts without copies between adjacent Nitro
stages. Page/Block boundaries are not ownership boundaries inside an island.

## 16. Adaptivity and specialization

Adaptive choices respond to measured physical facts: mask density, selectivity, domain size, run length, cardinality,
novelty, key width, state footprint, ordering, and locality.

Decisions occur outside inner position loops, at construction, first batch, stable shape, or bounded re-evaluation
points. Mid-stream re-adaptation resists oscillation.

Specialization can generate expression loops, grouping layouts, hash/probe kernels, comparators, decoders, and state
updates. Generated code derives from interfaces and physical layouts—not function names, queries, tables, or fixed SQL
arities. Adaptive mechanisms report admission, strategy, transitions, and achieved reduction.

## 17. Correctness, diagnostics, and tests

Correctness includes values, nulls, errors, ordering, multiplicity, physical position counts, ownership, memory
accounting, cancellation, and restart after cooperative checkpoints.

Architectural tests cover representative island compositions and corpus-wide non-decomposition; vector lifecycle and
cancellation order; branch-mask error isolation; encoding and companion-stream composition; equal-work counters;
calling-convention conformance; native Parquet type/encoding coverage; and scheduler restart under time sharing.

Tests assert capabilities and observable invariants. They must not infer architecture from package or class names.

## 18. Performance evidence

TPC-H, TPC-DS, ClickBench, and Engine Coverage are macro regression guards. Subject and control use the same SQL, plan
topology, input files, worker topology, concurrency, heap policy, and query execution order.

Primary metrics are latency, CPU core-seconds, allocated bytes, and peak memory. Reports include absolutes, a fixed
subject/control ratio, ranges, CPU-seconds per second of latency, geometric-mean suite summaries, and per-query spread.

Routine points should preferably take 15–45 seconds on the slower engine and normally remain under one minute. Larger
scale, capacity, skew, and memory-pressure cases form a separate stress tier.

Parameterized sweeps complement macro suites by varying adaptive inputs with per-operator attribution. Plans, input
work, spill, and blocked time are validity checks. Performance never overrides semantics or architecture.

## 19. Coverage and productization

This specification describes the target even where coverage is incomplete. Productization keeps explicit inventories
for types and nested combinations; scalar, aggregate, and window conventions; operators and compositions; Parquet
types, encodings, pages, and compression; connector/object-store I/O; exchange and result boundaries; failure,
cancellation, memory pressure, and spill coordination; and observability and rollout.

Adding a type, operator, calling convention, or physical path adds functional and benchmark coverage. Unsupported
entries fail at admission and are not silently delegated inside an island.

Rollout begins with plans whose complete region is supported, retains explicit host fallback at island admission, and
expands by closing gaps. Once admitted, an island executes wholly in Nitro or fails; runtime decomposition is not a
rollout mechanism.

## 20. Changing this specification

A change to concepts, contracts, boundaries, or deliberately unsupported behavior updates this document and adds a
decision record in the same Nitro change. The record explains the forces, consequences, and alternatives; this file
states only the resulting current contract.

Accepted records are immutable. A correction or reversal gets a new record that amends or supersedes the old one, and
both ends plus the index are updated together.

Implementation detail that does not alter a contract needs no spec edit. A performance experiment becomes a decision
only when it changes an admitted strategy or default; its evidence and rejected alternative then belong in a record.
