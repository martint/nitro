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

### 4.1 Module dependency direction

`nitro-spi` contains the classloader-neutral vectors, ownership, source, type, function, and provider contracts and
depends on no engine or connector implementation. `nitro-engine` implements evaluation and operators over the SPI.
`nitro-parquet` implements a source over the SPI without depending on the engine. `nitro-connector-loader` loads
providers while depending only on the SPI. Host/legacy compatibility belongs in explicit compatibility modules, and
tests/benchmarks may depend on all layers. Dependencies do not point from SPI or reusable providers up into engine,
host integration, tests, or benchmarks.

## 5. Vectors, streams, and logical types

Vectors represent physical storage. Logical meaning comes from type bindings.

The vector family includes fixed-width flat vectors, variable-width binary vectors, dictionary and RLE vectors,
arrays, maps, rows/structs, and compositional wrappers whose mappings can be resolved without flattening children.
Pooled vector storage may have more capacity than the logical or physical domain currently governed by a mask or
mapping. A consumer requires storage to cover that domain and must not infer additional valid positions from spare
capacity or require exact capacity equality. Every stream of a fixed-cardinality structural child covers the parent
domain; a null parent row may leave child contents unspecified, but it does not permit a shorter child vector.
Growing a fixed-cardinality structural vector is an atomic reconstruction: allocate or reuse the parent shell, grow
every child stream to cover the new domain, and only then attach the children. Generic copy-then-grow machinery must
not publish an enlarged parent with its former undersized children, even transiently.

The vector SPI represents contiguous logical-range copies directly. A caller must not expand a range into a
row-count-sized identity-position array. Flat vectors may bulk-copy storage, encoded vectors traverse their physical
runs or mappings, and the compatibility default copies individual positions without allocating range metadata.
Representation-specific range copying preserves the ordinary destination ownership and allocator-context rules.

Dense concatenation preserves a common encoded representation when it can compose the physical domains exactly. In
particular, concatenating RLE segments concatenates their run counts and materializes only their physical run-value
domains; it does not expand runs into one value per logical row. Mixed or incompatible representations retain the
general dense materialization fallback.

A physical carrier does not imply a logical type. An I64 carrier may hold BIGINT, a short decimal, date, time, or a
timestamp representation. Binary storage does not imply UTF-8. The type registry supplies equality, hashing,
comparison, coercion, display, and function semantics.

A type provider may describe a logical key as an ordered fixed-width tuple of canonical primitive lanes. A lane
either selects one raw primitive source or projects one 64-bit canonical value from one or more primitive sources.
Every source declares its carrier and structural field path. Paths, lane partitioning, carriers, and canonical bits
are physical layout metadata only; Nitro does not infer field meaning, a composite carrier type, or logical semantics
from them. Logical nullness belongs to the enclosing value, and every selected component must be non-null whenever
that value is non-null.

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

When a mask and an encoded value stream have different row-to-domain mappings, equal domain sizes do not establish
alignment. A consumer that can apply exact multiplicities may ask the encoded vector to count its selected domain
under the mask into caller-owned reusable scratch. The framework derives only physical frequencies; the registered
function decides whether weighted processing preserves its semantics.

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
One resolved binding may expose several of these capabilities for the same authoritative implementation. A
provider-owned batch implementation may, for example, also expose its exact scalar target so the compiler can compose
adjacent eligible calls into one generated loop without materializing intermediate vectors.

Registry-bound batch functions may request identity or ordering already composed from a logical type binding. The
returned operation works over the type's admitted vectors and their null streams, so functions such as structural
lookup do not materialize host carriers or reproduce type-specific equality. The engine composes physical traversal;
the type provider remains authoritative for leaf semantics, and the function provider remains authoritative for the
operation that consumes them.

An ordering request states its unordered-value placement explicitly. At minimum the type capability distinguishes
unordered-first from unordered-last comparison, and structural composition applies the same convention recursively
to nested nulls and unordered leaf values. A consumer must bind the convention required by its logical function; it
must not infer one convention from another or recognize a concrete logical type to repair the difference.

A type provider may additionally expose a lossless normalized ordering-key binder for a logical non-null domain whose
complete order fits in 64 bits. The binder resolves an admitted physical vector once and returns keys whose unsigned
comparison exactly agrees with logical comparison and whose equality exactly identifies peers. This is an explicit
provider proof, independent of carrier width and raw identity. Consumers must retain structural comparison for absent
or inapplicable binders and handle null placement separately.

A type provider may separately prove that raw equality of every admitted non-null physical representation is the
logical key identity. This proof lets generic grouping, join, distinct, and membership tables use their physical key
kernels even when the binding also publishes richer logical hash or comparison operations for other consumers. The
proof belongs to the provider and must be absent when normalization, canonicalization, unordered-value semantics, or
any other logical rule makes raw physical equality insufficient. Persistent key-table consumers reject the type when
neither this proof nor another supported physical key layout is available; they do not use row-wise semantic hashing
and equality as an execution bridge.

A provider may give the narrower structural proof that logical key identity is exactly raw equality of an ordered
fixed-width canonical tuple. A canonical lane is either one raw `I32`, `I64`, `F64`, or `BOOLEAN` source or a 64-bit
projection of one or more such sources through an exact provider target. Each source selects either the value itself
or a path of named structural fields. Providers declare multiple lanes when identity requires more than 64 bits.
Generated key consumers resolve supported wrapper mappings once per batch, load the exact primitive arrays directly,
and constant-link projections into the generated load/hash/probe loop. They do not pre-materialize a canonical vector
or invoke a generic projection interface, provider virtual method, or row-wise method-handle adapter. A declared
layout that does not match its admitted vector, selects an independently nullable component, has an incompatible
projection signature, or exceeds a consumer's supported generated shape is an admission error; it is not permission
to enter a row-wise semantic bridge. Types without this proof retain their exact semantic key operations for scalar
and other semantic consumers, but grouping, distinct, hash join, and membership reject unless another direct physical
key capability is available.

A persistent key consumer may compose an arbitrary ordered sequence of provider-proven physical identities in one
resolved layout. Each logical field contributes either its direct flat representation, including variable-width
storage, or its ordered canonical lanes. One generated table owns hashing, collision equality, fixed and
variable-width storage, growth, and payload for the complete key; consumers do not coordinate separate tables or
select an implementation from logical type identity, field count, or a named combination of representations.
Generated field operations load the resolved concrete storage and constant-link projections. Canonical fields retain
the first logical representative when their stored identity cannot reproduce grouped output. Unsupported field
capabilities or generated shapes reject before table mutation rather than entering an interpreted field-accessor or
row-wise semantic fallback.

A provider may also prove that one logical key is an ordered finite product of independently nullable child
identities. Each child declares a structural field path and its authoritative type binding and may recursively
declare another product. Resolution flattens the tree into the same persistent descriptor while retaining an explicit
presence boundary for every product node, all nullable ancestors of each leaf, and the leaf's direct or canonical
physical identity. Generated loops bind concrete arrays, binary regions, projections, and wrapper mappings once per
batch and skip descendant identity under a null product boundary. Nitro does not infer products from logical type
names or vector classes. Recursive identities that are not finite products of supported leaves reject rather than
entering a semantic object table.

A provider may separately prove that one logical key is an ordered repetition over one or more aligned physical child
streams. Each child output supplies its authoritative type binding and may recursively use direct identity, canonical
lanes, a finite product, or ordered repetition. Generated consumers bind the parent offsets, wrapper mappings, child
streams, null sources, primitive arrays, binary regions, and projections once per batch, then hash, write, and compare
the ordered ranges through direct generated loops. They do not allocate row objects or dispatch through accessors,
virtual child kernels, or method handles per element. Grouping retains the first logical representative when the
stored physical identity cannot reconstruct the provider value. Repeated-vector shape alone is not a semantic proof:
unordered identities such as maps require a distinct provider capability and remain unsupported until one exists.

A provider may prove instead that one logical key is an unordered repeated multiset of aligned child-output tuples,
with multiplicity contributing to identity. Generated consumers encode every recursively resolved entry tuple into
exact physical bytes, sort reusable primitive entry references by those bytes, and store and hash the entries in that
canonical order. Equality therefore does not depend on producer entry order or perform a quadratic entry search.
Scratch ordering and offset storage is allocator-owned and reused; the hot path creates no entry objects and performs
no per-entry virtual or method-handle dispatch. Nitro does not infer unordered semantics from a map vector, logical
type name, or repeated-output arity.

Stateful registry implementations may also request a composed key binder. Each bound vector exposes opaque hashing
and cross-vector identity over positions, allowing retained indexes such as map construction state to span owned
vector segments. The binder does not prescribe table layout, growth, payload retention, or duplicate policy.

An evaluator owns the mutable state created through its function execution context. State is thread-confined to that
evaluator, may be reused across its batches, and is closed when the evaluator closes if it implements
`AutoCloseable`. A resolved function binding remains immutable and must not retain evaluator-local state itself.

Higher-order function providers bind container arguments, captured outer expressions, and one or more nested
expressions. The host compiler lowers each nested expression through the same normalized, mask-aware evaluation IR as
an outer expression, with lambda parameters represented as ordinary nested-domain inputs. At execution, the provider
maps selected container entries and captures into that domain, invokes the nested plan under one or more derived masks,
and constructs the function result. A provider whose semantics include ordered short-circuiting narrows successive
masks to the next entry of each unresolved parent; decisive parents leave the domain before later entries are invoked.
This preserves per-parent order and error observability while retaining vectorized evaluation across parents. The
provider owns container semantics; neither the compiler nor evaluator recognizes specific higher-order functions.
Parent null, error, and inactive positions contribute no nested positions, while recursive vector structure and
encoded child streams remain available to the nested evaluator.

Generated adapters are ordinary implementations of the batch convention. They may generate direct invocation bytecode
around a constant method-handle target, hoist null/error classification, traverse selected positions, and write into
allocator-owned outputs. Generation does not transfer function semantics into the evaluator.

A provider that resolves the complete logical call may consume immutable specialization constants before lowering
the runtime batch signature. This is required when the host function uses a planning-only carrier that is not a Nitro
logical type, such as a compiled pattern or path. The provider binds the constant into the exact target, removes that
argument from the runtime vector signature, and gives distinct bound values distinct function identities. Only exact
non-null constants may be consumed this way; a non-constant unsupported argument rejects the call. The evaluator
neither vectorizes the host-only carrier nor recognizes the function that uses it.

When the result carrier is Boolean, the same generated adapter may implement the mask convention directly: evaluate
the composed scalar target only at active logical positions and compact the caller-owned mask in the same loop. This
form must preserve strict null/error exclusion and exact invocation counts. In particular, a nondeterministic target
is invoked once per selected logical row and is never collapsed over dictionary entries or RLE runs. Predicate
composition is admitted from resolved scalar capabilities and physical carriers, never from function identity.

Known-empty companion streams are physical constants, not row loops. If every strict argument is proven null-free, a
requested NULLS result uses the allocator's immutable all-false representation; the adapter must not scan logical
positions to rediscover that fact. The same rule applies to other provably empty companion streams.

Reference-carrier arguments are read through exact handles supplied by the logical type binding. Reference-carrier
results are appended immediately through a provider-owned result writer into Nitro vectors. Nitro does not retain an
array of host objects and does not infer logical meaning from `Slice`, `Block`, or another carrier class.

A strict reference-carrier target may return null for non-null inputs when its resolved semantics declare a nullable
result. The generated loop records such a return directly in the NULLS stream and must not pass null to the
provider-owned result writer. A NULLS-only request still invokes the target because result nullness is data-dependent,
but the resulting VALUES vector is not published. Nullable primitive-return targets require a distinct boxed or
explicit-null calling convention and are rejected until one is supplied.

An exact scalar target is eligible only when adapting its carrier does not introduce row-proportional allocation or
erase a useful physical representation. A composite host carrier assembled from several vector children is not an
efficient batch ABI merely because a value reader can construct it. The provider must retain or supply a physical
batch implementation over those children unless it supplies a physical projection program over a fixed-width
structural layout. Such a program names primitive vector components of arbitrary arity; generated code holds them in
locals across adjacent calls and materializes one allocator-owned structural result at the consumer boundary. Field
names and component meanings belong to the provider's physical type binding. Nitro assigns no logical type or
function semantics to the structure.

Generated projections use the evaluator's ordinary encoded-domain proof for both scalar and structural inputs. A
row-aligned structure whose children share a dictionary mapping is peeled once, evaluated over its physical domain,
and rewrapped with that mapping and its exact frequencies. A second, projection-specific peeling implementation is
not permitted.

Generic structural construction preserves a proven shared dictionary or run domain. The evaluator constructs the
provider-owned physical value once per backing-domain position and restores the original logical mapping; it does not
expand child expressions to logical-row width. Known-empty companion streams do not constrain the construction
domain. A backing vector may contain unused entries beyond the active domain, and a nested function may consume that
backing so long as it covers every position in the invocation domain.

Failure behavior describes the selected implementation, not merely the most conservative declaration in a host
catalog. A registry may refine a host `MAY_FAIL` declaration to `NEVER_FAILS` only for an exact implementation it owns
and can prove infallible for every value admitted by the bound signature. The proof cannot depend on observed data,
query identity, or an optimizer assumption. A genuinely fallible sibling operation remains `MAY_FAIL` and is excluded
from infallible expression-slice fusion.

A VALUES-only request ordinarily does not force NULLS or ERRORS materialization. Values beneath an unrequested
semantic null are unspecified. A framework-managed adapter for a fallible strict scalar is the exception: it must
request NULLS and ERRORS needed to prove which active rows may invoke the target. Requested companion streams preserve
exact semantics.

A provider may attach a failure mapper to a fallible scalar target. The generated adapter catches failures only
around invocation of that target. The mapper converts declared row failures to classloader-neutral `ErrorValue`
records and must rethrow cancellation, linkage, virtual-machine, and other non-row failures. The adapter writes
mapped failures to the requested ERRORS stream, continues with later selected rows, and propagates pre-existing input
diagnostics without invoking the target for those rows. Carrier readers and result writers remain outside the catch
region. Without a mapper, a target failure retains ordinary query-failure behavior. A mapper attached to a target
declared infallible is ignored and adds no generated exception path.

VALUES and ERRORS may be evaluated in separate invocations. When a fallible target has a failure mapper, a
VALUES-only invocation must therefore contain mapped target failures in invocation-local scratch even though it does
not publish an ERRORS stream. A later ERRORS invocation publishes the diagnostic. This rule also permits an encoded
physical domain to contain inactive logical values without turning an unobserved failure into a query failure.

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

Constrained reborrow, retention across source advancement, and open-batch availability checks are distinct
capabilities. Reborrow does not authorize polling a source while its current batch remains open. A consumer may
infer that an open batch is final only when the source can report whole-input availability without advancing or
invalidating it; temporary depletion of a reusable ingress is not completion. Otherwise, consumers constrain and
copy required payload, then close the batch before polling again. Nonempty inputs establish payload representation
from selected values rather than borrowing rejected payload merely to discover its schema.
Ordering values already copied into winner storage are not deferred payload. When no other output remains to be
read, selection does not retain a pending source batch or propagate a payload constraint to it.

Top-N ranking selects incrementally. `ROW_NUMBER` retains at most the requested row count per partition; `RANK`
retains peer groups while their preceding-row count is below the limit; and `DENSE_RANK` retains the requested number
of distinct peer groups. New better groups evict worse groups during ingestion. Partitioned ranking assigns identity
through the generic grouping contract and shares one allocator-owned row store across partitions; it does not create a
complete operator state per partition. Retained row state therefore scales with the observable qualifying result,
including required boundary ties, rather than with total input. Partition-key state necessarily scales with distinct
partition count. The host-session and island-pull entry points use the same physical state.

TopN selection stores every error-free binary-only ordering tuple in allocator-accounted mutable winner slots for
every positive result limit, then materializes compact variable-width output after selection. A small result limit is
not a reason to retain independently allocated one-position vector trees. Mixed and structural ordering tuples keep
their separately evidenced per-lane admission policies.

Operators are generic over functions, logical types, arity, query shape, tables, and column combinations.
Specialization comes through general physical interfaces or generated resolved layouts.

An operator may optionally publish dense forward output ranges to a synchronous downstream sink without constructing
a `Batch`, `Output`, or `Mask`. Capability negotiation occurs before either side advances or mutates state. The source
retains ownership of every borrowed result lane for the duration of the call, and the sink must consume it before the
call returns. Declining the capability preserves ordinary batch pull exactly; cancellation or failure releases the
current producer-owned range and closes through the same operator graph. Pass-through projections may remap this
capability, but computed expressions, filtering, distinct aggregation, and other operators requiring row identity or
selection retain batch pull unless they provide their own exact range contract.

A provider-owned forward range kernel may instead publish primitive contributions directly to provider-owned
downstream state. Each output declares a classloader-neutral carrier and exact null convention, while each downstream
unit declares the input channel and identical contribution shape it accepts. The engine matches the complete program
structurally before loading input and never compares function identity. Primitive `long` values may carry explicit
null contributions or an exact logical multiplicity for a contiguous run of one proven value; an aggregate
independent of values may consume range cardinality separately. The default repeated-value convention expands to
scalar contributions, while a provider may override it only when one state transition preserves its exact arithmetic,
null, error, and ordering semantics. Any missing or incompatible declaration retains vector range or batch pull
without partial advancement.

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

At an asynchronous exchange boundary, storage ownership and host reservation ownership move together. The exchange
reservation remains live while a batch is queued; after dequeue, a retaining consumer charges the same physical bytes
to its allocator context until the complete transferred batch lifetime closes. A blocking operator retains that batch
lifetime rather than taking individual vector objects out of the enclosing lease. Streaming consumers release the
transferred lifetime after the batch is consumed. No interval may leave live storage uncharged, and the handoff must
not double-charge one physical allocation.

Host-backed lazy input is charged when each column materializes, not by forcing the host page to load at ingress.
The reservation follows the complete source-batch lifetime and therefore covers a materialized vector retained by a
blocking consumer while preserving selection-driven lazy reads for columns that are never demanded.

The steady-state goal is no allocation proportional to row count for streaming scan/filter/project/aggregate shapes.
Initialization, bounded resizing, state growth, output materialization, and provider-required variable-size results may
allocate, but repeated batches should reuse their working set. Reuse includes encoded results: an owned RLE proposal may
replace its wrapper while retaining or replacing its physical value-domain storage under one explicit owner.
Immutable stream tuples and structural field tuples are also reusable storage metadata: a position-copy path retains
the existing tuple whenever its backing vectors are unchanged and traverses its fixed streams directly rather than
constructing maps or iterators. Provider operations invoked once per logical position must either bind physical access
outside that loop or use a non-allocating single-value access path; an accessor factory is not itself a per-position
calling convention.

Temporary domain lookup storage and the output mapping derived from it have separate lifetimes. A partition copier
may return its non-escaping lookup scratch to the primitive pool after remapping, but published mapping IDs must
remain valid for every output that shares them, independently of subsequent scratch reuse.

## 12. Stateful and extensible operators

### 12.1 Aggregation and window functions

Aggregation and window operators drive provider-supplied functions. Function state, intermediate representation, null
semantics, ordering, and final materialization belong to the registered implementation or type capabilities, not to
operator conditionals.

State is allocator-owned and may use fixed-width, segmented, recursive, dictionary-domain, or generated layouts.
Function-specific aggregation-state vector types belong with function implementations, not in the core vector
vocabulary.

A physical key layout's batch binding governs metadata queries as well as hashing, writing and comparison.
Specialized layouts must expose their bound null semantics through the shared capability, including when a caller
does not resupply input vectors. Ending a binding invalidates its encoding and nullability proofs.

A provider may bind exact reversible single-position updates for overlapping window frames. The window driver adds
newly entering positions and removes positions leaving a monotonically advancing frame; the provider retains all
function, null, and state semantics. A provider may decline the capability, in which case the driver reinitializes
and replays each complete frame. Physical bindings may be refreshed across retained source batches, but every binding
updates the same provider state and a position is removed only after that state has observed its addition. Replay
initialization invalidates all prior physical bindings.

A positional-frame window function may additionally declare forward batch-range result materialization. For a
selected capable function, the window operator may omit the complete result plane and eagerly produce independently
owned result streams no larger than the public output batch while preserving one complete-partition position index
and function state across successive ranges. A new partition resets that state; source-page transitions refresh
physical bindings without truncating the partition view; replay fallback remains valid inside any range. Functions
without the capability, and capable functions whose output is not selected, retain or omit the established complete
plane respectively. Source outputs remain independently lazy, and duplicate selected outputs own distinct vectors.

An aggregation provider may bind a reversible window range kernel for that forward materialization. The engine
    physically binds frame bounds and prepares one batch-sized destination before invoking the provider once for a
    contiguous output range. The bound frame cursor may drive the provider directly or expose an exact compact affine
    ROWS descriptor containing only partition size and constant preceding/following distances. The descriptor has no
    logical type or function meaning. It is admitted only when every retained physical source run proves the same
    single-run, non-null, error-free, and non-negative offset; otherwise the cursor retains exact per-row resolved
    bounds. A row-position index exposes source-run boundaries without flattening or allocating a position mapping so
    the proof scales with retained physical sources rather than logical rows. The provider owns state reset, null,
    inverse, empty-frame, and result semantics and must not retain the borrowed bounds or partition input view after the
    call. Equal frame traversal identities share one bound cursor or descriptor across sibling functions. Missing
    capabilities retain reversible single-position updates or complete-frame replay. Execution diagnostics report kernel
    positions, additions, removals, and results.

When every selected window output has forward range materialization and a downstream global physical aggregation
program accepts dense synchronous ranges, the window may deliver its provider-produced result streams directly to the
aggregation state. This bypasses batch transport wrappers and masks but does not transfer function semantics or result
storage ownership: window providers still construct result lanes, aggregation providers still interpret them, and the
engine only maps physical output channels. Filters, DISTINCT, grouping, incompatible schemas, legacy window functions,
and intervening computed expressions use normal batch pull.

When every downstream global aggregation unit supplies a structurally compatible primitive range binding, the same
composition may omit provider result vectors and generic aggregation traversal. The window provider emits primitive
values, exact repeated-value runs, or null contributions from its range kernel directly into opaque
aggregation-provider consumers; units such as row count that do not observe values receive exact range cardinality
instead. A producer coalesces only contiguous contributions whose physical equality it proves, and the consumer owns
the meaning of multiplicity. Multiple outputs and multiple consumers remain independently bound. Filters, DISTINCT,
grouping, carrier/null mismatches, or any unit without the capability decline atomically before the window loads and
retain the established fallback. Cancellation propagates from the consumer and graph close releases provider state
through the ordinary lifecycle.

An exact counted-key aggregation binds hashing, equality, retained-key copying, and result construction from its
input type provider. Its provider-owned live state may retain allocator-owned vector segments and refer to their
positions from generic primitive table metadata. Partial and final stages exchange the provider's portable recursive
value, such as `MAP(K, BIGINT)`; neither the aggregation operator nor evaluator recognizes the function, key type,
physical carrier, or intermediate layout. A carrier-specific state may exist only as a measured implementation tier
behind the same registry binding.

An aggregation implementation that accepts exact multiplicities may consume an independently encoded input by first
counting selected row ids into allocator-owned reusable domain scratch. Mapping identity permits direct zero-pass
domain reuse. Independently owned mappings may reuse the same counts only after an explicit O(rows) proof that their
complete ID sequences are equal; unequal mappings use the exact counting fallback. Neither path permits the engine to
infer function semantics.

A provider may describe each generated grouped update as an ordered, non-empty tuple of physical contributions and
an exact method handle over its opaque state, group id, and those carriers. Each input contribution declares one
allocation-free JVM carrier independently of its logical type. Fixed-width carriers are `long`, `double`, and
`boolean`. A binary-region carrier expands one contribution to `(byte[] data, int offset, int length)` without
allocating a per-row slice or copying its bytes. The carrier model is extended rather than adding interfaces or
contribution classes for every carrier or tuple shape. Generated kernels flatten the tuple into specialized physical
bindings, enforce the combined null convention, and link the target as a constant without naming the provider class
or allocating a row argument tuple.
An input contribution may select a provider-declared path of named fields through a structural physical value before
loading its declared carrier. An empty path selects the argument value itself. The engine interprets the path only
as recursive vector structure: it preserves outer dictionary, RLE, and region mappings and never assigns logical-type
or function meaning to field names. Logical nullness remains attached to the complete argument; component nullness is
not silently discarded, and an exact generated path is declined if a selected physical component is independently
nullable.
The target identity is part of the generated-kernel cache key. A provider may additionally supply a repeated-update
target that consumes the complete contribution tuple plus an encoded-domain multiplicity; absent that capability, the engine preserves semantics by
invoking the single update once per logical occurrence. This contract supports multiple updates and mixed physical
carriers without one engine interface for every carrier or state layout. A multi-input encoded-domain shortcut is
admitted only when every value and null mapping is proven to share the same physical domain; otherwise the generated
logical-row loop remains valid.
When that alignment is proven, every update supplies repeated semantics, no filter or distinct stage needs logical
row identity, and the physical reduction is large enough, Nitro may generate a separate domain kernel. The batch
boundary counts selected rows and resolves grouping identity once per used domain value. Generated code performs null
elision and one exact constant-linked provider update per used domain value with its logical multiplicity. Physical
domain positions and logical row positions are distinct kernel contracts; equal domain sizes alone never prove
alignment.
Planning must preserve the metadata required by a batch-conditional grouped-domain capability. A provider declares
that it may support the convention independently of a particular batch; this declaration requests exact grouping
domain metadata but never admits execution. Runtime still performs the complete value, null, domain-width, and row
mapping proof atomically before any aggregate state is mutated. Independently decoded mappings may be proven equal by
an established shared identity or by exact equality of their complete ID sequences.
Deterministic row-aligned scalar functions propagate requested exact domain metadata to their inputs. This matches
the evaluator's ability to execute the function over physical values and restore the same logical-row mapping on its
result. Non-deterministic functions execute per logical row and do not propagate that promise. A generated adapter's
descriptor and executable implementation must report the same determinism; retaining the flag only in registry
metadata is insufficient because physical admission consults the executable capability.

Grouping is a physical key-to-group operation. It can select flat, packed, dictionary-domain, generated composite, or
other general representations based on observed shape and immutable policy. It cannot recognize aggregate functions
or SQL types. Partial aggregation considers retained work, cardinality, state size, and reduction.

Provider-described fixed-width keys use one shared generated table contract for grouping and distinct. Batch binding
resolves primitive arrays, offsets, and flat/region/dictionary/nested-dictionary/RLE mappings outside the row loop;
the generated loop performs direct carrier loads, hashing, and exact probing without value-access interfaces or
provider dispatch. Grouping separately retains logical representatives for output, while distinct separately applies
its requested null-dropping or null-retaining semantics.

DISTINCT may normalize an eligible mixed flat key into that table without assigning logical meaning to its carriers.
Full-width primitive sources remain full-width; exact query-stable I32 identities for binary values and direct I32
sources may be bijectively packed into generated I64 lanes. The engine materializes no packed canonical lane vector:
batch binding supplies concrete source arrays and mappings, and generated code performs packing in locals. Once the
generated table contains a record, carrier mismatch, unsupported wrappers, value-ID exhaustion, or excessive lane
count fails before further mutation rather than switching table representations.

Grouping and distinct do not retain object tables that invoke provider semantic hashing or equality per row. A key
without a compatible direct or generated physical layout is rejected as unsupported coverage.

### 12.2 Pattern recognition

Pattern recognition is an engine-owned Nitro operator, not a host Page operator embedded inside an island. Planning
lowers the resolved row pattern, labels, navigation, skip, output, ordering, and partition semantics into immutable
engine-neutral descriptors. A restartable matcher operates on allocator-owned state and registry-bound definition,
measure, and match-local aggregate callbacks. Buffered rows preserve encodings when their source lifetime allows it and
otherwise transfer into owned storage.

### 12.3 Table functions

Table-function providers use a classloader-neutral Nitro batch SPI with explicit masks, input consumption, output
demand, allocator context, blocking, and owned/transfer output. Partitioning, ordering, markers, and pass-through row
identity are planned descriptors rather than hidden host Page channels.

Provider admission has three visible levels: host compatibility outside a Nitro island, generated/materializing
compatibility at a declared boundary, and Nitro-native batch execution inside a zero-adaptation island. Planning never
reports a compatibility provider as native.

## 13. Joins and dynamic filters

Join algorithms are separate from key semantics. Type bindings and registry functions provide exact hashing,
comparison, and residual predicates. Physical key layouts may be generated from resolved lanes.

A join over a provider-described persistent key layout uses the same generated exact table and physical binder as
grouping and distinct, including canonical fixed-width lanes and recursive finite products. Build insertion skips
every row with a null logical key; for a product key, any null descendant makes the join key null even though grouping
and distinct retain that nested null as part of identity. The join preserves duplicate row references, and probe
lookup does not mutate the table. Prepared probe views share immutable build state but own their batch-binding scratch.
No row-wise structural comparison or accessor interface is an execution bridge for an admitted persistent layout.
Hash joins and semi-join membership likewise reject keys that would require a row-wise semantic object table.

Build state and prepared membership are task-owned capabilities shareable across compatible probe drivers. Join output
preserves mappings and encodings when this avoids copies and satisfies ownership.

Capping initial hash allocation does not establish that a direct index can represent the build. A bounded sample
may avoid speculative direct construction when a selected, non-null key proves it exceeds that representation's
existing capability. Absence of such a witness is only provisional admission; later insertion retains exact fallback.
This proof must not silently substitute a narrower density budget for an otherwise valid direct domain. Compressed
batch insertion honors its existing admission budget and observes any direct-to-hash transition within the batch
before accessing direct storage again.

Dynamic filtering has one logical lifecycle: build evidence becomes a published immutable constraint with exact
semantics and bounded representation. Nitro operators and connector/source capabilities may consume it. The source may
use row-group statistics, page indexes, dictionaries, bloom filters, or value-level filtering; the join retains exact
enforcement as the correctness backstop.

Host dynamic-filter plumbing may carry the same logical constraint, but Nitro must not maintain two independent
semantic authorities. Future unification may use the host transport if it preserves Nitro timing, physical-domain, and
source-capability contracts.

Exact host-domain publication may use an injected factory to order primitive position indexes with the host's
resolved logical comparator. Flat collector values may be written directly into an independently owned Block
using the registered FLAT-to-BLOCK_BUILDER convention, without an intermediate boxed scalar collection. The
unordered-last comparator and host NaN predicate exclude only top-level NaNs; nested values retain their type semantics.
Publication must preserve the existing domain semantics, retained representation,
collection bounds, and partition-completeness and readiness rules. It must not infer logical ordering from a physical carrier. An explicit
ordered-position domain factory is a boundary capability; the ordinary host builder's default path remains unchanged.

At the source boundary, an admitted integral logical type may consume immutable host range endpoints through that
type's accessor without materializing per-range scalar objects. Unbounded and exclusive endpoints, null inclusion,
expansion budgets, and physical membership admission must retain the same semantics. This does not expose host
range or Block classes to Nitro core or change host filter readiness.

For a join with multiple equality criteria, the host may preserve the ordered build-key correlation as a bounded
tuple filter beside the independent column domains. The exact join predicate remains authoritative. A correlation is
valid only for the equality criteria and registered types from which it was built; partial partition state, mismatched
tuple order, a missing partition, or a non-equality comparison cannot contribute to it. Partitioned results union a
correlation only when every completed partition supplies the same ordered tuple shape. Intersections may retain
several independent necessary correlations.

The host boundary may transfer exclusively decoder-owned storage into an immutable filter summary after validation.
This does not permit borrowing mutable caller storage: ordinary constructors remain defensive, and framework-only
deserialization entry points must be marked as such. The ownership optimization must preserve the wire contract.

## 14. Sources and native Parquet execution

Connectors interact through the source and vector SPI. They receive allocator, type bindings, I/O capabilities, and
immutable source policies at construction. The engine does not know connector implementations.

The Nitro Parquet reader decodes directly into Nitro vectors, preserves useful dictionary/RLE structure, and advances
unneeded streams without decoding values. Missing types or encodings fail loudly; they do not fall back to `parquet-mr`
record materialization.

A filter may offer an exact typed static domain to a source through capability negotiation. The source assumes
semantic responsibility only when it accepts complete enforcement; until then the original filter remains the
residual authority. Rejection leaves ordinary expression evaluation unchanged. Physical admission may depend on
immutable source shape and bounded policy, but not on a query, column, function name, or SQL type. A reader kernel may
support broader shapes for correctness coverage than the shapes admitted by the production policy.

When encoded child streams prove that many repeated ARRAY or MAP parents are identical, the reader may recover a
bounded outer dictionary domain. Equality is established only from exact parent null state, repeated boundaries, and
the immutable physical identities of every child value/null/error stream. Admission is controlled by the immutable
materialization policy; failure to prove a sufficiently small domain retains the ordinary recursive vector.

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

At a synchronous remote-output egress, the host may declare a callback-scoped, non-retaining consumer that copies
every selected value into destination-owned storage before returning. The host integration may then expose an exact
temporary host view over borrowed Nitro value storage when every output channel's registered type adapter proves the
physical representation compatible. Dense views expose only the logical range. Sparse views may carry a temporary
selection mapping over the borrowed flat base, but the destination must consume that mapping directly or complete any
encoding-preserving serialization within the callback; neither the view, its base, nor a transitive wrapper may remain
reachable afterward. Semantic transformations such as `CHAR` normalization and unsupported vector layouts require
the ordinary owning adaptation path. The source batch closes only after the synchronous consumer returns.

A sparse borrowed base exposes only the prefix through the highest selected position. The original mask-domain
width is not a requirement that every vector retain storage for an unused suffix. Selected values must exist;
incidental pooled-array capacity must not hide a mismatch between advertised host positions and physical storage.

This optimization does not move partition assignment, buffering, serialization, backpressure, replication, skew,
encryption, or output accounting into Nitro. Nitro core remains independent of host `Page`/`Block` classes. The host
boundary reports borrowed versus owning adaptations and their conversion and append costs.

The host may attach ordered dynamic-filter identity/output-symbol groups to a producer fragment when it can trace all
probe symbols through exact lineage. Identity paths, reference-only projections, exchange position mappings,
set-operation layouts, and nested Nitro plans are eligible; opaque transformations are not. The metadata travels with
the fragment through task serialization and retry. Reused fragments merge distinct consumer groups.

An eligible Nitro remote-output boundary consumes the corresponding immutable tuple
filter before partitioning or host adaptation. Every required output must be present with the registered type, the
retained filter must be bounded and host-accounted, and an instance-owned selectivity sample may stop applying an
ineffective filter. This is part of normal Nitro execution, with no separate configuration opt-in (ADR-0104).

Optional tuple construction may be omitted when its native join probe has no possible consumer for that
representation. In the current integration, a fully local probe has no remote-output tuple consumer; native scans
still consume independent column domains and native join-filter capabilities. Unknown build-only fragments remain
conservative. Consumer admission does not change the plan, scalar-domain semantics, or exact join enforcement.

Registry-proven static binary equality pushdown is likewise part of normal filter execution, not a separate opt-in.
The original predicate is retained unless the source explicitly accepts complete semantic enforcement.

The host can charge separate memory reservations through allocator contexts without copies between adjacent Nitro
stages. Page/Block boundaries are not ownership boundaries inside an island.

## 16. Adaptivity and specialization

Physical tables may overlap independent initial lookups in bounded batches. Prefetched slot contents are only
evidence of a hit after exact key equality; an apparent miss must be resolved against earlier insertions in the same
batch. Growth invalidates slot coordinates. Batching preserves first-occurrence order, selection/null semantics,
and the existing representation-admission and growth rules. Scratch storage is allocator-owned and accounted;
batch size is immutable instance policy, not query-specific knowledge or an enabling feature flag.

Adaptive choices respond to measured physical facts: mask density, selectivity, domain size, run length, cardinality,
novelty, key width, state footprint, ordering, and locality.

A bounded independent-domain evaluation may combine dictionary arguments with single-run RLE arguments. The latter
contributes a one-entry domain; it must not force otherwise encoded work back to logical-row evaluation.

Decisions occur outside inner position loops, at construction, first batch, stable shape, or bounded re-evaluation
points. Mid-stream re-adaptation resists oscillation.

Specialization can generate expression loops, grouping layouts, hash/probe kernels, comparators, decoders, and state
updates. Generated code derives from interfaces and physical layouts—not function names, queries, tables, or fixed SQL
arities. Adaptive mechanisms report admission, strategy, transitions, and achieved reduction.

Persistent-key generation specializes one exact physical layout: its ordered direct fields, product-presence
boundaries, source carriers, source partitioning into canonical lanes, nullable ancestors, constant projection
targets, mappings, and table-storage policy. Primitive-array casts and
mapping references are hoisted before the logical-row loop; the loop contains carrier-specific array loads,
constant-linked canonical projections, and the generated exact probe. Static shape validation and batch binding may
inspect vector wrappers, but hot rows do not use generic method-handle invocation, virtual provider operations, or
polymorphic value-access interfaces. Logical types do not select generator or table classes.

Reversible DISTINCT normalization is one persistent-key physical policy. Its descriptor is derived from the complete
ordered carrier layout, not a query or named logical type. Every specified direct field requires its provider's
explicit raw-key-identity proof; a carrier, vector, or flat-handler kind does not establish admission. Binary value
IDs are exact for the query lifetime across admitted representation changes. A bounded initial physical sample may
select normalization only when every binary field demonstrates sufficient reuse; otherwise the already-authorized
direct flat physical table is selected before mutation. Once selected, normalization covers the complete positive
I32 ID domain and is not limited by an adaptive grouping interner ceiling. True physical-domain exhaustion rejects
before the affected batch mutates the table. Diagnostic opt-outs may compare physical policies, but are not runtime
correctness fallbacks.

There is no semantic object-table fallback for grouping, distinct, hash join, or membership. Missing physical-key
coverage is rejected at planning when capabilities suffice and otherwise at physical binding before rows are added.

Generated-kernel reuse requires exact equality of every physical property that changes emitted loads, mappings,
null handling, or loop structure. A hash may index a cache but must not itself prove compatibility. The hot batch
path compares a previously captured structural descriptor without allocation; it captures a new immutable descriptor
and resolves the corresponding kernel only when that structure changes.

## 17. Correctness, diagnostics, and tests

Correctness includes values, nulls, errors, ordering, multiplicity, physical position counts, ownership, memory
accounting, cancellation, and restart after cooperative checkpoints.

Architectural tests cover representative island compositions and corpus-wide non-decomposition; structured semantic
admission and rejection causes; vector lifecycle and cancellation order; branch-mask error isolation; encoding and
companion-stream composition; equal-work counters; calling-convention conformance; native Parquet type/encoding
coverage; and scheduler restart under time sharing.

Correlated dynamic-filter coverage includes ordered tuple hashing, independent-domain preservation, partition union
and intersection, exact lineage and remapping, non-equality exclusion, fragment task-transport round trips, retry
idempotence, reused-fragment consumers, bounded collection, and ineffective-filter admission.

Native dynamic-filter collector diagnostics identify the plan filter and report completed partition input positions
(selected rows passed to the collector, not the physical position range of a sparse mask),
independent scalar-domain representation, range count where available, and retained publication bytes. Summed
driver-local ranges are construction work, not global distinct cardinality; summed publication bytes are not peak
memory. Inspection must not materialize discrete values merely to count them. These counters describe collector
publication, not coordinator compaction or every possible dynamic-filter construction path.

Tests assert capabilities and observable invariants. They must not infer architecture from package or class names.

## 18. Performance evidence

TPC-H, TPC-DS, ClickBench, and Engine Coverage are macro regression guards. Subject and control use the same SQL, plan
topology, input files, worker topology, concurrency, heap policy, and query execution order.

Each engine uses one benchmark JVM and one distributed runner per suite. Ordered manifest points execute serially. A
point is one query or an ordered serial bundle of existing queries and is the adaptive warmup, measurement,
convergence, timing, and checkpoint identity. Each bundle iteration executes every member serially; additive metrics
are summed and peak memory is the maximum member peak. Each member retains its original SQL and independent result
validation, EXPLAIN, capability, and diagnostic evidence. A member failure fails the point and a partial bundle is
never checkpointed. A durable point checkpoint records its ordered members and is published only after all artifacts
pass validation; a failed suite may resume at the first uncheckpointed point in a replacement suite JVM. Shared JIT,
bounded allocator high-water state, immutable metadata, and file-cache state are part of the warmed suite condition.
Query-owned reservations and resources must return to baseline at query close.

Primary metrics are latency, CPU core-seconds, allocated bytes, and peak memory. Reports include absolutes, a fixed
subject/control ratio, ranges, CPU-seconds per second of latency, geometric-mean suite summaries, and per-query spread.

Optional structured pipeline timing evidence is exported from completed query statistics after measured iterations
and profiling stop. It retains per-member/repetition query, stage, task and pipeline identities, dependencies,
start/end envelopes and cumulative CPU/scheduled/blocked times. Missing timestamps remain null. An envelope is not
a continuous execution interval; cumulative times can overlap across drivers and cannot be added to reconstruct
latency. These diagnostics do not change host scheduling or retrospectively supply evidence for older runs.

Total allocation is measured with a cumulative counter which retains allocations by terminated threads, across the
same serial query interval on both engines. A sum over currently live threads is not a valid substitute. Unsupported
accounting or a decreasing counter invalidates allocation evidence rather than reporting zero. Run metadata identifies
the counter and its scope; whole-JVM allocation is not labelled operator-only allocation. Historical allocation
comparisons collected with a different or invalid counter are not merged with corrected measurements.

Routine Engine Coverage points take 25–45 seconds per execution on the slower engine. Its point manifest is the single
source of truth for harness execution and campaign audit; changing point membership, member order, or data scale
invalidates prior inventories. A warmed one-measurement control preflight publishes a complete duration inventory,
and routine launch and final audit fail unless every ordered point identity and member list is present, every Engine
Coverage point is in range, and the inventory hash matches. TPC-H, TPC-DS, and ClickBench remain fixed one-query macro
regression guards whose durations are recorded but do not gate admission. Larger scale, capacity, skew, and
memory-pressure cases form a separate stress tier.

Parameterized sweeps complement macro suites by varying adaptive inputs with per-operator attribution. Plans, input
work, spill, and blocked time are validity checks. Performance never overrides semantics or architecture.

## 19. Coverage and productization

This specification describes the target even where coverage is incomplete. Productization keeps explicit inventories
for types and nested combinations; scalar, aggregate, and window conventions; operators and compositions; Parquet
types, encodings, pages, and compression; connector/object-store I/O; exchange and result boundaries; failure,
cancellation, memory pressure, and spill coordination; and observability and rollout.

Adding a type, operator, calling convention, or physical path adds functional and benchmark coverage. Unsupported
entries fail at admission and are not silently delegated inside an island.

A provider-declared fixed-width key layout has cross-consumer coverage for grouping, distinct, joins, and membership,
including raw and projected lanes, mixed primitive sources, encoded mappings, sparse masks, null behavior, duplicate
multiplicity, and prepared build sharing. Mismatched carriers, nullable selected components, incompatible projection
signatures, unsupported representations, and unsupported generated lane counts fail loudly.

Mixed flat DISTINCT normalization covers arbitrary admitted LONG/BINARY field orderings that fit the shared generated
lane limit, including I32 packing, full-width I64 identity, binary dictionary-generation changes, sparse masks, and
null-row filtering. Unsupported transitions fail loudly before persistent state changes.

Key types and mixed key shapes without one direct or generated physical implementation are explicit coverage gaps.
They are not admitted through row-wise semantic key tables.

Rollout begins with plans whose complete region is supported, retains explicit host fallback at island admission, and
expands by closing gaps. Once admitted, an island executes wholly in Nitro or fails; runtime decomposition is not a
rollout mechanism.

New cross-fragment physical filters remain default-off until an activating macro inventory and adaptive breadth board
show equivalent plans and input, bounded memory, transport stability, and no material regression. A diagnostic opt-in
must be explicit in benchmark evidence.

## 20. Changing this specification

A change to concepts, contracts, boundaries, or deliberately unsupported behavior updates this document and adds a
decision record in the same Nitro change. The record explains the forces, consequences, and alternatives; this file
states only the resulting current contract.

Accepted records are immutable. A correction or reversal gets a new record that amends or supersedes the old one, and
both ends plus the index are updated together.

Implementation detail that does not alter a contract needs no spec edit. A performance experiment becomes a decision
only when it changes an admitted strategy or default; its evidence and rejected alternative then belong in a record.
