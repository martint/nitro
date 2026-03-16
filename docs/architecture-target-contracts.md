# Nitro Target Contracts

This document captures the intended architectural contracts for Nitro as it
evolves from the current prototype state toward a stream-first evaluation
engine.

The goal is not to describe the code exactly as it exists today. The goal is to
define the interfaces and responsibilities we want the codebase to converge on.

## Design Goals

- Keep the `Operator` abstraction as the batch execution backbone.
- Make the evaluator the single mechanism for scalar expression computation.
- Treat `VALUES`, `NULLS`, and `ERRORS` as first-class streams.
- Keep vectors focused on physical encoding, not semantic concerns like
  nullability.
- Preserve compact encodings when possible instead of eagerly flattening.
- Allow downstream operators to request only the streams they actually need.
- Preserve mask-driven, additive evaluation so partial work can be reused across
  columns and across stream requests within a batch.
- Keep the operator/runtime boundary suitable for realistic columnar sources
  such as Parquet, not just synthetic generators and in-memory tables.

For real columnar sources such as Parquet, scan operators should prefer
column-at-a-time, type-specific decode loops that write directly into Nitro
vectors. They should avoid row materialization and per-row type dispatch in the
hot path, since those patterns work against JVM optimization and Nitro's
vectorized execution model.

When the source format already exposes a compact column encoding such as a
dictionary-backed page, scan operators should preserve that encoding into
Nitro vectors when practical instead of eagerly flattening it away.

## Architectural Principles

### Operators orchestrate batches

Operators are responsible for:

- pulling batches from upstream operators
- propagating masks downstream and upstream
- exposing logical outputs for the current batch
- deciding when work is blocking vs. streaming

Operators are not responsible for implementing their own scalar expression
evaluation engines.

### The evaluator owns scalar semantics

The evaluator is responsible for:

- evaluating expression DAGs lazily
- memoizing only the results that benefit from reuse within a batch
- reusing intermediate results across multiple projected outputs
- computing only the positions requested by the current mask
- exposing all streams produced by an expression

Any operator that needs scalar computation over rows should delegate to the
evaluator rather than embedding its own expression scheduler.

### Streams are semantic outputs

For any logical expression or logical column, Nitro should model separate
streams such as:

- `VALUES`
- `NULLS`
- `ERRORS`

Additional streams may be introduced later if needed, but the key idea is that
nullability and error state are not hidden inside value vectors.

When a logical output does not expose a `NULLS` stream, that should mean "all
rows are non-null" for that batch rather than "nullability is unknown."

When a logical output does not expose an `ERRORS` stream, that should mean "no
rows failed" for that batch rather than "error state is unknown."

In both cases, absence should be interpreted as an all-false boolean stream
over the relevant row domain:

- absent `NULLS` means every row is "not null"
- absent `ERRORS` means every row is "not failed"

This keeps stream absence a valid fast path instead of forcing operators to
synthesize explicit all-false vectors eagerly, while still giving the evaluator
and operator boundary a precise semantic contract when those streams are
requested explicitly.

### Vectors are physical encodings

Vectors represent a single stream in some physical layout.

Examples:

- flat vectors for primitive types
- flat variable-width vectors such as `BinaryVector`
- flat nested vectors such as `ArrayVector`, `StructVector`, and `MapVector`
- `ConstantVector`
- `RleVector`
- future `DictionaryVector`

Nitro should provide builtin flat vectors for common physical types such as
`I64`, `I32`, `F64`, boolean, and binary/string payloads, while still allowing
custom vector implementations outside a closed builtin type list.

That should extend to additional physical types over time, including
variable-width families such as string and binary data. The vector contract
should therefore stay open to layouts whose payload is not a single primitive
array, for example offset-plus-bytes representations or other custom storage
schemes appropriate for variable-width values.

The current runtime already has a first builtin variable-width family for
binary/string payloads. That should be treated as the beginning of a broader
variable-width design space rather than a closed final layout.

The runtime now also has an initial nested-value family in `ArrayVector`,
`StructVector`, and `MapVector`, currently exercised by repeated `INT64`
Parquet input, top-level struct Parquet input, top-level map Parquet input,
field extraction, and simple primitives such as `cardinality` and
`map_contains_key_utf8`. That should be treated as the beginning of a broader
nested design space rather than a final layout for arrays, maps, or structs.

Nested vectors may themselves own child stream bundles. For example, an
`ArrayVector` may carry child `VALUES` plus child `NULLS` for nullable array
elements even when the parent array positions are all non-null, and a
`StructVector` may carry one `Streams` bundle per field with each field's own
`VALUES` and optional `NULLS` even when the parent struct stream itself also
has a separate `NULLS` stream. Field extraction should combine parent-struct
nulls with child-field nulls when producing a child reference, while the
container vector keeps those levels distinct internally. A `MapVector` may
likewise carry separate key and value stream bundles, while the parent map
stream may independently carry `NULLS` for absent maps and the value bundle may
carry its own `NULLS` for absent values within present maps. That child-stream
model should compose with the same explicit-stream contracts used for top-level
values instead of inventing a separate nullability mechanism for nested data.
Trait-aware dispatch should compose with nested vectors too. For example, map
key lookups over UTF-8 string keys may use the same UTF-8 and ASCII traits used
by top-level binary/string primitives, without requiring a different logical
type system for nested keys.

Vectors may also carry trait metadata that is narrower than their physical
family. For example, a `BinaryVector` may be known to represent UTF-8 strings,
and some string vectors may be known to contain only ASCII characters. Those
traits should remain optional and additive: they refine dispatch choices
without redefining the underlying vector family.

Vectors should not be forced to embed nullability. A null stream is just
another vector, typically a boolean-typed one.

The runtime vector contract should stay focused on physical encodings and
typed access patterns. Hot-path execution should not depend on generic boxed
per-element access such as `Object valueAt(int position)`. Callers should
branch on concrete vector shape outside the hot loop and then use the
type-specific arrays, runs, or ids exposed by that concrete encoding.

The physical encoding of a given logical stream is a runtime property of each
produced batch, not a fixed property of the logical column or expression.

For example, the same logical output stream may appear as:

- `RleVector` in one batch
- `DictionaryVector` in the next batch
- a flat vector in a later batch

This may happen at any point in the pipeline depending on the input data and
the behavior of upstream operators.

As a result:

- plans should express required capabilities and allowed behaviors, not fixed
  vector representations
- primitive functions and framework callbacks should cooperate on representation
  adaptation without forcing the planner to predict every transition in advance
- materialization decisions may be made per batch based on the execution path
  taken for that batch

### Allocation and ownership are explicit runtime concerns

Nitro should target a steady state where execution is almost allocation-free.
That requires explicit rules for reuse, mutation, and ownership transfer rather
than relying on convention.

The allocator is therefore not just a statistics collector. It is the single
mediator for:

- vector allocation
- mask allocation
- scratch buffer reuse
- capacity growth
- recycling
- ownership handoff between operators

No operator or function should allocate execution buffers directly with `new`
once the runtime path is mature. All execution buffers should come from the
allocator or a pool owned by the allocator.

This rule applies to copies and materialization as well as fresh allocation.
Vector interfaces should not expose convenience methods that clone or
materialize new execution buffers behind the allocator's back. Any such copy
must flow through allocator-mediated routines so pooling, accounting, and
ownership transfer remain correct.

### Ownership modes

Execution buffers, including vectors and masks, should follow an ownership
model. Conceptually, a buffer is in one of these modes:

- `owned`: exactly one component may mutate and later recycle it
- `borrowed`: the buffer may be read but must not be mutated or retained past
  the current batch contract
- `transferred`: ownership has moved downstream; the previous owner must no
  longer mutate or recycle it

The exact API can vary, but the semantics should stay consistent.

### Reuse rules

- A component may mutate only buffers it owns.
- Borrowed buffers are read-only from the borrower's point of view even if the
  underlying object is physically mutable.
- Reusing an existing output buffer is preferred over allocating a new one.
- Capacity growth should happen through allocator-mediated grow or replace
  operations.
- Recycling should return the buffer to allocator-managed pools rather than
  letting it become ordinary garbage.
- Once a buffer has been transferred, it must immediately become ineligible for
  scratch reuse by the previous owner, even if that owner resets its local
  state right afterward.
- Operators may keep private reusable scratch buffers when ownership never
  leaves the operator.
- Operators may transfer ownership of buffers downstream when doing so avoids a
  copy and the upstream operator no longer needs the buffer.

## Target Allocator Contract

The current allocator already tracks memory by context. The target allocator
should evolve into a resource manager for execution buffers.

### Responsibilities

The allocator should:

- allocate vectors and masks
- grow or replace existing buffers
- recycle buffers into per-type pools
- track allocations and peak usage by context
- support operator-local scratch reuse
- support ownership transfer and recycling

Ownership transfer should be object-based rather than context-name-based. If a
buffer was allocated under one context and later transferred by another
component, the allocator must detach it from the context that actually owns it
before any pool can reuse it.

### Conceptual shape

```java
interface Allocator
{
    <T extends Buffer> T allocate(Context context, BufferFactory<T> factory, int capacity);

    <T extends Buffer> T grow(Context context, T existing, int requiredCapacity);

    void recycle(Context context, Buffer buffer);

    <T extends Buffer> T transfer(T buffer);
}
```

This is illustrative only. The key architectural point is that all execution
buffers flow through a single mediator.

### Contexts

Allocation contexts should continue to exist because they are useful both for
profiling and for understanding where reuse is failing.

Longer term, hierarchical contexts are desirable so a pipeline can distinguish:

- operator-owned reusable buffers
- evaluator-owned reusable buffers
- one-off fallback allocations
- memory retained due to ownership transfer

## Target Mask Contract

Masks are part of the execution hot path and should be treated like pooled
execution buffers, not disposable values.

### Principles

- Masks should be reusable across batches where possible.
- Derived masks should prefer writing into owned output buffers rather than
  allocating fresh arrays for every set operation.
- Mask operations should continue to support sparse and dense-friendly
  execution paths.
- Boolean streams should remain convertible into masks without forcing
  unnecessary copies.
- Full-batch selection should be representable without materializing every row
  position eagerly.

### Intended shape

Nitro should keep the public execution concept named `Mask`, but the runtime
implementation should behave like a reusable selection buffer rather than a
pure value object.

Conceptually:

```java
final class Mask
{
    int size();

    int selectedCount();

    boolean all();

    boolean none();

    int position(int index);

    int maxPosition();

    boolean contains(int position);

    void selectAll(int size);

    void clear(int size);

    void copyFrom(Mask other);

    void intersectInPlace(Mask other);

    void differenceInPlace(Mask other);
}
```

This is illustrative, but the important design points are:

- `size` is the row-domain width for the batch
- `selectedCount` is the number of active rows
- `all()` means "all rows in `[0, size)` are selected," not "a dense positions
  array happens to contain every index"
- sparse positions should be stored in reusable capacity-backed storage when the
  mask is not `all()`
- iteration over selected rows should stay efficient in both `all()` and sparse
  modes

### Internal representation

The recommended initial representation is:

- `size`
- `selectedCount`
- `allSelected`
- `int[] positions` for sparse selection

with these invariants:

- `0 <= selectedCount <= size`
- if `allSelected`, then `selectedCount == size`
- if `allSelected`, any sparse positions storage is ignored
- if not `allSelected`, sparse positions are sorted and strictly increasing

This keeps the implementation simple while still delivering the main benefits:

- zero-allocation full-batch masks
- explicit row-domain size
- reusable sparse storage
- in-place narrowing on owned masks

### Execution behavior

The execution hot paths should prefer:

- `all()` fast paths for full-batch sequential loops
- direct iteration over selected positions for sparse masks

Membership checks such as `contains(position)` should remain available, but they
should be treated as a slower fallback rather than the primary execution mode.

### Mutation model

Owned masks should support in-place operations such as:

- `selectAll(size)`
- `clear(size)`
- `copyFrom(other)`
- `intersectInPlace(other)`
- `differenceInPlace(other)`

This lets operators and the evaluator reuse mask storage across batches instead
of allocating fresh mask objects for every refinement step.

### Ownership and handoff

- A mask returned from `next()` is borrowed by default for the lifetime of the
  current batch.
- An operator may keep and reuse its owned masks internally across batches.
- Ownership transfer of a mask should be explicit if downstream is expected to
  retain or mutate it.
- A derived mask may be evaluator-owned or operator-owned while it is still
  scratch state, but once a batch publishes that mask through `next()`, the
  mask becomes batch-owned and must no longer be eligible for upstream scratch
  reuse.
- Mask-producing operations such as union, difference, complement, and boolean
  filtering should eventually support writing into owned output masks.

### Size compatibility

Mask combination operations such as intersection and difference should operate
within one row domain. In practice, that means masks involved in an in-place
operation should agree on `size()`.

## Target IR Contract

Nitro should use one IR language with one semantic model, but it may operate on
that IR at different normalization levels.

### Core idea

The IR should be stream-first and producer-oriented:

- a variable denotes a producer
- streams are addressed explicitly from that producer
- masks are explicit expressions in the internal representation
- lowering rewrites convenient forms into a normalized subset of the same
  language rather than translating into a different IR

### Core entities

Conceptually, the IR revolves around:

- producers
- stream references
- mask expressions
- operations
- assignments

Conceptually:

```java
sealed interface Producer
{
}

record Variable(int id) implements Producer
{
}

record Input(int index) implements Producer
{
}

record StreamReference(Producer producer, Stream stream)
{
}

sealed interface MaskExpression
{
}

record AllMask() implements MaskExpression
{
}

record FromStreamMask(StreamReference source) implements MaskExpression
{
}

record NotMask(MaskExpression source) implements MaskExpression
{
}

record AndMask(List<MaskExpression> terms) implements MaskExpression
{
}

record OrMask(List<MaskExpression> terms) implements MaskExpression
{
}

record Assignment(Variable output, Operation operation, MaskExpression mask, Type type)
{
}
```

This is illustrative. The important properties are:

- variables name producers, not single vectors
- stream access is explicit
- masks are explicit in the internal model
- assignments define producer semantics under a mask

### General versus normalized form

The IR language should support:

- a general form that is convenient to construct and read
- a normalized form that is explicit enough for direct execution

These are not two different IRs. They are two forms of the same language.

The general form may contain:

- special forms such as `if`, `coalesce`, `case`, `try`, `and`, and `or`
- compact producer assignments with implicit companion-stream behavior
- textual sugar for masks and boolean-to-mask conversion

`AndMask` and `OrMask` should be n-ary forms. General IR may still contain
nested boolean mask trees, but normalization should flatten chains of the same
kind into one `AndMask` or `OrMask` term list. That keeps reorderable boolean
structure visible in the IR without carrying separate binary and n-ary node
types.

Mask-consuming operators should consume `MaskExpression` directly rather than
requiring a projected boolean `VALUES` reference first. If a filter, branch
condition, or other row-selection site semantically needs a mask, plans should
be able to pass a mask expression all the way to that consumer without
materializing an intermediate boolean vector solely to turn it back into a
mask.

When a mask-only consumer is given a boolean `VALUES` reference for
convenience, planning or normalization may derive a `MaskExpression` from the
referenced producer graph for obvious boolean forms such as `and`, `or`, and
`not`. That conversion should happen before execution reaches the hot loop.

The planning model should also have an explicit place to record those derived
mask plans for mask-only consumers. That lets operators and evaluators consult
precomputed `MaskExpression` metadata instead of rediscovering the same boolean
producer structure ad hoc at each consuming site.

That metadata should also influence ordinary stream planning. If a boolean
`VALUES` reference exists only to support mask consumers and is not itself a
projected output, its stream plan should be downgraded to scratch-style use
instead of being memoized or materialized like an ordinary value stream.

That downgrade should apply to the whole produced stream bundle for that
boolean producer, not just the `VALUES` reference. If `NULLS` or `ERRORS`
would otherwise be memoized only because they arrive alongside a mask-only
boolean value stream, they should also stay scratch-oriented unless some stream
from that bundle is explicitly projected.

The evaluator should consult that distinction at the producer-bundle level when
deciding whether to memoize. A scratch-oriented mask-only producer should not
even be treated as a memoization candidate when its stream bundle is produced.

Normalization should also apply that same derivation within mask positions
inside the IR itself, such as `Merge` conditions or assignment masks. If a
mask position contains a `ReferenceMask` over a boolean-producing reference,
normalization should prefer replacing it with the derived mask tree so later
planning and execution retain explicit mask structure.

The evaluator may still perform the same derivation as a fallback when a plan
reaches execution with an unresolved boolean `ReferenceMask`, but that should
be treated as a recovery path rather than the preferred steady state.

The normalized form should require:

- explicit stream references
- explicit mask expressions
- explicit control-flow structure in the IR rather than hidden inside special
  forms
- only primitive execution calls plus explicit structural operations such as
  copy and merge-like overlay

### Normalization framework

Normalization should be implemented as a rule-driven rewrite framework rather
than as one hardcoded normalizer method per special form.

In practice, Nitro should have:

- a normalizer that coordinates normalization over a plan
- a normalization context that owns emitted assignments and fresh-variable
  allocation
- a registry-ordered list of normalization rules
- one rule per special form or rewrite family

That keeps the normalization pass extensible. Adding support for a new special
form should usually mean registering a new rule instead of editing a central
switch statement.

The built-in rules for forms such as `if` and `coalesce` should follow the same
framework as any future rewrites. The core normalizer should be the driver, not
the place where every transformation is hardcoded.

### Normalized operation set

The normalized executable subset should stay intentionally small.

The evaluator should only need to execute a handful of operation kinds:

- `Literal`
- `Call`
- `Copy`
- `Merge`

Their intended roles are:

- `Literal`: produce structural constant or null-like results
- `Call`: invoke a primitive execution function with explicit stream arguments
  and an explicit mask
- `Copy`: perform a masked identity write from one stream into an output
- `Merge`: represent semantic overlay of partial results under explicit masks

Everything else should be lowered into combinations of these operations plus
explicit mask expressions and stream references.

### General-form operations

The general form may contain richer constructs for readability and planning,
including:

- `if`
- `coalesce`
- `case`
- `try`
- short-circuit `and`
- short-circuit `or`

These are part of the same IR language, but they are not part of the normalized
execution subset. They must be lowered before direct evaluation.

### Why keep `Merge`

Even if physical execution usually prefers direct masked writes into a final
output buffer, `Merge` should remain part of the normalized IR because it makes
overlay semantics explicit and gives normalization and validation a concrete
semantic target.

### Normalization

The normalization pass should stay within the same IR language.

Its job is to:

- eliminate control-flow special forms from executable regions
- rewrite implicit mask usage into explicit mask expressions
- introduce explicit stream-level producers when needed for execution,
  memoization, or planning
- make null/error handling explicit where required by the execution model
- ensure mask well-formedness for the resulting fragment

### Mask well-formedness

The internal IR should support validation of mask relationships.

At minimum, the architecture should define and enforce these ideas:

- an operation may only read positions that are guaranteed to be available under
  its evaluation mask
- branch-local writes must be restricted to the branch mask
- merge-like overlays must be validated against the enclosing evaluation mask
- any rewrite that relies on output-buffer preservation must preserve the domain
  of definition of the original expression

The exact validation algorithm is open, but the normalized form should be
checkable rather than relying on informal correctness arguments.

### Boolean streams and masks

Boolean streams and masks are closely related but should not be conflated in the
internal model.

The recommended direction is:

- keep masks explicit in the Java IR model
- allow compact textual sugar that desugars boolean streams into mask
  expressions where appropriate
- keep mask expressions as masks during evaluation for as long as possible
- materialize boolean vectors only when some downstream consumer truly needs a
  boolean stream representation

This gives the implementation precise internal semantics without making every
human-authored IR example excessively verbose.

When evaluating composite mask expressions:

- `AND` should evaluate the right side only on rows kept by the left side
- `OR` should evaluate the right side only on rows rejected by the left side

This keeps mask refinement local to the remaining active rows instead of
repeatedly materializing wider intermediate boolean vectors.

`ReferenceMask` should mean more than "read this boolean vector and treat true
as selected." When a boolean reference is used as a mask, evaluation should
consider the sibling streams of the same producer as well:

- `VALUES` determines which rows are true
- `NULLS` marks rows whose truth value is unknown
- `ERRORS` marks rows with row-local failure

This makes mask evaluation row-local null/error-aware rather than raw
boolean-only and aligns mask semantics with the rest of the stream model.

## Target Operator Contract

The current `Operator` API should remain the batch protocol, but its meaning
should become stream-aware.

### Responsibilities

An operator:

- exposes a fixed logical output schema
- returns a batch object for the current batch via `next()`
- accepts narrower masks via `constrain()`
- provides access to the streams for each logical output through the batch
- participates in explicit borrow and transfer semantics for masks and outputs

### Intended shape

The current `column(int)` method is too value-centric for the long-term design.
The target contract should expose a batch-scoped object that provides access to
logical outputs and their streams.

Conceptually:

```java
interface Operator extends AutoCloseable
{
    int outputCount();

    boolean hasNext();

    Batch next();

    void constrain(Mask mask);

    void close();
}

interface Batch
{
    Mask borrowMask();

    Mask takeMask();

    Output output(int index);
}

interface Output
{
    Vector borrow(Stream stream);

    Vector take(Stream stream);
}
```

This is illustrative rather than prescriptive. Equivalent designs are fine if
they preserve the same semantics.

What matters is that callers can request `NULLS` or `ERRORS` without pretending
they are ordinary value columns, and that ownership is explicit at the batch
boundary.

The contract should also permit operators to preserve compact encodings such as
constant, RLE, and dictionary when downstream consumers can operate on them.

Even when a batch has zero active rows, the batch object should still expose a
valid shape: the mask may be empty, but the outputs themselves must be
well-formed and borrowable for the lifetime of that batch. Empty batches should
not use `null` output placeholders.

### Semantics

- `Batch` represents one current batch of output from the operator.
- Borrowed masks and streams are valid only for that batch.
- Previously borrowed masks and streams are invalidated after the next call to
  `next()`.
- `constrain(mask)` narrows the rows of interest for the current batch only.
- Operators may use `constrain(mask)` to avoid materializing streams that are no
  longer needed.
- `takeMask()` and `take(stream)` transfer ownership to the caller.
- After ownership transfer, the batch no longer exposes the transferred buffer
  for that batch.

### Ownership-aware extensions

The batch object is the preferred place to carry ownership semantics and any
future batch-local metadata such as encoding information, schema details, or
profiling counters.

## Target Stream Reference Contract

Nitro needs one shared notion of "which stream are you asking for?" that can be
used consistently by:

- the evaluator
- `ProjectOperator`
- aggregations
- future planner or IR layers

### Reference

The existing evaluator IR already points in the right direction:

- a logical producer
- a specific stream of that producer

Conceptually:

```java
record Reference(Producer producer, Stream stream) {}
```

The "producer" part may be:

- an input column
- an expression result
- an aggregate result
- eventually another logical source such as a constant or symbol

The exact representation can vary. The important part is that stream selection
is explicit and uniform.

Boolean-valued references should also be usable as masks. In other words, a
boolean stream is both data and a potential row-selection input.

### Input access

The evaluator's input abstraction should become stream-aware as well.

Instead of:

```java
interface Input
{
    Vector get(int index, Mask mask);
}
```

the target is conceptually closer to:

```java
interface InputResolver
{
    Vector resolve(Reference reference, Mask mask);
}
```

This lets the evaluator request only the specific upstream stream it needs.

In practice, some evaluation steps will need multiple sibling streams for the
same producer, for example when a `ReferenceMask` needs `VALUES`, `NULLS`, and
`ERRORS` together. The important contract is therefore:

- stream selection is explicit at the call boundary
- callers may request multiple sibling streams of the same producer
- input resolution must not assume `VALUES` is the only meaningful stream

### Result access

Expression evaluation should produce a stream bundle rather than privileging
`VALUES` as the only real output.

Conceptually:

```java
interface Streams
{
    Vector get(Stream stream);
}
```

or:

```java
record Result(Map<Stream, Vector> streams) {}
```

The exact container is less important than these rules:

- any stream may be absent if it is not semantically produced
- functions should compute only the streams requested by callers, when feasible
- results remain batch-local and mask-aware
- the carrier type of the error stream is intentionally left open; it may be
  boolean in some phases and richer in others

Runtime APIs should make that optionality explicit. Callers should be able to
ask for an optional stream with a `borrowOrNull`-style access pattern instead
of open-coding stream membership checks at each call site.

When one evaluation produces multiple sibling streams such as `VALUES`,
`NULLS`, and `ERRORS`, those streams should be treated as one produced bundle
for memoization purposes. Requesting one sibling stream should allow later
requests for the others to reuse the same batch-local work instead of
recomputing the producer independently per stream.

When a producer bundle is planned for memoization, the evaluator may widen a
single requested stream to a requested sibling-set for that producer. That lets
one function evaluation produce the full memoized bundle in one pass instead of
discovering sibling streams later and re-entering the primitive.

Projected outputs should follow the same bundle rule even when the plan does
not separately memoize them. If multiple projected outputs expose sibling
streams from the same producer, the operator should evaluate that producer once
for the batch and let those outputs share the resulting stream bundle.

### Primitive function contract

Normalized execution should rely on primitive functions whose behavior is
explicit enough for planning and runtime decisions.

Primitive functions should not hide control flow. Their semantics should be
local to the supplied arguments, mask, output buffers, and calling convention.

Primitive functions should also avoid signaling row-local failures with Java
exceptions. If a row may fail independently of other rows, that failure should
be represented in the function's `ERRORS` stream instead. This keeps error
handling compatible with mask-driven evaluation, branch suppression, and
partial-batch execution.

The architecture should keep primitive-function metadata minimal.

For now, the important semantic metadata is:

- determinism
- null behavior when lowering or validation depends on it
- error behavior when lowering or validation depends on it

Other execution details should preferably be expressed through the calling
convention and framework callbacks rather than through a large metadata surface.

### Calling convention

Primitive functions should follow a calling convention that makes these rules
explicit:

- functions execute under an explicit mask
- functions are told which output streams the caller wants from this evaluation
- functions may be given a reusable output destination
- if a reusable output is supplied, functions must preserve positions outside
  the active mask
- if no output is supplied, functions may allocate or return a read-only encoded
  result for the current batch
- functions may request framework-mediated transformations or helpers when they
  cannot operate directly on the current input representation

In practice, reusable output should be treated as a writable flat-destination
concept. Encoded-preserving paths such as RLE-preserving evaluation are
primarily valid when the function is producing a fresh result rather than
overlaying into an existing destination.

Conceptually, the calling convention should look like:

```java
interface PrimitiveFunction
{
    Streams apply(
            List<Streams> inputs,
            Mask mask,
            Set<Stream> requestedStreams,
            Streams output,
            ExecutionContext context);
}
```

This is illustrative only, but the chosen signature should make output reuse
and masked writes part of the contract rather than implicit behavior.

`requestedStreams` is a request set, not a prohibition against reusing already
available sibling streams. A caller may ask only for `ERRORS`, for example, and
the primitive should be free to skip `VALUES` work when it can. But when the
plan intends to memoize a full producer bundle, the evaluator may request both
`VALUES` and `ERRORS` together so later sibling lookups can reuse one produced
result.

That selectivity applies to produced streams, not to semantic input
availability. When a primitive consumes a referenced `VALUES` stream, the
evaluator should still provide sibling input `NULLS` and `ERRORS` streams in
the same input bundle when those streams exist for that producer, because
row-local null and error state are part of the meaning of the input values.

The same bundle-selective rule should apply to non-primitive operations such as
`Literal`, `Copy`, and `Merge`. If a downstream consumer requests only
`NULLS` or `ERRORS` from a producer chain built out of those operations, the
evaluator should preserve that intent instead of materializing `VALUES` by
default.

### Type-system independence

Scalar registration at this layer should remain independent of any frontend
type system such as SQL types.

In particular:

- scalar declarations should not require SQL type names
- function identity should be separate from frontend type binding
- builtin vectors may provide common physical carrier types, but the scalar
  layer should not assume a closed global type list

Frontend-specific typing can be layered on top later without leaking those type
names into Nitro's core execution and function-registration contracts.

### Framework callbacks

If a primitive function cannot efficiently consume a particular batch-local
representation directly, it should be able to:

- handle the adaptation internally, or
- request help from the framework through callbacks or context services

This avoids forcing the planner to predict every encoding transition in advance.

Today, Nitro's concrete implementation strategy is that primitive functions own
their encoding dispatch internally. A function is expected to choose among its
`flat/flat`, `flat/rle`, `rle/flat`, and `rle/rle` paths itself. The
architecture may later move some of that into framework-generated support, but
the current contract should reflect that function authors are responsible for
those runtime choices.

The important execution boundary is:

- dispatch on encoding outside the hot loop
- keep the selected loop body concrete and array-based inside the hot loop

Nitro should therefore prefer shared loop-shape utilities and
function-specific concrete loops over per-row abstraction layers such as
polymorphic `value(position)` dispatch. The execution contract is defined over
the active mask and logical row domain, not over exact backing-array length
equality. Reused pooled vectors may have extra physical capacity as long as
they cover every row addressed by the current mask.

### Null and error behavior

The architecture should make room for at least these distinctions:

- null-propagating versus null-producing versus null-oblivious behavior
- error-producing versus error-capturing versus error-free behavior

These distinctions matter for:

- lowering of `try`, `coalesce`, and conditional forms
- stream-specific materialization
- adaptive reordering safety
- validation of normalized fragments

### Primitive versus special

A call is primitive if:

- it has no hidden control flow
- it can execute directly under an explicit mask
- its stream behavior is explicit enough for planning
- it obeys the output-buffer preservation rules when given reusable outputs

A call is not primitive if it requires semantic lowering first, for example:

- `if`
- `coalesce`
- `case`
- `try`
- short-circuit forms whose control flow is not yet explicit in the IR

The exact boundary can evolve, but the evaluator should execute only the
primitive subset.

### Selective memoization

Memoization should not be the default for every intermediate expression.
Instead, memoization should be a property of the execution plan.

The evaluator should distinguish between retained results and ephemeral ones,
but that distinction should come from the plan rather than from hard-coded
runtime defaults.

### Target evaluation plan contract

The evaluator should execute a plan that carries explicit per-stream buffering
policy.

Conceptually:

```java
record EvaluationPlan(
        List<ProducerPlan> producers,
        List<Reference> outputs)
{
}

record ProducerPlan(
        Producer producer,
        Map<Stream, StreamPlan> streams)
{
}

record StreamPlan(
        MaterializationPolicy materialization,
        MemoizationPolicy memoization)
{
}

enum MaterializationPolicy
{
    NONE,
    SCRATCH,
    MATERIALIZE
}

enum MemoizationPolicy
{
    NONE,
    MEMOIZE
}
```

This is illustrative only. The important point is that the abstraction carries
policy, not planner diagnostics about why that policy was chosen.

The plan should describe buffering requirements and execution capabilities, but
it should not force a single fixed vector encoding for a logical stream across
all batches.

### Materialization versus memoization

The plan should distinguish two separate questions:

- should this stream be materialized into a backing buffer at all?
- if materialized, should that buffer be retained across calls within the batch?

Those are related but not identical.

Examples:

- a constant stream may be memoized without needing a mutable materialized
  buffer
- a branch output may need materialization because it is filled incrementally
  under multiple masks
- a cheap single-use stream may use scratch materialization without any
  memoization

The evaluator should therefore avoid treating "buffer exists" and "result is
memoized" as the same concept.

### Plan-level policies

The intended meaning of the policies is:

- `MaterializationPolicy.NONE`: no backing buffer is required for the general
  case; the producer may expose a structural result directly
- `MaterializationPolicy.SCRATCH`: materialize into allocator-managed scratch
  storage that may be recycled once the consumer is finished
- `MaterializationPolicy.MATERIALIZE`: produce a retained materialized stream
  suitable for repeated access or incremental fill
- `MemoizationPolicy.NONE`: do not retain computed-mask state or result buffers
  beyond the active parent evaluation
- `MemoizationPolicy.MEMOIZE`: retain computed-mask state and result identity
  within the batch

The exact enum names are open, but the separation of concerns should remain.

These policies describe storage and lifetime requirements, not a fixed physical
encoding. The actual representation for a stream may vary from batch to batch
as long as it satisfies the requirements of the current consumer path.

### Policy combinations

Not every combination is equally useful. The expected meanings are:

| Materialization | Memoization | Typical meaning | Typical use |
|---|---|---|---|
| `NONE` | `NONE` | No retained backing buffer and no retained computed state | literals, trivial structural forwarding, cheap structural results |
| `NONE` | `MEMOIZE` | Retained structural result without ordinary materialized storage | constants and other immutable structural results worth retaining by identity |
| `SCRATCH` | `NONE` | Temporary buffer recycled after the active parent evaluation | single-use arithmetic, temporary decoded fallback, one-shot branch-local intermediates |
| `SCRATCH` | `MEMOIZE` | Generally undesirable; scratch lifetime conflicts with retained memoization | should usually be rewritten as `MATERIALIZE` plus `MEMOIZE` |
| `MATERIALIZE` | `NONE` | Stable physical buffer without retained memoization state | boundary cases where a stream must exist physically but is produced once and not incrementally revisited |
| `MATERIALIZE` | `MEMOIZE` | Retained physical buffer with retained computed-mask state | projected outputs, shared subexpressions, conditional branch outputs, expensive lazily loaded inputs |

The recommended guidance is:

- treat `SCRATCH` plus `MEMOIZE` as invalid or at least strongly discouraged
- use `NONE` plus `MEMOIZE` mainly for structural immutable results
- treat `MATERIALIZE` plus `MEMOIZE` as the standard full memoization case

### Structural forwarding versus writable destinations

A structural result is a valid complete result for a batch, but whether it can
remain structural depends on how that batch-local result is used.

For a given batch, a result may be forwarded unchanged when all of these are
true:

- it already fully represents the requested stream for that batch
- no later step writes into that same logical result
- downstream consumers can operate on the current encoding directly
- ownership can be borrowed or transferred without requiring mutation

For a given batch, a result must become a writable destination when any of these
are true:

- it will be filled incrementally over multiple masks
- it is the target of branch overlay or semantic merge behavior
- a downstream consumer requires mutable owned storage
- the current encoding does not support the required update pattern

The key rule is:

- sources may remain structural
- destinations must be writable

This is a batch-local execution decision, not a fixed property of the logical
stream across all batches.

### Memoization points

The execution plan should identify explicit buffering or materialization points.
Typical memoization points are:

- common subexpressions
- outputs of conditional branches that may be filled over multiple masks
- projected outputs requested independently by downstream operators
- leaves whose lazy loading cost justifies retaining the fetched stream

Everything else should flow through scratch buffers with allocator-managed
reuse.

These decisions should be made per producer and, ideally, per stream rather
than as a blanket rule for the entire expression node.

### Scratch evaluation

Non-memoized evaluation still needs reusable storage. The evaluator should use
allocator-managed scratch buffers for ephemeral intermediates.

Those scratch buffers should:

- be reused aggressively within a batch
- obey the same ownership rules as other execution buffers
- be recyclable as soon as the parent expression no longer needs them
- preserve positions outside the active mask only when required by the calling
  convention

This is how the evaluator can reduce memory retention without giving up
allocation reuse.

Because mask refinement is itself part of the execution hot path, the allocator
should also support pooled mask-mask operations in addition to mask-vector
operations. In particular, reusable intersection, difference, and union of
owned masks should be treated as first-class allocator services rather than as
incidental conveniences.

### ProjectOperator in the design

`ProjectOperator` is a thin adapter around the evaluator:

- it owns an evaluator for the current batch
- it maps projected outputs to `Reference`s
- each output may refer to `VALUES`, `NULLS`, or `ERRORS`
- requesting one output can reuse partial work from prior output
  requests in the same batch

### Output buffer rule

Any function that accepts a reusable output must preserve positions outside the
evaluation mask. This is the rule that makes in-place additive evaluation and
merge-style rewrites semantically valid.

### Encoding-aware dispatch

Operations should be vector-type aware. They should inspect physical encodings
and pick specialized paths when possible.

Examples:

- constant plus constant should remain constant
- RLE plus RLE should prefer RLE-preserving evaluation when valid
- dictionary plus dictionary should exploit shared dictionaries or shared
  indirection when possible
- flattening should be the fallback, not the default

At a minimum, builtin numeric and boolean operations should handle:

- flat plus flat
- flat plus RLE
- flat plus dictionary
- RLE plus flat
- dictionary plus flat
- RLE plus RLE
- dictionary plus dictionary

Support for additional mixed combinations such as dictionary with RLE may grow
incrementally, but flat, RLE, and dictionary should all fit within the same
execution rule: choose the loop shape outside the hot loop, then execute the
loop over concrete arrays and cursors inside it.

Physical encoding is not the only dimension that may matter for dispatch.
Logical or physical traits associated with a vector family may also justify
specialized execution paths, for example a string vector whose values are known
to be ASCII-only. The dispatch model should therefore remain open to
trait-directed specialization as long as the same hot-loop rule still holds:
select the specialized path outside the loop, then execute over concrete typed
state inside it.

Source operators may attach traits when the input format makes them cheap or
obvious to derive. For example, a Parquet scan over string-typed columns may
mark a binary/string vector as UTF-8, and may further mark it as ASCII-only
when that property can be established while decoding.

That same rule applies to access patterns within a single encoding. Execution
helpers may abstract over loop shape, run tracking, and mask traversal, but
they should still hand concrete typed arrays or cursor state into the actual
hot loop rather than reintroducing boxed or polymorphic per-row access.

This applies equally to scalar functions, predicate functions, and any future
merge-like evaluator operations.

In practice, shared support should focus on loop-shape utilities such as:

- run merging
- RLE position tracking
- mask-aware iteration
- full-batch versus sparse masked loop selection

Those utilities are the right place to share behavior without pushing
megamorphic per-row dispatch into the hot path.

When the evaluation mask covers the whole batch, functions should also prefer
specialized full-batch loop shapes over sparse masked loops. `mask.all()` is
not just a minor optimization hint; it is an important execution mode that
often enables simpler sequential loops, better RLE preservation, and less
branching in the hot path.

### Concrete execution decisions

The following execution choices should guide the runtime design:

- `merge` is primarily a semantic IR construct. It does not imply that execution
  must materialize a temporary result and then physically merge it into another
  vector.
- The preferred physical strategy for `merge`-like behavior is direct masked
  writes into the final output buffer, using explicit masks and output-buffer
  preservation rules.
- The evaluator may still support `Merge` as a fallback executable primitive so
  that normalized plans remain directly executable even when merge-like behavior
  was not compiled into a destination-writing strategy.
- Writable result vectors must support sparse and out-of-order population. A
  result may be filled in multiple masked passes and untouched positions may
  contain stale data.
- `Mask` remains the semantic row-selection mechanism. Encodings such as
  dictionary are physical optimizations and do not replace masks in the semantic
  model.
- `DictionaryVector` may be used as a physical representation of selected or
  repeated rows when that avoids copying, but planning and correctness should
  still be expressed in terms of masks.
- Runtime adaptive reordering is allowed for deterministic boolean mask forms
  such as `AndMask` and `OrMask`, provided that short-circuit, null, and error
  semantics remain unchanged.
- Adaptive reordering should be driven by observed cost and selectivity, but it
  should be scoped narrowly to forms whose semantics are order-insensitive under
  the established rules.
- This optimization belongs to boolean `MaskExpression` chains, not to
  arbitrary expression trees.
- Primitive execution functions should advertise enough behavior for the runtime
  to know whether they support direct masked writes, preserve encodings, or
  require flattening as a fallback.

### Adaptive reordering safety

Adaptive reordering should be allowed only for n-ary boolean mask forms whose
terms are:

- deterministic
- side-effect free
- row-local in their null and error behavior

Normalization should still flatten nested chains of the same boolean operator
so the evaluator can work from one explicit term list instead of recursively
rediscovering structure at runtime.

For reorderable `AND` and `OR`, Nitro should adopt order-insensitive row
semantics rather than strict left-to-right evaluation semantics.

For `AND`, per row:

- `FALSE` is decisive and suppresses later `NULL` and `ERROR`
- if no term is `FALSE`, then `ERROR` dominates `NULL`
- if no term is `FALSE` or `ERROR`, then `NULL` dominates `TRUE`

For `OR`, per row:

- `TRUE` is decisive and suppresses later `NULL` and `ERROR`
- if no term is `TRUE`, then `ERROR` dominates `NULL`
- if no term is `TRUE` or `ERROR`, then `NULL` dominates `FALSE`

This means errors observed during evaluation may be provisional until the row is
known not to be decided by a suppressing `FALSE` or `TRUE`.

Operationally, a reorderable evaluator should maintain row-local state for at
least:

- rows already decided `TRUE` or `FALSE`
- rows with provisional `NULL`
- rows with provisional `ERROR`
- rows still active for later terms

Later decisive `FALSE` for `AND` or `TRUE` for `OR` may suppress those
provisional null/error states for the affected rows.

If multiple unsuppressed terms produce errors for the same row, the visible
error should be chosen deterministically by source order rather than runtime
evaluation order.

Adaptive ordering should therefore be based on observed cost and decisiveness,
for example:

- for `AND`, prefer cheaper and more false-selective terms first
- for `OR`, prefer cheaper and more true-selective terms first

Any boolean form that does not satisfy the reorderability contract should retain
its original evaluation order after normalization.

## Target Accumulator Contract

Accumulators should operate on semantic stream references, not on nullable value
vectors.

### Responsibilities

An accumulator:

- defines its input dependencies in terms of stream references
- owns aggregation state for one aggregate function
- updates that state for a mask of rows
- exposes aggregate result streams explicitly

### Input contract

Today, accumulators consume bare column indexes through `ColumnAccessor`.
The target contract should allow them to resolve the exact streams they need.

Conceptually:

```java
interface StreamAccessor
{
    Vector resolve(Reference reference);
}
```

Examples:

- `sum(x)` reads `x/VALUES` and `x/NULLS`
- `count(x)` reads `x/NULLS`
- `count(*)` reads nothing
- `first(x)` may read `x/VALUES`, `x/NULLS`, and possibly `x/ERRORS`

This keeps null handling explicit and local to aggregate semantics.

### State contract

Aggregate state should not be modeled as a single nullable vector by default.
Some aggregates naturally have multiple state streams.

Conceptually:

```java
interface Accumulator
{
    State allocate(int size);

    void initialize(State state, int offset, int length);

    void accumulate(State state, int group, Mask mask, StreamAccessor inputs);

    void accumulate(State state, Vector groups, Mask mask, StreamAccessor inputs);

    Streams result(int maxGroup, State state, Streams output);
}
```

This is again illustrative. The key properties are:

- state may have multiple streams
- result may have multiple streams
- group and non-group accumulation share the same semantic model
- accumulators should preserve input encodings when possible and flatten only as
  a fallback

Aggregate state should follow the same stream model as operator outputs: a
state slot may have `VALUES`, `NULLS`, and other streams as needed, rather than
reintroducing nullable wrapper vectors internally.

### State versus output

Aggregate state and aggregate output should be treated as distinct layers:

- aggregate state is internal, mutable, and operator-owned
- aggregate output is batch-local and should use the same stream-oriented batch
  model as any other operator output

In other words, accumulators own long-lived state across input batches, but they
produce ordinary batch-scoped output streams when results are emitted.

### Ownership

Aggregate state should remain internal to the aggregation operator and should
not participate in downstream borrow or take semantics.

Aggregate output batches, on the other hand, should follow the same ownership
rules as other operator outputs:

- outputs are borrowed by default
- outputs may be transferred via the batch boundary
- encodings may vary from batch to batch

### Planning

Aggregate state planning and scalar stream planning are related but not
identical.

The recommended split is:

- scalar `StreamPlan` governs expression intermediates and operator-visible
  streams
- aggregate-state planning governs internal accumulator state layout and growth

Aggregate output planning should align with the ordinary batch/output model even
if aggregate-state planning uses different internal rules.

### Result batching

Grouped aggregation may produce more groups than are convenient to expose in a
single output batch.

The architecture should therefore allow aggregation operators to:

- retain internal state across many input batches
- produce result batches incrementally over the accumulated group space
- expose each result batch through the same batch object contract used by other
  operators

The default chunking policy should be contiguous group-id ranges over the
accumulated state space. Grouped-result emission should therefore remain
compatible with the general operator batching model while staying simple and
stable.

### Output contract

Aggregate outputs should be stream-addressable just like scalar expression
outputs. This avoids reintroducing vector-specific nullable wrappers at the
aggregate boundary.

## Future Considerations

The following topics were intentionally deferred rather than fixed in the core
architecture:

- Exact primitive-function signature and callback APIs. The architecture assumes
  an explicit masked-write calling convention, but the final Java signature is
  still open.
- Whether `Merge` remains available as a fallback executable primitive in the
  evaluator or is always compiled into destination planning plus masked writes.
- Additional normalized IR operation kinds beyond `Literal`, `Call`, `Copy`, and
  `Merge`, if implementation pressure later justifies them.
- Function-level null and error behavior enums. For now, null/error handling may
  be made explicit in the IR instead of being expressed as metadata on
  primitive functions.
- Using OpenJDK Code Reflection as a future mechanism for deriving or
  specializing vectorized adapters from scalar Java function definitions once
  that technology is mature enough for practical use.
- Whether masks eventually need more internal representations beyond `all` and
  sparse positions, such as range or bitset forms, if profiling shows those are
  worthwhile.
- More formal separation between IR mask semantics and runtime mask-buffer
  storage if implementation experience suggests the current conceptual split is
  still too loose.
- Exact grouped-result chunking policy for aggregation output batches.
- Whether planner- or runtime-visible encoding metadata becomes necessary later,
  beyond the current calling-convention and callback approach.
- Support for additional physical data types beyond the current builtin
  primitive and binary/string families, along with the vector/storage
  conventions needed to keep those types compatible with the stream-first
  execution model.
- Broader nested data-type support beyond the current `ArrayVector`,
  `StructVector`, `MapVector`, repeated-`INT64` Parquet path, top-level struct
  Parquet path, and top-level map Parquet path, including richer child streams,
  deeper nesting, and the execution rules needed to handle nested values
  without collapsing them back into row-oriented execution.
- Broader trait-based dispatch beyond the current UTF-8 and ASCII string
  example, so specialized execution can target additional vector-family traits
  without hardcoding those assumptions into the core type system.
- Broader Parquet integration beyond the current fixed-width and binary/string
  scan paths, including richer type coverage, better encoding preservation, and
  stronger interaction with projection/filter planning on real datasets.

## Non-Goals

This document does not define:

- SQL semantics for nulls or errors
- planner architecture
- optimizer rules
- exact Java type hierarchies for every vector kind
- the final representation type of error streams
- whether stream requests are pull-only or can be predeclared

Those can be decided independently as long as the contracts above remain true.
