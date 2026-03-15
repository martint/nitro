# Nitro Coding Conventions

## Be explicit about concrete vector assumptions

If a helper or code path semantically assumes one concrete vector type, prefer a
direct cast over a fake abstraction.

For example, prefer:

```java
return ((I64Vector) vector).values();
```

over:

```java
return switch (vector) {
    case I64Vector values -> values.values();
    default -> throw new IllegalArgumentException(...);
};
```

Use polymorphism or real multi-encoding dispatch only when the code genuinely
supports multiple runtime shapes. If there is only one valid shape, keep the
assumption explicit so the code is easier to read and maintain.

When code genuinely supports multiple encodings, dispatch once outside the hot
loop and keep the chosen loop body concrete. Prefer branching into specialized
array-based loops over introducing per-row polymorphic access such as
`value(position)` calls inside the loop.

## Keep one semantic kernel per function

When a function has multiple loop shapes or encoding-specific paths, keep the
core scalar semantics in one place.

For example, if `flat/flat` and `rle/rle` paths both implement the same
arithmetic or error rule, factor that rule into one helper and have the loop
variants call it instead of duplicating the logic.

This reduces the risk that one path drifts semantically from the others while
still allowing the hot loops themselves to stay specialized and readable.
