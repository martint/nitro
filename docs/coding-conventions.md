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
