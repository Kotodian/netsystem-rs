---
name: rust-conversion-design
description: Choose and review Rust From, Into, TryFrom, and TryInto implementations for domain conversions, API boundaries, and typed failure paths.
---

# Rust Conversion Design

Use this skill when a Rust API converts one domain value into another, when a
helper named `from_*`/`into_*` is being proposed, or when a caller is choosing
between `value.into()` and an explicit target conversion.

## Decide What Kind of Relationship Exists

First classify the operation rather than choosing a trait from its spelling:

- Use `From<Source> for Target` when every valid `Source` has one obvious,
  infallible, value-preserving `Target`. Implement `From`; callers receive the
  blanket `Into` implementation automatically.
- Use `TryFrom<Source> for Target` when conversion can reject input, including
  sentinel values, invalid ranges, missing ownership, or a wrong lifecycle
  state. Its error must be a concrete, owner-local type that gives the caller
  an actionable recovery path.
- Use `AsRef`, `Borrow`, or a normal borrow when the operation only exposes an
  existing value. Do not create a conversion wrapper for observation.
- Use a domain constructor or parser when the operation applies policy,
  configuration, validation, allocation, or multiple independent inputs. A
  `from_*` constructor is appropriate only when it is genuinely a conversion
  from the named source domain, not merely a convenient alternate `new`.

`From` is not a promise that a bit pattern can be reinterpreted. It is a
semantic, total conversion. If one input is invalid, the conversion belongs in
`TryFrom`, even when the implementation is only a checked subtraction or range
test.

## Implement the Source-to-Target Contract

Write the trait on the target type:

```rust
impl TryFrom<ThreadIndex> for WorkerId {
    type Error = WorkerIndexError;

    fn try_from(index: ThreadIndex) -> Result<Self, Self::Error> {
        // Reject the Main Thread index instead of manufacturing a worker.
        index.as_data_worker().ok_or(WorkerIndexError::MainThread)
    }
}
```

Do not implement `Into` directly when `From` can express the relationship.
Do not add a custom `from_thread_index`, `to_worker`, or similarly named helper
merely to avoid implementing the standard trait. Such a helper is justified
only when it performs a domain operation that is not a type conversion.

For fallible generic code, prefer the source-side bound:

```rust
fn route<T>(value: T) -> Result<WorkerId, T::Error>
where
    T: TryInto<WorkerId>,
{
    value.try_into()
}
```

Keep conversion errors typed. Do not replace them with `String`,
`Box<dyn Error>`, a catch-all `Other`, or an error that hides which input and
constraint failed.

## Choose the Call-Site Syntax

Use `.into()` or `.try_into()` when the destination type is already fixed by a
binding, function argument, return type, or trait bound:

```rust
let wire_value: u64 = session_handle.into();
let worker: WorkerId = thread_index.try_into()?;
```

Use `Target::from(value)` or `Target::try_from(value)` when inference is
ambiguous, multiple target conversions are in scope, or the target type is an
important part of the local explanation:

```rust
let encoded = u64::from(session_handle);
let worker = WorkerId::try_from(thread_index)?;
```

`u64::from(value)` is not a different conversion and is not inherently less
idiomatic. It is the explicit spelling of the same `From` implementation. The
rule is: prefer `.into()` when the target is unambiguous; prefer the explicit
target form when it improves type clarity. Never use `.into()` to conceal a
conversion whose target or failure behavior is unclear.

## Domain and Ownership Checks

Before adding a conversion, verify:

1. The source and target are values in different domain roles, not two views of
   one existing value.
2. The target owner is the crate that defines the target type or the boundary
   that owns the translation.
3. The conversion preserves the identity facts required by the target. Do not
   silently drop worker, generation, protocol, namespace, or lifecycle facts.
4. A rejected conversion leaves the source and shared state unchanged.
5. The trait does not introduce `dyn` dispatch, a wrapper solely for naming, or
   a compatibility alias for a removed API.

For thread-bound identities, distinguish a worker slot from a runtime thread
index explicitly. A Main Thread index that has no corresponding Data Worker is
a fallible conversion and must not be accepted by `From`.

## Review Checklist

- Is this a total semantic conversion? If not, use `TryFrom`.
- Is `From` implemented on the target, with no hand-written `Into`?
- Is the error concrete, typed, and owned by the failed operation's authority?
- Does the call site use `.into()` only where the target is evident?
- Would `Target::from`/`Target::try_from` make an ambiguous or safety-critical
  conversion clearer?
- Is a custom `from_*` actually a policy-bearing constructor rather than a
  standard conversion in disguise?
- Are ownership, identity, and lifecycle facts preserved and tested?
