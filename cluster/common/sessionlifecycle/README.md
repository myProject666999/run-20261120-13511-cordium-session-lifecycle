# sessionlifecycle

Explicit five-state lifecycle for sandbox sessions. The lifecycle is a state
machine with a single validated transition entry point, version-based
optimistic concurrency, injectable clocks, and timeout-driven reclamation —
it replaces the older pattern of inferring lifecycle from independent
booleans.

## States and legal transitions

| State | Can move to | Time bound |
|---|---|---|
| `Creating` | `Running`, `Destroying` | creation timeout → `Destroying` |
| `Running` | `Suspended`, `Destroying` | idle timeout → `Suspended` or `Destroying` (policy) |
| `Suspended` | `Running`, `Destroying` | max retention → `Destroying` |
| `Destroying` | `Destroyed` | none; release pipeline retried until it succeeds |
| `Destroyed` | _(terminal)_ | none |

All mutations go through `Manager` (`MarkRunning`, `Suspend`, `Resume`,
`Destroy`, `FinalizeDestroy`, `RecordActivity`), which validates each request
against the transition table. Illegal requests return a distinguishable
`*TransitionError` (use `errors.Is` with `ErrInvalidTransition` or
`ErrDestroyInProgress`); stored state is never silently rewritten.

### Idempotency

Repeating a request whose target state already holds is a no-op that does not
bump the version, so timeout-based retries are safe:

- `Suspend` on an already suspended session succeeds.
- `Destroy` on a destroying or destroyed session succeeds.
- `FinalizeDestroy` on a destroyed tombstone succeeds.

`Resume`/`Activate` on a `Running` session are **not** idempotent: they are
semantically invalid there and return `ErrInvalidTransition`.

A repeated destroy of a finalized session has two answers, selected by the
store/manager configuration:

- tombstone store (default): idempotent success, the `Destroyed` record stays;
- `PurgeOnFinalize: true`: the record is removed and a repeated destroy
  returns `ErrSessionNotFound`, matching an upstream cleaner that purges
  records itself.

## Concurrency: one compare-and-update primitive

`Store.CompareAndUpdate(id, expectedVersion, mutate)` is the only way state is
written. The Manager reads state + version, computes the target, and writes
back carrying the version; on `ErrVersionConflict` it re-reads and retries
rather than overwriting.

`RecordActivity` and idle reclamation (`ReapDue`) use the same primitive:

- if activity commits first, the version changes and the idle timer resets, so
  a reaper decision computed against the old version fails with
  `ErrVersionConflict` and is re-evaluated — a renewed session is never reaped
  against a stale decision;
- if the reaper commits first, a late activity update retries against the new
  state and is rejected cleanly instead of resurrecting the session.

## Clocks and recoverable deadlines

`Clock` is injectable (`SystemClock` for production, `FakeClock` for tests).

- In-process elapsed time uses the monotonic reading only. Wall-clock jumps
  (NTP steps) cannot trigger or suppress a timeout while the process lives.
- A `Deadline` persists the absolute wall-clock instant it is based on,
  because monotonic values are meaningless after a restart.
- On restart, `Manager.Rehydrate` rebuilds each monotonic basis from the
  persisted wall instant and marks it `Recovered`.
- A recovered deadline is due only once the local wall clock passes the
  persisted instant **plus `RecoveryGrace`**. A behind/ahead instance clock
  therefore reaps late, never early; multiple racing instances still
  serialize through the versioned compare-and-update.

## Reaper

`Reaper.ScanOnce` lists sessions in bounded batches (`BatchSize`, yielding for
`BatchPause` between them) so a mass expiry cannot overload the backend,
applies due timeout transitions, and runs the destroy pipeline for
`Destroying` sessions. `Run` loops until the context is canceled; with a
`FakeClock` it wakes immediately on clock movement, so tests never sleep for
real deadlines. After a restart the next wake-up is recomputed purely from
persisted records (`NextWakeup`) — there are no in-memory timers to lose.

### Destroy pipeline

`DestroyHooks` runs `ReleaseResources`, `CloseConnections`, `CleanupStorage`
in order. Each step must be idempotent. A failure short-circuits and leaves
the session in `Destroying` (it can never return to a live state); the whole
pipeline is retried on the next scan. A `Resume` arriving after partial
destruction is rejected with `ErrDestroyInProgress`.

## Legacy data

Records with only booleans and timestamps map onto the five states two ways:

- one-shot: `MigrateAll` / `MigrateLegacy`;
- lazy: `LazyLegacyStore` migrates a record on first access and removes the
  legacy copy.

The booleans are treated as cumulative ("the session once reached this
state"), so `created+running+suspended` is a normal trajectory mapped to its
highest-precedence state (`destroyed > suspended > running > created`). Truly
ambiguous inputs (no flag set, or a later state without the created flag)
return `MigrationAmbiguityError`; `MigrateLegacyWithPrecedence` /
`AllowAmbiguous` force the deterministic precedence mapping after an operator
has inspected the record. Deadlines cannot be reconstructed from booleans;
migrated sessions start without timers and receive them once a policy is
attached.
