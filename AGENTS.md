# AGENTS.md

This file guides coding agents working in `matrikkel-kafka-light`.

## Purpose

* Keep changes small, targeted, and repo-specific.
* Preserve broker/client behavior documented in `README.md`.
* Prefer existing patterns over introducing new abstractions.

## System Shape

* `apps/broker`: HTTP broker for publish/poll/commit.
* `libs/client`: Kotlin producer/consumer client.
* The design is Kafka-like but simplified (no partitions), with an intentionally migration-friendly API shape.

## Start Here

* `README.md`: intended external behavior, usage, and examples.
* `apps/broker/src/main/kotlin/no/kartverket/matrikkel/broker/api/`: HTTP API and endpoint definitions.
* `apps/broker/src/main/kotlin/no/kartverket/matrikkel/broker/service/records/`: record, lease, offset, persistence, and concurrency behavior.
* `apps/broker/src/main/kotlin/no/kartverket/matrikkel/broker/domain/TopicCatalog.kt`: topic configuration and policy.
* `libs/client/src/main/kotlin/no/kartverket/matrikkel/kafkaclient/`: client protocol, producer, and consumer behavior.

## Invariants

* Sequence numbers are monotonic per topic.
* Commits must fail for stale or invalid lease tokens.
* Poll/commit lease semantics must remain compatible with `README.md`.
* `423 Locked` is expected behavior for lease contention.
* Broker/client protocol compatibility must be preserved.

## Working Rules

* Read relevant tests and touched-area code before editing.
* Keep edits narrow and avoid unrelated refactors.
* Prefer existing code patterns and naming in the affected area.
* Update broker and client together when protocol or API behavior changes.
* Add or update tests for behavior changes.
* Update documentation and examples when user-visible behavior changes.
* Do not silently change HTTP status codes, payload fields, or lease semantics.
* Do not add dependencies without clear justification.

## Change Guidance

### Endpoint or Protocol Changes

* Start with `TopicRoutes.kt` and `RecordsService.kt`.
* Update `Protocol.kt` and client behavior when the wire contract changes.
* Check both broker and client tests.
* Update `README.md` if externally visible behavior changes.

### Persistence or Concurrency Changes

* Review the relevant files in `service/records/`.
* Preserve transaction and locking behavior unless the task explicitly requires changing it.
* Re-validate sequence, lease, and offset invariants.

### Topic Policy Changes

* Update `TopicCatalog.kt` and related validation paths.
* Check whether broker behavior, client assumptions, or README examples are affected.

## Verification

Run the smallest relevant verification first:

* `./gradlew :apps:broker:test`
* `./gradlew :libs:client:test`

For broader or cross-module changes:

* `./gradlew test`
* `./gradlew build`

## Documentation and Behavior
- Treat `README.md` as the intended external contract and usage documentation.
- Treat tests as the executable specification of current behavior.
- If tests, implementation, or README appear to conflict, do not silently choose one or rewrite behavior to make them agree. Preserve existing behavior unless the task clearly requires a change, and surface the discrepancy.