# DQL Recovery Attempt Finalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure a failed DQL reprocessing attempt has one terminal history record and can load its payload through the actual TM DQL API.

**Architecture:** Keep one `recovery_attempts` element per `batchId + attemptId`. The start callback creates the element as `RUNNING`; terminal callbacks update that same element with array filters, while duplicate and conflict classification remains idempotent. Replace the Engine's generic collection REST lookup with a dedicated TM metadata endpoint that returns only the recovery payload snapshot.

**Tech Stack:** Java, Spring MVC, Spring Data MongoDB, MongoTemplate, JUnit 5, Mockito, Maven.

**Spec:** `doc/TAP-12615-DLQ-controlled-reprocessing-detailed-design.md`, `docs/superpowers/specs/2026-08-28-TAP-12615-D07-recovery-callback-idempotency-design.md`

## Global Constraints

- Do not write to MySQL, MongoDB business sources, or any task source/target connector.
- TM may update only DQL metadata (`dql_events`, recovery batch metadata) for this feature.
- Preserve old `recovery_attempts` data; the detail response must collapse duplicate lifecycle snapshots with terminal state preferred.
- Preserve callback idempotency for `batchId + eventId + attemptId`.

### Task 1: TM terminal attempt update

**Files:**
- Modify: `manager/tm/src/main/java/com/tapdata/tm/dql/repository/DqlEventRepository.java`
- Test: `manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlEventRepositoryTest.java`

**Interfaces:**
- `startEventIdempotent(eventId, batchId, attempt)` continues to append one `RUNNING` attempt only when the identity does not exist.
- `completeEventIdempotent` and `failEventIdempotent` update the existing matching attempt in place and retain their `APPLIED`, `DUPLICATE`, `CONFLICT`, and `NOT_IN_BATCH` results.

- [ ] **Step 1: Write the failing repository test** asserting a terminal update uses `$set`/array-filter semantics for the existing `batch_id + attempt_id` rather than `$push`.
- [ ] **Step 2: Run the repository test and verify it fails because the current implementation pushes a second terminal attempt.**
- [ ] **Step 3: Implement the smallest repository update that replaces the matching non-terminal attempt, while keeping the event status, recovery summary, and counters unchanged.
- [ ] **Step 4: Add coverage for legacy duplicate snapshots and verify the transition classifier still returns `DUPLICATE` for repeated terminal callbacks.
- [ ] **Step 5: Run all DQL repository tests.

### Task 2: TM detail compatibility and batch-level compensation

**Files:**
- Modify: `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlEventWebMapper.java`
- Modify: `manager/tm/src/main/java/com/tapdata/tm/dql/repository/DqlEventRepository.java`
- Test: `manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlEventWebMapperTest.java`
- Test: `manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlEventRepositoryTest.java`
- Test: `manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlRecoveryBatchServiceTest.java`

**Interfaces:**
- `toAttempts` returns at most one public record for each `batchId + attemptId`, preferring terminal data and preserving recent-first order.
- Batch failure and timeout paths convert residual `RUNNING` attempts for the batch to `FAILED`/`TIMEOUT` before or together with releasing the DQL event lock.

- [ ] **Step 1: Write failing mapper and batch-failure tests for a stale `RUNNING` plus terminal duplicate.
- [ ] **Step 2: Run those tests and verify the stale running record is currently exposed.
- [ ] **Step 3: Implement read-side deduplication and terminal reconciliation for batch-level failure/timeout.
- [ ] **Step 4: Run TM recovery service, repository, and mapper tests.

### Task 3: Dedicated Engine-to-TM DQL payload endpoint

**Files:**
- Create: `manager/tm-common/src/main/java/com/tapdata/tm/dql/vo/DqlRecoveryPayloadVo.java`
- Modify: `manager/tm/src/main/java/com/tapdata/tm/dql/controller/DqlEventController.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/MongoDqlRecoveryEventSource.java`
- Test: `manager/tm/src/test/java/com/tapdata/tm/dql/controller/DqlEventControllerTest.java`
- Test: `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/MongoDqlRecoveryEventSourceTest.java`

**Interfaces:**
- TM exposes a read-only recovery payload endpoint under `/api/dql-events/{eventId}/recovery-payload`.
- Engine loads `DqlPayloadSnapshot` through `DqlTmClient`/`HttpClientMongoOperator` using the hyphenated resource path; no generic `dql_events` collection path is used.

- [ ] **Step 1: Write failing endpoint/client-source tests showing the current lookup requests the invalid `/api/dql_events` resource.
- [ ] **Step 2: Run the tests and verify they fail before the endpoint/client implementation.
- [ ] **Step 3: Implement the read-only TM endpoint with permission/task validation and payload-only response fields.
- [ ] **Step 4: Implement the Engine client call and map the payload response into `DqlPayloadSnapshot`.
- [ ] **Step 5: Run Engine and TM DQL recovery tests.

### Task 4: Full verification and scope review

**Files:**
- No additional production files.

- [ ] **Step 1: Run `git diff --check`.
- [ ] **Step 2: Build Engine and TM modules with offline Maven.
- [ ] **Step 3: Confirm the diff contains no source/target connector writes and document any remaining limitations.
