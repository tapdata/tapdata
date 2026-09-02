# DLQ Recovery Failure Lock Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure a failed DLQ reprocessing batch cannot leave a completed batch's task lock blocking the next reprocessing attempt.

**Architecture:** Keep the task lock while a recovery batch is genuinely active, but make terminal callback handling idempotently repair the lock. When a new submission encounters a lock whose owner batch is already terminal, reconcile that stale lock by owner batch id and retry acquisition. Engine behavior and PostgreSQL connector code remain unchanged.

**Tech Stack:** Java, Spring Data MongoDB, JUnit 5, Mockito, Maven.

**Spec:** `/Users/gavinxiao/kit/oracle/11g_ARCH/doc/dql/TAP-12615-DLQ-controlled-reprocessing-detailed-design.md`

## Global Constraints

- Only `PENDING` and `RECOVERY_FAILED` events may be submitted for recovery.
- Active recovery batches must continue to block concurrent recovery submissions.
- Terminal recovery batches must be retryable and must not retain a task lease.
- Do not modify PostgreSQL connector or PostgreSQL-specific code.

---

### Task 1: Add failing regression tests for stale terminal-batch locks

**Files:**
- Modify: `manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlRecoveryBatchServiceTest.java`
- Modify: `manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlRecoveryTaskLockRepositoryTest.java`

**Interfaces:**
- Tests will define the required lock-owner lookup behavior and terminal callback cleanup behavior.

- [ ] **Step 1: Write the failing tests**

  Add tests proving that a repeated terminal callback invokes task-lock release, and that a recovery start can reclaim a lock whose owning batch is already terminal before trying to create a new batch.

- [ ] **Step 2: Run the focused tests to verify they fail**

  Run: `./mvnw -pl manager/tm -Dtest=DqlRecoveryBatchServiceTest,DqlRecoveryTaskLockRepositoryTest test`

  Expected: FAIL because terminal callback paths currently return without releasing the lock and the repository has no owner lookup used for stale-lock reconciliation.

### Task 2: Repair terminal callback and stale-lock acquisition behavior

**Files:**
- Modify: `manager/tm/src/main/java/com/tapdata/tm/dql/repository/DqlRecoveryTaskLockRepository.java`
- Modify: `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlRecoveryBatchService.java`

**Interfaces:**
- `DqlRecoveryTaskLockRepository.findByTaskId(String)` returns the current task-lock entity, including its owning `batchId`.
- `DqlRecoveryBatchService` releases a lock on all terminal callback no-op paths and only reclaims a lock when its owner batch is terminal.

- [ ] **Step 1: Implement the minimal repository owner lookup**

  Query the unique task lock by `task_id`, returning `null` for blank task ids or no record.

- [ ] **Step 2: Reconcile only completed owners before rejecting a new start**

  On an acquisition conflict, load the lock owner and its batch. If the owner batch is terminal, release using the exact `(taskId, ownerBatchId)` pair and retry the atomic acquisition. Leave the lock untouched when the owner batch is missing, `CREATED`, `DISPATCHED`, or `RUNNING`.

- [ ] **Step 3: Make terminal callback handling repair the lock**

  Before returning from repeated `BATCH_FINISHED` or `BATCH_FAILED` callbacks, call the owner-specific release path. Preserve active-batch rejection and event ownership checks.

- [ ] **Step 4: Run the focused tests to verify they pass**

  Run: `./mvnw -pl manager/tm -Dtest=DqlRecoveryBatchServiceTest,DqlRecoveryTaskLockRepositoryTest test`

  Expected: PASS.

### Task 3: Verify regression coverage and repository scope

**Files:**
- Verify: `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlRecoveryBatchService.java`
- Verify: `manager/tm/src/main/java/com/tapdata/tm/dql/repository/DqlRecoveryTaskLockRepository.java`
- Verify: `manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlRecoveryBatchServiceTest.java`
- Verify: `manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlRecoveryTaskLockRepositoryTest.java`

- [ ] **Step 1: Run the complete TM DQL test set**

  Run: `./mvnw -pl manager/tm -Dtest='com.tapdata.tm.dql.**' test`

  Expected: PASS with no PostgreSQL modules or connector files changed.

- [ ] **Step 2: Inspect the final diff**

  Run: `git diff --check && git diff --stat && git status --short`

  Expected: Only the TM lock/recovery service, its tests, and this implementation plan are changed.
