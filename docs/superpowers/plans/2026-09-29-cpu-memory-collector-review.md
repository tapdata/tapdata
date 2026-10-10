# CpuMemoryCollector Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Address the correctness, lifecycle, concurrency, test-validity, and JDK import issues identified by `ply0011` in PR #3511 without changing the existing public capacity-registration API.

**Architecture:** CPU sampling will use one shared one-second sampling window for all selected tasks, so task count no longer multiplies the sleep interval. Registration and cleanup will remain serialized by `taskRegistrationLock`; dead-task cleanup will remove all per-task monitoring state atomically with the slot release.

**Tech Stack:** Java, JUnit 5, Mockito, Maven, `CompletableFuture`, `ThreadPoolExecutor`, `AtomicReference`.

**Spec:** GitHub PR #3511 review threads by `ply0011`.

## Global Constraints

- Preserve the existing `CpuMemoryCollector.registerCapacityFun(Function<Void, Integer>)` API.
- Keep CPU collection and cleanup on isolated executors.
- Keep task restriction values clamped to the supported range `[1, 500]`.
- Do not reintroduce unsynchronized map removal or stale per-task memory tracking.

## Review Focus

- A large task set must complete one CPU sample in about one sampling interval, not one second per executor batch.
- The system property value must remain the initial fallback until the dynamic setting provider supplies a value.
- Removing a dead task must remove all monitoring state and prevent further listener inserts.
- A concurrent registration must not be added to a list that has just been removed from `threadGroupMap`.
- Cleanup tests must exercise `ReferenceQueue.poll()` and prove cleared references are removed.

### Task 1: Shared CPU sampling window and registration defaults

**Files:**
- Modify: `iengine/api/src/main/java/io/tapdata/threadgroup/CpuMemoryCollector.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/schedule/CpuMemoryScheduler.java`
- Test: `iengine/api/src/test/java/io/tapdata/threadgroup/CpuMemoryCollectorTest.java`
- Test: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/schedule/CpuMemorySchedulerTest.java`

**Interfaces:**
- Keep `collectCpuUsage(List<String>, Map<String, Usage>)` as the entry point.
- Add internal shared-window sampling helpers only where needed.
- Expose only the initial normalized restriction needed by the scheduler fallback.

- [x] **Step 1: Write failing tests** for shared-window behavior and initial restriction fallback.
- [x] **Step 2: Run the focused tests and verify they fail for the missing behavior.**
- [x] **Step 3: Implement one before/after CPU sampling window and use the initial restriction for default/fallback capacity functions.
- [x] **Step 4: Run the focused tests and verify they pass.**

### Task 2: Dead-task cleanup and registration race

**Files:**
- Modify: `iengine/api/src/main/java/io/tapdata/threadgroup/CpuMemoryCollector.java`
- Test: `iengine/api/src/test/java/io/tapdata/threadgroup/CpuMemoryCollectorTest.java`

- [x] **Step 1: Write failing tests** for full dead-task state eviction and registration/removal serialization.
- [x] **Step 2: Run the focused tests and verify they fail for the current stale-state/race behavior.**
- [x] **Step 3: Implement a lock-guarded identity-checked removal and remove all per-task monitoring maps together.
- [x] **Step 4: Run the focused tests and verify they pass.**

### Task 3: Test validity and JDK collection import cleanup

**Files:**
- Modify: `iengine/api/src/main/java/io/tapdata/threadgroup/CpuMemoryCollector.java`
- Modify: `iengine/api/src/test/java/io/tapdata/threadgroup/CpuMemoryCollectorTest.java`

- [x] **Step 1: Replace obsolete `ReferenceQueue.remove(long)` stubs with real `poll()` coverage and shrink the long CPU isolation test.**
- [x] **Step 2: Run the focused tests and verify cleanup behavior is exercised.**
- [x] **Step 3: Replace the Ehcache internal map import with the JDK `ConcurrentHashMap` import.**
- [x] **Step 4: Run the complete relevant module tests, `git diff --check`, and targeted searches.**

## Deferred Review Item

The `Function<Void, Integer>` to `IntSupplier` suggestion is not a correctness fix. The current public registration signature is retained to avoid an unnecessary API change; the field replacement is already thread-safe through `AtomicReference`, and the callback execution remains serialized where restriction state is updated.
