# DQL Engine Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完成 TAP-12615 Engine E02-E10，使 DQL 事件可以从安全存储快照恢复并在运行中/暂停任务中顺序回放。

**Architecture:** `iengine-common` 提供可复制的 `TapdataDqlRecoveryEvent` 和 Payload 重建能力；`iengine-app` 通过事件源、源边界、屏障和 TM 回调适配器实现协调器。普通任务与 recovery-only runner 共用处理节点和目标写入链，使用 DQL 标记防止 recovery 失败再次创建 DQL 主记录。

**Tech Stack:** Java 17, Maven, JUnit 5, Mockito, Hazelcast Jet, existing TapData `TapdataEvent`/`TapRecordEvent`, `DqlPayloadSerializer`, WebSocket message handler.

**Spec:** `docs/superpowers/specs/2026-08-28-dql-engine-replay-design.md`

## Global Constraints

- DQL recovery event 只允许 `TapInsertRecordEvent`、`TapUpdateRecordEvent`、`TapDeleteRecordEvent`。
- Payload 必须使用 `tap-record-event-json-v1`，不完整或超限 Payload 不得进入回放。
- `exactlyOnceId`、table、before、after、time、referenceTime 和原始 info 必须保留。
- 批次严格按 TM 提供的 `orderedEventIds` 串行执行，事件完成前不得注入下一条。
- recovery 失败只更新原 `dql_events` 并追加 attempt，不创建新的 DQL 主记录。
- 运行中任务不改变任务业务状态；暂停任务完成后保持暂停，不启动普通 source reader。
- 每个步骤完成前必须通过 `git diff --check`、相关单元测试和模块编译。

### Task 1: E02 recovery event model

**Files:**
- Create: `iengine/iengine-common/src/main/java/com/tapdata/entity/TapdataDqlRecoveryEvent.java`
- Create: `iengine/iengine-common/src/test/java/com/tapdata/entity/TapdataDqlRecoveryEventTest.java`
- Modify: `doc/TAP-12615-DQL-controlled-reprocessing-development-plan.md:139`
- Modify: `doc/TAP-12615-DQL-development-progress/README.md`
- Create: `doc/TAP-12615-DQL-development-progress/steps/E02-dql-recovery-event.md`

**Interfaces:**
- Consumes: `io.tapdata.dql.model.DqlPayloadSnapshot`, `io.tapdata.dql.serializer.DqlPayloadSerializer`, `io.tapdata.entity.event.dml.TapRecordEvent`.
- Produces: `TapdataDqlRecoveryEvent.createBegin(String)`, `createData(String, String, String, String, Long, DqlPayloadSnapshot)`, `createEnd(String)`; `isDataEvent()` and `isRecoveryEvent(TapEvent)` for later coordinator and capture code.

- [x] **Step 1: Write the failing test**

Add tests proving that `createData` reconstructs insert, update and delete events from snapshots; preserves `exactlyOnceId`, table, before/after, event times and original info; adds `DQL_RECOVERY`, `DQL_EVENT_ID`, `DQL_BATCH_ID`, `DQL_ATTEMPT_ID`; rejects incomplete snapshots; and preserves recovery fields after `clone()`.

```java
@Test
void dataEventRebuildsDmlAndKeepsExactlyOnceIdentity() {
    TapInsertRecordEvent original = TapInsertRecordEvent.create()
            .table("orders").after(Map.of("id", 1, "status", "paid"));
    original.setTime(100L);
    original.setReferenceTime(90L);
    original.setExactlyOnceId("eo-1");
    original.setInfo(Map.of("sourceOffset", "binlog:12"));
    DqlPayloadSnapshot snapshot = new DqlPayloadSerializer().serialize(original);

    TapdataDqlRecoveryEvent recovery = TapdataDqlRecoveryEvent.createData(
            "batch-1", "event-1", "attempt-1", "operator-1", 8L, snapshot);

    TapInsertRecordEvent restored = assertInstanceOf(TapInsertRecordEvent.class, recovery.getTapEvent());
    assertEquals("event-1", recovery.getEventId());
    assertEquals("batch-1", recovery.getBatchId());
    assertEquals("attempt-1", recovery.getAttemptId());
    assertEquals("eo-1", restored.getExactlyOnceId());
    assertEquals(Map.of("id", 1, "status", "paid"), restored.getAfter());
    assertEquals(Boolean.TRUE, restored.getInfo().get(TapdataDqlRecoveryEvent.INFO_KEY_DQL_RECOVERY));
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `mvn -pl iengine/iengine-common -Dtest=TapdataDqlRecoveryEventTest test`

Expected: FAIL to compile because `TapdataDqlRecoveryEvent` and its factories do not exist.

- [x] **Step 3: Write minimal implementation**

Implement the class with `SyncStage.CDC`, constants `INFO_KEY_DQL_RECOVERY`, `INFO_KEY_DQL_EVENT_ID`, `INFO_KEY_DQL_BATCH_ID`, `INFO_KEY_DQL_ATTEMPT_ID`, `TYPE_BEGIN`, `TYPE_DATA`, `TYPE_END`, fields `batchId`, `attemptId`, `recoveryType`, `operatorId`, `taskVersion`, and the inherited `eventId` as the DQL event ID. `createData` must call `new DqlPayloadSerializer().deserialize(snapshot)`, reject null/incomplete payloads through the serializer, set the DML event, then append recovery metadata to a copied info map so the snapshot object is not mutated. `clone(TapdataEvent)` must copy every recovery field after calling `super.clone`.

- [x] **Step 4: Run test to verify it passes**

Run: `mvn -pl iengine/iengine-common -Dtest=TapdataDqlRecoveryEventTest,DqlPayloadSerializerTest test`

Expected: all selected tests PASS with zero failures and zero errors.

- [x] **Step 5: Update progress record and commit**

Record actual code paths, design decision about reusing inherited `eventId`, TDD red/green evidence, and the Maven result in `E02-dql-recovery-event.md`. Change E02 to 已完成 and keep E03-E10 未开始 in the progress index and development plan. Run `git diff --check`, then:

```bash
git add iengine/iengine-common/src/main/java/com/tapdata/entity/TapdataDqlRecoveryEvent.java iengine/iengine-common/src/test/java/com/tapdata/entity/TapdataDqlRecoveryEventTest.java doc/TAP-12615-DLQ-controlled-reprocessing-development-plan.md doc/TAP-12615-DQL-development-progress/README.md doc/TAP-12615-DQL-development-progress/steps/E02-dql-recovery-event.md
git commit -m "feat(TAP-12615): add dql recovery event"
```

### Task 2: E03 serial recovery coordinator

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCoordinator.java`
- Create: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImpl.java`
- Create: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryEventSource.java`
- Create: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryEventSink.java`
- Create: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryBarrier.java`
- Create: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryExecutionPolicy.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImplTest.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryMessageHandler.java`
- Modify: `doc/TAP-12615-DLQ-controlled-reprocessing-development-plan.md:140`
- Create: `doc/TAP-12615-DQL-development-progress/steps/E03-recovery-coordinator.md`

**Interfaces:**
- Consumes: E02 event factory and E01 `DqlRecoveryMessageDto`.
- Produces: one asynchronous coordinator execution per claimed batch; event callback order and policy-driven continue/stop behavior.

- [x] **Step 1: Write the failing test**

Define `DqlRecoveryEventSource.load(String eventId)`, `DqlRecoveryEventSink.enqueue(TapdataDqlRecoveryEvent)`, `DqlRecoveryBarrier.await(String eventId, long timeoutMillis)`, and `DqlRecoveryExecutionPolicy.continueAfterFailure()`. Test that `start` reports events in message order, enqueues only one DATA event before waiting for its barrier, continues after a failed barrier when the policy allows it, and stops when the policy disallows it.

- [x] **Step 2: Run test to verify it fails**

Run: `mvn -pl iengine/iengine-app -Dtest=DqlRecoveryCoordinatorImplTest test`

Expected: FAIL to compile because the coordinator implementation and execution interfaces do not exist.

- [x] **Step 3: Write minimal implementation**

Implement a single-threaded per-batch execution that iterates the immutable `orderedEventIds`, loads each complete snapshot, creates an attempt ID before DATA injection, calls the sink, waits on the barrier, and sends one terminal result through the existing recovery report abstraction. Keep the `start` method non-blocking for the WebSocket handler and guard a batch execution with an atomic terminal flag.

- [x] **Step 4: Run test to verify it passes**

Run: `mvn -pl iengine/iengine-app -Dtest=DqlRecoveryCoordinatorImplTest,DqlRecoveryMessageHandlerTest test`

Expected: all selected tests PASS.

- [x] **Step 5: Commit**

Update E03 progress documentation and commit with `feat(TAP-12615): add recovery coordinator`.

### Task 3: E04 live-task source read gate

**Files:**
- Create: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlSourceReadGate.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlSourceReadGateTest.java`
- Modify: live source enqueue boundary in `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/HazelcastSourcePdkBaseNode.java`
- Create: `doc/TAP-12615-DQL-development-progress/steps/E04-live-source-read-gate.md`

**Interfaces:**
- Produces: `open()`, `beginPausing()`, `enterRecoveryOnly()`, `beginResuming()`, `awaitDrained(long)`, `allow(TapdataEvent)` and `close()`.

- [x] **Step 1: Write the failing test** — cover `OPEN`, `PAUSING`, `RECOVERY_ONLY`, and `RESUMING`, including rejection of normal events in recovery-only mode and acceptance of DQL recovery/barrier events.
- [x] **Step 2: Run test to verify it fails** — full reactor testCompile failed because `DqlSourceReadGate` was missing.
- [x] **Step 3: Write minimal implementation** — use a lock/condition state machine; never mutate `TaskDto.status`; make `close()` restore `OPEN` even after an exception; integrate it into `HazelcastSourcePdkBaseNode.enqueue`.
- [x] **Step 4: Run test to verify it passes** — gate, coordinator, handler and the affected Engine modules compiled and passed; no independent production Source enqueue test existed, so the source base class compile regression was included.
- [x] **Step 5: Commit** — record source gate integration and commit `feat(TAP-12615): gate live source reads for dql recovery`.

### Task 4: E05 recovery-only runner for paused tasks

**Files:**
- Create: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryOnlyRunner.java`
- Create: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlReplaySourceNode.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlRecoveryOnlyRunnerTest.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImpl.java`
- Create: `doc/TAP-12615-DQL-development-progress/steps/E05-recovery-only-runner.md`

**Interfaces:**
- Produces: a runner lifecycle that accepts a task snapshot and recovery batch without calling normal task start or source connection read APIs.

- [x] **Step 1: Write the failing test** — prove paused task status is unchanged, source read is never started, saved events are emitted in order, and all runner resources close.
- [x] **Step 2: Run test to verify it fails** — test execution first exposed an invalid `null` Payload fixture; E02 serializer validation remained unchanged.
- [x] **Step 3: Write minimal implementation** — build the runner behind a narrow factory, replace only the source boundary with replay input, and close every created resource in reverse order.
- [x] **Step 4: Run test to verify it passes** — runner and coordinator tests passed with 5/5; affected Engine modules compiled successfully.
- [x] **Step 5: Commit** — record paused-task semantics and commit `feat(TAP-12615): add paused task recovery runner`.

### Task 5: E06 source-boundary injection

**Files:**
- Create: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlSourceBoundaryInjector.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlSourceBoundaryInjectorTest.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImpl.java`
- Create: `doc/TAP-12615-DQL-development-progress/steps/E06-source-boundary-injection.md`

**Interfaces:**
- Produces: injection into the source node selected from the task DAG, never direct target-node writes.

- [x] **Step 1: Write the failing test** — cover a single source, multiple tables, missing source node, a target-only DAG, and ambiguous multi-source rejection.
- [x] **Step 2: Run test to verify it fails** — the test matrix was authored before the injector implementation; the reactor verification below captures the compile/test boundary after implementation.
- [x] **Step 3: Write minimal implementation** — resolve only DAG `getSourceNodes()` entries that are `DataParentNode` instances and have a registered runtime boundary; fail closed for missing, target-only, processor-only, and ambiguous source graphs.
- [x] **Step 4: Run test to verify it passes** — injector and coordinator live-source selection tests passed; affected Engine modules compiled successfully.
- [x] **Step 5: Commit** — record DAG resolution and commit `feat(TAP-12615): inject dql recovery at source boundary`.

### Task 6: E07 per-event barrier and result判定

**Files:**
- Create: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryBarrierCoordinator.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImpl.java`
- Modify: target/processor event completion boundaries that already handle `TapdataCountDownLatchEvent`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlRecoveryBarrierCoordinatorTest.java`
- Create: `doc/TAP-12615-DQL-development-progress/steps/E07-recovery-barrier.md`

**Interfaces:**
- Produces: one barrier per event, target completion before next injection, `FAILED` or `TIMEOUT` on terminal wait failure.

- [ ] **Step 1: Write the failing test** — assert the second event cannot be enqueued until the first target outcome is known; assert timeout produces one terminal result.
- [ ] **Step 2: Run test to verify it fails** — run the focused barrier test and observe the missing sequencing behavior.
- [ ] **Step 3: Write minimal implementation** — create the latch event at the source boundary, wait with the configured timeout, combine target callback and latch outcome, and release local latch state in finally.
- [ ] **Step 4: Run test to verify it passes** — run barrier, coordinator, and target completion tests.
- [ ] **Step 5: Commit** — record timeout and ordering evidence and commit `feat(TAP-12615): enforce dql recovery barriers`.

### Task 7: E08 prevent recursive DQL capture

**Files:**
- Modify: `iengine/modules/skip-error-event-module/src/main/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTask.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastProcessorBaseNode.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/HazelcastTargetPdkDataNode.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlRecoveryCaptureGuardTest.java`
- Create: `doc/TAP-12615-DQL-development-progress/steps/E08-recovery-capture-guard.md`

**Interfaces:**
- Produces: recovery failures routed to original event result reporting without a second `DqlEventReporter.report` call.

- [ ] **Step 1: Write the failing test** — feed a recovery-marked event through target and processor failure paths and assert no new DQL report is created while the original event failure callback is invoked.
- [ ] **Step 2: Run test to verify it fails** — run the focused capture guard test and observe recursive reporting.
- [ ] **Step 3: Write minimal implementation** — centralize `TapdataDqlRecoveryEvent.isRecoveryEvent(tapEvent)` detection and branch before normal DQL capture; preserve existing behavior for non-recovery events.
- [ ] **Step 4: Run test to verify it passes** — run C07-C12 capture regression and recovery guard tests.
- [ ] **Step 5: Commit** — record the no-recursion invariant and commit `feat(TAP-12615): guard dql recovery from recursive capture`.

### Task 8: E09 batch failure compensation

**Files:**
- Create: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryFailureCompensator.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImpl.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryEventHandler.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlRecoveryFailureCompensatorTest.java`
- Create: `doc/TAP-12615-DQL-development-progress/steps/E09-recovery-compensation.md`

**Interfaces:**
- Produces: best-effort BATCH_FAILED reporting with deterministic cleanup for task stop, restart, version mismatch, runner init failure, source-gate resume failure and callback failure.

- [ ] **Step 1: Write the failing test** — cover each failure source and assert cleanup executes once, no local executor/latch remains, and a failed batch report is attempted.
- [ ] **Step 2: Run test to verify it fails** — run the focused compensation test and observe missing cleanup/reporting.
- [ ] **Step 3: Write minimal implementation** — make compensation idempotent with an atomic guard, execute cleanup in reverse lifecycle order, preserve the original failure as the report message, and never throw from cleanup over the original exception.
- [ ] **Step 4: Run test to verify it passes** — run compensation, handler, coordinator and existing stop/reset/delete message regressions.
- [ ] **Step 5: Commit** — record failure matrix and commit `feat(TAP-12615): compensate failed dql recovery batches`.

### Task 9: E10 Engine replay regression

**Files:**
- Create: `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlRecoveryReplayRegressionTest.java`
- Modify: `doc/TAP-12615-DLQ-controlled-reprocessing-development-plan.md:147`
- Modify: `doc/TAP-12615-DQL-development-progress/README.md`
- Create: `doc/TAP-12615-DQL-development-progress/steps/E10-engine-replay-regression.md`
- Create: `doc/TAP-12615-DQL-development-progress/milestones/M4-engine-replay-closure.md`

**Interfaces:**
- Consumes: E01-E09 production boundaries and existing task lifecycle handlers.
- Produces: M4 Engine replay evidence for live/paused I/U/D, order, barrier, continue/stop, timeout, duplicate message, restart and source-gate restoration.

- [ ] **Step 1: Write the failing regression matrix** — add tests for all listed scenarios and assert no DQL主记录增长 during recovery, stable attempt counts, ordered target writes, and unchanged task state.
- [ ] **Step 2: Run test to verify it fails** — run `mvn -pl iengine/iengine-app -Dtest=DqlRecoveryReplayRegressionTest test`; classify failures by missing integration boundary.
- [ ] **Step 3: Implement only integration fixes** — do not weaken assertions; fix wiring, lifecycle cleanup or callback mapping required by the regression matrix.
- [ ] **Step 4: Run the full Engine DQL verification** — run the E01-E10 focused suite, the existing C01-C12 capture suite, and `git diff --check`.
- [ ] **Step 5: Commit and close M4** — update README, plan, E10 summary and M4 milestone summary, then commit `feat(TAP-12615): complete engine dql replay`.

## Plan self-review

- E02 covers the complete storage-snapshot-to-event boundary and is independently testable before E03.
- E03-E07 separate orchestration, source gating, runner selection, source injection and barriers so each can be rejected independently.
- E08 is isolated at existing capture boundaries and preserves non-DQL compatibility.
- E09 owns all cleanup and TM convergence attempts; E10 verifies the cross-component contract without adding new production behavior.
- No Web work or performance threshold is included, matching the approved design and project plan.
