package io.tapdata.flow.engine.V2.throttle;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.AgentThrottleDeltaResponse;
import com.tapdata.tm.commons.throttle.TaskThrottleApplyReceipt;
import com.tapdata.tm.commons.throttle.TaskThrottlePolicyDto;
import com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import com.tapdata.tm.commons.throttle.WaitReason;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastTargetPdkBaseNode;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.metric.throttle.MonotonicClock;
import io.tapdata.observable.metric.throttle.ThrottleAggregatorRegistry;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class EngineThrottleNodeIntegrationUnitTest {

    private TaskThrottleRuntimeRegistry registry;

    @BeforeEach
    void setup() {
        registry = new TaskThrottleRuntimeRegistry(
                new AgentMemoryBudgetPool(10_000_000L, 50_000L, 5_000L, 0L, MonotonicClock.SYSTEM),
                MonotonicClock.SYSTEM
        );
        registry.setAgentId("agent-it-1");
        TaskThrottleRuntimeRegistry.setInstanceForTest(registry);
    }

    @AfterEach
    void tearDown() {
        registry.resetForTest();
        TaskThrottleRuntimeRegistry.setInstanceForTest(new TaskThrottleRuntimeRegistry());
    }

    private static List<TapRecordEvent> insertEvents(int count) {
        List<TapRecordEvent> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(TapInsertRecordEvent.create()
                    .init()
                    .table("orders")
                    .after(Map.of("id", i, "payload", "val-" + i)));
        }
        return list;
    }

    @Test
    void sourceAdmitSlicesBatchAttachesLeaseInfoAndReleasesBudgetOnTargetCompletion() throws Exception {
        String taskId = new ObjectId().toHexString();
        Map<ThrottlePhase, ThrottlePhaseLimit> limits = new EnumMap<>(ThrottlePhase.class);
        limits.put(ThrottlePhase.FULL, ThrottlePhaseLimit.builder()
                .enabled(true)
                .eventsPerSec(10_000L) // normalBurst = 1_000/slice
                .bytesPerSec(50_000_000L)
                .build());
        TaskThrottlePolicyDto policy = TaskThrottlePolicyDto.builder()
                .tenantId("t1")
                .taskKind("sync")
                .taskId(taskId)
                .desiredVersion(1L)
                .phases(limits)
                .build();

        TaskThrottleRuntime runtime = registry.openRuntime(new TaskRunKey("t1", "sync", taskId, 3L), policy, true);
        List<TapdataEvent> inFlightEvents = new ArrayList<>();

        TaskThrottleRuntime.AdmitOutcome outcome = runtime.admitSourceBatch(
                "src-1",
                "conn-src",
                ThrottlePhase.FULL,
                insertEvents(25),
                e -> ((TapRecordEvent) e).getTableId(),
                () -> false,
                slice -> {
                    for (TapRecordEvent rec : slice.events()) {
                        TapdataEvent td = new TapdataEvent();
                        td.setTapEvent(rec);
                        TaskThrottleRuntime.attachLeaseInfo(
                                td, slice.lease().getLeaseId(), slice.sourceNodeId(), slice.batchSeq());
                        inFlightEvents.add(td);
                    }
                }
        );
        assertEquals(TaskThrottleRuntime.AdmitOutcome.ADMITTED, outcome);
        assertEquals(25, inFlightEvents.size());
        assertNotNull(inFlightEvents.get(0).getInfo(TaskThrottleRuntime.INFO_KEY_LEASE_ID));
        assertTrue(runtime.getInFlightBudget().snapshot().usedBytes() > 0L);

        // Simulate target finishing all 25 events -> capacity budget drains cleanly to 0
        runtime.onTargetEventsCompleted("tgt-1", inFlightEvents);
        assertEquals(0L, runtime.getInFlightBudget().snapshot().usedBytes());
        assertEquals(0L, runtime.getInFlightBudget().snapshot().usedBatches());
    }

    @Test
    void postDebitCancellationAccountsForCancelledEventsWithoutReissuingTokens() {
        String taskId = new ObjectId().toHexString();
        Map<ThrottlePhase, ThrottlePhaseLimit> limits = new EnumMap<>(ThrottlePhase.class);
        limits.put(ThrottlePhase.FULL, ThrottlePhaseLimit.builder()
                .enabled(true)
                .eventsPerSec(1_000L)
                .bytesPerSec(10_000_000L)
                .build());
        TaskThrottlePolicyDto policy = TaskThrottlePolicyDto.builder()
                .tenantId("t1").taskKind("sync").taskId(taskId).desiredVersion(1L).phases(limits).build();

        TaskThrottleRuntime runtime = registry.openRuntime(new TaskRunKey("t1", "sync", taskId, 8L), policy, true);
        AtomicBoolean cancelled = new AtomicBoolean(false);

        // Simulate downstream enqueue rejecting due to concurrent stop after slice tokens were debited
        assertThrows(IllegalStateException.class, () -> runtime.admitSourceBatch(
                "src-1",
                "conn-src",
                ThrottlePhase.FULL,
                insertEvents(3),
                e -> "orders",
                cancelled::get,
                slice -> {
                    cancelled.set(true);
                    throw new IllegalStateException("queue closed after debit");
                }
        ));

        List<TaskThrottleTelemetrySample> samples = runtime.snapshotAndRollTelemetry();
        assertFalse(samples.isEmpty());
        assertEquals(3L, samples.get(0).getCancelledAfterDebitEvents());
        assertTrue(samples.get(0).getCancelledEstimatedBytes() > 0L);
        assertEquals(0L, runtime.getInFlightBudget().snapshot().usedBytes());
    }

    @Test
    void targetCheckpointAppendsEpochForProtectedRunAndBlocksOnceFenced() {
        ObjectId oid = new ObjectId();
        String taskId = oid.toHexString();

        TaskDto task = new TaskDto();
        task.setId(oid);
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);

        HazelcastTargetPdkBaseNode node = mock(HazelcastTargetPdkBaseNode.class);
        ClientMongoOperator mongo = mock(ClientMongoOperator.class);
        DataProcessorContext ctx = new DataProcessorContext.DataProcessorContextBuilder().withTaskDto(task).build();
        ReflectionTestUtils.setField(node, "dataProcessorContext", ctx);
        ReflectionTestUtils.setField(node, "clientMongoOperator", mongo);
        ReflectionTestUtils.setField(node, "obsLogger", mock(ObsLogger.class));
        ReflectionTestUtils.setField(node, "saveSnapshotLock", new Object());
        ReflectionTestUtils.setField(node, "flushOffset", new AtomicBoolean(true));
        ReflectionTestUtils.setField(node, "uploadDagService", new AtomicBoolean(false));
        SyncProgress progress = new SyncProgress();
        progress.setSyncStage("CDC");
        progress.setStreamOffset("cp-1");
        ReflectionTestUtils.setField(node, "syncProgressMap", Map.of("src,tgt", progress));
        doCallRealMethod().when(node).saveToSnapshot();

        // 1. Unregistered runtime -> exact legacy collection string preserved
        assertTrue(node.saveToSnapshot());
        ArgumentCaptor<String> colCaptor = ArgumentCaptor.forClass(String.class);
        verify(mongo, times(1)).insertOne(any(), colCaptor.capture());
        assertEquals("Task/syncProgress/" + taskId, colCaptor.getValue());

        // 2. Protected runtime opened at runEpoch=5 -> appends ?runEpoch=5&agentId=...
        TaskThrottleRuntime runtime = registry.openRuntime(new TaskRunKey("t1", "sync", taskId, 5L), null, true);
        assertTrue(node.saveToSnapshot());
        verify(mongo, times(2)).insertOne(any(), colCaptor.capture());
        assertTrue(colCaptor.getValue().contains("runEpoch=5"));
        assertTrue(colCaptor.getValue().contains("agentId=agent-it-1"));

        // 3. Stale epoch fenced via TM delta directive (authorizedRunEpoch=6) -> blocks checkpoint persistence
        AgentThrottleDeltaResponse delta = AgentThrottleDeltaResponse.builder()
                .cursor(99L)
                .sampleConfigVersion(2L)
                .cdcSampleRefreshIntervalSeconds(15)
                .directives(List.of(AgentThrottleDeltaResponse.TaskDirective.builder()
                        .taskId(taskId)
                        .authorizedRunEpoch(6L)
                        .protectedRun(true)
                        .build()))
                .build();
        registry.applyDelta(delta);

        assertTrue(runtime.isFenced());
        assertEquals(15, runtime.getInputMeter().getCdcSampleCache().getRefreshIntervalSeconds());
        assertFalse(node.saveToSnapshot());
        // No 3rd insertOne issued!
        verify(mongo, times(2)).insertOne(any(), anyString());
    }

    @Test
    void syncClientPublishesCapabilitiesReceiptsTelemetryAndStopEvidenceWithResilience() {
        String taskId = new ObjectId().toHexString();
        Map<ThrottlePhase, ThrottlePhaseLimit> limits = new EnumMap<>(ThrottlePhase.class);
        limits.put(ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).eventsPerSec(500L).build());
        TaskThrottlePolicyDto policy = TaskThrottlePolicyDto.builder()
                .tenantId("t1").taskKind("sync").taskId(taskId).desiredVersion(2L).phases(limits).build();
        TaskThrottleRuntime runtime = registry.openRuntime(new TaskRunKey("t1", "sync", taskId, 11L), policy, true);
        runtime.getAggregator().activateSlot(ThrottlePhase.FULL, com.tapdata.tm.commons.throttle.ThrottleDirection.INPUT, 2L);

        ClientMongoOperator mockOp = mock(ClientMongoOperator.class);
        AgentThrottleSyncClient client = new AgentThrottleSyncClient(mockOp, registry);

        assertTrue(client.registerCapabilities());
        verify(mockOp).postOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_CAPABILITIES), eq(Object.class));

        assertTrue(client.flushPendingReceipts());
        verify(mockOp).postOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_RECEIPTS), eq(Object.class));

        assertTrue(client.flushTelemetrySamples());
        verify(mockOp).postOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_TELEMETRY), eq(Object.class));

        // Stop produces verifiable evidence; TM outage keeps evidence queued for retry
        TaskThrottleStopEvidenceDto ev = registry.closeRuntime(taskId, true, "graceful test stop");
        assertNotNull(ev);
        assertEquals(11L, ev.getOldRunEpoch());
        assertEquals("agent-it-1", ev.getOldAgentId());
        assertTrue(ev.isWriteSettled());

        doThrow(new RuntimeException("TM down")).when(mockOp)
                .postOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_STOP_EVIDENCE), eq(Object.class));
        assertFalse(client.reportStopEvidence(ev));
        assertEquals(1, registry.drainPendingStopEvidences().size());
    }

    @Test
    void syncCycleAutoRegistersCapabilitiesReportsSampleVersionUpdatesAndLazyOpensRunningTasks() {
        String runningTaskId = new ObjectId().toHexString();
        ClientMongoOperator mockOp = mock(ClientMongoOperator.class);
        AgentThrottleSyncClient client = new AgentThrottleSyncClient(mockOp, registry);

        registry.registerRunningTask(runningTaskId, "t1", "migrate");
        assertNull(registry.getActive(runningTaskId));

        Map<ThrottlePhase, ThrottlePhaseLimit> cdcLimits = new EnumMap<>(ThrottlePhase.class);
        cdcLimits.put(ThrottlePhase.CDC, ThrottlePhaseLimit.builder().enabled(true).eventsPerSec(50L).build());
        TaskThrottlePolicyDto hotPolicy = TaskThrottlePolicyDto.builder()
                .tenantId("t1").taskKind("migrate").taskId(runningTaskId).desiredVersion(1L).phases(cdcLimits).build();
        AgentThrottleDeltaResponse deltaWithSampleAndHotPolicy = AgentThrottleDeltaResponse.builder()
                .cursor(120L)
                .sampleConfigVersion(5L)
                .cdcSampleRefreshIntervalSeconds(15)
                .directives(List.of(AgentThrottleDeltaResponse.TaskDirective.builder()
                        .tenantId("t1")
                        .taskKind("migrate")
                        .taskId(runningTaskId)
                        .authorizedRunEpoch(0L)
                        .protectedRun(false)
                        .policy(hotPolicy)
                        .build()))
                .build();
        when(mockOp.findOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_DELTA), eq(AgentThrottleDeltaResponse.class)))
                .thenReturn(deltaWithSampleAndHotPolicy)
                .thenReturn(AgentThrottleDeltaResponse.builder().cursor(120L).sampleConfigVersion(5L).cdcSampleRefreshIntervalSeconds(15).build());

        // Cycle 1: scheduledSyncCycle auto-registers capabilities once, then re-registers once after sampleConfigVersion advances
        client.scheduledSyncCycle();
        verify(mockOp, times(2)).postOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_CAPABILITIES), eq(Object.class));
        verify(mockOp, times(1)).postOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_RECEIPTS), eq(Object.class));
        TaskThrottleRuntime lazyOpened = registry.getActive(runningTaskId);
        assertNotNull(lazyOpened);
        assertEquals(15, lazyOpened.getInputMeter().getCdcSampleCache().getRefreshIntervalSeconds());

        // Cycle 2 within 60s refresh window with unchanged sample version: no redundant capabilities POST
        client.scheduledSyncCycle();
        verify(mockOp, times(2)).postOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_CAPABILITIES), eq(Object.class));

        registry.unregisterRunningTask(runningTaskId);
    }

    @Test
    void schedulerTenantResolutionFallbackAndAggregatorRegistryLifecycle() {
        TaskDto onlyUserId = new TaskDto();
        onlyUserId.setUserId("user-uid-1");
        assertEquals("user-uid-1", TapdataTaskScheduler.resolveThrottleTenantId(onlyUserId, null));

        TaskDto withCustomId = new TaskDto();
        withCustomId.setCustomId("custom-1");
        withCustomId.setUserId("user-uid-1");
        assertEquals("custom-1", TapdataTaskScheduler.resolveThrottleTenantId(withCustomId, null));

        TaskThrottlePolicyDto withPolicyTenant = TaskThrottlePolicyDto.builder().tenantId("policy-tenant-1").build();
        assertEquals("policy-tenant-1", TapdataTaskScheduler.resolveThrottleTenantId(withCustomId, withPolicyTenant));
        assertEquals("", TapdataTaskScheduler.resolveThrottleTenantId(null, null));

        String taskId = new ObjectId().toHexString();
        TaskThrottleRuntime runtime = registry.openRuntime(new TaskRunKey("user-uid-1", "migrate", taskId, 1L), null, true);
        assertSame(runtime.getAggregator(), ThrottleAggregatorRegistry.get(taskId));
        registry.closeRuntime(taskId, true, "done");
        assertNull(ThrottleAggregatorRegistry.get(taskId));
    }

    @Test
    void syncClientHandlesMissingOperatorUnknownAgentAndControlPlaneErrorsGracefully() {
        AgentThrottleSyncClient noOpClient = new AgentThrottleSyncClient(null, null);
        assertFalse(noOpClient.ensureCapabilitiesRegistered(true));
        assertFalse(noOpClient.registerCapabilities());
        assertFalse(noOpClient.pullDeltaAndApply());
        assertFalse(noOpClient.flushPendingReceipts());
        assertFalse(noOpClient.flushTelemetrySamples());
        assertTrue(noOpClient.reportStopEvidence(null));
        noOpClient.flushPendingStopEvidences();

        TaskThrottleStopEvidenceDto ev = TaskThrottleStopEvidenceDto.builder()
                .tenantId("t1").taskKind("migrate").taskId("task-x").oldRunEpoch(1L).build();
        TaskThrottleRuntimeRegistry prev = TaskThrottleRuntimeRegistry.getInstance();
        try {
            TaskThrottleRuntimeRegistry temp = new TaskThrottleRuntimeRegistry();
            TaskThrottleRuntimeRegistry.setInstanceForTest(temp);
            assertSame(temp, TaskThrottleRuntimeRegistry.getInstance());

            AgentThrottleSyncClient tempNoOp = new AgentThrottleSyncClient(null, temp);
            assertFalse(tempNoOp.ensureCapabilitiesRegistered(true));
            assertFalse(tempNoOp.registerCapabilities());
            assertFalse(tempNoOp.reportStopEvidence(ev));
            assertEquals(1, temp.drainPendingStopEvidences().size());

            ClientMongoOperator failingOp = mock(ClientMongoOperator.class);
            when(failingOp.findOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_DELTA), eq(AgentThrottleDeltaResponse.class)))
                    .thenThrow(new RuntimeException("delta timeout"));
            doThrow(new RuntimeException("post failed")).when(failingOp).postOne(anyMap(), anyString(), eq(Object.class));

            temp.setAgentId("agent-err-1");
            TaskThrottlePolicyDto p = TaskThrottlePolicyDto.builder()
                    .tenantId("t1").taskKind("migrate").taskId("task-y").desiredVersion(1L).build();
            TaskThrottleRuntime rt = temp.openRuntime(new TaskRunKey("t1", "migrate", "task-y", 1L), p, true);
            rt.getAggregator().activateSlot(ThrottlePhase.FULL, com.tapdata.tm.commons.throttle.ThrottleDirection.INPUT, 1L);

            AgentThrottleSyncClient errClient = new AgentThrottleSyncClient(failingOp, temp);
            assertFalse(errClient.registerCapabilities());
            assertFalse(errClient.pullDeltaAndApply());
            assertFalse(errClient.flushPendingReceipts());
            assertFalse(errClient.flushTelemetrySamples());
            TaskThrottleRuntimeRegistry.setInstanceForTest(null);
            assertNotNull(TaskThrottleRuntimeRegistry.getInstance());
        } finally {
            TaskThrottleRuntimeRegistry.setInstanceForTest(prev);
        }
    }

    @Test
    void stopEvidenceDropsPermanentRejectionsAndCapsTransientRetries() {
        ClientMongoOperator mockOp = mock(ClientMongoOperator.class);
        AgentThrottleSyncClient client = new AgentThrottleSyncClient(mockOp, registry);

        TaskThrottleStopEvidenceDto staleEv = TaskThrottleStopEvidenceDto.builder()
                .tenantId("t1").taskKind("migrate").taskId("deleted-task")
                .oldRunEpoch(1L).evidenceRef("ev-stale-1").build();
        doThrow(new RuntimeException("STALE_RUN_EPOCH: No protected throttle run"))
                .when(mockOp).postOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_STOP_EVIDENCE), eq(Object.class));
        registry.requeueStopEvidence(staleEv);
        client.flushPendingStopEvidences();
        assertTrue(registry.drainPendingStopEvidences().isEmpty());

        TaskThrottleStopEvidenceDto flakyEv = TaskThrottleStopEvidenceDto.builder()
                .tenantId("t1").taskKind("migrate").taskId("flaky-task")
                .oldRunEpoch(2L).evidenceRef("ev-flaky-1").build();
        doThrow(new RuntimeException("connection reset"))
                .when(mockOp).postOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_STOP_EVIDENCE), eq(Object.class));
        for (int i = 1; i < AgentThrottleSyncClient.MAX_STOP_EVIDENCE_RETRIES; i++) {
            assertFalse(client.reportStopEvidence(flakyEv));
            assertEquals(1, registry.drainPendingStopEvidences().size());
        }
        assertFalse(client.reportStopEvidence(flakyEv));
        assertTrue(registry.drainPendingStopEvidences().isEmpty());
    }

    @Test
    void cdcUnknownBytesSampleBoundsResampleRetriesAndDegradesGracefullyWithoutSpinning() throws Exception {
        String taskId = new ObjectId().toHexString();
        Map<ThrottlePhase, ThrottlePhaseLimit> limits = new EnumMap<>(ThrottlePhase.class);
        limits.put(ThrottlePhase.CDC, ThrottlePhaseLimit.builder()
                .enabled(true)
                .bytesPerSec(50_000_000L) // bytes-only, no QPS
                .build());
        limits.put(ThrottlePhase.FULL, ThrottlePhaseLimit.builder()
                .enabled(true)
                .bytesPerSec(50_000_000L)
                .build());
        TaskThrottlePolicyDto policy = TaskThrottlePolicyDto.builder()
                .tenantId("t1").taskKind("sync").taskId(taskId).desiredVersion(1L).phases(limits).build();
        TaskThrottleRuntime runtime = registry.openRuntime(new TaskRunKey("t1", "sync", taskId, 1L), policy, true);

        // Build CDC batch spanning 6 tables (> coldStartSamplesPerBatch=4): initial estimate is null,
        // bounded retry must re-meter remaining groups and admit cleanly without permanent deadlock!
        List<TapRecordEvent> multiTableCdc = new ArrayList<>();
        for (int t = 0; t < 6; t++) {
            multiTableCdc.add(TapInsertRecordEvent.create().init().table("tbl_" + t).after(Map.of("id", t, "v", "payload-" + t)));
        }
        List<TapRecordEvent> admitted = new ArrayList<>();
        TaskThrottleRuntime.AdmitOutcome cdcOutcome = runtime.admitSourceBatch(
                "src-1", "conn-1", ThrottlePhase.CDC, multiTableCdc,
                e -> ((TapRecordEvent) e).getTableId(), () -> false, slice -> admitted.addAll(slice.events()));
        assertEquals(TaskThrottleRuntime.AdmitOutcome.ADMITTED, cdcOutcome);
        assertEquals(6, admitted.size());

        // Cyclic payload causes FULL MeterBudgetExceededException (estimatedBytes=null) -> must degrade and pass instead of spinning
        Map<String, Object> cyclic = new java.util.HashMap<>();
        cyclic.put("self", cyclic);
        List<TapRecordEvent> cyclicFull = List.of(TapInsertRecordEvent.create().init().table("cyclic_tbl").after(cyclic));
        List<TapRecordEvent> fullAdmitted = new ArrayList<>();
        TaskThrottleRuntime.AdmitOutcome fullOutcome = runtime.admitSourceBatch(
                "src-1", "conn-1", ThrottlePhase.FULL, cyclicFull,
                e -> "cyclic_tbl", () -> false, slice -> fullAdmitted.addAll(slice.events()));
        assertEquals(TaskThrottleRuntime.AdmitOutcome.ADMITTED, fullOutcome);
        assertEquals(1, fullAdmitted.size());
    }

    @Test
    void skewedBatchSubslicesDynamicallyAndLazyZeroEpochRuntimePreservesCheckpointAndReceiptsRequeue() throws Exception {
        String taskId = new ObjectId().toHexString();
        Map<ThrottlePhase, ThrottlePhaseLimit> limits = new EnumMap<>(ThrottlePhase.class);
        limits.put(ThrottlePhase.FULL, ThrottlePhaseLimit.builder()
                .enabled(true)
                .bytesPerSec(50_000L) // normalBurst=50KB, maxAtomic=250KB
                .build());
        TaskThrottlePolicyDto policy = TaskThrottlePolicyDto.builder()
                .tenantId("t1").taskKind("sync").taskId(taskId).desiredVersion(1L).phases(limits).build();
        TaskThrottleRuntime runtime = registry.openRuntime(new TaskRunKey("t1", "sync", taskId, 2L), policy, true);

        // Skewed batch: 40 tiny 20B events + 4 large ~4KB events (single slice average could exceed 1s burst if unsplit)
        List<TapRecordEvent> skewed = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            skewed.add(TapInsertRecordEvent.create().init().table("t").after(Map.of("i", i)));
        }
        String bigVal = "x".repeat(4_000);
        for (int i = 0; i < 4; i++) {
            skewed.add(TapInsertRecordEvent.create().init().table("t").after(Map.of("i", 100 + i, "big", bigVal)));
        }
        List<TapRecordEvent> passed = new ArrayList<>();
        TaskThrottleRuntime.AdmitOutcome outcome = runtime.admitSourceBatch(
                "src-1", "conn-1", ThrottlePhase.FULL, skewed, e -> "t", () -> false,
                slice -> passed.addAll(slice.events()));
        assertEquals(TaskThrottleRuntime.AdmitOutcome.ADMITTED, outcome);
        assertEquals(44, passed.size());

        // Lazy runtime opened with authorizedRunEpoch=0 MUST NOT mark protectedRun or attach ?runEpoch=0
        String lazyTaskId = new ObjectId().toHexString();
        registry.registerRunningTask(lazyTaskId, "t1", "migrate");
        AgentThrottleDeltaResponse delta = AgentThrottleDeltaResponse.builder()
                .cursor(50L)
                .directives(List.of(AgentThrottleDeltaResponse.TaskDirective.builder()
                        .tenantId("t1").taskKind("migrate").taskId(lazyTaskId).authorizedRunEpoch(0L).protectedRun(false)
                        .policy(policy).build()))
                .build();
        registry.applyDelta(delta);
        TaskThrottleRuntime lazyRt = registry.getActive(lazyTaskId);
        assertNotNull(lazyRt);
        assertFalse(lazyRt.isProtectedRun());
        assertEquals("Task/syncProgress/" + lazyTaskId,
                lazyRt.buildSyncProgressResource("Task/syncProgress/" + lazyTaskId, "agent-it-1"));

        // Receipt POST failure MUST requeue receipts so next cycle delivers them cleanly
        ClientMongoOperator mockOp = mock(ClientMongoOperator.class);
        AgentThrottleSyncClient syncClient = new AgentThrottleSyncClient(mockOp, registry);
        doThrow(new RuntimeException("transient outage"))
                .when(mockOp).postOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_RECEIPTS), eq(Object.class));
        assertFalse(syncClient.flushPendingReceipts());
        List<TaskThrottleApplyReceipt> pendingAfterFail = registry.drainPendingReceipts();
        assertFalse(pendingAfterFail.isEmpty());
        registry.requeueReceipts(pendingAfterFail);
        doReturn(Map.of()).when(mockOp).postOne(anyMap(), eq(AgentThrottleSyncClient.RESOURCE_RECEIPTS), eq(Object.class));
        assertTrue(syncClient.flushPendingReceipts());
    }

    @Test
    void applyDeltaHoldsCursorOnFailureAndSchedulerWiresDagTopologyCorrectly() {
        String taskId = new ObjectId().toHexString();
        TaskThrottleRuntime runtime = registry.openRuntime(new TaskRunKey("t1", "sync", taskId, 3L), null, true);

        // Failing directive leaves lastDeltaCursor unchanged; successful pass advances cursor
        registry.applyDelta(AgentThrottleDeltaResponse.builder()
                .cursor(200L)
                .directives(List.of(AgentThrottleDeltaResponse.TaskDirective.builder()
                        .taskId(taskId)
                        .authorizedRunEpoch(3L)
                        .policy(null)
                        .build()))
                .build());
        assertEquals(200L, registry.getLastDeltaCursor());

        TaskThrottleRuntime spyRuntime = spy(runtime);
        registry.unregisterRunningTask(taskId);
        @SuppressWarnings("unchecked")
        java.util.concurrent.ConcurrentHashMap<String, TaskThrottleRuntime> map =
                (java.util.concurrent.ConcurrentHashMap<String, TaskThrottleRuntime>) ReflectionTestUtils.getField(registry, "activeRuntimes");
        map.put(taskId, spyRuntime);
        doThrow(new RuntimeException("simulated apply failure")).when(spyRuntime).applyPolicy(any(), any(), any());

        TaskThrottlePolicyDto p = TaskThrottlePolicyDto.builder().taskId(taskId).desiredVersion(2L).build();
        registry.applyDelta(AgentThrottleDeltaResponse.builder()
                .cursor(300L)
                .directives(List.of(AgentThrottleDeltaResponse.TaskDirective.builder()
                        .taskId(taskId).authorizedRunEpoch(3L).policy(p).build()))
                .build());
        assertEquals(200L, registry.getLastDeltaCursor()); // Held at 200 on error!

        // DAG topology extraction separates independent branches for WriteBackpressureSignal
        TableNode s1 = new TableNode();
        s1.setId("src-1");
        TableNode s2 = new TableNode();
        s2.setId("src-2");
        TableNode t1 = new TableNode();
        t1.setId("tgt-1");
        TableNode t2 = new TableNode();
        t2.setId("tgt-2");
        List<Node> nodes = List.of(s1, s2, t1, t2);
        List<Edge> edges = List.of(new Edge("src-1", "tgt-1"), new Edge("src-2", "tgt-2"));
        DAG dag = DAG.build(new com.tapdata.tm.commons.task.dto.Dag(edges, nodes));
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        taskDto.setDag(dag);
        Map<String, java.util.Set<String>> topology = TapdataTaskScheduler.extractSourceToTargetTopology(taskDto);
        assertEquals(java.util.Set.of("tgt-1"), topology.get("src-1"));
        assertEquals(java.util.Set.of("tgt-2"), topology.get("src-2"));

        runtime.registerTopology(topology);
        runtime.getBackpressureSignal().markTargetWaiting("tgt-1", WaitReason.RATE_WAIT);
        assertTrue(runtime.getBackpressureSignal().shouldPauseSourcePull("src-1"));
        assertFalse(runtime.getBackpressureSignal().shouldPauseSourcePull("src-2"));
    }
}
