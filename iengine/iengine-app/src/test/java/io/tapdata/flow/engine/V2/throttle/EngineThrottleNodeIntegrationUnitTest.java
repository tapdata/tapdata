package io.tapdata.flow.engine.V2.throttle;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.AgentThrottleDeltaResponse;
import com.tapdata.tm.commons.throttle.TaskThrottlePolicyDto;
import com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
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
}
