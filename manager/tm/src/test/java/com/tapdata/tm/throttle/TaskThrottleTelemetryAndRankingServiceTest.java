package com.tapdata.tm.throttle;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.AgentThrottleCapabilities;
import com.tapdata.tm.commons.throttle.AgentThrottleDeltaResponse;
import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.TaskThrottleApplyReceipt;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import com.tapdata.tm.commons.throttle.TimeQuality;
import com.tapdata.tm.commons.throttle.WaitReason;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.chart.ChartViewService;
import com.tapdata.tm.throttle.controller.AgentThrottleController;
import com.tapdata.tm.throttle.dto.TaskThrottleLiveStatusView;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyUpdateRequest;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyView;
import com.tapdata.tm.throttle.dto.TaskThrottleRankingResponse;
import com.tapdata.tm.throttle.entity.TaskThrottleChangeLogEntity;
import com.tapdata.tm.throttle.entity.TaskThrottlePolicyEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleTelemetryEntity;
import com.tapdata.tm.throttle.service.TaskThrottleExecutionGuardService;
import com.tapdata.tm.throttle.service.TaskThrottlePolicyService;
import com.tapdata.tm.throttle.service.TaskThrottleSampleConfigService;
import com.tapdata.tm.throttle.service.TaskThrottleTelemetryAndRankingService;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TaskThrottleTelemetryAndRankingServiceTest {

    private MutableClock clock;
    private InMemoryMongoOperations mongo;
    private TaskThrottlePolicyService policyService;
    private TaskThrottleExecutionGuardService guardService;
    private TaskThrottleSampleConfigService sampleConfigService;
    private TaskThrottleTelemetryAndRankingService telemetryService;
    private AgentThrottleController agentController;
    private ChartViewService chartViewService;
    private UserDetail user;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(1_700_000_000_000L);
        mongo = new InMemoryMongoOperations();
        policyService = new TaskThrottlePolicyService(mongo.ops(), clock);
        guardService = new TaskThrottleExecutionGuardService(mongo.ops(), clock);
        sampleConfigService = new TaskThrottleSampleConfigService(mongo.ops(), clock);
        telemetryService = new TaskThrottleTelemetryAndRankingService(mongo.ops(), sampleConfigService, clock);
        chartViewService = mock(ChartViewService.class);
        telemetryService.setChartViewService(chartViewService);

        policyService.setExecutionGuardService(guardService);
        policyService.setTelemetryAndRankingService(telemetryService);

        agentController = new AgentThrottleController(guardService, telemetryService);
        user = mock(UserDetail.class);
        when(user.getCustomerId()).thenReturn("tenant-1");
        when(user.getUserId()).thenReturn("user-1");
        when(user.getUsername()).thenReturn("alice");
    }

    @Test
    void pullDeltaSupportsCursorIncrementsAndFencingForDisplacedAgent() {
        registerAgent("agent-1");
        registerAgent("agent-2");

        ObjectId oid = MongoUtils.toObjectId("65f000000000000000000201");
        String taskId = oid.toHexString();
        policyService.updatePolicy("sync", taskId, updateReq(0L, 1_000_000L), user);

        TaskDto taskDto = buildTask(oid, "sync", "t1");
        guardService.authorizeStartOrTakeover(taskDto, "agent-1");

        AgentThrottleDeltaResponse full = agentController.pullDelta("agent-1", 0L).getData();
        assertTrue(full.isFullSync());
        assertEquals(1, full.getDirectives().size());
        assertFalse(full.getDirectives().get(0).isFenced());
        assertEquals(1L, full.getDirectives().get(0).getAuthorizedRunEpoch());

        long cursor1 = full.getCursor();
        AgentThrottleDeltaResponse emptyIncr = agentController.pullDelta("agent-1", cursor1).getData();
        assertFalse(emptyIncr.isFullSync());
        assertTrue(emptyIncr.getDirectives().isEmpty());

        // Advance policy on same agent -> incremental delta returns v2
        clock.advanceMillis(100L);
        policyService.updatePolicy("sync", taskId, updateReq(1L, 2_000_000L), user);
        AgentThrottleDeltaResponse v2Incr = agentController.pullDelta("agent-1", cursor1).getData();
        assertEquals(1, v2Incr.getDirectives().size());
        assertEquals(2L, v2Incr.getDirectives().get(0).getPolicy().getDesiredVersion());

        // Takeover to agent-2 -> old agent-1 must receive fenced=true directive
        clock.advanceMillis(100L);
        agentController.uploadStopEvidence(com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto.builder()
                .taskId(taskId)
                .oldAgentId("agent-1")
                .oldRunEpoch(1L)
                .evidenceRef("graceful-1")
                .processExited(false)
                .writeSettled(true)
                .build());
        guardService.authorizeStartOrTakeover(taskDto, "agent-2");

        AgentThrottleDeltaResponse oldAgentDelta = agentController.pullDelta("agent-1", v2Incr.getCursor()).getData();
        assertEquals(1, oldAgentDelta.getDirectives().size());
        assertTrue(oldAgentDelta.getDirectives().get(0).isFenced());
        assertEquals(2L, oldAgentDelta.getDirectives().get(0).getAuthorizedRunEpoch());
    }

    @Test
    void ingestReceiptsHandlesIdempotencyMonotonicOrderingAndFailureRetention() {
        registerAgent("agent-1");
        ObjectId oid = MongoUtils.toObjectId("65f000000000000000000202");
        String taskId = oid.toHexString();

        policyService.updatePolicy("sync", taskId, updateReq(0L, 1_000_000L), user);
        TaskDto taskDto = buildTask(oid, "sync", "t2");
        guardService.authorizeStartOrTakeover(taskDto, "agent-1");
        clock.advanceMillis(10L);
        policyService.updatePolicy("sync", taskId, updateReq(1L, 2_000_000L), user);

        String taskKey = TaskThrottlePolicyEntity.buildTaskKey("tenant-1", "sync", taskId);

        // Apply v2 first -> appliedVersion = 2
        telemetryService.ingestReceipts("agent-1", List.of(TaskThrottleApplyReceipt.builder()
                .taskKey(taskKey)
                .runEpoch(1L)
                .policyVersion(2L)
                .outcome("APPLIED")
                .skippedVersions(List.of(1L))
                .appliedAt(clock.millis())
                .build()));

        // Redelivery of v2 + out-of-order v1 + stale epoch 0 must NOT regress appliedVersion=2
        telemetryService.ingestReceipts("agent-1", List.of(
                TaskThrottleApplyReceipt.builder().taskKey(taskKey).runEpoch(1L).policyVersion(2L).outcome("APPLIED").build(),
                TaskThrottleApplyReceipt.builder().taskKey(taskKey).runEpoch(1L).policyVersion(1L).outcome("APPLIED").build(),
                TaskThrottleApplyReceipt.builder().taskKey(taskKey).runEpoch(0L).policyVersion(9L).outcome("APPLIED").build()
        ));

        TaskThrottlePolicyView afterApplied = policyService.getPolicyView("sync", taskId, user);
        assertEquals(2L, afterApplied.getAppliedVersion());
        assertEquals(2_000_000L, afterApplied.getAppliedPhases().get(ThrottlePhase.FULL).getBytesPerSec());
        assertEquals(TaskThrottlePolicyEntity.APPLY_APPLIED, afterApplied.getApplyStatus());

        // Deduplicated receipt rows in ChangeLog: exactly 1 row for (taskKey,1,2)
        long receipt2Count = mongo.ops().findAll(TaskThrottleChangeLogEntity.class).stream()
                .filter(c -> TaskThrottleChangeLogEntity.EVENT_APPLY_RECEIPT.equals(c.getEventType())
                        && c.getRunEpoch() == 1L && c.getPolicyVersion() == 2L)
                .count();
        assertEquals(1L, receipt2Count);

        // Failed receipt keeps last appliedVersion=2 and appliedPhases intact while marking APPLY_FAILED
        clock.advanceMillis(10L);
        policyService.updatePolicy("sync", taskId, updateReq(2L, 3_000_000L), user);
        telemetryService.ingestReceipts("agent-1", List.of(TaskThrottleApplyReceipt.builder()
                .taskKey(taskKey)
                .runEpoch(1L)
                .policyVersion(3L)
                .outcome("FAILED")
                .reason("Agent apply rejected")
                .appliedAt(clock.millis())
                .build()));

        TaskThrottlePolicyView afterFailed = policyService.getPolicyView("sync", taskId, user);
        assertEquals(2L, afterFailed.getAppliedVersion());
        assertEquals(2_000_000L, afterFailed.getAppliedPhases().get(ThrottlePhase.FULL).getBytesPerSec());
        assertEquals(TaskThrottlePolicyEntity.APPLY_FAILED, afterFailed.getApplyStatus());
        assertEquals("Agent apply rejected", afterFailed.getLastApplyReason());
    }

    @Test
    void evaluateLiveStatusEnforcesEpochSeqBackfillCutoffAnd15sUnknownExpiry() {
        registerAgent("agent-1");
        ObjectId oid = MongoUtils.toObjectId("65f000000000000000000203");
        String taskId = oid.toHexString();
        policyService.updatePolicy("sync", taskId, updateReq(0L, 1_000_000L), user);
        guardService.authorizeStartOrTakeover(buildTask(oid, "sync", "t3"), "agent-1");

        // Fresh RATE_WAIT telemetry -> active
        telemetryService.ingestTelemetry("agent-1", Map.of(taskId, List.of(
                sample(taskId, 1L, 10L, ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                        clock.millis() - 5_000L, clock.millis(), WaitReason.RATE_WAIT,
                        EnumSet.of(ThrottleDimension.BYTES), 1_000L, SampleQuality.VALID, TimeQuality.ALIGNED)
        )));
        TaskThrottleLiveStatusView live = telemetryService.evaluateLiveStatus(taskId);
        assertTrue(live.isThrottlingActive());
        assertFalse(live.isStatusUnknown());
        assertEquals(Set.of(ThrottleDimension.BYTES), live.getActiveDimensionsByPhase().get(ThrottlePhase.FULL));

        // Historical backfill (>30s old) and lower seq / stale epoch must NOT override current live state
        telemetryService.ingestTelemetry("agent-1", Map.of(taskId, List.of(
                sample(taskId, 1L, 99L, ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                        clock.millis() - 65_000L, clock.millis() - 60_000L, WaitReason.NONE,
                        EnumSet.noneOf(ThrottleDimension.class), 500L, SampleQuality.VALID, TimeQuality.ALIGNED),
                sample(taskId, 1L, 5L, ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                        clock.millis() - 5_000L, clock.millis(), WaitReason.NONE,
                        EnumSet.noneOf(ThrottleDimension.class), 500L, SampleQuality.VALID, TimeQuality.ALIGNED)
        )));
        assertTrue(telemetryService.evaluateLiveStatus(taskId).isThrottlingActive());

        // Advance > 15s without new telemetry -> statusUnknown=true, throttlingActive=false
        clock.advanceMillis(16_000L);
        TaskThrottleLiveStatusView expired = telemetryService.evaluateLiveStatus(taskId);
        assertTrue(expired.isStatusUnknown());
        assertFalse(expired.isThrottlingActive());

        // Duplicate upload (seq=10) must be deduplicated in Mongo, leaving 3 distinct docs (seq 10, 99, 5)
        telemetryService.ingestTelemetry("agent-1", Map.of(taskId, List.of(
                sample(taskId, 1L, 10L, ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                        clock.millis() - 21_000L, clock.millis() - 16_000L, WaitReason.RATE_WAIT,
                        EnumSet.of(ThrottleDimension.BYTES), 1_000L, SampleQuality.VALID, TimeQuality.ALIGNED)
        )));
        assertEquals(3, mongo.ops().find(new Query(), TaskThrottleTelemetryEntity.class).size());
    }

    @Test
    void rankingComputesUnionDeduplicatedCoverageAt90PercentAndExcludesInvalidTasks() {
        long end = clock.millis();
        long start = end - 300_000L;

        ObjectId oidA = MongoUtils.toObjectId("65f000000000000000000301");
        ObjectId oidB = MongoUtils.toObjectId("65f000000000000000000302");
        ObjectId oidC = MongoUtils.toObjectId("65f000000000000000000303");
        ObjectId oidD = MongoUtils.toObjectId("65f000000000000000000304");
        ObjectId oidE = MongoUtils.toObjectId("65f000000000000000000305");

        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(List.of(
                buildTask(oidA, "sync", "task-A-overlap"),
                buildTask(oidB, "migrate", "task-B-top"),
                buildTask(oidC, "logCollector", "task-C-low-cov"),
                buildTask(oidD, "sync", "task-D-unaligned"),
                buildTask(oidE, "sync", "task-E-missing-bytes")
        ));

        // Enable throttle policy only on task-A to verify throttleEnabled flag propagation
        policyService.updatePolicy("sync", oidA.toHexString(), updateReq(0L, 5_000_000L), user);

        telemetryService.ingestTelemetry("agent-1", Map.of(
                // Task A: FULL [0, 200s] + CDC [100s, 280s] -> union = 280s (93.3% >= 90%), total bytes = 280_000_000 -> 1.0 MB/s
                oidA.toHexString(), List.of(
                        sample(oidA.toHexString(), 1L, 1L, ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                                start, start + 200_000L, WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class),
                                200_000_000L, SampleQuality.VALID, TimeQuality.ALIGNED),
                        sample(oidA.toHexString(), 1L, 2L, ThrottlePhase.CDC, ThrottleDirection.OUTPUT,
                                start + 100_000L, start + 280_000L, WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class),
                                80_000_000L, SampleQuality.VALID, TimeQuality.ALIGNED)
                ),
                // Task B: [0, 290s] -> 290s (96.7%), 580_000_000 -> 2.0 MB/s
                oidB.toHexString(), List.of(
                        sample(oidB.toHexString(), 1L, 1L, ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                                start, start + 290_000L, WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class),
                                580_000_000L, SampleQuality.VALID, TimeQuality.ALIGNED)
                ),
                // Task C: 240s -> 80% < 90% -> excluded
                oidC.toHexString(), List.of(
                        sample(oidC.toHexString(), 1L, 1L, ThrottlePhase.CDC, ThrottleDirection.OUTPUT,
                                start, start + 240_000L, WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class),
                                1_000_000L, SampleQuality.VALID, TimeQuality.ALIGNED)
                ),
                // Task D: unaligned clock -> excluded
                oidD.toHexString(), List.of(
                        sample(oidD.toHexString(), 1L, 1L, ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                                start, start + 295_000L, WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class),
                                1_000_000L, SampleQuality.VALID, TimeQuality.UNALIGNED)
                ),
                // Task E: pending sample quality -> excluded
                oidE.toHexString(), List.of(
                        sample(oidE.toHexString(), 1L, 1L, ThrottlePhase.CDC, ThrottleDirection.OUTPUT,
                                start, start + 295_000L, WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class),
                                null, SampleQuality.ACCUMULATING, TimeQuality.ALIGNED)
                )
        ));

        TaskThrottleRankingResponse ranking =
                telemetryService.queryEstimatedDataRateRanking(user, ThrottleDirection.OUTPUT, 300L, 5);

        assertEquals(ThrottleConstants.METER_VERSION, ranking.getMeterVersion());
        assertEquals(2, ranking.getItems().size());
        // Sorted descending: Task B (2.0 MB/s) before Task A (1.0 MB/s)
        assertEquals("task-B-top", ranking.getItems().get(0).getTaskName());
        assertEquals(2.0d, ranking.getItems().get(0).getEstimatedMbPerSec(), 1e-6);
        assertFalse(ranking.getItems().get(0).isThrottleEnabled());

        TaskThrottleRankingResponse.RankingItem itemA = ranking.getItems().get(1);
        assertEquals("task-A-overlap", itemA.getTaskName());
        assertEquals(280.0d, itemA.getCoveredSeconds(), 1e-6); // union, NOT 380s
        assertEquals(1.0d, itemA.getEstimatedMbPerSec(), 1e-6);
        assertTrue(itemA.isThrottleEnabled());

        assertEquals(3, ranking.getExcluded().size());
        Map<String, String> excludedReasons = new java.util.HashMap<>();
        for (TaskThrottleRankingResponse.ExcludedItem ex : ranking.getExcluded()) {
            excludedReasons.put(ex.getTaskName(), ex.getReason());
        }
        assertEquals("COVERAGE_BELOW_90_PERCENT", excludedReasons.get("task-C-low-cov"));
        assertEquals("CLOCK_UNALIGNED", excludedReasons.get("task-D-unaligned"));
        assertEquals("MISSING_BYTES_OR_SAMPLE", excludedReasons.get("task-E-missing-bytes"));
    }

    @Test
    void evaluateLiveStatusesBatchPopulatesPolicyConfiguredAndConfiguredPhases() {
        registerAgent("agent-1");
        String configuredId = MongoUtils.toObjectId("65f000000000000000000401").toHexString();
        String disabledId = MongoUtils.toObjectId("65f000000000000000000402").toHexString();
        String unconfiguredId = MongoUtils.toObjectId("65f000000000000000000403").toHexString();

        policyService.updatePolicy("sync", configuredId, updateReq(0L, 1_500_000L), user);
        policyService.updatePolicy("sync", disabledId, TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(0L)
                .phases(Map.of(ThrottlePhase.FULL, ThrottlePhaseLimit.builder()
                        .enabled(false)
                        .bytesPerSec(1_500_000L)
                        .build()))
                .build(), user);

        Map<String, TaskThrottleLiveStatusView> views = telemetryService.evaluateLiveStatuses(
                List.of(configuredId, disabledId, unconfiguredId));

        TaskThrottleLiveStatusView configured = views.get(configuredId);
        assertNotNull(configured);
        assertTrue(configured.isPolicyConfigured());
        assertEquals(Set.of(ThrottlePhase.FULL), configured.getConfiguredPhases());
        assertFalse(configured.isThrottlingActive());

        TaskThrottleLiveStatusView disabled = views.get(disabledId);
        assertNotNull(disabled);
        assertFalse(disabled.isPolicyConfigured());
        assertTrue(disabled.getConfiguredPhases().isEmpty());

        TaskThrottleLiveStatusView unconfigured = views.get(unconfiguredId);
        assertNotNull(unconfigured);
        assertFalse(unconfigured.isPolicyConfigured());
        assertTrue(unconfigured.getConfiguredPhases().isEmpty());
    }

    private void registerAgent(String agentId) {
        guardService.registerCapabilities(AgentThrottleCapabilities.builder()
                .agentId(agentId)
                .throttleSupported(true)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .supportedTaskKinds(Set.of("sync", "migrate", "logCollector"))
                .build());
    }

    private static TaskDto buildTask(ObjectId id, String syncType, String name) {
        TaskDto dto = new TaskDto();
        dto.setId(id);
        dto.setSyncType(syncType);
        dto.setType("initial_sync+cdc");
        dto.setName(name);
        dto.setCustomId("tenant-1");
        dto.setUserId("user-1");
        return dto;
    }

    private static TaskThrottlePolicyUpdateRequest updateReq(long expectedVer, long bytesPerSec) {
        return TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(expectedVer)
                .phases(Map.of(ThrottlePhase.FULL, ThrottlePhaseLimit.builder()
                        .enabled(true)
                        .bytesPerSec(bytesPerSec)
                        .build()))
                .build();
    }

    private static TaskThrottleTelemetrySample sample(
            String taskId,
            long runEpoch,
            long seq,
            ThrottlePhase phase,
            ThrottleDirection dir,
            long startUtc,
            long endUtc,
            WaitReason waitReason,
            Set<ThrottleDimension> waitingDims,
            Long estimatedBytes,
            SampleQuality sampleQuality,
            TimeQuality timeQuality) {
        return TaskThrottleTelemetrySample.builder()
                .runEpoch(runEpoch)
                .sampleSeq(seq)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .phase(phase)
                .direction(dir)
                .windowStartUtc(startUtc)
                .windowEndUtc(endUtc)
                .durationNanos((endUtc - startUtc) * 1_000_000L)
                .timeQuality(timeQuality)
                .appliedVersion(1L)
                .admittedEvents(1000L)
                .estimatedAdmittedBytes(estimatedBytes)
                .sampleCount(estimatedBytes == null ? 0L : 10L)
                .sampleQuality(sampleQuality)
                .waitReason(waitReason)
                .waitingDimensions(waitingDims)
                .bufferWatermark(0.2d)
                .logPositionStatus(LogPositionStatus.NORMAL)
                .build();
    }

    private static final class MutableClock extends Clock {
        private final AtomicLong millis;

        MutableClock(long initialMillis) {
            this.millis = new AtomicLong(initialMillis);
        }

        void advanceMillis(long delta) {
            millis.addAndGet(delta);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis.get());
        }
    }
}
