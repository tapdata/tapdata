package com.tapdata.tm.throttle;

import com.tapdata.tm.base.exception.BizException;
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
import com.tapdata.tm.throttle.service.TaskThrottleIndexInitializer;
import com.tapdata.tm.throttle.service.TaskThrottlePolicyService;
import com.tapdata.tm.throttle.service.TaskThrottleSampleConfigService;
import com.tapdata.tm.throttle.service.TaskThrottleTelemetryAndRankingService;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

    @Test
    void ingestReceiptsFallsBackToTaskIdWhenTenantPrefixIsMissingAndCanonicalizesAuditLog() {
        registerAgent("agent-1");
        ObjectId oid = MongoUtils.toObjectId("65f000000000000000000410");
        String taskId = oid.toHexString();

        UserDetail daasUser = mock(UserDetail.class);
        when(daasUser.getCustomerId()).thenReturn(null);
        when(daasUser.getUserId()).thenReturn("daas-user-42");
        when(daasUser.getUsername()).thenReturn("admin");

        TaskDto taskDto = new TaskDto();
        taskDto.setId(oid);
        taskDto.setSyncType("migrate");
        taskDto.setType("initial_sync+cdc");
        taskDto.setName("daas-migrate");
        taskDto.setCustomId(null);
        taskDto.setUserId("daas-user-42");

        policyService.updatePolicy("migrate", taskId, updateReq(0L, 1_000_000L), daasUser);
        guardService.authorizeStartOrTakeover(taskDto, "agent-1");

        // Receipt arrives with blank tenant prefix ":migrate:<taskId>"
        String blankTenantKey = TaskThrottlePolicyEntity.buildTaskKey("", "migrate", taskId);
        telemetryService.ingestReceipts("agent-1", List.of(TaskThrottleApplyReceipt.builder()
                .taskKey(blankTenantKey)
                .runEpoch(1L)
                .policyVersion(1L)
                .outcome("APPLIED")
                .appliedAt(clock.millis())
                .build()));

        TaskThrottlePolicyView view = policyService.getPolicyView("migrate", taskId, daasUser);
        assertEquals(1L, view.getAppliedVersion());
        assertEquals(TaskThrottlePolicyEntity.APPLY_APPLIED, view.getApplyStatus());
        assertNotNull(view.getAppliedPhases());

        String canonicalKey = TaskThrottlePolicyEntity.buildTaskKey("daas-user-42", "migrate", taskId);
        TaskThrottleChangeLogEntity receiptLog = mongo.ops().findAll(TaskThrottleChangeLogEntity.class).stream()
                .filter(c -> TaskThrottleChangeLogEntity.EVENT_APPLY_RECEIPT.equals(c.getEventType())
                        && taskId.equals(c.getTaskId()))
                .findFirst()
                .orElse(null);
        assertNotNull(receiptLog);
        assertEquals(canonicalKey, receiptLog.getTaskKey());
        assertEquals("daas-user-42", receiptLog.getTenantId());
        assertEquals("migrate", receiptLog.getTaskKind());
    }

    @Test
    void indexInitializerEnsuresTtlAndHotQueryIndexesResilientlyAndIngestSetsExpireAt() {
        registerAgent("agent-1");
        ObjectId oid = MongoUtils.toObjectId("65f000000000000000000411");
        String taskId = oid.toHexString();
        policyService.updatePolicy("sync", taskId, updateReq(0L, 1_000_000L), user);
        guardService.authorizeStartOrTakeover(buildTask(oid, "sync", "t-ttl"), "agent-1");

        long endUtc = clock.millis();
        telemetryService.ingestTelemetry("agent-1", Map.of(taskId, List.of(
                sample(taskId, 1L, 1L, ThrottlePhase.CDC, ThrottleDirection.OUTPUT,
                        endUtc - 5_000L, endUtc, WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class),
                        10_000L, SampleQuality.VALID, TimeQuality.ALIGNED)
        )));
        List<TaskThrottleTelemetryEntity> savedDocs = mongo.ops().findAll(TaskThrottleTelemetryEntity.class);
        assertEquals(1, savedDocs.size());
        assertEquals(new Date(endUtc), savedDocs.get(0).getExpireAt());

        MongoOperations mockMongo = mock(MongoOperations.class);
        IndexOperations telemetryIdxOps = mock(IndexOperations.class);
        IndexOperations policyIdxOps = mock(IndexOperations.class);
        doReturn(telemetryIdxOps).when(mockMongo).indexOps(TaskThrottleTelemetryEntity.class);
        doReturn(policyIdxOps).when(mockMongo).indexOps(TaskThrottlePolicyEntity.class);
        doThrow(new RuntimeException("existing ttl options conflict"))
                .doReturn("idx_ok")
                .when(telemetryIdxOps).ensureIndex(any(IndexDefinition.class));

        TaskThrottleIndexInitializer initializer = new TaskThrottleIndexInitializer(mockMongo);
        assertDoesNotThrow(initializer::ensureIndexes);
        verify(telemetryIdxOps, times(2)).ensureIndex(any(IndexDefinition.class));
        verify(policyIdxOps, times(1)).ensureIndex(any(IndexDefinition.class));
    }

    @Test
    void liveStatusExposesNonRateWaitsRejectsFutureEpochAndResetsWhenAuthorizedEpochDrops() {
        registerAgent("agent-1");
        ObjectId oid = MongoUtils.toObjectId("65f000000000000000000412");
        String taskId = oid.toHexString();
        policyService.updatePolicy("sync", taskId, updateReq(0L, 1_000_000L), user);
        guardService.authorizeStartOrTakeover(buildTask(oid, "sync", "t-live-wait"), "agent-1"); // epoch=1

        // 1. Future runEpoch (99 > authorized 1) must NOT hijack tracker
        telemetryService.ingestTelemetry("agent-1", Map.of(taskId, List.of(
                sample(taskId, 99L, 1L, ThrottlePhase.CDC, ThrottleDirection.OUTPUT,
                        clock.millis() - 5_000L, clock.millis(), WaitReason.RATE_WAIT,
                        EnumSet.of(ThrottleDimension.QPS), 1_000L, SampleQuality.VALID, TimeQuality.ALIGNED)
        )));
        TaskThrottleLiveStatusView afterFuture = telemetryService.evaluateLiveStatus(taskId);
        assertNull(afterFuture.getLastReportedAtUtc());
        assertEquals(0L, afterFuture.getRunEpoch());

        // 2. Non-RATE_WAIT (WRITE_IO_WAIT, BUFFER_WAIT, BACKPRESSURE_WAIT) is preserved in activeWaits without faking throttlingActive
        telemetryService.ingestTelemetry("agent-1", Map.of(taskId, List.of(
                TaskThrottleTelemetrySample.builder()
                        .runEpoch(1L).sampleSeq(2L).meterVersion(ThrottleConstants.METER_VERSION)
                        .phase(ThrottlePhase.CDC).direction(ThrottleDirection.OUTPUT)
                        .windowStartUtc(clock.millis() - 5_000L).windowEndUtc(clock.millis())
                        .durationNanos(5_000_000_000L).timeQuality(TimeQuality.ALIGNED).appliedVersion(1L)
                        .admittedEvents(100L).estimatedAdmittedBytes(10_000L).sampleCount(2L)
                        .sampleQuality(SampleQuality.VALID).waitReason(WaitReason.WRITE_IO_WAIT)
                        .waitingDimensions(EnumSet.noneOf(ThrottleDimension.class)).bufferWatermark(0.85d)
                        .logPositionStatus(null)
                        .build()
        )));
        TaskThrottleLiveStatusView liveIoWait = telemetryService.evaluateLiveStatus(taskId);
        assertFalse(liveIoWait.isThrottlingActive());
        assertEquals(1, liveIoWait.getActiveWaits().size());
        assertEquals(WaitReason.WRITE_IO_WAIT, liveIoWait.getActiveWaits().get(0).getWaitReason());
        assertEquals(LogPositionStatus.UNKNOWN, liveIoWait.getActiveWaits().get(0).getLogPositionStatus());

        // 3. Policy takeover/rebuild pushes authorizedEpoch to 2, then lowers/rebuilds to 1: tracker resets cleanly
        String taskKey = TaskThrottlePolicyEntity.buildTaskKey("tenant-1", "sync", taskId);
        mongo.ops().updateFirst(
                Query.query(org.springframework.data.mongodb.core.query.Criteria.where("_id").is(taskKey)),
                new org.springframework.data.mongodb.core.query.Update().set("currentRunEpoch", 2L),
                TaskThrottlePolicyEntity.class);
        telemetryService.ingestTelemetry("agent-1", Map.of(taskId, List.of(
                sample(taskId, 2L, 1L, ThrottlePhase.CDC, ThrottleDirection.INPUT,
                        clock.millis() - 5_000L, clock.millis(), WaitReason.BUFFER_WAIT,
                        EnumSet.noneOf(ThrottleDimension.class), 1_000L, SampleQuality.VALID, TimeQuality.ALIGNED)
        )));
        assertEquals(2L, telemetryService.evaluateLiveStatus(taskId).getRunEpoch());

        mongo.ops().updateFirst(
                Query.query(org.springframework.data.mongodb.core.query.Criteria.where("_id").is(taskKey)),
                new org.springframework.data.mongodb.core.query.Update().set("currentRunEpoch", 1L),
                TaskThrottlePolicyEntity.class);
        clock.advanceMillis(20_000L);
        assertTrue(telemetryService.evaluateLiveStatus(taskId).isStatusUnknown());
        telemetryService.ingestTelemetry("agent-1", Map.of(taskId, List.of(
                sample(taskId, 1L, 1L, ThrottlePhase.CDC, ThrottleDirection.INPUT,
                        clock.millis() - 5_000L, clock.millis(), WaitReason.BACKPRESSURE_WAIT,
                        EnumSet.noneOf(ThrottleDimension.class), 1_000L, SampleQuality.VALID, TimeQuality.ALIGNED)
        )));
        TaskThrottleLiveStatusView recovered = telemetryService.evaluateLiveStatus(taskId);
        assertEquals(1L, recovered.getRunEpoch());
        assertFalse(recovered.isStatusUnknown());
        assertEquals(WaitReason.BACKPRESSURE_WAIT, recovered.getActiveWaits().get(0).getWaitReason());
    }

    @Test
    void rankingHonorsCustomWindowSecondsInRangeAndRejectsInvalidValues() {
        long end = clock.millis();
        long start600 = end - 600_000L;
        ObjectId oid = MongoUtils.toObjectId("65f000000000000000000413");
        String taskId = oid.toHexString();
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(List.of(buildTask(oid, "sync", "win-600-task")));

        // 560s covered out of 600s -> 93.3% >= 90%, 560MB / 560s = 1.0 MB/s
        telemetryService.ingestTelemetry("agent-1", Map.of(taskId, List.of(
                sample(taskId, 1L, 1L, ThrottlePhase.CDC, ThrottleDirection.OUTPUT,
                        start600, start600 + 560_000L, WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class),
                        560_000_000L, SampleQuality.VALID, TimeQuality.ALIGNED)
        )));
        TaskThrottleRankingResponse win600 =
                telemetryService.queryEstimatedDataRateRanking(user, ThrottleDirection.OUTPUT, 600L, 5);
        assertEquals(600L, win600.getWindowSeconds());
        assertEquals(1, win600.getItems().size());
        assertEquals(1.0d, win600.getItems().get(0).getEstimatedMbPerSec(), 1e-6);

        for (long invalid : new long[]{-1L, 0L, 60L, 7200L}) {
            BizException ex = assertThrows(BizException.class,
                    () -> telemetryService.queryEstimatedDataRateRanking(user, ThrottleDirection.OUTPUT, invalid, 5));
            assertEquals(ThrottleConstants.ERR_INVALID_WINDOW, ex.getErrorCode());
        }
    }

    @Test
    void staleStopEvidenceDoesNotClearLiveStatusOfCurrentEpochAndRankingIsolatesRunEpoch() {
        registerAgent("agent-live-1");
        ObjectId t1Oid = MongoUtils.toObjectId("65f000000000000000000301");
        String t1Id = t1Oid.toHexString();
        TaskDto t1 = buildTask(t1Oid, "sync", "epoch-isolated-task");

        policyService.updatePolicy("sync", t1Id, updateReq(0L, 1_000_000L), user);
        assertEquals(1L, guardService.authorizeStartOrTakeover(t1, "agent-live-1").runEpoch());

        telemetryService.ingestTelemetry("agent-live-1", Map.of(t1Id, List.of(sample(
                t1Id, 1L, 1L, ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                clock.millis() - 300_000L, clock.millis(),
                WaitReason.NONE, Set.of(), 3_000_000_000L, SampleQuality.VALID, TimeQuality.ALIGNED))));

        assertNotNull(guardService.submitStopEvidence(com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto.builder()
                .taskId(t1Id).oldAgentId("agent-live-1").oldRunEpoch(1L).evidenceRef("ev-1").writeSettled(true).build(), false));
        registerAgent("agent-live-2");
        assertEquals(2L, guardService.authorizeStartOrTakeover(t1, "agent-live-2").runEpoch());

        telemetryService.ingestTelemetry("agent-live-2", Map.of(t1Id, List.of(sample(
                t1Id, 2L, 1L, ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                clock.millis() - 300_000L, clock.millis(),
                WaitReason.RATE_WAIT, Set.of(ThrottleDimension.BYTES), 300_000_000L, SampleQuality.VALID, TimeQuality.ALIGNED))));
        assertTrue(telemetryService.evaluateLiveStatus(t1Id).isThrottlingActive());

        // Stale stop evidence arriving late must NOT clear live status of epoch 2
        agentController.uploadStopEvidence(com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto.builder()
                .taskId(t1Id).oldAgentId("agent-live-1").oldRunEpoch(1L).evidenceRef("late-stale-ev").writeSettled(true).build());
        assertTrue(telemetryService.evaluateLiveStatus(t1Id).isThrottlingActive());

        // Ranking must ONLY aggregate epoch 2 samples (1 MB/s), NOT merge epoch 1 (10 MB/s) + epoch 2
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(List.of(t1));
        TaskThrottleRankingResponse ranking = telemetryService.queryEstimatedDataRateRanking(
                user, ThrottleDirection.OUTPUT, 300L, 5);
        assertEquals(1, ranking.getItems().size());
        assertEquals(300_000_000L, ranking.getItems().get(0).getTotalEstimatedBytes());
        assertEquals(1.0d, ranking.getItems().get(0).getEstimatedMbPerSec(), 0.05d);
    }

    @Test
    void applyThrottleStatusWhereRewritesPrePaginationCriteriaSafely() {
        registerAgent("agent-w-1");
        ObjectId enabledOid = MongoUtils.toObjectId("65f000000000000000000401");
        String enabledId = enabledOid.toHexString();
        policyService.updatePolicy("sync", enabledId, updateReq(0L, 1_000_000L), user);

        com.tapdata.tm.base.dto.Where enabledWhere = new com.tapdata.tm.base.dto.Where();
        enabledWhere.put("throttleStatus", "enabled");
        assertTrue(telemetryService.applyThrottleStatusWhere(enabledWhere));
        assertFalse(enabledWhere.containsKey("throttleStatus"));
        assertEquals(Map.of("$in", List.of(enabledOid)), enabledWhere.get("_id"));

        com.tapdata.tm.base.dto.Where disabledWhere = new com.tapdata.tm.base.dto.Where();
        disabledWhere.put("throttleStatus", "disabled");
        disabledWhere.put("_id", enabledOid);
        assertTrue(telemetryService.applyThrottleStatusWhere(disabledWhere));
        assertTrue(disabledWhere.containsKey("$and"));

        com.tapdata.tm.base.dto.Where waitingWhere = new com.tapdata.tm.base.dto.Where();
        waitingWhere.put("throttleStatus", "waiting_agent");
        assertFalse(telemetryService.applyThrottleStatusWhere(waitingWhere));

        telemetryService.ingestTelemetry("agent-w-1", Map.of(enabledId, List.of(sample(
                enabledId, 0L, 1L, ThrottlePhase.CDC, ThrottleDirection.INPUT,
                clock.millis() - 5_000L, clock.millis(),
                WaitReason.RATE_WAIT, Set.of(ThrottleDimension.QPS), 100L, SampleQuality.VALID, TimeQuality.ALIGNED))));
        com.tapdata.tm.base.dto.Where throttlingWhere = new com.tapdata.tm.base.dto.Where();
        throttlingWhere.put("throttleStatus", "throttling");
        assertTrue(telemetryService.applyThrottleStatusWhere(throttlingWhere));
        assertEquals(Map.of("$in", List.of(enabledOid)), throttlingWhere.get("_id"));

        // While enabledId is actively throttling, configured_idle returns false (empty set)
        com.tapdata.tm.base.dto.Where idleWhileActiveWhere = new com.tapdata.tm.base.dto.Where();
        idleWhileActiveWhere.put("throttleStatus", "configured_idle");
        assertFalse(telemetryService.applyThrottleStatusWhere(idleWhileActiveWhere));

        // Second configured task without active throttling matches configured_idle
        ObjectId idleOid = MongoUtils.toObjectId("65f000000000000000000402");
        policyService.updatePolicy("sync", idleOid.toHexString(), updateReq(0L, 2_000_000L), user);
        com.tapdata.tm.base.dto.Where idleWhere = new com.tapdata.tm.base.dto.Where();
        idleWhere.put("throttleStatus", "configured_idle");
        assertTrue(telemetryService.applyThrottleStatusWhere(idleWhere));
        assertEquals(Map.of("$in", List.of(idleOid)), idleWhere.get("_id"));

        // Advancing clock past 15s marks enabledId statusUnknown=true so unknown filter matches enabledId
        clock.advanceMillis(16_000L);
        com.tapdata.tm.base.dto.Where unknownWhere = new com.tapdata.tm.base.dto.Where();
        unknownWhere.put("throttleStatus", "unknown");
        assertTrue(telemetryService.applyThrottleStatusWhere(unknownWhere));
        assertEquals(Map.of("$in", List.of(enabledOid)), unknownWhere.get("_id"));
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
