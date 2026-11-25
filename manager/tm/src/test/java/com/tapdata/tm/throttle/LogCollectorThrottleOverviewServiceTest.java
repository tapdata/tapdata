package com.tapdata.tm.throttle;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import com.tapdata.tm.commons.throttle.TimeQuality;
import com.tapdata.tm.commons.throttle.WaitReason;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskServiceImpl;
import com.tapdata.tm.throttle.controller.TaskThrottleController;
import com.tapdata.tm.throttle.dto.LogCollectorThrottleOverviewView;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyUpdateRequest;
import com.tapdata.tm.throttle.service.LogCollectorThrottleOverviewService;
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
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LogCollectorThrottleOverviewServiceTest {

    private InMemoryMongoOperations mongo;
    private TaskThrottlePolicyService policyService;
    private TaskThrottleExecutionGuardService guardService;
    private TaskThrottleSampleConfigService sampleConfigService;
    private TaskThrottleTelemetryAndRankingService telemetryService;
    private LogCollectorThrottleOverviewService overviewService;
    private TaskThrottleController controller;
    private TaskServiceImpl taskService;
    private UserDetail user;
    private Clock fixedClock;

    @BeforeEach
    void setUp() {
        long nowMs = 1_700_000_000_000L;
        fixedClock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.UTC);
        mongo = new InMemoryMongoOperations();
        policyService = new TaskThrottlePolicyService(mongo.ops(), fixedClock);
        guardService = new TaskThrottleExecutionGuardService(mongo.ops(), fixedClock);
        sampleConfigService = new TaskThrottleSampleConfigService(mongo.ops(), fixedClock);
        telemetryService = new TaskThrottleTelemetryAndRankingService(mongo.ops(), sampleConfigService, fixedClock);
        taskService = mock(TaskServiceImpl.class);

        policyService.setTaskService(taskService);
        policyService.setExecutionGuardService(guardService);
        policyService.setTelemetryAndRankingService(telemetryService);

        overviewService = new LogCollectorThrottleOverviewService(mongo.ops(), policyService, telemetryService, fixedClock);
        overviewService.setTaskService(taskService);

        user = mock(UserDetail.class);
        when(user.getCustomerId()).thenReturn("tenant-1");
        when(user.getUserId()).thenReturn("user-1");
        when(user.getUsername()).thenReturn("alice");

        controller = new TaskThrottleController(policyService, guardService, telemetryService, sampleConfigService) {
            @Override
            protected UserDetail currentUser() {
                return user;
            }
        };
        controller.setLogCollectorOverviewService(overviewService);
    }

    @Test
    void separatesCaptureWriteAndAuthorizedConsumersWhileRedactingHiddenConsumers() {
        ObjectId lcOid = MongoUtils.toObjectId("65f000000000000000000501");
        String lcId = lcOid.toHexString();
        LogCollectorNode lcNode = new LogCollectorNode();
        lcNode.setConnectionIds(List.of("conn-shared-1"));
        lcNode.setStorageTime(7);
        DAG lcDag = mock(DAG.class);
        when(lcDag.getSources()).thenReturn(List.of(lcNode));

        TaskDto collectorTask = new TaskDto();
        collectorTask.setId(lcOid);
        collectorTask.setName("share-collector-1");
        collectorTask.setSyncType("logCollector");
        collectorTask.setType("cdc");
        collectorTask.setStatus(TaskDto.STATUS_RUNNING);
        collectorTask.setCustomId("tenant-1");
        collectorTask.setDag(lcDag);
        when(taskService.checkExistById(eq(lcOid), eq(user))).thenReturn(collectorTask);

        // Save logCollector CDC rate limit policy
        policyService.updatePolicy("logCollector", lcId, TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(0L)
                .phases(Map.of(ThrottlePhase.CDC, ThrottlePhaseLimit.builder()
                        .enabled(true)
                        .bytesPerSec(10_000_000L)
                        .eventsPerSec(5_000L)
                        .build()))
                .build(), user);

        long now = fixedClock.millis();
        // Collector captures (CDC + INPUT) at 4.0 MB/s, writes shared storage (CDC + OUTPUT) at 2.0 MB/s with RATE_WAIT
        telemetryService.ingestTelemetry("agent-1", Map.of(lcId, List.of(
                telemetrySample(ThrottlePhase.CDC, ThrottleDirection.INPUT, now - 5_000L, now,
                        20_000_000L, 1_000L, WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class), LogPositionStatus.NORMAL),
                telemetrySample(ThrottlePhase.CDC, ThrottleDirection.OUTPUT, now - 5_000L, now,
                        10_000_000L, 500L, WaitReason.RATE_WAIT, EnumSet.of(ThrottleDimension.BYTES), LogPositionStatus.WARN_70)
        )));

        // Total consumers across roles = 3; current user can view only 1 (`visible-sync-1`)
        ObjectId visibleConsumerOid = MongoUtils.toObjectId("65f000000000000000000502");
        String visibleConsumerId = visibleConsumerOid.toHexString();
        TaskDto visibleConsumer = new TaskDto();
        visibleConsumer.setId(visibleConsumerOid);
        visibleConsumer.setName("visible-sync-1");
        visibleConsumer.setSyncType("sync");
        visibleConsumer.setType("cdc");
        visibleConsumer.setStatus(TaskDto.STATUS_RUNNING);
        visibleConsumer.setCustomId("tenant-1");
        visibleConsumer.setAttrs(Map.of("syncProgress", Map.of(
                "[\"s1\",\"t1\"]", "{\"eventTime\":" + (now - 12_000L) + "}")));
        when(taskService.checkExistById(eq(visibleConsumerOid), eq(user))).thenReturn(visibleConsumer);

        policyService.updatePolicy("sync", visibleConsumerId, TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(0L)
                .phases(Map.of(ThrottlePhase.CDC, ThrottlePhaseLimit.builder().enabled(true).eventsPerSec(300L).build()))
                .build(), user);

        telemetryService.ingestTelemetry("agent-1", Map.of(visibleConsumerId, List.of(
                telemetrySample(ThrottlePhase.CDC, ThrottleDirection.OUTPUT, now - 5_000L, now,
                        2_500_000L, 250L, WaitReason.RATE_WAIT, EnumSet.of(ThrottleDimension.QPS), LogPositionStatus.CRITICAL_90)
        )));

        when(taskService.count(any(Query.class))).thenReturn(3L);
        when(taskService.findAllDto(any(Query.class), eq(user))).thenReturn(List.of(visibleConsumer));

        LogCollectorThrottleOverviewView overview = controller.getLogCollectorOverview(lcId).getData();
        assertNotNull(overview);
        assertEquals(lcId, overview.getCollectorTaskId());
        assertEquals("share-collector-1", overview.getCollectorTaskName());
        assertEquals(7, overview.getStorageTimeDays());
        assertEquals(1L, overview.getPolicyView().getDesiredVersion());

        // Collector capture vs write snapshots
        assertEquals(4.0d, overview.getSourceCapture().getEstimatedMbPerSec(), 1e-6);
        assertEquals(200.0d, overview.getSourceCapture().getQps(), 1e-6);
        assertEquals(WaitReason.NONE, overview.getSourceCapture().getWaitReason());

        assertEquals(2.0d, overview.getLogWrite().getEstimatedMbPerSec(), 1e-6);
        assertEquals(WaitReason.RATE_WAIT, overview.getLogWrite().getWaitReason());
        assertEquals(LogPositionStatus.WARN_70, overview.getLogWrite().getLogPositionStatus());

        // Cross-permission counts & redacted hidden count
        assertEquals(3, overview.getTotalConsumerCount());
        assertEquals(2, overview.getHiddenConsumerCount());
        assertEquals(1, overview.getVisibleConsumers().size());
        assertEquals(now - 12_000L, overview.getOldestConsumerReplayTime());

        LogCollectorThrottleOverviewView.ConsumerThrottleSummary consumerSummary = overview.getVisibleConsumers().get(0);
        assertEquals(visibleConsumerId, consumerSummary.getTaskId());
        assertEquals("visible-sync-1", consumerSummary.getTaskName());
        assertEquals(12_000L, consumerSummary.getConsumerDelayMs());
        assertTrue(consumerSummary.isThrottleEnabled());
        assertEquals(0.5d, consumerSummary.getEstimatedOutputMbPerSec(), 1e-6);
        assertEquals(LogPositionStatus.CRITICAL_90, consumerSummary.getLogPositionStatus());
        assertTrue(consumerSummary.getLiveStatus().isThrottlingActive());
    }

    private static TaskThrottleTelemetrySample telemetrySample(
            ThrottlePhase phase,
            ThrottleDirection dir,
            long startUtc,
            long endUtc,
            long bytes,
            long events,
            WaitReason waitReason,
            EnumSet<ThrottleDimension> waitingDims,
            LogPositionStatus posStatus) {
        return TaskThrottleTelemetrySample.builder()
                .runEpoch(1L)
                .sampleSeq(dir == ThrottleDirection.INPUT ? 1L : 2L)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .phase(phase)
                .direction(dir)
                .windowStartUtc(startUtc)
                .windowEndUtc(endUtc)
                .durationNanos((endUtc - startUtc) * 1_000_000L)
                .timeQuality(TimeQuality.ALIGNED)
                .appliedVersion(1L)
                .admittedEvents(events)
                .estimatedAdmittedBytes(bytes)
                .sampleCount(5L)
                .sampleQuality(SampleQuality.VALID)
                .waitReason(waitReason)
                .waitingDimensions(waitingDims)
                .logPositionStatus(posStatus)
                .build();
    }
}
