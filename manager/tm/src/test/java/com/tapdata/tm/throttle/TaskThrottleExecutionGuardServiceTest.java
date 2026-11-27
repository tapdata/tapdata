package com.tapdata.tm.throttle;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.AgentThrottleCapabilities;
import com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.controller.TaskController;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.throttle.controller.AgentThrottleController;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyUpdateRequest;
import com.tapdata.tm.throttle.entity.TaskThrottlePolicyEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleStopEvidenceEntity;
import com.tapdata.tm.throttle.service.TaskThrottleExecutionGuardService;
import com.tapdata.tm.throttle.service.TaskThrottlePolicyService;
import com.tapdata.tm.throttle.service.TaskThrottleSampleConfigService;
import com.tapdata.tm.throttle.service.TaskThrottleTelemetryAndRankingService;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TaskThrottleExecutionGuardServiceTest {

    private InMemoryMongoOperations mongo;
    private TaskThrottleExecutionGuardService guardService;
    private TaskThrottlePolicyService policyService;
    private TaskThrottleTelemetryAndRankingService telemetryService;
    private AgentThrottleController agentController;
    private ObjectId taskOid;
    private String taskId;

    @BeforeEach
    void setUp() {
        mongo = new InMemoryMongoOperations();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_700_000_000_000L), ZoneOffset.UTC);
        guardService = new TaskThrottleExecutionGuardService(mongo.ops(), clock);
        policyService = new TaskThrottlePolicyService(mongo.ops(), clock);
        policyService.setExecutionGuardService(guardService);
        TaskThrottleSampleConfigService sampleConfigService = new TaskThrottleSampleConfigService(mongo.ops(), clock);
        telemetryService = new TaskThrottleTelemetryAndRankingService(mongo.ops(), sampleConfigService, clock);
        agentController = new AgentThrottleController(guardService, telemetryService);
        taskOid = MongoUtils.toObjectId("65f000000000000000000101");
        taskId = taskOid.toHexString();
    }

    @Test
    void registersCapabilitiesFiltersCompatibleAgentsAndUpdatesWaitingGate() {
        agentController.registerCapabilities(AgentThrottleCapabilities.builder()
                .agentId("agent-compatible")
                .throttleSupported(true)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .supportedTaskKinds(Set.of("sync", "migrate", "logCollector"))
                .supportedDimensions(Set.of(ThrottleDimension.BYTES, ThrottleDimension.QPS))
                .build());
        agentController.registerCapabilities(AgentThrottleCapabilities.builder()
                .agentId("agent-legacy")
                .throttleSupported(false)
                .meterVersion("legacy-v0")
                .build());

        assertTrue(guardService.isAgentCompatible("agent-compatible", "sync"));
        assertFalse(guardService.isAgentCompatible("agent-legacy", "sync"));
        assertFalse(guardService.isAgentCompatible("agent-unknown", "sync"));
        assertEquals(List.of("agent-compatible"),
                guardService.filterCompatibleAgentIds(List.of("agent-legacy", "agent-compatible", "agent-unknown"), "migrate"));

        UserDetail user = mock(UserDetail.class);
        policyService.updatePolicy("sync", taskId, TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(0L)
                .phases(Map.of(ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).bytesPerSec(500_000L).build()))
                .build(), user);

        TaskDto taskDto = new TaskDto();
        taskDto.setId(taskOid);
        taskDto.setSyncType("sync");

        // Incompatible agent blocked with WAITING_COMPATIBLE_AGENT gate
        TaskThrottleExecutionGuardService.AuthorizationDecision blocked =
                guardService.authorizeStartOrTakeover(taskDto, "agent-legacy");
        assertFalse(blocked.allowed());
        assertEquals(ThrottleConstants.ERR_UNSUPPORTED_PATH, blocked.errorCode());
        TaskThrottlePolicyEntity gated = guardService.findPolicyByTaskId(taskId);
        assertEquals(TaskThrottlePolicyEntity.STOP_GATE_WAITING_COMPATIBLE_AGENT, gated.getStopGateStatus());

        // Compatible agent gets first runEpoch = 1
        TaskThrottleExecutionGuardService.AuthorizationDecision allowed =
                guardService.authorizeStartOrTakeover(taskDto, "agent-compatible");
        assertTrue(allowed.allowed());
        assertTrue(allowed.protectedRun());
        assertEquals(1L, allowed.runEpoch());
        assertNotNull(allowed.policy());
    }

    @Test
    void requiresVerifiableStopEvidenceBlocksTimeoutRescheduleAndConsumesEvidenceOnce() {
        registerCompatibleAgent("agent-a");
        registerCompatibleAgent("agent-b");

        UserDetail user = mock(UserDetail.class);
        policyService.updatePolicy("sync", taskId, TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(0L)
                .phases(Map.of(ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).eventsPerSec(200L).build()))
                .build(), user);

        TaskDto taskDto = new TaskDto();
        taskDto.setId(taskOid);
        taskDto.setSyncType("sync");

        TaskThrottleExecutionGuardService.AuthorizationDecision run1 =
                guardService.authorizeStartOrTakeover(taskDto, "agent-a");
        assertEquals(1L, run1.runEpoch());

        // Timeout reschedule must be blocked without verified stop evidence
        assertTrue(guardService.shouldBlockTimeoutReschedule(taskDto));
        TaskThrottleExecutionGuardService.AuthorizationDecision blockedTakeover =
                guardService.authorizeStartOrTakeover(taskDto, "agent-b");
        assertFalse(blockedTakeover.allowed());
        assertEquals(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED, blockedTakeover.errorCode());
        assertEquals(TaskThrottlePolicyEntity.STOP_GATE_WAITING_EVIDENCE,
                guardService.findPolicyByTaskId(taskId).getStopGateStatus());

        // Manual stop evidence requires writeSettled=true && processExited=true
        BizException missingExit = assertThrows(BizException.class, () -> guardService.submitStopEvidence(
                TaskThrottleStopEvidenceDto.builder()
                        .taskId(taskId)
                        .oldAgentId("agent-a")
                        .oldRunEpoch(1L)
                        .evidenceRef("manual-1")
                        .processExited(false)
                        .writeSettled(true)
                        .build(), true));
        assertEquals(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED, missingExit.getErrorCode());

        // Stale epoch evidence rejected
        BizException staleEpoch = assertThrows(BizException.class, () -> guardService.submitStopEvidence(
                TaskThrottleStopEvidenceDto.builder()
                        .taskId(taskId)
                        .oldAgentId("agent-a")
                        .oldRunEpoch(99L)
                        .evidenceRef("manual-stale")
                        .processExited(true)
                        .writeSettled(true)
                        .build(), true));
        assertEquals(ThrottleConstants.ERR_STALE_RUN_EPOCH, staleEpoch.getErrorCode());

        // Submit valid manual stop evidence -> timeout reschedule block lifts and takeover consumes once
        TaskThrottleStopEvidenceEntity saved = guardService.submitStopEvidence(
                TaskThrottleStopEvidenceDto.builder()
                        .taskId(taskId)
                        .oldAgentId("agent-a")
                        .oldRunEpoch(1L)
                        .evidenceRef("manual-ok")
                        .processExited(true)
                        .writeSettled(true)
                        .submittedBy("ops-admin")
                        .note("Confirmed process exited")
                        .build(), true);
        assertFalse(saved.isConsumed());
        assertFalse(guardService.shouldBlockTimeoutReschedule(taskDto));

        TaskThrottleExecutionGuardService.AuthorizationDecision run2 =
                guardService.authorizeStartOrTakeover(taskDto, "agent-b");
        assertTrue(run2.allowed());
        assertEquals(2L, run2.runEpoch());

        TaskThrottleStopEvidenceEntity consumedEv = mongo.ops().findOne(
                Query.query(Criteria.where("_id").is(saved.getId())), TaskThrottleStopEvidenceEntity.class);
        assertNotNull(consumedEv);
        assertTrue(consumedEv.isConsumed());
        assertEquals(2L, consumedEv.getConsumedByRunEpoch());

        // Subsequent restart attempt without new epoch evidence must fail
        TaskThrottleExecutionGuardService.AuthorizationDecision blockedAgain =
                guardService.authorizeStartOrTakeover(taskDto, "agent-a");
        assertFalse(blockedAgain.allowed());
        assertEquals(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED, blockedAgain.errorCode());
    }

    @Test
    void enforcesCheckpointFencingAndIntegratesWithTaskControllerUpdateSyncProgress() {
        registerCompatibleAgent("agent-curr");

        // Unprotected task returns false from validateCheckpointWrite
        assertFalse(guardService.validateCheckpointWrite(taskId, null, null));

        UserDetail user = mock(UserDetail.class);
        policyService.updatePolicy("sync", taskId, TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(0L)
                .phases(Map.of(ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).bytesPerSec(1_000_000L).build()))
                .build(), user);
        TaskDto taskDto = new TaskDto();
        taskDto.setId(taskOid);
        taskDto.setSyncType("sync");
        assertEquals(1L, guardService.authorizeStartOrTakeover(taskDto, "agent-curr").runEpoch());

        TaskController controller = new TaskController();
        TaskService mockTaskService = mock(TaskService.class);
        ReflectionTestUtils.setField(controller, "taskService", mockTaskService);
        ReflectionTestUtils.setField(controller, "taskThrottleExecutionGuardService", guardService);

        // Stale epoch rejected before touching taskService
        BizException staleCp = assertThrows(BizException.class,
                () -> controller.updateSyncProgress(taskId, 0L, "agent-curr", "{\"offset\":1}"));
        assertEquals(ThrottleConstants.ERR_STALE_RUN_EPOCH, staleCp.getErrorCode());

        // Wrong agentId rejected
        BizException wrongAgentCp = assertThrows(BizException.class,
                () -> controller.updateSyncProgress(taskId, 1L, "agent-stale", "{\"offset\":1}"));
        assertEquals(ThrottleConstants.ERR_STALE_RUN_EPOCH, wrongAgentCp.getErrorCode());

        // Matching epoch & agentId succeeds and forwards runEpoch to atomic checkpoint update
        controller.updateSyncProgress(taskId, 1L, "agent-curr", "{\"offset\":42}");
        verify(mockTaskService).updateSyncProgress(
                org.mockito.ArgumentMatchers.eq(taskOid),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq(1L));

        // Unprotected task passing runEpoch=0 (e.g., stale client query) falls back to 2-arg updateSyncProgress
        String lazyTaskId = new ObjectId().toHexString();
        controller.updateSyncProgress(lazyTaskId, 0L, "agent-curr", "{\"offset\":7}");
        verify(mockTaskService).updateSyncProgress(
                org.mockito.ArgumentMatchers.eq(MongoUtils.toObjectId(lazyTaskId)),
                org.mockito.ArgumentMatchers.any());
    }

    @Test
    void inactivePolicyNeverBlocksTimeoutRescheduleAndUnstartedEpochCanBeReleased() {
        registerCompatibleAgent("agent-a");
        policyService.updatePolicy("sync", taskId, TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(0L)
                .phases(Map.of(ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).bytesPerSec(500_000L).build()))
                .build(), mock(UserDetail.class));

        TaskDto taskDto = new TaskDto();
        taskDto.setId(taskOid);
        taskDto.setSyncType("sync");

        // protectedRunActive=false, currentRunEpoch=0 -> heartbeat timeout reschedule must NOT be blocked!
        assertFalse(guardService.shouldBlockTimeoutReschedule(taskDto));
        assertEquals(TaskThrottlePolicyEntity.STOP_GATE_NONE,
                guardService.findPolicyByTaskId(taskId).getStopGateStatus());

        // Scheduling authorizes runEpoch=1, then fails before WAIT_RUN -> releaseUnstartedEpoch restores clean scheduling
        TaskThrottleExecutionGuardService.AuthorizationDecision auth =
                guardService.authorizeStartOrTakeover(taskDto, "agent-a");
        assertTrue(auth.allowed());
        assertTrue(auth.protectedRun());
        assertTrue(guardService.releaseUnstartedEpoch(taskId, auth.runEpoch(), "agent-a", "start aborted"));
        assertFalse(guardService.shouldBlockTimeoutReschedule(taskDto));

        TaskThrottleExecutionGuardService.AuthorizationDecision retryAuth =
                guardService.authorizeStartOrTakeover(taskDto, "agent-a");
        assertTrue(retryAuth.allowed());
        assertEquals(2L, retryAuth.runEpoch());
    }

    @Test
    void agentUploadStopEvidenceIgnoresStaleOrDeletedTaskIdempotentlyWithoutFailingManualStrictness() {
        String missingTaskId = new ObjectId().toHexString();
        var missingRes = agentController.uploadStopEvidence(TaskThrottleStopEvidenceDto.builder()
                .taskId(missingTaskId)
                .oldAgentId("agent-a")
                .oldRunEpoch(1L)
                .evidenceRef("deleted-task-stop")
                .writeSettled(true)
                .build());
        assertNotNull(missingRes);
        assertNull(missingRes.getData());

        // Stale epoch against an existing policy is also acknowledged idempotently by agent endpoint
        registerCompatibleAgent("agent-a");
        policyService.updatePolicy("migrate", taskId, TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(0L)
                .phases(Map.of(ThrottlePhase.CDC, ThrottlePhaseLimit.builder().enabled(true).eventsPerSec(100L).build()))
                .build(), mock(UserDetail.class));
        TaskDto taskDto = new TaskDto();
        taskDto.setId(taskOid);
        taskDto.setSyncType("migrate");
        assertEquals(1L, guardService.authorizeStartOrTakeover(taskDto, "agent-a").runEpoch());

        var staleRes = agentController.uploadStopEvidence(TaskThrottleStopEvidenceDto.builder()
                .taskId(taskId)
                .oldAgentId("agent-a")
                .oldRunEpoch(99L)
                .evidenceRef("stale-agent-stop")
                .writeSettled(true)
                .build());
        assertNotNull(staleRes);
        assertNull(staleRes.getData());
    }

    @Test
    void releaseUnstartedEpochIfPresentHandlesAttrsAndPolicyFallback() {
        registerCompatibleAgent("agent-a");
        policyService.updatePolicy("sync", taskId, TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(0L)
                .phases(Map.of(ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).bytesPerSec(500_000L).build()))
                .build(), mock(UserDetail.class));

        TaskDto taskDto = new TaskDto();
        taskDto.setId(taskOid);
        taskDto.setSyncType("sync");
        taskDto.setAgentId("agent-a");

        TaskThrottleExecutionGuardService.AuthorizationDecision auth1 = guardService.authorizeStartOrTakeover(taskDto, "agent-a");
        assertTrue(auth1.allowed());
        taskDto.setAttrs(new java.util.HashMap<>(Map.of("throttleRunEpoch", auth1.runEpoch(), "throttleProtected", true)));
        assertTrue(guardService.releaseUnstartedEpochIfPresent(taskDto, "sendStartMsg failed"));

        TaskThrottleExecutionGuardService.AuthorizationDecision auth2 = guardService.authorizeStartOrTakeover(taskDto, "agent-a");
        assertTrue(auth2.allowed());
        assertEquals(auth1.runEpoch() + 1L, auth2.runEpoch());

        // Also supports WAIT_RUN pause/timeout when caller TaskDto only carries id & agentId
        registerCompatibleAgent("agent-b");
        TaskDto minimalDto = new TaskDto();
        minimalDto.setId(taskOid);
        minimalDto.setAgentId("agent-a");
        assertTrue(guardService.releaseUnstartedEpochIfPresent(minimalDto, "WAIT_RUN paused"));
        assertTrue(guardService.authorizeStartOrTakeover(taskDto, "agent-b").allowed());
    }

    private void registerCompatibleAgent(String agentId) {
        guardService.registerCapabilities(AgentThrottleCapabilities.builder()
                .agentId(agentId)
                .throttleSupported(true)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .supportedTaskKinds(Set.of("sync", "migrate", "logCollector"))
                .build());
    }
}
