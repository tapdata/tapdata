package com.tapdata.tm.throttle;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.AgentThrottleCapabilities;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.task.service.TaskServiceImpl;
import com.tapdata.tm.throttle.controller.TaskThrottleController;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyUpdateRequest;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyView;
import com.tapdata.tm.throttle.dto.TaskThrottleVersionConflictException;
import com.tapdata.tm.throttle.entity.TaskThrottleChangeLogEntity;
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
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskThrottlePolicyServiceAndControllerTest {

    private InMemoryMongoOperations mongo;
    private TaskThrottlePolicyService policyService;
    private TaskThrottleExecutionGuardService guardService;
    private TaskThrottleSampleConfigService sampleConfigService;
    private TaskThrottleTelemetryAndRankingService telemetryService;
    private TaskThrottleController controller;
    private TaskServiceImpl taskService;
    private UserDetail user;
    private ObjectId taskOid;

    @BeforeEach
    void setUp() {
        mongo = new InMemoryMongoOperations();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_700_000_000_000L), ZoneOffset.UTC);
        policyService = new TaskThrottlePolicyService(mongo.ops(), clock);
        guardService = new TaskThrottleExecutionGuardService(mongo.ops(), clock);
        sampleConfigService = new TaskThrottleSampleConfigService(mongo.ops(), clock);
        telemetryService = new TaskThrottleTelemetryAndRankingService(mongo.ops(), sampleConfigService, clock);
        taskService = mock(TaskServiceImpl.class);

        policyService.setTaskService(taskService);
        policyService.setExecutionGuardService(guardService);
        policyService.setTelemetryAndRankingService(telemetryService);

        user = mock(UserDetail.class);
        when(user.getCustomerId()).thenReturn("tenant-1");
        when(user.getUserId()).thenReturn("user-1");
        when(user.getUsername()).thenReturn("alice");

        taskOid = MongoUtils.toObjectId("65f000000000000000000001");
        TaskDto syncTask = buildTask(taskOid, "sync", "initial_sync+cdc", null);
        when(taskService.checkExistById(eq(taskOid), eq(user))).thenReturn(syncTask);

        controller = new TaskThrottleController(policyService, guardService, telemetryService, sampleConfigService) {
            @Override
            protected UserDetail currentUser() {
                return user;
            }
        };
    }

    @Test
    void supportsSyncMigrateLogCollectorAndRejectsUnsupportedKindsAndPaths() {
        assertEquals("sync", policyService.normalizeAndValidateKind("sync"));
        assertEquals("migrate", policyService.normalizeAndValidateKind("migrate"));
        assertEquals("logCollector", policyService.normalizeAndValidateKind("log_collector"));
        assertEquals("logCollector", policyService.normalizeAndValidateKind("logCollector"));

        for (String badKind : List.of("testRun", "mem_cache", "shareCache")) {
            BizException ex = assertThrows(BizException.class, () -> policyService.normalizeAndValidateKind(badKind));
            assertEquals(ThrottleConstants.ERR_UNSUPPORTED_TASK_KIND, ex.getErrorCode());
        }

        ObjectId crossJvmOid = MongoUtils.toObjectId("65f000000000000000000002");
        TaskDto crossJvmTask = buildTask(crossJvmOid, "sync", "initial_sync+cdc", Map.of("crossJvm", true));
        when(taskService.checkExistById(eq(crossJvmOid), eq(user))).thenReturn(crossJvmTask);

        BizException pathEx = assertThrows(BizException.class,
                () -> policyService.getPolicyView("sync", crossJvmOid.toHexString(), user));
        assertEquals(ThrottleConstants.ERR_UNSUPPORTED_PATH, pathEx.getErrorCode());
    }

    @Test
    void enforcesTaskModeAndRateBoundariesAndNeverFabricatesAppliedPhases() {
        String taskId = taskOid.toHexString();
        TaskThrottlePolicyView emptyView = controller.getPolicy("sync", taskId).getData();
        assertEquals(0L, emptyView.getDesiredVersion());
        assertEquals(0L, emptyView.getAppliedVersion());
        assertNull(emptyView.getAppliedPhases());

        // initial_sync cannot enable CDC phase
        TaskDto initialOnly = buildTask(taskOid, "sync", "initial_sync", null);
        when(taskService.checkExistById(eq(taskOid), eq(user))).thenReturn(initialOnly);
        BizException cdcOnInitial = assertThrows(BizException.class,
                () -> controller.updatePolicy("sync", taskId, updateReq(0L, Map.of(
                        ThrottlePhase.CDC, ThrottlePhaseLimit.builder().enabled(true).eventsPerSec(100L).build()))));
        assertEquals(ThrottleConstants.ERR_INVALID_RATE, cdcOnInitial.getErrorCode());

        // cdc-only cannot enable FULL phase
        TaskDto cdcOnly = buildTask(taskOid, "sync", "cdc", null);
        when(taskService.checkExistById(eq(taskOid), eq(user))).thenReturn(cdcOnly);
        BizException fullOnCdc = assertThrows(BizException.class,
                () -> controller.updatePolicy("sync", taskId, updateReq(0L, Map.of(
                        ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).bytesPerSec(10_000L).build()))));
        assertEquals(ThrottleConstants.ERR_INVALID_RATE, fullOnCdc.getErrorCode());

        // restore dual mode and check out-of-range / empty limits
        when(taskService.checkExistById(eq(taskOid), eq(user))).thenReturn(buildTask(taskOid, "sync", "initial_sync+cdc", null));
        assertEquals(ThrottleConstants.ERR_INVALID_RATE, assertThrows(BizException.class,
                () -> controller.updatePolicy("sync", taskId, updateReq(0L, Map.of(
                        ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).bytesPerSec(999L).build()))))
                .getErrorCode());
        assertEquals(ThrottleConstants.ERR_INVALID_RATE, assertThrows(BizException.class,
                () -> controller.updatePolicy("sync", taskId, updateReq(0L, Map.of(
                        ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).build()))))
                .getErrorCode());
    }

    @Test
    void casUpdateReturns409OnVersionConflictAndFlushesAuditOutboxIdempotently() {
        String taskId = taskOid.toHexString();
        Map<ThrottlePhase, ThrottlePhaseLimit> v1Phases = Map.of(
                ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).bytesPerSec(5_000_000L).eventsPerSec(2_000L).build());

        ResponseEntity<ResponseMessage<Object>> createdResp = controller.updatePolicy("sync", taskId, updateReq(0L, v1Phases));
        assertEquals(HttpStatus.OK, createdResp.getStatusCode());
        TaskThrottlePolicyView createdView = (TaskThrottlePolicyView) createdResp.getBody().getData();
        assertEquals(1L, createdView.getDesiredVersion());
        assertNull(createdView.getAppliedPhases());

        // Audit outbox should have flushed into ChangeLog and drained from policy document
        List<TaskThrottleChangeLogEntity> changes = controller.listChanges("sync", taskId, 0, 50).getData();
        assertEquals(1, changes.size());
        assertEquals(TaskThrottleChangeLogEntity.EVENT_POLICY_CHANGE, changes.get(0).getEventType());
        assertEquals(1L, changes.get(0).getPolicyVersion());
        TaskThrottlePolicyEntity storedAfterFlush = mongo.ops().findOne(new Query(), TaskThrottlePolicyEntity.class);
        assertNotNull(storedAfterFlush);
        assertTrue(storedAfterFlush.getPendingAuditOutbox() == null || storedAfterFlush.getPendingAuditOutbox().isEmpty());

        // Stale expectedVersion=0L must trigger HTTP 409 Conflict with latestView
        ResponseEntity<ResponseMessage<Object>> conflictResp = controller.updatePolicy("sync", taskId, updateReq(0L, v1Phases));
        assertEquals(HttpStatus.CONFLICT, conflictResp.getStatusCode());
        assertEquals(ThrottleConstants.ERR_VERSION_CONFLICT, conflictResp.getBody().getCode());
        @SuppressWarnings("unchecked")
        Map<String, Object> conflictData = (Map<String, Object>) conflictResp.getBody().getData();
        assertEquals(1L, conflictData.get("latestVersion"));
        TaskThrottlePolicyView latestView = (TaskThrottlePolicyView) conflictData.get("latestView");
        assertEquals(1L, latestView.getDesiredVersion());

        // Direct service CAS conflict check
        assertThrows(TaskThrottleVersionConflictException.class,
                () -> policyService.updatePolicy("sync", taskId, updateReq(99L, v1Phases), user));
    }

    @Test
    void rejectsPolicyWriteWhenAuditOutboxReachesSafetyCap() {
        String taskId = taskOid.toHexString();
        mongo.setFailChangeLogInserts(true);

        // Pre-seed a policy whose pendingAuditOutbox already reached MAX_OUTBOX_CAPACITY
        List<TaskThrottlePolicyEntity.AuditOutboxEntry> fullOutbox = new ArrayList<>();
        for (int i = 0; i < TaskThrottlePolicyEntity.MAX_OUTBOX_CAPACITY; i++) {
            fullOutbox.add(TaskThrottlePolicyEntity.AuditOutboxEntry.builder()
                    .entryId("e-" + i)
                    .dedupKey("CHANGE:tenant-1:sync:" + taskId + ":" + (i + 1))
                    .eventType(TaskThrottleChangeLogEntity.EVENT_POLICY_CHANGE)
                    .afterDesiredVersion(i + 1L)
                    .occurredAt(1_700_000_000_000L + i)
                    .build());
        }
        String taskKey = TaskThrottlePolicyEntity.buildTaskKey("tenant-1", "sync", taskId);
        mongo.seedRawDocument(TaskThrottlePolicyEntity.class, TaskThrottlePolicyEntity.builder()
                .id(taskKey)
                .tenantId("tenant-1")
                .taskKind("sync")
                .taskId(taskId)
                .desiredVersion(1L)
                .pendingAuditOutbox(fullOutbox)
                .build());

        BizException ex = assertThrows(BizException.class, () -> policyService.updatePolicy(
                "sync", taskId,
                updateReq(1L, Map.of(ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).eventsPerSec(10L).build())),
                user));
        assertEquals(TaskThrottlePolicyService.ERR_AUDIT_OUTBOX_FULL, ex.getErrorCode());

        // Recovery: allow ChangeLog inserts again, next update drains outbox and succeeds
        mongo.setFailChangeLogInserts(false);
        TaskThrottlePolicyView recovered = policyService.updatePolicy(
                "sync", taskId,
                updateReq(1L, Map.of(ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).eventsPerSec(10L).build())),
                user);
        assertEquals(2L, recovered.getDesiredVersion());
    }

    @Test
    void copyAndDeleteLifecycleHooksPreserveImmutableAuditAndCoverControllerRoutes() {
        String srcId = taskOid.toHexString();
        controller.updatePolicy("sync", srcId, updateReq(0L, Map.of(
                ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).bytesPerSec(2_000_000L).build())));

        ObjectId copyOid = MongoUtils.toObjectId("65f000000000000000000099");
        TaskDto copied = buildTask(copyOid, "sync", "initial_sync+cdc", null);
        when(taskService.checkExistById(eq(copyOid), eq(user))).thenReturn(copied);

        policyService.copyPolicy(srcId, copied, user);
        TaskThrottlePolicyView copiedView = controller.getPolicy("sync", copyOid.toHexString()).getData();
        assertEquals(1L, copiedView.getDesiredVersion());
        assertEquals(2_000_000L, copiedView.getDesiredPhases().get(ThrottlePhase.FULL).getBytesPerSec());

        // Seed stop evidence and test onTaskDeleted keeps immutable audit rows
        mongo.ops().insert(TaskThrottleStopEvidenceEntity.builder()
                .id(srcId + ":agent-1:1:ref-1")
                .taskId(srcId)
                .oldAgentId("agent-1")
                .oldRunEpoch(1L)
                .evidenceRef("ref-1")
                .processExited(true)
                .writeSettled(true)
                .build());
        policyService.onTaskDeleted(srcId);
        assertEquals(0, mongo.ops().find(new Query(), TaskThrottleStopEvidenceEntity.class).size());
        assertTrue(mongo.ops().findAll(TaskThrottleChangeLogEntity.class).stream()
                .anyMatch(row -> srcId.equals(row.getTaskId())));

        // Verify sample config & live status & ranking routes via controller
        assertEquals(30, controller.getSampleConfig().getData().getIntervalSeconds());
        assertEquals(60, controller.updateSampleConfig(Map.of("intervalSeconds", 60)).getData().getIntervalSeconds());
        assertTrue(controller.getLiveStatuses(List.of(copyOid.toHexString())).getData().containsKey(copyOid.toHexString()));
        assertEquals(ThrottleConstants.METER_VERSION,
                controller.getRanking(ThrottleDirection.OUTPUT, 300L, 5).getData().getMeterVersion());
    }

    @Test
    void enforcesSettingsPermissionWhenModuleAccessIsConfigured() {
        PermissionService permissionService = mock(PermissionService.class);
        SettingsService settingsService = mock(SettingsService.class);
        when(settingsService.isCloud()).thenReturn(false);
        when(permissionService.checkCurrentUserHasPermission(
                TaskThrottleController.SETTINGS_PERMISSION, "user-1")).thenReturn(false);

        controller.setModuleAccess(new ModuleAccess(permissionService, settingsService));

        BizException denied = assertThrows(BizException.class,
                () -> controller.updateSampleConfig(Map.of("intervalSeconds", 90)));
        assertEquals("NotAuthorized", denied.getErrorCode());
        assertEquals(1L, sampleConfigService.getCurrentConfig().configVersion());

        when(permissionService.checkCurrentUserHasPermission(
                TaskThrottleController.SETTINGS_PERMISSION, "user-1")).thenReturn(true);
        assertEquals(90, controller.updateSampleConfig(Map.of("intervalSeconds", 90)).getData().getIntervalSeconds());
        assertEquals(2L, sampleConfigService.getCurrentConfig().configVersion());
    }

    @Test
    void singlePolicyViewIncludesConfiguredLiveStatusAndRunningTaskActivationSyncsEpochAttrs() {
        guardService.registerCapabilities(AgentThrottleCapabilities.builder()
                .agentId("agent-run-1")
                .throttleSupported(true)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .supportedTaskKinds(Set.of("sync", "migrate", "logCollector"))
                .build());

        ObjectId runningOid = MongoUtils.toObjectId("65f000000000000000000108");
        String runningTaskId = runningOid.toHexString();
        TaskDto runningTask = buildTask(runningOid, "migrate", "initial_sync+cdc", null);
        runningTask.setStatus(TaskDto.STATUS_RUNNING);
        runningTask.setAgentId("agent-run-1");
        when(taskService.checkExistById(eq(runningOid), eq(user))).thenReturn(runningTask);

        ResponseEntity<ResponseMessage<Object>> v1Resp = controller.updatePolicy("migrate", runningTaskId, updateReq(0L, Map.of(
                ThrottlePhase.CDC, ThrottlePhaseLimit.builder().enabled(true).eventsPerSec(60L).build())));
        assertEquals(HttpStatus.OK, v1Resp.getStatusCode());
        TaskThrottlePolicyView v1 = (TaskThrottlePolicyView) v1Resp.getBody().getData();
        assertEquals(1L, v1.getCurrentRunEpoch());
        assertTrue(v1.isProtectedRunActive());
        assertEquals("agent-run-1", v1.getCurrentAgentId());
        assertNotNull(v1.getLiveStatus());
        assertTrue(v1.getLiveStatus().isPolicyConfigured());
        assertEquals(Set.of(ThrottlePhase.CDC), v1.getLiveStatus().getConfiguredPhases());
        verify(taskService, times(1)).update(any(Query.class), any(Update.class));

        // Subsequent update on same running agent preserves currentRunEpoch=1L without redundant Task.attrs write
        ResponseEntity<ResponseMessage<Object>> v2Resp = controller.updatePolicy("migrate", runningTaskId, updateReq(1L, Map.of(
                ThrottlePhase.CDC, ThrottlePhaseLimit.builder().enabled(true).eventsPerSec(120L).build())));
        assertEquals(HttpStatus.OK, v2Resp.getStatusCode());
        TaskThrottlePolicyView v2 = (TaskThrottlePolicyView) v2Resp.getBody().getData();
        assertEquals(1L, v2.getCurrentRunEpoch());
        assertTrue(v2.isProtectedRunActive());
        verify(taskService, times(1)).update(any(Query.class), any(Update.class));

        TaskThrottlePolicyView singleView = controller.getPolicy("migrate", runningTaskId).getData();
        assertNotNull(singleView.getLiveStatus());
        assertTrue(singleView.getLiveStatus().isPolicyConfigured());
        assertEquals(Set.of(ThrottlePhase.CDC), singleView.getLiveStatus().getConfiguredPhases());
    }

    private static TaskDto buildTask(ObjectId id, String syncType, String type, Map<String, Object> attrs) {
        TaskDto dto = new TaskDto();
        dto.setId(id);
        dto.setSyncType(syncType);
        dto.setType(type);
        dto.setCustomId("tenant-1");
        dto.setUserId("user-1");
        dto.setAttrs(attrs);
        return dto;
    }

    private static TaskThrottlePolicyUpdateRequest updateReq(long expectedVer, Map<ThrottlePhase, ThrottlePhaseLimit> phases) {
        EnumMap<ThrottlePhase, ThrottlePhaseLimit> map = new EnumMap<>(ThrottlePhase.class);
        map.putAll(phases);
        return TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(expectedVer)
                .phases(map)
                .build();
    }
}
