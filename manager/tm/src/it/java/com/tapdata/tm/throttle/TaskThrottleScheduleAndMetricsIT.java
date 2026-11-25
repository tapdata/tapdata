package com.tapdata.tm.throttle;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.AgentThrottleCapabilities;
import com.tapdata.tm.commons.throttle.AgentThrottleDeltaResponse;
import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.TaskThrottleApplyReceipt;
import com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import com.tapdata.tm.commons.throttle.TimeQuality;
import com.tapdata.tm.commons.throttle.WaitReason;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.controller.TaskController;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.TaskCollectionObjService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TaskServiceImpl;
import com.tapdata.tm.task.service.chart.ChartViewService;
import com.tapdata.tm.task.service.impl.TaskScheduleServiceImpl;
import com.tapdata.tm.throttle.controller.AgentThrottleController;
import com.tapdata.tm.throttle.dto.TaskThrottleLiveStatusView;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyUpdateRequest;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyView;
import com.tapdata.tm.throttle.dto.TaskThrottleRankingResponse;
import com.tapdata.tm.throttle.entity.TaskThrottleChangeLogEntity;
import com.tapdata.tm.throttle.entity.TaskThrottlePolicyEntity;
import com.tapdata.tm.throttle.service.TaskThrottleExecutionGuardService;
import com.tapdata.tm.throttle.service.TaskThrottlePolicyService;
import com.tapdata.tm.throttle.service.TaskThrottleSampleConfigService;
import com.tapdata.tm.throttle.service.TaskThrottleTelemetryAndRankingService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 任务限速调度交集、停止证据、代次断点保护、增量下发与指标排行多服务协作集成验证（L2）。
 *
 * <p>在轻量 Spring {@link GenericApplicationContext} 中装配真实限速控制面服务、
 * {@link TaskScheduleServiceImpl}、{@link AgentThrottleController}、{@link TaskController}
 * 及 {@link TaskServiceImpl} 原子断点落库链，以可变时钟替代真实等待。</p>
 */
@Timeout(15)
class TaskThrottleScheduleAndMetricsIT {

    private static final String USER_OID = "66f000000000000000000901";

    private GenericApplicationContext context;
    private MutableClock clock;
    private InMemoryMongoOperations mongo;
    private TaskThrottlePolicyService policyService;
    private TaskThrottleExecutionGuardService guardService;
    private TaskThrottleSampleConfigService sampleConfigService;
    private TaskThrottleTelemetryAndRankingService telemetryService;
    private AgentThrottleController agentController;
    private TaskScheduleServiceImpl scheduleService;
    private TaskController taskController;

    private TaskRepository mockTaskRepo;
    private TaskServiceImpl realTaskService;
    private TaskService mockScheduleTaskService;
    private WorkerService workerService;
    private AgentGroupService agentGroupService;
    private StateMachineService stateMachineService;
    private MonitoringLogsService monitoringLogsService;
    private ChartViewService chartViewService;
    private UserDetail user;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(1_790_000_000_000L);
        mongo = new InMemoryMongoOperations();

        user = new UserDetail(USER_OID, "tenant-it", "alice", "pwd", List.of());

        mockTaskRepo = mock(TaskRepository.class);
        when(mockTaskRepo.update(any(Query.class), any(Update.class)))
                .thenReturn(UpdateResult.acknowledged(1L, 1L, null));
        realTaskService = new TaskServiceImpl(mockTaskRepo);

        mockScheduleTaskService = mock(TaskService.class);
        workerService = mock(WorkerService.class);
        agentGroupService = mock(AgentGroupService.class);
        stateMachineService = mock(StateMachineService.class);
        monitoringLogsService = mock(MonitoringLogsService.class);
        chartViewService = mock(ChartViewService.class);

        UserService userService = mock(UserService.class);
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(user);

        context = new GenericApplicationContext();
        context.registerBean(TaskThrottleSampleConfigService.class,
                () -> new TaskThrottleSampleConfigService(mongo.ops(), clock));
        context.registerBean(TaskThrottleExecutionGuardService.class,
                () -> new TaskThrottleExecutionGuardService(mongo.ops(), clock));
        context.registerBean(TaskThrottleTelemetryAndRankingService.class, () -> {
            TaskThrottleTelemetryAndRankingService svc = new TaskThrottleTelemetryAndRankingService(
                    mongo.ops(), context.getBean(TaskThrottleSampleConfigService.class), clock);
            svc.setChartViewService(chartViewService);
            return svc;
        });
        context.registerBean(TaskThrottlePolicyService.class, () -> {
            TaskThrottlePolicyService svc = new TaskThrottlePolicyService(mongo.ops(), clock);
            svc.setExecutionGuardService(context.getBean(TaskThrottleExecutionGuardService.class));
            svc.setTelemetryAndRankingService(context.getBean(TaskThrottleTelemetryAndRankingService.class));
            return svc;
        });
        context.registerBean(AgentThrottleController.class, () -> new AgentThrottleController(
                context.getBean(TaskThrottleExecutionGuardService.class),
                context.getBean(TaskThrottleTelemetryAndRankingService.class)));
        context.registerBean(TaskScheduleServiceImpl.class, () -> {
            TaskScheduleServiceImpl s = new TaskScheduleServiceImpl();
            s.setWorkerService(workerService);
            s.setTaskService(mockScheduleTaskService);
            s.setMessageQueueService(mock(MessageQueueService.class));
            s.setMonitoringLogsService(monitoringLogsService);
            s.setTaskCollectionObjService(mock(TaskCollectionObjService.class));
            s.setStateMachineService(stateMachineService);
            s.setUserService(userService);
            s.setAgentGroupService(agentGroupService);
            s.setDataSourceService(mock(DataSourceService.class));
            ReflectionTestUtils.setField(s, "taskThrottleExecutionGuardService",
                    context.getBean(TaskThrottleExecutionGuardService.class));
            return s;
        });
        context.registerBean(TaskController.class, () -> {
            TaskController c = new TaskController();
            ReflectionTestUtils.setField(c, "taskService", realTaskService);
            ReflectionTestUtils.setField(c, "taskThrottleExecutionGuardService",
                    context.getBean(TaskThrottleExecutionGuardService.class));
            return c;
        });
        context.refresh();

        policyService = context.getBean(TaskThrottlePolicyService.class);
        guardService = context.getBean(TaskThrottleExecutionGuardService.class);
        sampleConfigService = context.getBean(TaskThrottleSampleConfigService.class);
        telemetryService = context.getBean(TaskThrottleTelemetryAndRankingService.class);
        agentController = context.getBean(AgentThrottleController.class);
        scheduleService = context.getBean(TaskScheduleServiceImpl.class);
        taskController = context.getBean(TaskController.class);
    }

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    @DisplayName("调度交集优先过滤兼容 Agent 并在缺失兼容执行器时阻塞启动签发")
    void should_filter_compatible_agents_and_block_incompatible_start() {
        registerCompatibleAgent("agent-compat-1");
        agentController.registerCapabilities(AgentThrottleCapabilities.builder()
                .agentId("agent-legacy")
                .throttleSupported(false)
                .meterVersion("legacy-v0")
                .build());

        assertThat(guardService.filterCompatibleAgentIds(
                List.of("agent-legacy", "agent-compat-1", "agent-missing"), "sync"))
                .containsExactly("agent-compat-1");

        ObjectId oid = MongoUtils.toObjectId("66f000000000000000000201");
        String taskId = oid.toHexString();
        policyService.updatePolicy("sync", taskId, policyReq(0L, 2_000_000L), user);

        // 1. 组内既有旧版也有兼容节点时，真实 TaskScheduleServiceImpl 自动避开旧节点并签发 epoch=1
        TaskDto groupTask = buildTask(oid, "group-sched-task", "sync");
        groupTask.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
        groupTask.setAccessNodeProcessId("group-1");
        groupTask.setStatus(TaskDto.STATUS_SCHEDULING);
        when(mockScheduleTaskService.findByTaskId(eq(oid), eq("user_id"))).thenReturn(groupTask);
        when(mockScheduleTaskService.findById(eq(oid), eq(user))).thenReturn(groupTask);
        when(agentGroupService.getProcessNodeListWithGroup(any(TaskDto.class), any()))
                .thenReturn(List.of("agent-legacy", "agent-compat-1"));
        when(workerService.findAvailableAgentByAccessNode(any(), any()))
                .thenReturn(List.of(worker("agent-legacy"), worker("agent-compat-1")));
        when(workerService.scheduleTaskToEngineWithStrictAgent(any(), any(), anyString(), anyString()))
                .thenAnswer(inv -> {
                    TaskDto t = inv.getArgument(0);
                    CalculationEngineVo vo = new CalculationEngineVo();
                    vo.setProcessId(t.getAgentId());
                    vo.setProcessName(t.getAgentId());
                    vo.setAvailable(1);
                    vo.setRunningNum(0);
                    vo.setTaskLimit(100);
                    vo.setThreadLog(new ArrayList<>());
                    return vo;
                });
        when(mockScheduleTaskService.subCronOrPlanNum(any(), any(Integer.class))).thenReturn(0);
        when(mockScheduleTaskService.runningTaskNum(anyString(), any())).thenReturn(0);
        when(stateMachineService.executeAboutTask(any(TaskDto.class), eq(DataFlowEvent.SCHEDULE_SUCCESS), eq(user)))
                .thenReturn(StateMachineResult.ok());

        scheduleService.scheduling(groupTask, user, false);
        assertThat(groupTask.getAgentId()).isEqualTo("agent-compat-1");
        assertThat(groupTask.getAttrs()).containsEntry("throttleRunEpoch", 1L);
        assertThat(groupTask.getAttrs()).containsEntry("throttleProtected", true);

        // 2. 当任务仅指向不兼容节点 agent-legacy 时，调度守护拒绝放行并进入等待兼容通道门禁
        ObjectId blockedOid = MongoUtils.toObjectId("66f000000000000000000202");
        String blockedId = blockedOid.toHexString();
        policyService.updatePolicy("sync", blockedId, policyReq(0L, 1_000_000L), user);
        TaskDto manualLegacyTask = buildTask(blockedOid, "legacy-manual-task", "sync");
        manualLegacyTask.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        manualLegacyTask.setAccessNodeProcessId("agent-legacy");
        manualLegacyTask.setAccessNodeProcessIdList(List.of("agent-legacy"));
        manualLegacyTask.setStatus(TaskDto.STATUS_SCHEDULING);
        when(mockScheduleTaskService.findByTaskId(eq(blockedOid), eq("user_id"))).thenReturn(manualLegacyTask);
        when(agentGroupService.getProcessNodeListWithGroup(eq(manualLegacyTask), any()))
                .thenReturn(List.of("agent-legacy"));

        org.mockito.Mockito.clearInvocations(stateMachineService);
        scheduleService.scheduling(manualLegacyTask, user, false);
        verify(stateMachineService, never()).executeAboutTask(
                any(TaskDto.class), eq(DataFlowEvent.SCHEDULE_SUCCESS), any());
        TaskThrottlePolicyEntity gated = guardService.findPolicyByTaskId(blockedId);
        assertThat(gated.getStopGateStatus()).isEqualTo(TaskThrottlePolicyEntity.STOP_GATE_WAITING_COMPATIBLE_AGENT);
        assertThat(gated.getCurrentRunEpoch()).isZero();
    }

    @Test
    @DisplayName("受保护运行接管与超时重调必须依赖未消费停止证据，并在 CAS 消费后立即失效")
    void should_enforce_stop_evidence_cas_consumption_and_block_timeout_reschedule() {
        registerCompatibleAgent("agent-compat-1");
        registerCompatibleAgent("agent-compat-2");

        ObjectId oid = MongoUtils.toObjectId("66f000000000000000000203");
        String taskId = oid.toHexString();
        policyService.updatePolicy("sync", taskId, policyReq(0L, 1_500_000L), user);
        TaskDto dto = buildTask(oid, "protected-sync", "sync");

        TaskThrottleExecutionGuardService.AuthorizationDecision run1 =
                guardService.authorizeStartOrTakeover(dto, "agent-compat-1");
        assertThat(run1.allowed()).isTrue();
        assertThat(run1.runEpoch()).isEqualTo(1L);

        assertThat(guardService.shouldBlockTimeoutReschedule(dto)).isTrue();
        TaskThrottleExecutionGuardService.AuthorizationDecision blocked =
                guardService.authorizeStartOrTakeover(dto, "agent-compat-2");
        assertThat(blocked.allowed()).isFalse();
        assertThat(blocked.errorCode()).isEqualTo(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED);
        assertThat(guardService.findPolicyByTaskId(taskId).getStopGateStatus())
                .isEqualTo(TaskThrottlePolicyEntity.STOP_GATE_WAITING_EVIDENCE);

        // Agent 优雅停止后上报停止证据 -> 超时重调与新 Agent 接管解除阻塞，且证据仅消费一次
        agentController.uploadStopEvidence(TaskThrottleStopEvidenceDto.builder()
                .taskId(taskId)
                .oldAgentId("agent-compat-1")
                .oldRunEpoch(1L)
                .evidenceRef("graceful-stop-1")
                .processExited(false)
                .writeSettled(true)
                .build());

        assertThat(guardService.shouldBlockTimeoutReschedule(dto)).isFalse();
        TaskThrottleExecutionGuardService.AuthorizationDecision run2 =
                guardService.authorizeStartOrTakeover(dto, "agent-compat-2");
        assertThat(run2.allowed()).isTrue();
        assertThat(run2.runEpoch()).isEqualTo(2L);

        // 无新停止证据不得借旧证据再次接管
        TaskThrottleExecutionGuardService.AuthorizationDecision blockedAgain =
                guardService.authorizeStartOrTakeover(dto, "agent-compat-1");
        assertThat(blockedAgain.allowed()).isFalse();
        assertThat(blockedAgain.errorCode()).isEqualTo(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED);
    }

    @Test
    @DisplayName("断点上报通过 TaskController 校验并在落库 Query 中附加 runEpoch 原子条件")
    void should_fence_stale_epoch_checkpoints_and_forward_authorized_run_epoch() {
        registerCompatibleAgent("agent-compat-1");
        registerCompatibleAgent("agent-compat-2");

        ObjectId oid = MongoUtils.toObjectId("66f000000000000000000204");
        String taskId = oid.toHexString();
        policyService.updatePolicy("sync", taskId, policyReq(0L, 1_500_000L), user);
        TaskDto dto = buildTask(oid, "fenced-checkpoint-task", "sync");

        assertThat(guardService.authorizeStartOrTakeover(dto, "agent-compat-1").runEpoch()).isEqualTo(1L);
        agentController.uploadStopEvidence(TaskThrottleStopEvidenceDto.builder()
                .taskId(taskId)
                .oldAgentId("agent-compat-1")
                .oldRunEpoch(1L)
                .evidenceRef("graceful-cp-1")
                .writeSettled(true)
                .build());
        assertThat(guardService.authorizeStartOrTakeover(dto, "agent-compat-2").runEpoch()).isEqualTo(2L);

        // 旧代次 runEpoch=1 与旧 agentId 均被拒绝且零库写
        assertThatThrownBy(() -> taskController.updateSyncProgress(taskId, 1L, "agent-compat-1", "{\"offset\":10}"))
                .isInstanceOfSatisfying(BizException.class,
                        ex -> assertThat(ex.getErrorCode()).isEqualTo(ThrottleConstants.ERR_STALE_RUN_EPOCH));
        assertThatThrownBy(() -> taskController.updateSyncProgress(taskId, 2L, "agent-compat-1", "{\"offset\":10}"))
                .isInstanceOfSatisfying(BizException.class,
                        ex -> assertThat(ex.getErrorCode()).isEqualTo(ThrottleConstants.ERR_STALE_RUN_EPOCH));
        verify(mockTaskRepo, never()).update(any(Query.class), any(Update.class));

        // 当前授权代次放行，真实 TaskServiceImpl 生成含 attrs.throttleRunEpoch=2 的原子条件 Query
        taskController.updateSyncProgress(taskId, 2L, "agent-compat-2", "{\"edge1\":{\"offset\":99}}");
        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mockTaskRepo).update(queryCaptor.capture(), updateCaptor.capture());

        Document criteriaDoc = queryCaptor.getValue().getQueryObject();
        assertThat(criteriaDoc.get("_id")).isEqualTo(oid);
        assertThat(criteriaDoc.get("attrs.throttleRunEpoch")).isEqualTo(2L);
        Document setDoc = (Document) updateCaptor.getValue().getUpdateObject().get("$set");
        assertThat(setDoc).containsKey("attrs.syncProgress.edge1");
    }

    @Test
    @DisplayName("差量策略与系统采样配置热更下发、旧 Agent fencing 及回执去重单调更新与失败保留")
    void should_deliver_incremental_delta_fencing_and_handle_idempotent_monotonic_receipts() {
        registerCompatibleAgent("agent-compat-1");
        registerCompatibleAgent("agent-compat-2");

        ObjectId oid = MongoUtils.toObjectId("66f000000000000000000205");
        String taskId = oid.toHexString();
        policyService.updatePolicy("sync", taskId, policyReq(0L, 2_000_000L), user);
        TaskDto dto = buildTask(oid, "delta-receipt-task", "sync");
        guardService.authorizeStartOrTakeover(dto, "agent-compat-1");

        AgentThrottleDeltaResponse delta1 = agentController.pullDelta("agent-compat-1", 0L).getData();
        assertThat(delta1.isFullSync()).isTrue();
        assertThat(delta1.getDirectives()).hasSize(1);
        assertThat(delta1.getDirectives().get(0).isFenced()).isFalse();
        assertThat(delta1.getCdcSampleRefreshIntervalSeconds()).isEqualTo(30);

        // 热更系统采样配置至 60s，并把策略升到 v2
        clock.advanceMillis(100L);
        sampleConfigService.updateInterval(60);
        policyService.updatePolicy("sync", taskId, policyReq(1L, 4_000_000L), user);

        AgentThrottleDeltaResponse delta2 = agentController.pullDelta("agent-compat-1", delta1.getCursor()).getData();
        assertThat(delta2.isFullSync()).isFalse();
        assertThat(delta2.getCdcSampleRefreshIntervalSeconds()).isEqualTo(60);
        assertThat(delta2.getDirectives()).hasSize(1);
        assertThat(delta2.getDirectives().get(0).getPolicy().getDesiredVersion()).isEqualTo(2L);

        // 回执幂等、跳过中间版本 v1 后单调应用 v2、重复和乱序 v1 不回退、失败回执保留 v2
        String taskKey = TaskThrottlePolicyEntity.buildTaskKey("tenant-it", "sync", taskId);
        agentController.uploadReceipts(Map.of(
                "agentId", "agent-compat-1",
                "receipts", List.of(
                        TaskThrottleApplyReceipt.builder()
                                .taskKey(taskKey)
                                .runEpoch(1L)
                                .policyVersion(2L)
                                .outcome("APPLIED")
                                .skippedVersions(List.of(1L))
                                .appliedAt(clock.millis())
                                .build(),
                        TaskThrottleApplyReceipt.builder()
                                .taskKey(taskKey)
                                .runEpoch(1L)
                                .policyVersion(2L)
                                .outcome("APPLIED")
                                .build(),
                        TaskThrottleApplyReceipt.builder()
                                .taskKey(taskKey)
                                .runEpoch(1L)
                                .policyVersion(1L)
                                .outcome("APPLIED")
                                .build())));

        clock.advanceMillis(10L);
        policyService.updatePolicy("sync", taskId, policyReq(2L, 8_000_000L), user);
        agentController.uploadReceipts(Map.of(
                "agentId", "agent-compat-1",
                "receipts", List.of(TaskThrottleApplyReceipt.builder()
                        .taskKey(taskKey)
                        .runEpoch(1L)
                        .policyVersion(3L)
                        .outcome("FAILED")
                        .reason("Disk buffer allocation rejected")
                        .appliedAt(clock.millis())
                        .build())));

        TaskThrottlePolicyView viewAfterFail = policyService.getPolicyView("sync", taskId, user);
        assertThat(viewAfterFail.getAppliedVersion()).isEqualTo(2L);
        assertThat(viewAfterFail.getAppliedPhases().get(ThrottlePhase.FULL).getBytesPerSec()).isEqualTo(4_000_000L);
        assertThat(viewAfterFail.getApplyStatus()).isEqualTo(TaskThrottlePolicyEntity.APPLY_FAILED);
        assertThat(viewAfterFail.getLastApplyReason()).isEqualTo("Disk buffer allocation rejected");

        long v2ReceiptCount = mongo.ops().findAll(TaskThrottleChangeLogEntity.class).stream()
                .filter(c -> TaskThrottleChangeLogEntity.EVENT_APPLY_RECEIPT.equals(c.getEventType())
                        && c.getPolicyVersion() == 2L && c.getRunEpoch() == 1L)
                .count();
        assertThat(v2ReceiptCount).isEqualTo(1L);

        // 切换至 agent-compat-2 后，旧 agent-compat-1 增量拉取收到 fenced=true 指令
        agentController.uploadStopEvidence(TaskThrottleStopEvidenceDto.builder()
                .taskId(taskId)
                .oldAgentId("agent-compat-1")
                .oldRunEpoch(1L)
                .evidenceRef("graceful-fencing-1")
                .writeSettled(true)
                .build());
        guardService.authorizeStartOrTakeover(dto, "agent-compat-2");

        AgentThrottleDeltaResponse oldAgentDelta =
                agentController.pullDelta("agent-compat-1", delta2.getCursor()).getData();
        assertThat(oldAgentDelta.getDirectives()).hasSize(1);
        assertThat(oldAgentDelta.getDirectives().get(0).isFenced()).isTrue();
        assertThat(oldAgentDelta.getDirectives().get(0).getAuthorizedRunEpoch()).isEqualTo(2L);
    }

    @Test
    @DisplayName("实时等待 5s 续报/15s 超时未知/30s 历史回填隔离及 >=90% 并集覆盖率 Dashboard 排行与权限过滤")
    void should_evaluate_live_wait_status_5s_renewal_15s_unknown_and_90pct_ranking() {
        registerCompatibleAgent("agent-compat-1");

        ObjectId oidA = MongoUtils.toObjectId("66f000000000000000000301");
        ObjectId oidB = MongoUtils.toObjectId("66f000000000000000000302");
        ObjectId oidHidden = MongoUtils.toObjectId("66f000000000000000000303");
        String taskA = oidA.toHexString();
        String taskB = oidB.toHexString();
        String taskHidden = oidHidden.toHexString();

        policyService.updatePolicy("sync", taskA, policyReq(0L, 5_000_000L), user);
        guardService.authorizeStartOrTakeover(buildTask(oidA, "rank-qualified", "sync"), "agent-compat-1");

        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(List.of(
                buildTask(oidA, "rank-qualified", "sync"),
                buildTask(oidB, "rank-below-90", "migrate")));

        long windowEnd = clock.millis();
        long windowStart = windowEnd - 300_000L;

        // taskA：两个重叠子窗口合并后精确覆盖 280s (93.3% >= 90%)；taskB 仅覆盖 200s (<90%)；taskHidden 不可见
        agentController.uploadTelemetry(Map.of(
                "agentId", "agent-compat-1",
                "samplesByTask", Map.of(
                        taskA, List.of(
                                telemetry(taskA, 1L, 1L, ThrottleConstants.METER_VERSION,
                                        ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                                        windowStart, windowStart + 200_000L, 280_000_000L,
                                        WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class)),
                                telemetry(taskA, 1L, 2L, ThrottleConstants.METER_VERSION,
                                        ThrottlePhase.CDC, ThrottleDirection.OUTPUT,
                                        windowStart + 100_000L, windowStart + 280_000L, 280_000_000L,
                                        WaitReason.RATE_WAIT, EnumSet.of(ThrottleDimension.BYTES))),
                        taskB, List.of(
                                telemetry(taskB, 1L, 1L, ThrottleConstants.METER_VERSION,
                                        ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                                        windowStart, windowStart + 200_000L, 500_000_000L,
                                        WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class))),
                        taskHidden, List.of(
                                telemetry(taskHidden, 1L, 1L, ThrottleConstants.METER_VERSION,
                                        ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                                        windowStart, windowEnd, 9_000_000_000L,
                                        WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class))))));

        // 注入一条旧版本样本应被 metricVersion 过滤
        agentController.uploadTelemetry(Map.of(
                "agentId", "agent-compat-1",
                "samplesByTask", Map.of(taskA, List.of(
                        telemetry(taskA, 1L, 3L, "legacy-size",
                                ThrottlePhase.FULL, ThrottleDirection.OUTPUT,
                                windowStart, windowEnd, 9_000_000_000L,
                                WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class))))));

        // 发一条新鲜 RATE_WAIT 样本（>280s -> 至窗口终点，使 liveTracker 有近期激活，再加 4s 检查仍在限速中）
        clock.advanceMillis(4_000L);
        long freshEnd = clock.millis();
        agentController.uploadTelemetry(Map.of(
                "agentId", "agent-compat-1",
                "samplesByTask", Map.of(taskA, List.of(
                        telemetry(taskA, 1L, 4L, ThrottleConstants.METER_VERSION,
                                ThrottlePhase.CDC, ThrottleDirection.OUTPUT,
                                freshEnd - 4_000L, freshEnd, 8_000_000L,
                                WaitReason.RATE_WAIT, EnumSet.of(ThrottleDimension.QPS))))));

        // 超过 30s 的历史回填样本（WaitReason.NONE）不得覆盖实时限速状态
        agentController.uploadTelemetry(Map.of(
                "agentId", "agent-compat-1",
                "samplesByTask", Map.of(taskA, List.of(
                        telemetry(taskA, 1L, 50L, ThrottleConstants.METER_VERSION,
                                ThrottlePhase.CDC, ThrottleDirection.OUTPUT,
                                freshEnd - 65_000L, freshEnd - 60_000L, 1_000L,
                                WaitReason.NONE, EnumSet.noneOf(ThrottleDimension.class))))));

        TaskThrottleLiveStatusView live4s = telemetryService.evaluateLiveStatus(taskA);
        assertThat(live4s.isThrottlingActive()).isTrue();
        assertThat(live4s.isStatusUnknown()).isFalse();
        assertThat(live4s.getActiveDimensionsByPhase().get(ThrottlePhase.CDC)).containsExactly(ThrottleDimension.QPS);

        TaskThrottleRankingResponse ranking = telemetryService.queryEstimatedDataRateRanking(
                user, ThrottleDirection.OUTPUT, 300L, 5);
        assertThat(ranking.getMeterVersion()).isEqualTo(ThrottleConstants.METER_VERSION);
        assertThat(ranking.getItems()).hasSize(1);
        assertThat(ranking.getItems().get(0).getTaskId()).isEqualTo(taskA);
        assertThat(ranking.getItems().get(0).getCoverageRatio()).isGreaterThanOrEqualTo(0.90d);
        assertThat(ranking.getItems().get(0).isThrottleEnabled()).isTrue();
        assertThat(ranking.getExcluded()).extracting(TaskThrottleRankingResponse.ExcludedItem::getTaskId)
                .containsExactly(taskB);

        // 推进 16s 未收到续报 -> 实时等待态转为 statusUnknown=true，避免陈旧误报
        clock.advanceMillis(16_000L);
        TaskThrottleLiveStatusView expired = telemetryService.evaluateLiveStatus(taskA);
        assertThat(expired.isStatusUnknown()).isTrue();
        assertThat(expired.isThrottlingActive()).isFalse();
    }

    private void registerCompatibleAgent(String agentId) {
        agentController.registerCapabilities(AgentThrottleCapabilities.builder()
                .agentId(agentId)
                .throttleSupported(true)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .supportedTaskKinds(Set.of("sync", "migrate", "logCollector"))
                .supportedDimensions(Set.of(ThrottleDimension.BYTES, ThrottleDimension.QPS))
                .build());
    }

    private static Worker worker(String processId) {
        Worker w = new Worker();
        w.setProcessId(processId);
        return w;
    }

    private static TaskDto buildTask(ObjectId id, String name, String syncType) {
        TaskDto dto = new TaskDto();
        dto.setId(id);
        dto.setName(name);
        dto.setSyncType(syncType);
        dto.setType("initial_sync+cdc");
        dto.setCustomId("tenant-it");
        dto.setUserId(USER_OID);
        dto.setCanOpenInspect(false);
        return dto;
    }

    private static TaskThrottlePolicyUpdateRequest policyReq(long expectedVersion, long bytesPerSec) {
        return TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(expectedVersion)
                .phases(Map.of(ThrottlePhase.FULL, ThrottlePhaseLimit.builder()
                        .enabled(true)
                        .bytesPerSec(bytesPerSec)
                        .build()))
                .build();
    }

    private static TaskThrottleTelemetrySample telemetry(
            String ignoredTaskId,
            long runEpoch,
            long seq,
            String meterVersion,
            ThrottlePhase phase,
            ThrottleDirection direction,
            long winStart,
            long winEnd,
            long bytes,
            WaitReason waitReason,
            Set<ThrottleDimension> waitingDims) {
        return TaskThrottleTelemetrySample.builder()
                .runEpoch(runEpoch)
                .sampleSeq(seq)
                .meterVersion(meterVersion)
                .phase(phase)
                .direction(direction)
                .windowStartUtc(winStart)
                .windowEndUtc(winEnd)
                .durationNanos((winEnd - winStart) * 1_000_000L)
                .timeQuality(TimeQuality.ALIGNED)
                .sampleQuality(SampleQuality.VALID)
                .admittedEvents(Math.max(1L, bytes / 1_000L))
                .estimatedAdmittedBytes(bytes)
                .waitReason(waitReason)
                .waitingDimensions(waitingDims)
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

        @Override
        public long millis() {
            return millis.get();
        }
    }
}
