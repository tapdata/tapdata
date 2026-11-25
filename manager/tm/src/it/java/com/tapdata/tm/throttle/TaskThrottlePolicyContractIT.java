package com.tapdata.tm.throttle;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.base.handler.ExceptionHandler;
import com.tapdata.tm.base.security.LoginUserResolver;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.AgentThrottleCapabilities;
import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.TimeQuality;
import com.tapdata.tm.commons.throttle.WaitReason;
import com.tapdata.tm.config.LocaleConfig;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.config.security.LoginUserInterceptor;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.task.service.TaskServiceImpl;
import com.tapdata.tm.throttle.controller.TaskThrottleController;
import com.tapdata.tm.throttle.entity.TaskThrottleChangeLogEntity;
import com.tapdata.tm.throttle.service.LogCollectorThrottleOverviewService;
import com.tapdata.tm.throttle.service.TaskThrottleExecutionGuardService;
import com.tapdata.tm.throttle.service.TaskThrottlePolicyService;
import com.tapdata.tm.throttle.service.TaskThrottleSampleConfigService;
import com.tapdata.tm.throttle.service.TaskThrottleTelemetryAndRankingService;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.types.ObjectId;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 任务限速控制面 REST 契约集成验证（L1）。
 *
 * <p>通过 MockMvc 走真实 {@link TaskThrottleController}、生产 {@link LocaleConfig} 注册的
 * {@link LoginUserInterceptor} 与 {@link ExceptionHandler}，联动真实策略/守护/采样配置/遥测服务及
 * {@link ModuleAccess} 权限拦截，验证认证、CAS 409 冲突、路径校验、停止证据、系统设置边界及脱敏视图。</p>
 */
@Timeout(15)
class TaskThrottlePolicyContractIT {

    private static final String ADMIN_ID = "throttle-admin";
    private static final String READONLY_ID = "throttle-readonly";
    private static final long FIXED_NOW_MS = 1_790_000_000_000L;

    private MockMvc mvc;
    private UserDetail adminUser;
    private UserDetail readonlyUser;
    private TaskServiceImpl taskService;
    private TaskThrottleExecutionGuardService guardService;
    private TaskThrottleTelemetryAndRankingService telemetryService;

    private ObjectId syncTaskOid;
    private ObjectId migrateTaskOid;
    private ObjectId collectorTaskOid;
    private ObjectId crossJvmTaskOid;
    private ObjectId testRunTaskOid;
    private ObjectId initialOnlyOid;
    private ObjectId cdcOnlyOid;

    @BeforeEach
    void setUp() {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(FIXED_NOW_MS), ZoneOffset.UTC);
        InMemoryMongoOperations mongo = new InMemoryMongoOperations();

        adminUser = new UserDetail(ADMIN_ID, "tenant-a", "admin-op", "pwd", List.of());
        readonlyUser = new UserDetail(READONLY_ID, "tenant-b", "readonly-op", "pwd", List.of());

        UserServiceStubHolder holder = new UserServiceStubHolder(adminUser, readonlyUser);

        ProductComponent productComponent = mock(ProductComponent.class);
        when(productComponent.isDAAS()).thenReturn(false);

        LoginUserResolver resolver = new LoginUserResolver();
        ReflectionTestUtils.setField(resolver, "userService", holder.userService);
        ReflectionTestUtils.setField(resolver, "accessTokenService", mock(AccessTokenService.class));
        ReflectionTestUtils.setField(resolver, "productComponent", productComponent);

        taskService = mock(TaskServiceImpl.class);
        TaskThrottlePolicyService policyService = new TaskThrottlePolicyService(mongo.ops(), clock);
        guardService = new TaskThrottleExecutionGuardService(mongo.ops(), clock);
        TaskThrottleSampleConfigService sampleConfigService = new TaskThrottleSampleConfigService(mongo.ops(), clock);
        telemetryService = new TaskThrottleTelemetryAndRankingService(mongo.ops(), sampleConfigService, clock);
        LogCollectorThrottleOverviewService overviewService =
                new LogCollectorThrottleOverviewService(mongo.ops(), policyService, telemetryService, clock);

        policyService.setTaskService(taskService);
        policyService.setExecutionGuardService(guardService);
        policyService.setTelemetryAndRankingService(telemetryService);
        overviewService.setTaskService(taskService);

        PermissionService permissionService = mock(PermissionService.class);
        SettingsService settingsService = mock(SettingsService.class);
        when(settingsService.isCloud()).thenReturn(false);
        when(permissionService.checkCurrentUserHasPermission(TaskThrottleController.SETTINGS_PERMISSION, ADMIN_ID))
                .thenReturn(true);
        when(permissionService.checkCurrentUserHasPermission(TaskThrottleController.SETTINGS_PERMISSION, READONLY_ID))
                .thenReturn(false);
        ModuleAccess moduleAccess = new ModuleAccess(permissionService, settingsService);

        TaskThrottleController controller = new TaskThrottleController(
                policyService, guardService, telemetryService, sampleConfigService);
        controller.setLogCollectorOverviewService(overviewService);
        controller.setModuleAccess(moduleAccess);
        ReflectionTestUtils.setField(controller, "loginUserResolver", resolver);

        syncTaskOid = MongoUtils.toObjectId("66f000000000000000000101");
        migrateTaskOid = MongoUtils.toObjectId("66f000000000000000000102");
        collectorTaskOid = MongoUtils.toObjectId("66f000000000000000000103");
        crossJvmTaskOid = MongoUtils.toObjectId("66f000000000000000000104");
        testRunTaskOid = MongoUtils.toObjectId("66f000000000000000000105");
        initialOnlyOid = MongoUtils.toObjectId("66f000000000000000000106");
        cdcOnlyOid = MongoUtils.toObjectId("66f000000000000000000107");

        when(taskService.checkExistById(eq(syncTaskOid), eq(adminUser)))
                .thenReturn(task(syncTaskOid, "sync-task", "sync", "initial_sync+cdc", null));
        when(taskService.checkExistById(eq(migrateTaskOid), eq(adminUser)))
                .thenReturn(task(migrateTaskOid, "migrate-task", "migrate", "initial_sync+cdc", null));

        LogCollectorNode lcNode = new LogCollectorNode();
        lcNode.setConnectionIds(List.of("conn-shared-it"));
        lcNode.setStorageTime(7);
        DAG lcDag = mock(DAG.class);
        when(lcDag.getSources()).thenReturn(List.of(lcNode));
        TaskDto collectorDto = task(collectorTaskOid, "share-collector", "logCollector", "cdc", null);
        collectorDto.setStatus(TaskDto.STATUS_RUNNING);
        collectorDto.setDag(lcDag);
        when(taskService.checkExistById(eq(collectorTaskOid), eq(adminUser))).thenReturn(collectorDto);

        when(taskService.checkExistById(eq(crossJvmTaskOid), eq(adminUser)))
                .thenReturn(task(crossJvmTaskOid, "cross-jvm-task", "sync", "initial_sync+cdc", Map.of("crossJvm", true)));
        when(taskService.checkExistById(eq(testRunTaskOid), eq(adminUser)))
                .thenReturn(task(testRunTaskOid, "test-run-task", "testRun", "initial_sync", null));
        when(taskService.checkExistById(eq(initialOnlyOid), eq(adminUser)))
                .thenReturn(task(initialOnlyOid, "initial-only", "sync", "initial_sync", null));
        when(taskService.checkExistById(eq(cdcOnlyOid), eq(adminUser)))
                .thenReturn(task(cdcOnlyOid, "cdc-only", "sync", "cdc", null));

        // 复用生产 LocaleConfig 注册真实 LoginUserInterceptor
        LocaleConfig localeConfig = new LocaleConfig();
        ReflectionTestUtils.setField(localeConfig, "loginUserInterceptor", new LoginUserInterceptor(resolver));
        List<HandlerInterceptor> registered = new ArrayList<>();
        InterceptorRegistry registry = new InterceptorRegistry() {
            @Override
            public org.springframework.web.servlet.config.annotation.InterceptorRegistration addInterceptor(
                    HandlerInterceptor interceptor) {
                registered.add(interceptor);
                return super.addInterceptor(interceptor);
            }
        };
        localeConfig.addInterceptors(registry);
        assertThat(registered).anyMatch(LoginUserInterceptor.class::isInstance);

        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ExceptionHandler())
                .addInterceptors(registered.toArray(new HandlerInterceptor[0]))
                .build();
    }

    @AfterEach
    void tearDown() {
        RequestContextHolder.resetRequestAttributes();
    }

    @Test
    @DisplayName("未认证访问限速策略、采样设置与排行接口均返回 401")
    void should_reject_anonymous_requests_with_401() throws Exception {
        mvc.perform(get("/api/task-throttle/sync/" + syncTaskOid.toHexString()))
                .andExpect(status().isUnauthorized());
        mvc.perform(get("/api/task-throttle/sample-config"))
                .andExpect(status().isUnauthorized());
        mvc.perform(get("/api/task-throttle/ranking"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("支持三类任务限速策略并拒绝不支持的任务类型、跨JVM路径及非法速率配置")
    void should_support_three_task_kinds_and_reject_unsupported_kind_or_path() throws Exception {
        mvc.perform(asAdmin(put("/api/task-throttle/sync/" + syncTaskOid.toHexString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedVersion\":0,\"phases\":{\"FULL\":{\"enabled\":true,\"bytesPerSec\":2000000,\"eventsPerSec\":500}}}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.taskKind").value("sync"))
                .andExpect(jsonPath("$.data.desiredVersion").value(1));

        mvc.perform(asAdmin(put("/api/task-throttle/migrate/" + migrateTaskOid.toHexString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedVersion\":0,\"phases\":{\"CDC\":{\"enabled\":true,\"eventsPerSec\":300}}}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.taskKind").value("migrate"))
                .andExpect(jsonPath("$.data.desiredVersion").value(1));

        mvc.perform(asAdmin(put("/api/task-throttle/logCollector/" + collectorTaskOid.toHexString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedVersion\":0,\"phases\":{\"CDC\":{\"enabled\":true,\"bytesPerSec\":10000000}}}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.taskKind").value("logCollector"))
                .andExpect(jsonPath("$.data.desiredVersion").value(1));

        // 非首期支持类型返回 UNSUPPORTED_TASK_KIND
        mvc.perform(asAdmin(get("/api/task-throttle/testRun/" + testRunTaskOid.toHexString())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(ThrottleConstants.ERR_UNSUPPORTED_TASK_KIND));
        mvc.perform(asAdmin(get("/api/task-throttle/shareCache/" + testRunTaskOid.toHexString())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(ThrottleConstants.ERR_UNSUPPORTED_TASK_KIND));

        // 跨 JVM 不受支持执行路径返回 UNSUPPORTED_PATH
        mvc.perform(asAdmin(get("/api/task-throttle/sync/" + crossJvmTaskOid.toHexString())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(ThrottleConstants.ERR_UNSUPPORTED_PATH));

        // 任务运行模式与限速阶段不匹配、过小速率及空限速值返回 INVALID_RATE
        mvc.perform(asAdmin(put("/api/task-throttle/sync/" + initialOnlyOid.toHexString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedVersion\":0,\"phases\":{\"CDC\":{\"enabled\":true,\"eventsPerSec\":100}}}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(ThrottleConstants.ERR_INVALID_RATE));

        mvc.perform(asAdmin(put("/api/task-throttle/sync/" + cdcOnlyOid.toHexString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedVersion\":0,\"phases\":{\"FULL\":{\"enabled\":true,\"bytesPerSec\":5000}}}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(ThrottleConstants.ERR_INVALID_RATE));

        mvc.perform(asAdmin(put("/api/task-throttle/sync/" + syncTaskOid.toHexString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedVersion\":1,\"phases\":{\"FULL\":{\"enabled\":true,\"bytesPerSec\":999}}}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(ThrottleConstants.ERR_INVALID_RATE));

        mvc.perform(asAdmin(put("/api/task-throttle/sync/" + syncTaskOid.toHexString())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedVersion\":1,\"phases\":{\"FULL\":{\"enabled\":true}}}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(ThrottleConstants.ERR_INVALID_RATE));
    }

    @Test
    @DisplayName("策略 CAS 版本冲突返回 HTTP 409 与最新视图，保留不可变变更历史并拦截越权任务访问")
    void should_enforce_cas_409_conflict_and_persist_change_log_audit() throws Exception {
        String id = syncTaskOid.toHexString();
        mvc.perform(asAdmin(put("/api/task-throttle/sync/" + id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedVersion\":0,\"phases\":{\"FULL\":{\"enabled\":true,\"bytesPerSec\":5000000,\"eventsPerSec\":2000}}}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.desiredVersion").value(1))
                .andExpect(jsonPath("$.data.appliedVersion").value(0))
                .andExpect(jsonPath("$.data.appliedPhases").value(Matchers.nullValue()));

        mvc.perform(asAdmin(get("/api/task-throttle/sync/" + id + "/changes")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(1))
                .andExpect(jsonPath("$.data[0].eventType").value(TaskThrottleChangeLogEntity.EVENT_POLICY_CHANGE))
                .andExpect(jsonPath("$.data[0].policyVersion").value(1));

        // 使用过期 expectedVersion=0 触发 HTTP 409 Conflict
        mvc.perform(asAdmin(put("/api/task-throttle/sync/" + id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedVersion\":0,\"phases\":{\"FULL\":{\"enabled\":true,\"bytesPerSec\":1000000}}}")))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value(ThrottleConstants.ERR_VERSION_CONFLICT))
                .andExpect(jsonPath("$.data.latestVersion").value(1))
                .andExpect(jsonPath("$.data.latestView.desiredVersion").value(1));

        // 无该任务访问权限的另一租户用户应被拒绝
        mvc.perform(asReadonly(get("/api/task-throttle/sync/" + id)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("Task.NotFound"));
    }

    @Test
    @DisplayName("人工停止证据接口强制校验证据字段并在有效提交后记录审计")
    void should_validate_manual_stop_evidence_endpoint() throws Exception {
        guardService.registerCapabilities(AgentThrottleCapabilities.builder()
                .agentId("agent-old")
                .throttleSupported(true)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .supportedTaskKinds(Set.of("sync", "migrate", "logCollector"))
                .build());
        String id = syncTaskOid.toHexString();
        mvc.perform(asAdmin(put("/api/task-throttle/sync/" + id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedVersion\":0,\"phases\":{\"FULL\":{\"enabled\":true,\"eventsPerSec\":1000}}}")))
                .andExpect(status().isOk());

        TaskDto syncDto = task(syncTaskOid, "sync-task", "sync", "initial_sync+cdc", null);
        assertThat(guardService.authorizeStartOrTakeover(syncDto, "agent-old").runEpoch()).isEqualTo(1L);

        // 缺少 processExited=true 被拒绝
        mvc.perform(asAdmin(post("/api/task-throttle/sync/" + id + "/stop-evidence")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"oldAgentId\":\"agent-old\",\"oldRunEpoch\":1,\"evidenceRef\":\"ref-missing-exit\",\"processExited\":false,\"writeSettled\":true}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED));

        // 缺少 writeSettled=true 被拒绝
        mvc.perform(asAdmin(post("/api/task-throttle/sync/" + id + "/stop-evidence")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"oldAgentId\":\"agent-old\",\"oldRunEpoch\":1,\"evidenceRef\":\"ref-missing-write\",\"processExited\":true,\"writeSettled\":false}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED));

        // 完整人工停止证据成功保存并写入变更日志
        mvc.perform(asAdmin(post("/api/task-throttle/sync/" + id + "/stop-evidence")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"oldAgentId\":\"agent-old\",\"oldRunEpoch\":1,\"evidenceRef\":\"ref-verified-1\",\"processExited\":true,\"writeSettled\":true,\"note\":\"pid exited on host\"}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.source").value("OPERATOR_MANUAL"))
                .andExpect(jsonPath("$.data.consumed").value(false));

        mvc.perform(asAdmin(get("/api/task-throttle/sync/" + id + "/changes")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[?(@.eventType=='STOP_EVIDENCE')].outcome").value(Matchers.hasItem("OPERATOR_MANUAL")));
    }

    @Test
    @DisplayName("增量采样间隔接口校验权限与 5-300 秒整数边界")
    void should_enforce_sample_config_range_and_permission() throws Exception {
        mvc.perform(asAdmin(get("/api/task-throttle/sample-config")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.intervalSeconds").value(30))
                .andExpect(jsonPath("$.data.configVersion").value(1));

        // 无系统设置权限用户修改被拦截
        mvc.perform(asReadonly(put("/api/task-throttle/sample-config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"intervalSeconds\":60}")))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("NotAuthorized"));

        mvc.perform(asAdmin(get("/api/task-throttle/sample-config")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.intervalSeconds").value(30))
                .andExpect(jsonPath("$.data.configVersion").value(1));

        // 越界、小数、非数字均返回 INVALID_RATE
        for (String invalidPayload : List.of(
                "{\"intervalSeconds\":4}",
                "{\"intervalSeconds\":301}",
                "{\"intervalSeconds\":12.5}",
                "{\"intervalSeconds\":\"abc\"}")) {
            mvc.perform(asAdmin(put("/api/task-throttle/sample-config")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(invalidPayload)))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.code").value(ThrottleConstants.ERR_INVALID_RATE));
        }

        // 上下边界 5 与 300 合法并单调递增版本号
        mvc.perform(asAdmin(put("/api/task-throttle/sample-config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"intervalSeconds\":5}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.intervalSeconds").value(5))
                .andExpect(jsonPath("$.data.configVersion").value(2));

        mvc.perform(asAdmin(put("/api/task-throttle/sample-config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"intervalSeconds\":300}")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.intervalSeconds").value(300))
                .andExpect(jsonPath("$.data.configVersion").value(3));
    }

    @Test
    @DisplayName("批量 live-status 准确返回配置阶段，共享挖掘总览严格区分可见消费者与跨权限脱敏计数")
    void should_return_live_status_with_configured_phases_and_masked_log_collector_overview() throws Exception {
        String syncId = syncTaskOid.toHexString();
        String unconfiguredId = migrateTaskOid.toHexString();
        mvc.perform(asAdmin(put("/api/task-throttle/sync/" + syncId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedVersion\":0,\"phases\":{\"FULL\":{\"enabled\":true,\"bytesPerSec\":3000000}}}")))
                .andExpect(status().isOk());

        mvc.perform(asAdmin(get("/api/task-throttle/live-status?taskIds=" + syncId + "," + unconfiguredId)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data." + syncId + ".policyConfigured").value(true))
                .andExpect(jsonPath("$.data." + syncId + ".configuredPhases[0]").value("FULL"))
                .andExpect(jsonPath("$.data." + syncId + ".throttlingActive").value(false))
                .andExpect(jsonPath("$.data." + unconfiguredId + ".policyConfigured").value(false));

        String lcId = collectorTaskOid.toHexString();
        mvc.perform(asAdmin(put("/api/task-throttle/logCollector/" + lcId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedVersion\":0,\"phases\":{\"CDC\":{\"enabled\":true,\"eventsPerSec\":4000}}}")))
                .andExpect(status().isOk());

        telemetryService.ingestTelemetry("agent-lc", Map.of(lcId, List.of(
                TaskThrottleTelemetrySample.builder()
                        .runEpoch(1L)
                        .sampleSeq(1L)
                        .meterVersion(ThrottleConstants.METER_VERSION)
                        .phase(ThrottlePhase.CDC)
                        .direction(ThrottleDirection.INPUT)
                        .windowStartUtc(FIXED_NOW_MS - 5_000L)
                        .windowEndUtc(FIXED_NOW_MS)
                        .durationNanos(5_000_000_000L)
                        .timeQuality(TimeQuality.ALIGNED)
                        .sampleQuality(SampleQuality.VALID)
                        .admittedEvents(1_000L)
                        .estimatedAdmittedBytes(5_000_000L)
                        .waitReason(WaitReason.NONE)
                        .logPositionStatus(LogPositionStatus.NORMAL)
                        .build(),
                TaskThrottleTelemetrySample.builder()
                        .runEpoch(1L)
                        .sampleSeq(2L)
                        .meterVersion(ThrottleConstants.METER_VERSION)
                        .phase(ThrottlePhase.CDC)
                        .direction(ThrottleDirection.OUTPUT)
                        .windowStartUtc(FIXED_NOW_MS - 5_000L)
                        .windowEndUtc(FIXED_NOW_MS)
                        .durationNanos(5_000_000_000L)
                        .timeQuality(TimeQuality.ALIGNED)
                        .sampleQuality(SampleQuality.VALID)
                        .admittedEvents(800L)
                        .estimatedAdmittedBytes(4_000_000L)
                        .waitReason(WaitReason.RATE_WAIT)
                        .waitingDimensions(EnumSet.of(ThrottleDimension.QPS))
                        .logPositionStatus(LogPositionStatus.WARN_70)
                        .build()
        )));

        TaskDto visibleConsumer = task(syncTaskOid, "visible-consumer", "sync", "cdc",
                Map.of("syncProgress", Map.of("edge", "{\"eventTime\":" + (FIXED_NOW_MS - 10_000L) + "}")));
        when(taskService.count(any(Query.class))).thenReturn(3L);
        when(taskService.findAllDto(any(Query.class), eq(adminUser))).thenReturn(List.of(visibleConsumer));

        mvc.perform(asAdmin(get("/api/task-throttle/logCollector/" + lcId + "/overview")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.collectorTaskId").value(lcId))
                .andExpect(jsonPath("$.data.storageTimeDays").value(7))
                .andExpect(jsonPath("$.data.totalConsumerCount").value(3))
                .andExpect(jsonPath("$.data.hiddenConsumerCount").value(2))
                .andExpect(jsonPath("$.data.visibleConsumers.length()").value(1))
                .andExpect(jsonPath("$.data.visibleConsumers[0].taskName").value("visible-consumer"))
                .andExpect(jsonPath("$.data.sourceCapture.waitReason").value("NONE"))
                .andExpect(jsonPath("$.data.logWrite.waitReason").value("RATE_WAIT"))
                .andExpect(jsonPath("$.data.logWrite.logPositionStatus").value("WARN_70"));
    }

    private MockHttpServletRequestBuilder asAdmin(MockHttpServletRequestBuilder builder) {
        return builder.header("user_id", ADMIN_ID);
    }

    private MockHttpServletRequestBuilder asReadonly(MockHttpServletRequestBuilder builder) {
        return builder.header("user_id", READONLY_ID);
    }

    private static TaskDto task(
            ObjectId oid, String name, String syncType, String type, Map<String, Object> attrs) {
        TaskDto dto = new TaskDto();
        dto.setId(oid);
        dto.setName(name);
        dto.setSyncType(syncType);
        dto.setType(type);
        dto.setCustomId("tenant-a");
        dto.setUserId(ADMIN_ID);
        dto.setAttrs(attrs);
        return dto;
    }

    private static final class UserServiceStubHolder {
        private final com.tapdata.tm.user.service.UserService userService;

        UserServiceStubHolder(UserDetail adminUser, UserDetail readonlyUser) {
            this.userService = mock(com.tapdata.tm.user.service.UserService.class);
            when(this.userService.loadUserByExternalId(ADMIN_ID)).thenReturn(adminUser);
            when(this.userService.loadUserByExternalId(READONLY_ID)).thenReturn(readonlyUser);
        }
    }
}
