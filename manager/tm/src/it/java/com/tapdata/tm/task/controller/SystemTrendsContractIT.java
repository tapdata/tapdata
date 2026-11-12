package com.tapdata.tm.task.controller;

import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.base.handler.ExceptionHandler;
import com.tapdata.tm.base.security.LoginUserResolver;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.config.security.LoginUserInterceptor;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.chart.ChartViewService;
import com.tapdata.tm.task.service.dashboard.TaskDashboardService;
import com.tapdata.tm.task.vo.TaskDashboardVo;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiRequestTrend;
import com.tapdata.tm.v2.api.monitor.service.ApiMetricsChartQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.web.context.request.RequestContextHolder;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 系统趋势 REST 契约黑盒验证（L1）。
 *
 * <p>通过 MockMvc 走真实 TaskController + LoginUserInterceptor + ExceptionHandler，
 * 只替换 TaskService 的下游依赖，验证路由、参数绑定、7 天边界、非法节点与未认证行为。</p>
 */
class SystemTrendsContractIT {

    private static final long NOW_SECONDS = 1_790_000_000L;
    private static final long MAX_RANGE_SECONDS = 7L * 24L * 60L * 60L;
    private static final String USER_ID = "user-a";

    private MockMvc mvc;
    private UserDetail user;
    private TaskService taskService;
    private TaskDashboardService dashboardService;
    private ClusterStateService clusterStateService;

    @BeforeEach
    void setUp() {
        user = new UserDetail(USER_ID, "customer", USER_ID, "password", List.of());

        UserService userService = mock(UserService.class);
        when(userService.loadUserByExternalId(USER_ID)).thenReturn(user);
        ProductComponent productComponent = mock(ProductComponent.class);
        when(productComponent.isDAAS()).thenReturn(false);

        LoginUserResolver resolver = new LoginUserResolver();
        ReflectionTestUtils.setField(resolver, "userService", userService);
        ReflectionTestUtils.setField(resolver, "accessTokenService", mock(AccessTokenService.class));
        ReflectionTestUtils.setField(resolver, "productComponent", productComponent);

        dashboardService = new TaskDashboardService();
        ReflectionTestUtils.setField(dashboardService, "clock", Clock.fixed(Instant.ofEpochSecond(NOW_SECONDS), ZoneOffset.UTC));
        ReflectionTestUtils.setField(dashboardService, "chartViewService", mock(ChartViewService.class));
        ReflectionTestUtils.setField(dashboardService, "measurementServiceV2", mock(MeasurementServiceV2.class));
        ReflectionTestUtils.setField(dashboardService, "dataSourceService", mock(DataSourceService.class));
        clusterStateService = mock(ClusterStateService.class);
        ReflectionTestUtils.setField(dashboardService, "clusterStateService", clusterStateService);

        ApiMetricsChartQuery apiMetricsChartQuery = mock(ApiMetricsChartQuery.class);
        ApiRequestTrend emptyTrend = new ApiRequestTrend();
        emptyTrend.setTs(new ArrayList<>());
        emptyTrend.setValues(new ArrayList<>());
        emptyTrend.setStatus("no_data");
        emptyTrend.setTraceability("complete");
        emptyTrend.setCoverage("unknown");
        when(apiMetricsChartQuery.homepageRequestTrendV2(any(), nullable(String.class))).thenReturn(emptyTrend);
        ReflectionTestUtils.setField(dashboardService, "apiMetricsChartQuery", apiMetricsChartQuery);

        taskService = mock(TaskService.class);
        when(taskService.dashboard(any(), nullable(String.class), nullable(Long.class), nullable(String.class),
                nullable(Integer.class), nullable(String.class), nullable(Long.class), nullable(Long.class),
                nullable(Integer.class)))
                .thenAnswer(invocation -> dashboardService.dashboard(
                        invocation.getArgument(0),
                        invocation.getArgument(1),
                        invocation.getArgument(2),
                        invocation.getArgument(3),
                        invocation.getArgument(4),
                        invocation.getArgument(5),
                        invocation.getArgument(6),
                        invocation.getArgument(7),
                        invocation.getArgument(8)));
        when(taskService.dashboardNodes(any())).thenReturn(new ArrayList<>());

        TaskController controller = new TaskController();
        ReflectionTestUtils.setField(controller, "taskService", taskService);
        ReflectionTestUtils.setField(controller, "loginUserResolver", resolver);

        mvc = org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ExceptionHandler())
                .addInterceptors(new LoginUserInterceptor(resolver))
                .build();
    }

    @AfterEach
    void tearDown() {
        RequestContextHolder.resetRequestAttributes();
    }

    private MockHttpServletRequestBuilder authenticated(MockHttpServletRequestBuilder builder) {
        return builder.header("user_id", USER_ID);
    }

    private String rangeRequest(long startAt, long endAt) {
        return "/api/task/dashboard?dashboardType=trends&type=range&step=3600"
                + "&startAt=" + startAt + "&endAt=" + endAt + "&trendSchemaVersion=2";
    }

    @Test
    @DisplayName("7 天边界与合法自定义范围可查询，节点接口注册")
    void allowsSevenDayRangeAndNodeOptions() throws Exception {
        long endAt = NOW_SECONDS;
        long startAt = endAt - MAX_RANGE_SECONDS;

        mvc.perform(authenticated(get(rangeRequest(startAt, endAt))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").exists());

        TaskDashboardVo.NodeOption option = new TaskDashboardVo.NodeOption();
        option.setPhysicalNodeId("node-a");
        when(taskService.dashboardNodes(any())).thenReturn(List.of(option));
        mvc.perform(authenticated(get("/api/task/dashboard/nodes")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].physicalNodeId").value("node-a"));
    }

    @Test
    @DisplayName("超过 7 天被拒绝并返回 query.range.too.large")
    void rejectsRangeLongerThanSevenDays() throws Exception {
        long endAt = NOW_SECONDS;
        long startAt = endAt - MAX_RANGE_SECONDS - 1L;

        mvc.perform(authenticated(get(rangeRequest(startAt, endAt))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("query.range.too.large"));
    }

    @Test
    @DisplayName("未来 endAt、反向、相等与缺一端均返回 task.dashboard.query.invalid")
    void rejectsInvalidRangeShapes() throws Exception {
        mvc.perform(authenticated(get(rangeRequest(NOW_SECONDS - 60L, NOW_SECONDS + 60L))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("task.dashboard.query.invalid"));

        mvc.perform(authenticated(get(rangeRequest(NOW_SECONDS, NOW_SECONDS - 60L))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("task.dashboard.query.invalid"));

        mvc.perform(authenticated(get(rangeRequest(NOW_SECONDS, NOW_SECONDS))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("task.dashboard.query.invalid"));

        mvc.perform(authenticated(get("/api/task/dashboard?dashboardType=trends&type=range"
                + "&startAt=" + (NOW_SECONDS - 60L) + "&trendSchemaVersion=2")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("task.dashboard.query.invalid"));
    }

    @Test
    @DisplayName("非法节点 UUID 返回 server.not.exists，不返回全量数据")
    void rejectsUnknownPhysicalNode() throws Exception {
        when(clusterStateService.findAll(any(org.springframework.data.mongodb.core.query.Query.class))).thenReturn(new ArrayList<>());
        mvc.perform(authenticated(get(rangeRequest(NOW_SECONDS - 3600L, NOW_SECONDS)
                + "&physicalNodeId=unknown-node")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value("server.not.exists"));
    }

    @Test
    @DisplayName("未认证访问趋势与节点接口均返回 401")
    void rejectsAnonymousRequests() throws Exception {
        mvc.perform(get(rangeRequest(NOW_SECONDS - 3600L, NOW_SECONDS)))
                .andExpect(status().isUnauthorized());
        mvc.perform(get("/api/task/dashboard/nodes"))
                .andExpect(status().isUnauthorized());
    }
}
