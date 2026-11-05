package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.security.LoginUserResolver;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.vo.TaskDashboardVo;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskControllerDashboardContractTest {
    private TaskController controller;
    private TaskService taskService;
    private UserDetail user;

    @BeforeEach
    void setUp() {
        controller = new TaskController();
        taskService = mock(TaskService.class);
        user = mock(UserDetail.class);
        LoginUserResolver loginUserResolver = mock(LoginUserResolver.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(loginUserResolver.resolve(any(HttpServletRequest.class), isNull())).thenReturn(user);
        ReflectionTestUtils.setField(controller, "taskService", taskService);
        ReflectionTestUtils.setField(controller, "loginUserResolver", loginUserResolver);
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
    }

    @AfterEach
    void tearDown() {
        RequestContextHolder.resetRequestAttributes();
    }

    @Test
    void dashboardForwardsV2TrendParametersAndTheNodeOptionsRouteIsRegistered() throws Exception {
        long startAt = 720_000_000L;
        long endAt = startAt + 3600L;
        TaskDashboardVo expected = new TaskDashboardVo();
        when(taskService.dashboard(user, "range", 3600L, "trends", 5,
                "node-a", startAt, endAt, 2)).thenReturn(expected);

        ResponseMessage<TaskDashboardVo> response = controller.dashboard("range", 3600L, "trends", 5,
                "node-a", startAt, endAt, 2);

        assertSame(expected, response.getData());
        verify(taskService).dashboard(user, "range", 3600L, "trends", 5,
                "node-a", startAt, endAt, 2);

        Method nodesMethod = TaskController.class.getMethod("dashboardNodes");
        GetMapping mapping = nodesMethod.getAnnotation(GetMapping.class);
        assertNotNull(mapping);
        assertArrayEquals(new String[]{"/dashboard/nodes"}, mapping.value());
        List<TaskDashboardVo.NodeOption> nodes = List.of(new TaskDashboardVo.NodeOption());
        when(taskService.dashboardNodes(user)).thenReturn(nodes);
        assertSame(nodes, controller.dashboardNodes().getData());
        verify(taskService).dashboardNodes(user);
    }
}
