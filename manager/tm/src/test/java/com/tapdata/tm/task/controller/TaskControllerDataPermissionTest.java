package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.group.service.GroupInfoService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.function.Function;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class TaskControllerDataPermissionTest {
    private UserDetail userDetail;
    private HttpServletRequest request;
    private TaskService taskService;
    private TaskController controller;

    @BeforeEach
    void setUp() {
        userDetail = mock(UserDetail.class);
        request = mock(HttpServletRequest.class);
        taskService = mock(TaskService.class);
        controller = spy(new TaskController());
        ReflectionTestUtils.setField(controller, "taskService", taskService);
        doReturn(userDetail).when(controller).getLoginUser();
    }

    @Test
    void logCollectorStopUsesStopPermission() {
        String id = new ObjectId().toHexString();

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowLogCollectorPermission(helper, DataPermissionActionEnums.Stop);

            controller.stop(request, id, false);

            verify(taskService).pause(MongoUtils.toObjectId(id), userDetail, false);
        }
    }

    @Test
    void logCollectorRenewUsesResetPermission() {
        String id = new ObjectId().toHexString();

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowLogCollectorPermission(helper, DataPermissionActionEnums.Reset);

            controller.renew(request, id);

            verify(taskService).renew(MongoUtils.toObjectId(id), userDetail);
        }
    }

    @Test
    void logCollectorDeleteUsesDeletePermission() {
        String id = new ObjectId().toHexString();
        GroupInfoService groupInfoService = mock(GroupInfoService.class);
        ReflectionTestUtils.setField(controller, "groupInfoService", groupInfoService);

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowLogCollectorPermission(helper, DataPermissionActionEnums.Delete);

            controller.delete(request, id);

            verify(taskService).remove(MongoUtils.toObjectId(id), userDetail);
            verify(groupInfoService).removeResourceReferences(java.util.Collections.singletonList(id), userDetail);
        }
    }

    @Test
    void logCollectorRenewDoesNotExecuteWhenPermissionIsDenied() {
        String id = new ObjectId().toHexString();

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            denyPermission(helper, DataPermissionActionEnums.Reset);

            BizException exception = Assertions.assertThrows(BizException.class, () -> controller.renew(request, id));

            Assertions.assertEquals("insufficient.permissions", exception.getErrorCode());
            verify(taskService, never()).renew(any(ObjectId.class), same(userDetail));
        }
    }

    @Test
    void logCollectorBatchRenewDoesNotExecuteWhenPermissionIsDenied() {
        String id = new ObjectId().toHexString();
        HttpServletResponse response = mock(HttpServletResponse.class);

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            denyPermission(helper, DataPermissionActionEnums.Reset);

            Assertions.assertThrows(BizException.class, () -> controller.batchRenew(java.util.Collections.singletonList(id), null, request, response));

            verify(taskService, never()).batchRenew(any(), same(userDetail), same(request), same(response));
        }
    }

    @Test
    void logCollectorBatchRenewExecutesWithinEachTaskPermissionCheck() {
        String id = new ObjectId().toHexString();
        ObjectId objectId = MongoUtils.toObjectId(id);
        HttpServletResponse response = mock(HttpServletResponse.class);
        doReturn(java.util.Collections.emptyList()).when(taskService).batchRenew(
                eq(java.util.Collections.singletonList(objectId)),
                same(userDetail),
                same(request),
                same(response)
        );

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowLogCollectorPermission(helper, DataPermissionActionEnums.Reset);

            controller.batchRenew(java.util.Collections.singletonList(id), null, request, response);

            verify(taskService).batchRenew(
                    eq(java.util.Collections.singletonList(objectId)),
                    same(userDetail),
                    same(request),
                    same(response)
            );
        }
    }

    private void allowLogCollectorPermission(MockedStatic<DataPermissionHelper> helper, DataPermissionActionEnums action) {
        helper.when(() -> DataPermissionHelper.checkOfQuery(
                same(userDetail),
                eq(DataPermissionDataTypeEnums.Task),
                eq(action),
                any(),
                any(),
                any(),
                any()
        )).thenAnswer(invocation -> {
            Function<TaskDto, DataPermissionMenuEnums> menuResolver = invocation.getArgument(4);
            TaskDto taskDto = new TaskDto();
            taskDto.setSyncType(TaskDto.SYNC_TYPE_LOG_COLLECTOR);
            Assertions.assertEquals(DataPermissionMenuEnums.LogCollectorTack, menuResolver.apply(taskDto));
            return ((Supplier<?>) invocation.getArgument(5)).get();
        });
    }

    private void denyPermission(MockedStatic<DataPermissionHelper> helper, DataPermissionActionEnums action) {
        helper.when(() -> DataPermissionHelper.checkOfQuery(
                same(userDetail),
                eq(DataPermissionDataTypeEnums.Task),
                eq(action),
                any(),
                any(),
                any(),
                any()
        )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(6)).get());
    }
}
