package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.task.bean.LogCollectorVo;
import com.tapdata.tm.task.bean.LogCollectorEditVo;
import com.tapdata.tm.task.bean.LogCollectorDetailVo;
import com.tapdata.tm.task.param.SaveShareCacheParam;
import com.tapdata.tm.task.service.LogCollectorService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.vo.ShareCacheVo;
import com.tapdata.tm.task.vo.ShareCacheDetailVo;
import com.tapdata.tm.utils.MongoUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.bson.types.ObjectId;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class SharedTaskControllerDataPermissionTest {
    private UserDetail userDetail;

    @BeforeEach
    void setUp() {
        userDetail = mock(UserDetail.class);
    }

    @Test
    void logCollectorListUsesLogCollectorAllDataPermission() {
        LogCollectorController controller = spy(new LogCollectorController());
        LogCollectorService service = mock(LogCollectorService.class);
        controller.setLogCollectorService(service);
        doReturn(userDetail).when(controller).getLoginUser();
        Page<LogCollectorVo> page = new Page<>();
        doReturn(page).when(service).find(any(), same(userDetail));

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowDataPermissionCheck(helper, DataPermissionMenuEnums.LogCollectorTack);

            controller.find(null);

            verify(service).find(any(), same(userDetail));
        }
    }

    @Test
    void sharedCacheListUsesSharedCacheAllDataPermission() {
        CacheTaskController controller = spy(new CacheTaskController());
        TaskService service = mock(TaskService.class);
        ReflectionTestUtils.setField(controller, "taskService", service);
        doReturn(userDetail).when(controller).getLoginUser();
        Page<ShareCacheVo> page = new Page<>();
        doReturn(page).when(service).findShareCache(any(), same(userDetail));

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowDataPermissionCheck(helper, DataPermissionMenuEnums.MemCacheTack);

            controller.find("{\"where\":{}}");

            verify(service).findShareCache(any(), same(userDetail));
        }
    }

    @Test
    void logCollectorEditUsesLogCollectorAllDataPermission() {
        LogCollectorController controller = spy(new LogCollectorController());
        LogCollectorService service = mock(LogCollectorService.class);
        controller.setLogCollectorService(service);
        doReturn(userDetail).when(controller).getLoginUser();
        String id = new ObjectId().toHexString();
        LogCollectorEditVo editVo = new LogCollectorEditVo();

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowEditPermissionCheck(helper, DataPermissionMenuEnums.LogCollectorTack, id);

            controller.update(id, editVo);

            verify(service).update(same(editVo), same(userDetail));
        }
    }

    @Test
    void logCollectorDetailUsesLogCollectorAllDataPermission() {
        LogCollectorController controller = spy(new LogCollectorController());
        LogCollectorService service = mock(LogCollectorService.class);
        controller.setLogCollectorService(service);
        doReturn(userDetail).when(controller).getLoginUser();
        String id = new ObjectId().toHexString();
        LogCollectorDetailVo detail = new LogCollectorDetailVo();
        doReturn(detail).when(service).findDetail(id, userDetail);

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowViewPermissionCheck(helper, DataPermissionMenuEnums.LogCollectorTack, id);

            controller.findDetail(id);

            verify(service).findDetail(id, userDetail);
        }
    }

    @Test
    void sharedCacheEditUsesSharedCacheAllDataPermission() {
        CacheTaskController controller = spy(new CacheTaskController());
        TaskService service = mock(TaskService.class);
        ReflectionTestUtils.setField(controller, "taskService", service);
        doReturn(userDetail).when(controller).getLoginUser();
        String id = new ObjectId().toHexString();
        SaveShareCacheParam param = mock(SaveShareCacheParam.class);
        doReturn(new TaskDto()).when(service).updateShareCacheTask(id, param, userDetail);

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowEditPermissionCheck(helper, DataPermissionMenuEnums.MemCacheTack, id);

            controller.updateById(id, param);

            verify(service).updateShareCacheTask(id, param, userDetail);
        }
    }

    @Test
    void sharedCacheDetailUsesSharedCacheViewPermission() {
        CacheTaskController controller = spy(new CacheTaskController());
        TaskService service = mock(TaskService.class);
        ReflectionTestUtils.setField(controller, "taskService", service);
        doReturn(userDetail).when(controller).getLoginUser();
        String id = new ObjectId().toHexString();
        ShareCacheDetailVo detail = new ShareCacheDetailVo();
        doReturn(detail).when(service).findShareCacheById(id);

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowViewPermissionCheck(helper, DataPermissionMenuEnums.MemCacheTack, id);

            controller.findById(id, null);

            verify(service).findShareCacheById(id);
        }
    }

    @Test
    void sharedCacheStopUsesStopPermission() {
        CacheTaskController controller = spy(new CacheTaskController());
        TaskService service = mock(TaskService.class);
        ReflectionTestUtils.setField(controller, "taskService", service);
        doReturn(userDetail).when(controller).getLoginUser();
        String id = new ObjectId().toHexString();

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowSingleDataPermissionCheck(helper, DataPermissionMenuEnums.MemCacheTack, DataPermissionActionEnums.Stop, id);

            controller.stop(id, false);

            verify(service).pause(MongoUtils.toObjectId(id), userDetail, false);
        }
    }

    @Test
    void sharedCacheDeleteUsesDeletePermission() {
        CacheTaskController controller = spy(new CacheTaskController());
        TaskService service = mock(TaskService.class);
        ReflectionTestUtils.setField(controller, "taskService", service);
        doReturn(userDetail).when(controller).getLoginUser();
        String id = new ObjectId().toHexString();

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowSingleDataPermissionCheck(helper, DataPermissionMenuEnums.MemCacheTack, DataPermissionActionEnums.Delete, id);

            controller.delete(id);

            verify(service).remove(MongoUtils.toObjectId(id), userDetail);
        }
    }

    @Test
    void sharedCacheRenewUsesResetPermission() {
        CacheTaskController controller = spy(new CacheTaskController());
        TaskService service = mock(TaskService.class);
        ReflectionTestUtils.setField(controller, "taskService", service);
        doReturn(userDetail).when(controller).getLoginUser();
        String id = new ObjectId().toHexString();

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowSingleDataPermissionCheck(helper, DataPermissionMenuEnums.MemCacheTack, DataPermissionActionEnums.Reset, id);

            controller.renew(id);

            verify(service).renew(MongoUtils.toObjectId(id), userDetail);
        }
    }

    @Test
    void sharedCacheEditDoesNotExecuteWhenPermissionIsDenied() {
        CacheTaskController controller = spy(new CacheTaskController());
        TaskService service = mock(TaskService.class);
        ReflectionTestUtils.setField(controller, "taskService", service);
        doReturn(userDetail).when(controller).getLoginUser();
        String id = new ObjectId().toHexString();
        SaveShareCacheParam param = mock(SaveShareCacheParam.class);

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            helper.when(() -> DataPermissionHelper.check(
                    same(userDetail),
                    eq(DataPermissionMenuEnums.MemCacheTack),
                    eq(DataPermissionActionEnums.Edit),
                    eq(DataPermissionDataTypeEnums.Task),
                    eq(id),
                    any(),
                    any()
            )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(6)).get());

            Assertions.assertThrows(BizException.class, () -> controller.updateById(id, param));

            verifyNoInteractions(service);
        }
    }

    @Test
    void sharedCacheDeleteDoesNotExecuteWhenPermissionIsDenied() {
        CacheTaskController controller = spy(new CacheTaskController());
        TaskService service = mock(TaskService.class);
        ReflectionTestUtils.setField(controller, "taskService", service);
        doReturn(userDetail).when(controller).getLoginUser();
        String id = new ObjectId().toHexString();

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            helper.when(() -> DataPermissionHelper.check(
                    same(userDetail),
                    eq(DataPermissionMenuEnums.MemCacheTack),
                    eq(DataPermissionActionEnums.Delete),
                    eq(DataPermissionDataTypeEnums.Task),
                    eq(id),
                    any(),
                    any()
            )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(6)).get());

            BizException exception = Assertions.assertThrows(BizException.class, () -> controller.delete(id));

            Assertions.assertEquals("insufficient.permissions", exception.getErrorCode());
            verifyNoInteractions(service);
        }
    }

    @Test
    void sharedCacheBatchDeleteDoesNotExecuteWhenPermissionIsDenied() {
        CacheTaskController controller = spy(new CacheTaskController());
        TaskService service = mock(TaskService.class);
        ReflectionTestUtils.setField(controller, "taskService", service);
        doReturn(userDetail).when(controller).getLoginUser();
        String id = new ObjectId().toHexString();

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            helper.when(() -> DataPermissionHelper.check(
                    same(userDetail),
                    eq(DataPermissionMenuEnums.MemCacheTack),
                    eq(DataPermissionActionEnums.Delete),
                    eq(DataPermissionDataTypeEnums.Task),
                    eq(id),
                    any(),
                    any()
            )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(6)).get());

            Assertions.assertThrows(BizException.class, () -> controller.batchDelete(
                    java.util.Collections.singletonList(id),
                    mock(jakarta.servlet.http.HttpServletRequest.class),
                    mock(jakarta.servlet.http.HttpServletResponse.class)
            ));

            verifyNoInteractions(service);
        }
    }

    @Test
    void sharedCacheBatchStopExecutesWithinEachTaskPermissionCheck() {
        CacheTaskController controller = spy(new CacheTaskController());
        TaskService service = mock(TaskService.class);
        ReflectionTestUtils.setField(controller, "taskService", service);
        doReturn(userDetail).when(controller).getLoginUser();
        String id = new ObjectId().toHexString();
        ObjectId objectId = MongoUtils.toObjectId(id);
        jakarta.servlet.http.HttpServletRequest request = mock(jakarta.servlet.http.HttpServletRequest.class);
        jakarta.servlet.http.HttpServletResponse response = mock(jakarta.servlet.http.HttpServletResponse.class);
        doReturn(java.util.Collections.emptyList()).when(service).batchStop(
                eq(java.util.Collections.singletonList(objectId)),
                same(userDetail),
                same(request),
                same(response)
        );

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            allowSingleDataPermissionCheck(helper, DataPermissionMenuEnums.MemCacheTack, DataPermissionActionEnums.Stop, id);

            controller.batchStop(java.util.Collections.singletonList(id), request, response);

            verify(service).batchStop(
                    eq(java.util.Collections.singletonList(objectId)),
                    same(userDetail),
                    same(request),
                    same(response)
            );
        }
    }

    private void allowSingleDataPermissionCheck(
            MockedStatic<DataPermissionHelper> helper,
            DataPermissionMenuEnums menuEnums,
            DataPermissionActionEnums action,
            String id
    ) {
        helper.when(() -> DataPermissionHelper.check(
                same(userDetail),
                eq(menuEnums),
                eq(action),
                eq(DataPermissionDataTypeEnums.Task),
                eq(id),
                any(),
                any()
        )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(5)).get());
    }

    private void allowDataPermissionCheck(MockedStatic<DataPermissionHelper> helper, DataPermissionMenuEnums menuEnums) {
        helper.when(() -> DataPermissionHelper.check(
                same(userDetail),
                eq(menuEnums),
                eq(DataPermissionActionEnums.View),
                eq(DataPermissionDataTypeEnums.Task),
                isNull(),
                any(),
                any()
        )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(5)).get());
    }

    private void allowEditPermissionCheck(MockedStatic<DataPermissionHelper> helper, DataPermissionMenuEnums menuEnums, String id) {
        helper.when(() -> DataPermissionHelper.check(
                same(userDetail),
                eq(menuEnums),
                eq(DataPermissionActionEnums.Edit),
                eq(DataPermissionDataTypeEnums.Task),
                eq(id),
                any(),
                any()
        )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(5)).get());
    }

    private void allowViewPermissionCheck(MockedStatic<DataPermissionHelper> helper, DataPermissionMenuEnums menuEnums, String id) {
        helper.when(() -> DataPermissionHelper.check(
                same(userDetail),
                eq(menuEnums),
                eq(DataPermissionActionEnums.View),
                eq(DataPermissionDataTypeEnums.Task),
                eq(id),
                any(),
                any()
        )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(5)).get());
    }
}
