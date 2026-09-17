package com.tapdata.tm.ds.controller;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import jakarta.servlet.http.HttpServletRequest;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class DataSourceControllerTest {

    private DataSourceController controller;
    private DataSourceService dataSourceService;
    private UserDetail userDetail;
    private HttpServletRequest request;

    @BeforeEach
    void setUp() {
        controller = spy(new DataSourceController());
        dataSourceService = mock(DataSourceService.class);
        userDetail = mock(UserDetail.class);
        request = mock(HttpServletRequest.class);
        controller.setDataSourceService(dataSourceService);
        doReturn(userDetail).when(controller).getLoginUser();
    }

    @Nested
    class DeletePermissionTest {

        @Test
        void shouldDeleteWhenPermissionCheckPasses() {
            String id = new ObjectId().toHexString();
            try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
                helper.when(() -> DataPermissionHelper.checkOfQuery(
                        same(userDetail),
                        eq(DataPermissionDataTypeEnums.Connections),
                        eq(DataPermissionActionEnums.Delete),
                        any(),
                        any(),
                        any(),
                        any()
                )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(5)).get());

                controller.delete(request, id);

                verify(dataSourceService).delete(userDetail, id);
            }
        }

        @Test
        void shouldNotDeleteWhenPermissionCheckFails() {
            String id = new ObjectId().toHexString();
            try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
                helper.when(() -> DataPermissionHelper.checkOfQuery(
                        same(userDetail),
                        eq(DataPermissionDataTypeEnums.Connections),
                        eq(DataPermissionActionEnums.Delete),
                        any(),
                        any(),
                        any(),
                        any()
                )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(6)).get());

                BizException exception = assertThrows(BizException.class, () -> controller.delete(request, id));
                assertEquals("insufficient.permissions", exception.getErrorCode());
                verify(dataSourceService, never()).delete(any(), any());
            }
        }
    }
}
