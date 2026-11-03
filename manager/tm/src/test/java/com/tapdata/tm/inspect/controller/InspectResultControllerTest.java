package com.tapdata.tm.inspect.controller;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

class InspectResultControllerTest {
    private InspectResultController controller;
    private UserDetail userDetail;

    @BeforeEach
    void setUp() {
        controller = spy(new InspectResultController());
        userDetail = mock(UserDetail.class);
        doReturn(userDetail).when(controller).getLoginUser();
    }

    @Test
    void testCheckInspectUsesAllDataPermission() {
        String inspectId = new ObjectId().toHexString();
        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            helper.when(() -> DataPermissionHelper.check(
                    same(userDetail),
                    eq(DataPermissionMenuEnums.INSPECT_TACK),
                    eq(DataPermissionActionEnums.View),
                    eq(DataPermissionDataTypeEnums.INSPECT),
                    eq(inspectId),
                    any(),
                    any()
            )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(5)).get());

            Assertions.assertDoesNotThrow(() -> controller.checkInspect(inspectId, DataPermissionActionEnums.View));
            helper.verify(() -> DataPermissionHelper.check(
                    same(userDetail),
                    eq(DataPermissionMenuEnums.INSPECT_TACK),
                    eq(DataPermissionActionEnums.View),
                    eq(DataPermissionDataTypeEnums.INSPECT),
                    eq(inspectId),
                    any(),
                    any()
            ));
        }
    }

    @Test
    void testCheckInspectThrowsWhenPermissionCheckFails() {
        String inspectId = new ObjectId().toHexString();
        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            helper.when(() -> DataPermissionHelper.check(
                    same(userDetail),
                    eq(DataPermissionMenuEnums.INSPECT_TACK),
                    eq(DataPermissionActionEnums.View),
                    eq(DataPermissionDataTypeEnums.INSPECT),
                    eq(inspectId),
                    any(),
                    any()
            )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(6)).get());

            BizException exception = Assertions.assertThrows(
                    BizException.class,
                    () -> controller.checkInspect(inspectId, DataPermissionActionEnums.View)
            );
            Assertions.assertEquals("insufficient.permissions", exception.getErrorCode());
        }
    }
}
