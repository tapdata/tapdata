package com.tapdata.tm.inspect;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.inspect.CustomQueryRequest;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.inspect.service.CustomQueryInspectSupport;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.IDataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.function.Supplier;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CustomQueryAuthorizationRedesignTest {
    @Test void previewChecksTheConnectionDocumentAndExplicitlyDeniesUnauthorizedAccess() {
        Object previous = ReflectionTestUtils.getField(DataPermissionHelper.class,"iDataPermissionHelper");
        IDataPermissionHelper permissions = mock(IDataPermissionHelper.class);
        new DataPermissionHelper(permissions);
        try {
            DataSourceService sources = mock(DataSourceService.class);
            CustomQueryInspectSupport support = new CustomQueryInspectSupport();
            ReflectionTestUtils.setField(support,"dataSourceService",sources);
            ObjectId id = new ObjectId();
            UserDetail user = mock(UserDetail.class);
            CustomQueryRequest request = new CustomQueryRequest();request.setConnectionId(id.toHexString());request.setQuery("select 1 v");
            Supplier<DataSourceConnectionDto> document = () -> new DataSourceConnectionDto();
            when(sources.dataPermissionFindById(eq(id),any(Field.class))).thenReturn(document);
            doAnswer(call -> {
                assertSame(document,call.getArgument(3));
                Supplier<?> denied = call.getArgument(6);
                return denied.get();
            }).when(permissions).checkOfQuery(same(user),eq(DataPermissionDataTypeEnums.Connections),
                    eq(DataPermissionActionEnums.View),same(document),any(),any(),any());
            assertThrows(BizException.class, () -> support.authorize(request,user));
            verify(sources,never()).getById(any(),any(),anyBoolean(),any());
        } finally { ReflectionTestUtils.setField(DataPermissionHelper.class,"iDataPermissionHelper",previous); }
    }
}
