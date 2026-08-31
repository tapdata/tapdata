package com.tapdata.tm.userLog.aop;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import org.aspectj.lang.ProceedingJoinPoint;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import static org.mockito.Mockito.*;

class DatasourceAopTest {
    private DatasourceAop datasourceAop;
    private UserLogService userLogService;
    private DataSourceService dataSourceService;
    private ProceedingJoinPoint joinPoint;
    private UserDetail userDetail;

    @BeforeEach
    void beforeEach() {
        datasourceAop = new DatasourceAop();
        userLogService = mock(UserLogService.class);
        dataSourceService = mock(DataSourceService.class);
        joinPoint = mock(ProceedingJoinPoint.class);
        userDetail = mock(UserDetail.class);
        datasourceAop.userLogService = userLogService;
        datasourceAop.dataSourceService = dataSourceService;

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("User-Agent", "Mozilla/5.0");
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
    }

    @AfterEach
    void afterEach() {
        RequestContextHolder.resetRequestAttributes();
    }

    @Test
    void shouldRecordSubmittedConnectionEditEvenWhenStatusIsTesting() throws Throwable {
        DataSourceConnectionDto update = connection("connection", true);
        DataSourceConnectionDto result = connection("connection", true);
        when(joinPoint.getArgs()).thenReturn(new Object[]{userDetail, update, true});
        when(joinPoint.proceed(any(Object[].class))).thenReturn(result);
        when(dataSourceService.findById(update.getId())).thenReturn(connection("connection", null));

        datasourceAop.afterUpdateReturning(joinPoint);

        verify(userLogService).addUserLog(Modular.CONNECTION, Operation.UPDATE,
                userDetail, update.getId().toString(), "connection");
    }

    @Test
    void shouldNotRecordConnectionTest() throws Throwable {
        DataSourceConnectionDto update = connection("connection", false);
        DataSourceConnectionDto result = connection("connection", false);
        when(joinPoint.getArgs()).thenReturn(new Object[]{userDetail, update, false});
        when(joinPoint.proceed(any(Object[].class))).thenReturn(result);
        when(dataSourceService.findById(update.getId())).thenReturn(connection("connection", null));

        datasourceAop.afterUpdateReturning(joinPoint);

        verifyNoInteractions(userLogService);
    }

    @Test
    void shouldNotRecordSchemaLoadAsConnectionEdit() throws Throwable {
        DataSourceConnectionDto update = connection("connection", null);
        update.setStatus(null);
        update.setLoadFieldsStatus(DataSourceConnectionDto.LOAD_FIELD_STATUS_LOADING);
        DataSourceConnectionDto result = connection("connection", null);
        when(joinPoint.getArgs()).thenReturn(new Object[]{userDetail, update, false});
        when(joinPoint.proceed(any(Object[].class))).thenReturn(result);
        when(dataSourceService.findById(update.getId())).thenReturn(connection("connection", null));

        datasourceAop.afterUpdateReturning(joinPoint);

        verifyNoInteractions(userLogService);
    }

    private DataSourceConnectionDto connection(String name, Boolean submit) {
        DataSourceConnectionDto dto = new DataSourceConnectionDto();
        dto.setId(new ObjectId("64b000000000000000000001"));
        dto.setName(name);
        dto.setStatus("testing");
        dto.setSubmit(submit);
        return dto;
    }
}
