package com.tapdata.tm.v2.api.schema.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.proxy.service.impl.RemoteCaller;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ReLoadApiSchemaServiceTest {

    private static UserDetail userDetail() {
        return new UserDetail("u1", "c1", "name", "pwd", Collections.emptyList());
    }

    @Test
    void testReloadApiSchemaConnectionIdBlank() {
        ReLoadApiSchemaService service = new ReLoadApiSchemaService();
        ReflectionTestUtils.setField(service, "remoteCaller", mock(RemoteCaller.class));
        BizException ex = assertThrows(BizException.class, () -> service.reloadApiSchema(" ", "t", mock(HttpServletRequest.class), mock(HttpServletResponse.class), userDetail()));
        assertEquals("schema.reload.connectionId", ex.getErrorCode());
    }

    @Test
    void testReloadApiSchemaTableNameBlank() {
        ReLoadApiSchemaService service = new ReLoadApiSchemaService();
        ReflectionTestUtils.setField(service, "remoteCaller", mock(RemoteCaller.class));
        BizException ex = assertThrows(BizException.class, () -> service.reloadApiSchema("c", " ", mock(HttpServletRequest.class), mock(HttpServletResponse.class), userDetail()));
        assertEquals("schema.reload.tableName", ex.getErrorCode());
    }

    @Test
    void testReloadApiSchemaNormal() {
        RemoteCaller remoteCaller = mock(RemoteCaller.class);
        ReLoadApiSchemaService service = new ReLoadApiSchemaService();
        ReflectionTestUtils.setField(service, "remoteCaller", remoteCaller);

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);

        service.reloadApiSchema("c1", "t1", request, response, userDetail());

        ArgumentCaptor<ServiceCaller> captor = ArgumentCaptor.forClass(ServiceCaller.class);
        verify(remoteCaller, times(1)).callMethod(captor.capture(), any(), any(), any());
        ServiceCaller serviceCaller = captor.getValue();
        assertEquals("DiscoverSchemaService", serviceCaller.getClassName());
        assertEquals("discoverSpecifySchema", serviceCaller.getMethod());
        assertTrue(serviceCaller.getArgs().length >= 2);
        assertEquals("c1", String.valueOf(serviceCaller.getArgs()[0]));
        assertEquals("t1", String.valueOf(serviceCaller.getArgs()[1]));
    }
}

