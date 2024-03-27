package com.tapdata.tm.function.inspect.controller;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.function.inspect.dto.InspectFunctionDto;
import com.tapdata.tm.function.inspect.service.InspectFunctionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InspectControllerTest {
    InspectController controller;
    InspectFunctionService service;
    Filter filter;
    Where where;
    @BeforeEach
    void init() {
        filter = mock(Filter.class);
        where = mock(Where.class);
        controller = mock(InspectController.class);
        service = mock(InspectFunctionService.class);

        when(filter.getWhere()).thenReturn(where);

        when(where.get("type")).thenReturn("system");

        when(service.find(any(Filter.class))).thenReturn(mock(Page.class));
        when(service.find(any(Filter.class), any(UserDetail.class))).thenReturn(mock(Page.class));

        when(controller.parseFilter(anyString())).thenReturn(filter);
        when(controller.find(anyString())).thenCallRealMethod();
        when(controller.find(null)).thenCallRealMethod();
        when(controller.success(any(Page.class))).thenReturn(mock(ResponseMessage.class));
        when(controller.getLoginUser()).thenReturn(mock(UserDetail.class));
        ReflectionTestUtils.setField(controller, "functionService", service);
    }

    @Test
    void testFindNormalJson() {
        ResponseMessage<Page<InspectFunctionDto>> pageResponseMessage = controller.find("{}");
        verify(service, times(1)).find(any(Filter.class));
        verify(service, times(0)).find(any(Filter.class), any(UserDetail.class));

        verify(controller, times(1)).parseFilter(anyString());
        verify(controller, times(1)).success(any());

        verify(filter, times(1)).getWhere();
        verify(where, times(1)).get("type");
    }

    @Test
    void testFindNullJson() {
        ResponseMessage<Page<InspectFunctionDto>> pageResponseMessage = controller.find(null);
        verify(service, times(1)).find(any(Filter.class), any(UserDetail.class));
        verify(controller, times(1)).parseFilter(null);
        verify(controller, times(1)).success(any());
        verify(filter, times(0)).getWhere();
        verify(where, times(0)).get("type");
    }

    @Test
    void testWhereIsNull() {
        when(filter.getWhere()).thenReturn(null);
        ResponseMessage<Page<InspectFunctionDto>> pageResponseMessage = controller.find("{}");
        verify(service, times(0)).find(any(Filter.class));
        verify(service, times(1)).find(any(Filter.class), any(UserDetail.class));

        verify(controller, times(1)).parseFilter(anyString());
        verify(controller, times(1)).success(any());
        verify(filter, times(1)).getWhere();
        verify(where, times(0)).get("type");
    }

    @Test
    void testWhereNotSystem() {
        ResponseMessage<Page<InspectFunctionDto>> pageResponseMessage = controller.find("{}");
        verify(service, times(1)).find(any(Filter.class));
        verify(service, times(0)).find(any(Filter.class), any(UserDetail.class));

        verify(controller, times(1)).parseFilter(anyString());
        verify(controller, times(1)).success(any());

        verify(filter, times(1)).getWhere();
        verify(where, times(1)).get("type");
    }
}