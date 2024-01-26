package com.tapdata.tm.Settings.controller;

import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.ResponseMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
@ExtendWith(MockitoExtension.class)
class SettingsControllerTest {

    private SettingsController settingsController;

    @Mock
    private SettingsService mockSettingsService;
    @BeforeEach
    void before(){
        settingsController = new SettingsController();
        ReflectionTestUtils.setField(settingsController,"settingsService",mockSettingsService);
    }


    @Test
    void testFind_byAgent(){
        try(MockedStatic<RequestContextHolder> holderMockedStatic = Mockito.mockStatic(RequestContextHolder.class)){
            MockHttpServletRequest httpServletRequest = new MockHttpServletRequest();
            httpServletRequest.addHeader("user-agent","FlowEngine");
            ServletRequestAttributes servletRequestAttributes = new ServletRequestAttributes(httpServletRequest);
            holderMockedStatic.when(RequestContextHolder::currentRequestAttributes).thenReturn(servletRequestAttributes);
            List<SettingsDto> except = new ArrayList<>();
            except.add(new SettingsDto());
            when(mockSettingsService.findALl(anyString(),any(Filter.class))).thenReturn(except);
            ResponseMessage result = settingsController.find("1",null);
            Assertions.assertEquals(except.get(0),((List<?>)result.getData()).get(0));
        }
    }

    @Test
    void testFind_byWeb(){
        try(MockedStatic<RequestContextHolder> holderMockedStatic = Mockito.mockStatic(RequestContextHolder.class)){
            MockHttpServletRequest httpServletRequest = new MockHttpServletRequest();
            ServletRequestAttributes servletRequestAttributes = new ServletRequestAttributes(httpServletRequest);
            holderMockedStatic.when(RequestContextHolder::currentRequestAttributes).thenReturn(servletRequestAttributes);
            ResponseMessage result = settingsController.find("1",null);
            Assertions.assertEquals(0,((List<?>)result.getData()).size());
        }
    }
}
