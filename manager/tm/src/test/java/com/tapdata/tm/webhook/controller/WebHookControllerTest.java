package com.tapdata.tm.webhook.controller;


import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.WebUtils;
import com.tapdata.tm.webhook.dto.HookOneHistoryDto;
import com.tapdata.tm.webhook.dto.WebHookInfoDto;
import com.tapdata.tm.webhook.server.WebHookAdapterService;
import com.tapdata.tm.webhook.server.WebHookHttpUtilService;
import com.tapdata.tm.webhook.server.WebHookService;
import com.tapdata.tm.webhook.vo.WebHookInfoVo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Locale;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WebHookControllerTest {
    WebHookController webHookController;
    WebHookService<WebHookInfoVo> webHookService;
    WebHookAdapterService webHookAdapter;
    WebHookHttpUtilService webHookHttpUtil;
    UserDetail user;

    @BeforeEach
    void init() {
        user = mock(UserDetail.class);
        webHookController = mock(WebHookController.class);
        webHookService = mock(WebHookService.class);
        webHookHttpUtil = mock(WebHookHttpUtilService.class);
        webHookAdapter = mock(WebHookAdapterService.class);

        doCallRealMethod().when(webHookController).setWebHookService(webHookService);
        webHookController.setWebHookService(webHookService);

        when(webHookController.getLoginUser()).thenReturn(user);
    }

    @Test
    void testCurrentUserWebHookInfoList() {
        Page<WebHookInfoVo> page = mock(Page.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        Locale locale = mock(Locale.class);
        Filter filter = mock(Filter.class);
        when(webHookController.parseFilter("{}")).thenReturn(filter);
        when(webHookController.success(page)).thenReturn(mock(ResponseMessage.class));
        when(webHookService.list(filter, user, locale)).thenReturn(page);
        when(webHookController.currentUserWebHookInfoList("{}", request)).thenCallRealMethod();
        try (MockedStatic<WebUtils> wu = mockStatic(WebUtils.class)) {
            wu.when(() -> WebUtils.getLocale(request)).thenReturn(locale);
            Assertions.assertNotNull(webHookController.currentUserWebHookInfoList("{}", request));
            verify(webHookController).parseFilter("{}");
            verify(webHookController).success(page);
            verify(webHookService).list(filter, user, locale);
        }
    }

    @Test
    void testCreate() {
        WebHookInfoVo vo = new WebHookInfoVo();
        WebHookInfoDto dto = new WebHookInfoDto();
        HttpServletRequest request = mock(HttpServletRequest.class);
        Locale locale = mock(Locale.class);
        when(webHookController.success(vo)).thenReturn(mock(ResponseMessage.class));
        when(webHookService.create(dto, user)).thenReturn(vo);
        when(webHookController.create(dto, request)).thenCallRealMethod();
        try (MockedStatic<WebUtils> wu = mockStatic(WebUtils.class)) {
            wu.when(() -> WebUtils.getLocale(request)).thenReturn(locale);
            Assertions.assertNotNull(webHookController.create(dto, request));
            verify(webHookController).success(vo);
            verify(webHookService).create(dto, user);
            Assertions.assertEquals(locale, dto.getLocale());
        }
    }

    @Test
    void testFindWebHookByHookId() {
        WebHookInfoVo vo = new WebHookInfoVo();
        when(webHookController.success(vo)).thenReturn(mock(ResponseMessage.class));
        when(webHookService.findWebHookByHookId("id", user)).thenReturn(vo);
        when(webHookController.findWebHookByHookId("id")).thenCallRealMethod();
        ResponseMessage<WebHookInfoVo> hook = webHookController.findWebHookByHookId("id");
        Assertions.assertNotNull(hook);
        verify(webHookController).success(vo);
        verify(webHookService).findWebHookByHookId("id", user);
        verify(webHookController).findWebHookByHookId("id");
    }

    @Test
    void testUpdate() {
        WebHookInfoVo vo = new WebHookInfoVo();
        WebHookInfoDto dto = new WebHookInfoDto();
        when(webHookController.success(vo)).thenReturn(mock(ResponseMessage.class));
        when(webHookService.update(dto, user)).thenReturn(vo);
        when(webHookController.update(dto)).thenCallRealMethod();
        ResponseMessage<WebHookInfoVo> hook = webHookController.update(dto);
        Assertions.assertNotNull(hook);
        verify(webHookController).success(vo);
        verify(webHookService).update(dto, user);
        verify(webHookController).update(dto);
    }

    @Nested
    class CloseOneWebHookByHookIdTest {
        @Test
        void testCloseOneWebHookByHookId() {
            WebHookInfoVo vo = new WebHookInfoVo();
            when(webHookController.success(vo)).thenReturn(mock(ResponseMessage.class));
            List mock = mock(List.class);
            when(mock.isEmpty()).thenReturn(false);
            when(mock.get(0)).thenReturn(vo);
            when(webHookService.close(any(String[].class), any(UserDetail.class))).thenReturn(mock);
            when(webHookController.closeOneWebHookByHookId("id")).thenCallRealMethod();
            ResponseMessage<WebHookInfoVo> hook = webHookController.closeOneWebHookByHookId("id");
            Assertions.assertNotNull(hook);
            verify(webHookController).success(vo);
            verify(webHookService).close(any(String[].class), any(UserDetail.class));
        }
        @Test
        void testEmpty() {
            when(webHookController.success(null)).thenReturn(mock(ResponseMessage.class));
            List mock = mock(List.class);
            when(mock.isEmpty()).thenReturn(true);
            when(webHookService.close(any(String[].class), any(UserDetail.class))).thenReturn(mock);
            when(webHookController.closeOneWebHookByHookId("id")).thenCallRealMethod();
            ResponseMessage<WebHookInfoVo> hook = webHookController.closeOneWebHookByHookId("id");
            Assertions.assertNotNull(hook);
            Assertions.assertNull(hook.getData());
            verify(webHookController).success(null);
            verify(webHookService).close(any(String[].class), any(UserDetail.class));
        }
    }
    @Nested
    class ReOpenIdTest {
        @Test
        void testCloseOneWebHookByHookId() {
            WebHookInfoVo vo = new WebHookInfoVo();
            when(webHookController.success(vo)).thenReturn(mock(ResponseMessage.class));
            List mock = mock(List.class);
            when(mock.isEmpty()).thenReturn(false);
            when(mock.get(0)).thenReturn(vo);
            when(webHookService.reOpen(any(String[].class), any(UserDetail.class))).thenReturn(mock);
            when(webHookController.reOpenOne("id")).thenCallRealMethod();
            ResponseMessage<WebHookInfoVo> hook = webHookController.reOpenOne("id");
            Assertions.assertNotNull(hook);
            verify(webHookController).success(vo);
            verify(webHookService).reOpen(any(String[].class), any(UserDetail.class));
        }
        @Test
        void testEmpty() {
            when(webHookController.failed("webhook.reOpen.failed")).thenReturn(mock(ResponseMessage.class));
            List mock = mock(List.class);
            when(mock.isEmpty()).thenReturn(true);
            when(webHookService.reOpen(any(String[].class), any(UserDetail.class))).thenReturn(mock);
            when(webHookController.reOpenOne("id")).thenCallRealMethod();
            ResponseMessage<WebHookInfoVo> hook = webHookController.reOpenOne("id");
            Assertions.assertNotNull(hook);
            verify(webHookController).failed("webhook.reOpen.failed");
            verify(webHookService).reOpen(any(String[].class), any(UserDetail.class));
        }
    }

    @Test
    void testCloseWebHookByHookIds() {
        List<WebHookInfoVo> mock = mock(List.class);
        when(webHookController.success(mock)).thenReturn(mock(ResponseMessage.class));
        String[] ids = new String[]{"id"};
        when(webHookService.close(ids, user)).thenReturn(mock);
        when(webHookController.closeWebHookByHookIds(ids)).thenCallRealMethod();
        ResponseMessage<List<WebHookInfoVo>> hook = webHookController.closeWebHookByHookIds(ids);
        Assertions.assertNotNull(hook);
        verify(webHookController).success(mock);
        verify(webHookService).close(ids, user);
    }
    @Test
    void testReOpenAll() {
        List<WebHookInfoVo> mock = mock(List.class);
        when(webHookController.success(mock)).thenReturn(mock(ResponseMessage.class));
        String[] ids = new String[]{"id"};
        when(webHookService.reOpen(ids, user)).thenReturn(mock);
        when(webHookController.reOpenAll(ids)).thenCallRealMethod();
        ResponseMessage<List<WebHookInfoVo>> hook = webHookController.reOpenAll(ids);
        Assertions.assertNotNull(hook);
        verify(webHookController).success(mock);
        verify(webHookService).reOpen(ids, user);
    }

    @Test
    void testDeleteOneWebHookByHookId() {
        when(webHookController.success()).thenReturn(mock(ResponseMessage.class));
        doNothing().when(webHookService).delete(any(String[].class), any(UserDetail.class));
        when(webHookController.deleteOneWebHookByHookId("id")).thenCallRealMethod();
        ResponseMessage<Void> hook = webHookController.deleteOneWebHookByHookId("id");
        Assertions.assertNotNull(hook);
        verify(webHookController).success();
        verify(webHookService).delete(any(String[].class), any(UserDetail.class));
    }

    @Test
    void testDeleteWebHookByHookIds() {
        when(webHookController.success()).thenReturn(mock(ResponseMessage.class));
        String[] ids = new String[]{"id"};
        doNothing().when(webHookService).delete(ids, user);
        when(webHookController.deleteWebHookByHookIds(ids)).thenCallRealMethod();
        webHookController.deleteWebHookByHookIds(ids);
        verify(webHookController).success();
        verify(webHookService).delete(ids, user);
    }

    @Test
    void testPing() {
        try(MockedStatic<MessageUtil> mu = mockStatic(MessageUtil.class)) {
            mu.when(() -> MessageUtil.getMessage("webhook.ping.succeed")).thenReturn("ducceed");
            HookOneHistoryDto vo = new HookOneHistoryDto();
            WebHookInfoDto dto = new WebHookInfoDto();
            when(webHookController.success(vo)).thenReturn(mock(ResponseMessage.class));
            when(webHookService.ping(dto, user)).thenReturn(vo);
            when(webHookController.ping(dto)).thenCallRealMethod();
            ResponseMessage<HookOneHistoryDto> hook = webHookController.ping(dto);
            Assertions.assertNotNull(hook);
            verify(webHookController).success(vo);
            verify(webHookService).ping(dto, user);
            verify(webHookController).ping(dto);
        }
    }

}