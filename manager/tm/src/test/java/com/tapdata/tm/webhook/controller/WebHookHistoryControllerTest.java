package com.tapdata.tm.webhook.controller;


import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.WebUtils;
import com.tapdata.tm.webhook.dto.HookOneHistoryDto;
import com.tapdata.tm.webhook.params.HistoryPageParam;
import com.tapdata.tm.webhook.server.WebHookHistoryService;
import com.tapdata.tm.webhook.vo.WebHookHistoryInfoVo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import javax.servlet.http.HttpServletRequest;
import java.util.Locale;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WebHookHistoryControllerTest {
    WebHookHistoryService<WebHookHistoryInfoVo> webHookHistoryService;
    WebHookHistoryController controller;
    UserDetail user;

    @BeforeEach
    void init() {
        user = mock(UserDetail.class);
        controller = mock(WebHookHistoryController.class);
        webHookHistoryService = mock(WebHookHistoryService.class);
        ReflectionTestUtils.setField(controller, "webHookHistoryService", webHookHistoryService);
        when(controller.getLoginUser()).thenReturn(user);
    }

    @Test
    void testCurrentUserWebHookInfoList() {
        HistoryPageParam pageParam = new HistoryPageParam();
        Locale locale = mock(Locale.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        Page<WebHookHistoryInfoVo> page = mock(Page.class);
        when(controller.success(page)).thenReturn(mock(ResponseMessage.class));
        when(webHookHistoryService.list(pageParam, user, locale)).thenReturn(page);
        when(controller.currentUserWebHookInfoList(pageParam, request)).thenCallRealMethod();
        try (MockedStatic<WebUtils> wu = mockStatic(WebUtils.class)) {
            wu.when(() -> WebUtils.getLocale(request)).thenReturn(locale);
            Assertions.assertNotNull(controller.currentUserWebHookInfoList(pageParam, request));
            verify(controller).getLoginUser();
            verify(controller).success(page);
            verify(webHookHistoryService).list(pageParam, user, locale);
        }
    }

    @Test
    void testReSend() {
        WebHookHistoryInfoVo vo = new WebHookHistoryInfoVo();
        HookOneHistoryDto historyDto = new HookOneHistoryDto();
        when(webHookHistoryService.reSend(historyDto, user)).thenReturn(vo);
        when(controller.success(vo)).thenReturn(mock(ResponseMessage.class));
        when(controller.reSend(historyDto)).thenCallRealMethod();
        Assertions.assertNotNull(controller.reSend(historyDto));
        verify(webHookHistoryService).reSend(historyDto, user);
        verify(controller).success(vo);
    }
}