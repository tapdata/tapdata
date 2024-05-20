package com.tapdata.tm.webhook.controller;


import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.WebUtils;
import com.tapdata.tm.webhook.dto.HookOneHistoryDto;
import com.tapdata.tm.webhook.params.HistoryPageParam;
import com.tapdata.tm.webhook.server.WebHookHistoryService;
import com.tapdata.tm.webhook.vo.WebHookHistoryInfoVo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import javax.servlet.http.HttpServletRequest;
import java.util.Locale;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
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

    @Nested
    class CurrentUserWebHookInfoListTest {
        @Test
        void testCurrentUserWebHookInfoListNormal() {
            Locale locale = mock(Locale.class);
            HttpServletRequest request = mock(HttpServletRequest.class);
            Page<WebHookHistoryInfoVo> page = mock(Page.class);
            when(controller.success(page)).thenReturn(mock(ResponseMessage.class));
            when(webHookHistoryService.list(any(HistoryPageParam.class), any(UserDetail.class), any(Locale.class))).thenReturn(page);
            when(controller.currentUserWebHookInfoList("id", 0, 1, request)).thenCallRealMethod();
            try (MockedStatic<WebUtils> wu = mockStatic(WebUtils.class)) {
                wu.when(() -> WebUtils.getLocale(request)).thenReturn(locale);
                Assertions.assertNotNull(controller.currentUserWebHookInfoList("id", 0, 1, request));
                verify(controller).getLoginUser();
                verify(controller).success(page);
                verify(webHookHistoryService).list(any(HistoryPageParam.class), any(UserDetail.class), any(Locale.class));
            }
        }
        @Test
        void testCurrentUserWebHookInfoListIdInvalid() {
            HistoryPageParam pageParam = new HistoryPageParam();
            Locale locale = mock(Locale.class);
            HttpServletRequest request = mock(HttpServletRequest.class);
            Page<WebHookHistoryInfoVo> page = mock(Page.class);
            when(controller.success(page)).thenReturn(mock(ResponseMessage.class));
            when(webHookHistoryService.list(pageParam, user, locale)).thenReturn(page);
            when(controller.currentUserWebHookInfoList(null, 0, 1, request)).thenCallRealMethod();
            try (MockedStatic<WebUtils> wu = mockStatic(WebUtils.class)) {
                wu.when(() -> WebUtils.getLocale(request)).thenReturn(locale);
                Assertions.assertThrows(BizException.class, () -> {
                    try {
                        controller.currentUserWebHookInfoList(null, 0, 1, request);
                    } catch (BizException e) {
                        Assertions.assertEquals("webhook.history.hook.id.error", e.getErrorCode());
                        throw e;
                    }
                });
                verify(controller, times(0)).getLoginUser();
                verify(controller, times(0)).success(page);
                verify(webHookHistoryService, times(0)).list(pageParam, user, locale);
            }
        }
        @Test
        void testCurrentUserWebHookInfoListPageFromInvalid() {
            HistoryPageParam pageParam = new HistoryPageParam();
            Locale locale = mock(Locale.class);
            HttpServletRequest request = mock(HttpServletRequest.class);
            Page<WebHookHistoryInfoVo> page = mock(Page.class);
            when(controller.success(page)).thenReturn(mock(ResponseMessage.class));
            when(webHookHistoryService.list(pageParam, user, locale)).thenReturn(page);
            when(controller.currentUserWebHookInfoList("id", -1, 1, request)).thenCallRealMethod();
            try (MockedStatic<WebUtils> wu = mockStatic(WebUtils.class)) {
                wu.when(() -> WebUtils.getLocale(request)).thenReturn(locale);
                Assertions.assertThrows(BizException.class, () -> {
                    try {
                        controller.currentUserWebHookInfoList("id", -1, 1, request);
                    } catch (BizException e) {
                        Assertions.assertEquals("webhook.history.page.from.error", e.getErrorCode());
                        throw e;
                    }
                });
                verify(controller, times(0)).getLoginUser();
                verify(controller, times(0)).success(page);
                verify(webHookHistoryService, times(0)).list(pageParam, user, locale);
            }
        }
        @Test
        void testCurrentUserWebHookInfoListPageSizeInvalid() {
            HistoryPageParam pageParam = new HistoryPageParam();
            Locale locale = mock(Locale.class);
            HttpServletRequest request = mock(HttpServletRequest.class);
            Page<WebHookHistoryInfoVo> page = mock(Page.class);
            when(controller.success(page)).thenReturn(mock(ResponseMessage.class));
            when(webHookHistoryService.list(pageParam, user, locale)).thenReturn(page);
            when(controller.currentUserWebHookInfoList("id", 0, -1, request)).thenCallRealMethod();
            try (MockedStatic<WebUtils> wu = mockStatic(WebUtils.class)) {
                wu.when(() -> WebUtils.getLocale(request)).thenReturn(locale);
                Assertions.assertThrows(BizException.class, () -> {
                    try {
                        controller.currentUserWebHookInfoList("id", 0, -1, request);
                    } catch (BizException e) {
                        Assertions.assertEquals("webhook.history.page.size.error", e.getErrorCode());
                        throw e;
                    }
                });
                verify(controller, times(0)).getLoginUser();
                verify(controller, times(0)).success(page);
                verify(webHookHistoryService, times(0)).list(pageParam, user, locale);
            }
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