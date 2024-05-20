package com.tapdata.tm.webhook.server.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.webhook.enums.ConstVariable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class HistoryServiceTest {
    HistoryService service;
    @BeforeEach
    void init() {
        service = new HistoryService();
    }

    @Test
    void testList() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.list(null, null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }

    @Test
    void testDeleteHookHistory() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.deleteHookHistory(null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }

    @Test
    void testReSend() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.reSend(null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }

    @Test
    void testPushManyHistory() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.pushManyHistory(null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }

    @Test
    void testPushHistory() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.pushHistory(null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
}