package com.tapdata.tm.webhook.server.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.webhook.enums.ConstVariable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class WebHookDefaultServiceTest {
    WebHookDefaultService service;
    @BeforeEach
    void init() {
        service = new WebHookDefaultService();
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
    void testFindWebHookByHookId() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.findWebHookByHookId(null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
    @Test
    void testCreate() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.create(null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
    @Test
    void testUpdate() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.update(null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
    @Test
    void testUpdatePingResult() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.updatePingResult(null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
    @Test
    void testClose() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.close(null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
    @Test
    void testDelete() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.delete(null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
    @Test
    void testFindMyOpenHookInfoList() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.findMyOpenHookInfoList(null, null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
    @Test
    void testCheckUrl() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.checkUrl(null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
    @Test
    void testPing() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.ping(null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
    @Test
    void testReOpen() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.reOpen(null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
}