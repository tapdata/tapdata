package com.tapdata.tm.webhook.server.impl;


import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.webhook.enums.ConstVariable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class HttpUtilServiceTest {
    HttpUtilService service;
    @BeforeEach
    void init() {
        service = new HttpUtilService();
    }

    @Test
    void testCheckURL() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.checkURL(null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
    @Test
    void testPost() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.post(null, null, null, null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
    @Test
    void testPost2() {
        Assertions.assertThrows(BizException.class, () -> {
            try(MockedStatic<MessageUtil> mu = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
                mu.when(() -> MessageUtil.getMessage(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION)).thenReturn("message");
                service.post(null);
            } catch (BizException e) {
                Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                throw e;
            }
        });
    }
}