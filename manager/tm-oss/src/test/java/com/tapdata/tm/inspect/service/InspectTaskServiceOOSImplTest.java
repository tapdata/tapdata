package com.tapdata.tm.inspect.service;

import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.webhook.enums.ConstVariable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

class InspectTaskServiceOOSImplTest {
    InspectTaskServiceOOSImpl impl;
    @BeforeEach
    void init() {
        impl = new InspectTaskServiceOOSImpl();
    }

    @Test
    void inspectTaskRun() {
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.inspectTaskRun(mock(Where.class), mock(InspectDto.class), mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void inspectTaskStop() {
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.inspectTaskStop("id", mock(InspectDto.class), mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void inspectTaskError() {
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.inspectTaskError("id", mock(InspectDto.class), mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void inspectTaskDone() {
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.inspectTaskDone("id", mock(InspectDto.class), mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void executeInspect() {
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.executeInspect(mock(Where.class), mock(InspectDto.class), mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void startInspectTask() {
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.startInspectTask(mock(InspectDto.class), "id");
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void stopInspectTask() {
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.stopInspectTask(mock(InspectDto.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void findTaskList() {
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.findTaskList(mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void findConnectionList() {
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.findConnectionList(mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }
}