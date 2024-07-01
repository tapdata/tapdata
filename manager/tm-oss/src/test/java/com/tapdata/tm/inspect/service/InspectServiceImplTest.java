package com.tapdata.tm.inspect.service;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.repository.InspectRepository;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.webhook.enums.ConstVariable;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InspectServiceImplTest {
    InspectServiceImpl impl;
    @BeforeEach
    void init() {
        impl = mock(InspectServiceImpl.class);
    }

    @Test
    void testDataPermissionFindById() {
        when(impl.dataPermissionFindById(any(ObjectId.class), any(Field.class))).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.dataPermissionFindById(mock(ObjectId.class), mock(Field.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void beforeSave() {
        doCallRealMethod().when(impl).beforeSave(any(InspectDto.class), any(UserDetail.class));
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.beforeSave(mock(InspectDto.class), mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void delete() {
        when(impl.delete(anyString(), any(UserDetail.class))).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.delete("id", mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void list() {
        when(impl.list(any(Filter.class), any(UserDetail.class))).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Page<InspectDto> list = impl.list(mock(Filter.class), mock(UserDetail.class));
            Assertions.assertEquals(0L, list.getTotal());
            Assertions.assertNotNull(list.getItems());
            Assertions.assertEquals(0, list.getItems().size());
        }
    }

    @Test
    void findById() {
        when(impl.findById(any(Filter.class), any(UserDetail.class))).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.findById(mock(Filter.class), mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void saveInspect() {
        doCallRealMethod().when(impl).saveInspect(any(TaskDto.class), any(UserDetail.class));
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.saveInspect(mock(TaskDto.class), mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void createCheckByTask() {
        when(impl.createCheckByTask(any(TaskDto.class), any(UserDetail.class))).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.createCheckByTask(mock(TaskDto.class), mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void findByTaskIdList() {
        when(impl.findAll(any(Query.class))).thenReturn(new ArrayList<>());
        when(impl.findByTaskIdList(anyList())).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertDoesNotThrow(() -> impl.findByTaskIdList(mock(List.class)));
            verify(impl).findAll(any(Query.class));
        }
    }

    @Test
    void deleteByTaskId() {
        when(impl.deleteByTaskId(anyString())).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertDoesNotThrow(() -> impl.deleteByTaskId("id"));
        }
    }

    @Test
    void doExecuteInspect() {
        when(impl.doExecuteInspect(any(Where.class), any(InspectDto.class), any(UserDetail.class))).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.doExecuteInspect(mock(Where.class), mock(InspectDto.class), mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void updateById() {
        when(impl.updateById(any(ObjectId.class), any(InspectDto.class), any(UserDetail.class))).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.updateById(mock(ObjectId.class), mock(InspectDto.class), mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void findByName() {
        when(impl.findByName(anyString())).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.findByName("id");
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void importData() {
        doCallRealMethod().when(impl).importData(anyString(), anyString(), any(UserDetail.class));
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.importData("{}", "{}", mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void setRepeatInspectTask() {
        doCallRealMethod().when(impl).setRepeatInspectTask();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertDoesNotThrow(impl::setRepeatInspectTask);
        }
    }

    @Test
    void updateStatusById() {
        when(impl.updateStatusById(anyString(), any(InspectStatusEnum.class))).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.updateStatusById("id", mock(InspectStatusEnum.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void updateStatusByIds() {
        when(impl.updateStatusByIds(anyList(), any(InspectStatusEnum.class))).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.updateStatusByIds(mock(List.class), mock(InspectStatusEnum.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void inspectPreview() {
        when(impl.inspectPreview(any(UserDetail.class))).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.inspectPreview(mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void findByStatus() {
        when(impl.findByStatus(any(InspectStatusEnum.class))).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.findByStatus(mock(InspectStatusEnum.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void findByResult() {
        when(impl.findByResult(anyBoolean())).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.findByResult(true);
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void cleanDeadInspect() {
        doCallRealMethod().when(impl).cleanDeadInspect();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertDoesNotThrow(impl::cleanDeadInspect);
        }
    }

    @Test
    void supplementAlarm() {
        doCallRealMethod().when(impl).supplementAlarm(any(InspectDto.class), any(UserDetail.class));
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.supplementAlarm(mock(InspectDto.class), mock(UserDetail.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Test
    void findAllByIds() {
        when(impl.findAllByIds(anyList())).thenCallRealMethod();
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)) {
            messageUtilMockedStatic.when(() -> MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    impl.findAllByIds(mock(List.class));
                } catch (BizException e) {
                    Assertions.assertEquals(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION, e.getErrorCode());
                    throw e;
                }
            });
        }
    }



}
