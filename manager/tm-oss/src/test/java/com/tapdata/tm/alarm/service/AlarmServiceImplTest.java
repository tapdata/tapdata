package com.tapdata.tm.alarm.service;

import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.impl.AlarmServiceImpl;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.MessageUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

public class AlarmServiceImplTest {
    AlarmServiceImpl alarmService = new AlarmServiceImpl();

    @Test
    void testSave(){
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)){
            messageUtilMockedStatic.when(()->MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class,()->alarmService.save(mock(AlarmInfo.class)));
        }
    }
    @Test
    void testClose(){
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)){
            messageUtilMockedStatic.when(()->MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class,()->alarmService.close(null,mock(UserDetail.class)));
        }
    }
    @Test
    void testList(){
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)){
            messageUtilMockedStatic.when(()->MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class,()->alarmService.list(null,null,null,null,null,null,null,null));
        }
    }
    @Test
    void testFind(){
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)){
            messageUtilMockedStatic.when(()->MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class,()->alarmService.find(null,null,null));
        }
    }

    @Test
    void testAdd(){
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)){
            messageUtilMockedStatic.when(()->MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class,()->alarmService.add(null,null));
        }
    }

    @Test
    void testCheckOpen(){
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)){
            messageUtilMockedStatic.when(()->MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class,()->alarmService.checkOpen(null,null,null,null,null));
        }
    }

    @Test
    void testCheckOpen2(){
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)){
            messageUtilMockedStatic.when(()->MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class,()->alarmService.checkOpen(null,null,null,null));
        }
    }

    @Test
    void testCloseWhenInspectTaskRunning(){
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)){
            messageUtilMockedStatic.when(()->MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class,()->alarmService.closeWhenInspectTaskRunning(null));
        }
    }

    @Test
    void testUpdateTaskAlarm(){
        try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)){
            messageUtilMockedStatic.when(()->MessageUtil.getMessage(anyString())).thenReturn("error");
            Assertions.assertThrows(BizException.class,()->alarmService.updateTaskAlarm(null));
        }
    }

}
