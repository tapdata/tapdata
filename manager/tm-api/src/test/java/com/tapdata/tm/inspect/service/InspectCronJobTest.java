package com.tapdata.tm.inspect.service;

import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.SpringContextHelper;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InspectCronJobTest {
    Logger log;
    UserDetail userDetail;
    InspectCronJob job;

    @BeforeEach
    void init() {
        job = mock(InspectCronJob.class);
        userDetail = mock(UserDetail.class);
        log = mock(Logger.class);
        mockSlf4jLog(job, log);
    }

    @Nested
    class ExecuteTest {
        JobExecutionContext jobExecutionContext;
        InspectService inspectService;
        InspectTaskService inspectTaskService;
        UserService userService;
        JobKey jobKey;
        JobDetail jobDetail;
        InspectDto inspectDto;
        @BeforeEach
        void init() throws JobExecutionException {
            jobExecutionContext = mock(JobExecutionContext.class);
            inspectService = mock(InspectService.class);
            inspectTaskService = mock(InspectTaskService.class);
            userService = mock(UserService.class);
            jobKey = mock(JobKey.class);
            jobDetail = mock(JobDetail.class);
            inspectDto = mock(InspectDto.class);



            when(jobKey.getName()).thenReturn(new ObjectId().toHexString());
            when(jobKey.getGroup()).thenReturn("group");
            when(jobDetail.getKey()).thenReturn(jobKey);
            when(jobExecutionContext.getJobDetail()).thenReturn(jobDetail);

            when(inspectService.findById(any(ObjectId.class))).thenReturn(inspectDto);

            when(inspectDto.getUserId()).thenReturn(new ObjectId().toHexString());
            when(userService.loadUserById(any(ObjectId.class))).thenReturn(userDetail);

            when(inspectTaskService.executeInspect(any(Where.class), any(InspectDto.class), any(UserDetail.class))).thenReturn(mock(InspectDto.class));

            doNothing().when(log).info(anyString());
            doNothing().when(log).info(anyString(), anyString(), anyString());
            doCallRealMethod().when(job).execute(jobExecutionContext);
        }

        @Test
        void testNormal() {
            when(inspectDto.getStatus()).thenReturn(InspectStatusEnum.WAITING.getValue());
            try (MockedStatic<SpringContextHelper> sch = mockStatic(SpringContextHelper.class)) {
                sch.when(() -> SpringContextHelper.getBean(InspectService.class)).thenReturn(inspectService);
                sch.when(() -> SpringContextHelper.getBean(InspectTaskService.class)).thenReturn(inspectTaskService);
                sch.when(() -> SpringContextHelper.getBean(UserService.class)).thenReturn(userService);
                Assertions.assertDoesNotThrow(() -> job.execute(jobExecutionContext));
                verify(jobKey, times(1)).getName();
                verify(jobKey, times(1)).getGroup();
                verify(jobExecutionContext, times(1)).getJobDetail();
                verify(jobDetail, times(1)).getKey();
                verify(inspectService, times(1)).findById(any(ObjectId.class));
                verify(inspectDto, times(1)).getStatus();
                verify(inspectDto, times(1)).getUserId();
                verify(userService, times(1)).loadUserById(any(ObjectId.class));
                verify(inspectTaskService, times(1)).executeInspect(any(Where.class), any(InspectDto.class), any(UserDetail.class));
            }
        }

        @Test
        void testRUNNING() {
            when(inspectDto.getStatus()).thenReturn(InspectStatusEnum.RUNNING.getValue());
            try (MockedStatic<SpringContextHelper> sch = mockStatic(SpringContextHelper.class)) {
                sch.when(() -> SpringContextHelper.getBean(InspectService.class)).thenReturn(inspectService);
                sch.when(() -> SpringContextHelper.getBean(InspectTaskService.class)).thenReturn(inspectTaskService);
                sch.when(() -> SpringContextHelper.getBean(UserService.class)).thenReturn(userService);
                Assertions.assertDoesNotThrow(() -> job.execute(jobExecutionContext));
                verify(jobKey, times(1)).getName();
                verify(jobKey, times(1)).getGroup();
                verify(jobExecutionContext, times(1)).getJobDetail();
                verify(jobDetail, times(1)).getKey();
                verify(inspectService, times(1)).findById(any(ObjectId.class));
                verify(inspectDto, times(1)).getStatus();
                verify(inspectDto, times(0)).getUserId();
                verify(userService, times(0)).loadUserById(any(ObjectId.class));
                verify(inspectTaskService, times(0)).executeInspect(any(Where.class), any(InspectDto.class), any(UserDetail.class));
            }
        }

        @Test
        void testSCHEDULING() {
            when(inspectDto.getStatus()).thenReturn(InspectStatusEnum.SCHEDULING.getValue());
            try (MockedStatic<SpringContextHelper> sch = mockStatic(SpringContextHelper.class)) {
                sch.when(() -> SpringContextHelper.getBean(InspectService.class)).thenReturn(inspectService);
                sch.when(() -> SpringContextHelper.getBean(InspectTaskService.class)).thenReturn(inspectTaskService);
                sch.when(() -> SpringContextHelper.getBean(UserService.class)).thenReturn(userService);
                Assertions.assertDoesNotThrow(() -> job.execute(jobExecutionContext));
                verify(jobKey, times(1)).getName();
                verify(jobKey, times(1)).getGroup();
                verify(jobExecutionContext, times(1)).getJobDetail();
                verify(jobDetail, times(1)).getKey();
                verify(inspectService, times(1)).findById(any(ObjectId.class));
                verify(inspectDto, times(1)).getStatus();
                verify(inspectDto, times(0)).getUserId();
                verify(userService, times(0)).loadUserById(any(ObjectId.class));
                verify(inspectTaskService, times(0)).executeInspect(any(Where.class), any(InspectDto.class), any(UserDetail.class));
            }
        }
    }

    public static void mockSlf4jLog(Object mockTo, Logger log) {
        try {
            Field logF = mockTo.getClass().getDeclaredField("log");
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(logF, logF.getModifiers() & ~Modifier.FINAL);
            logF.setAccessible(true);
            logF.set(mockTo, log);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}