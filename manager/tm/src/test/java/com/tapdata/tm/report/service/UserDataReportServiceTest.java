package com.tapdata.tm.report.service;

import com.tapdata.tm.report.dto.*;
import com.tapdata.tm.report.service.platform.GoogleAnalyticsPlatform;
import com.tapdata.tm.report.service.platform.ReportPlatform;
import lombok.SneakyThrows;
import org.junit.jupiter.api.*;
import org.mockito.*;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class UserDataReportServiceTest {
    private UserDataReportService userDataReportService;
    private ReportPlatform reportPlatform;
    @BeforeEach
    void beforeEach(){
        userDataReportService = mock(UserDataReportService.class);
        reportPlatform = mock(GoogleAnalyticsPlatform.class);
        ReflectionTestUtils.setField(userDataReportService,"reportPlatform",reportPlatform);
        ReflectionTestUtils.setField(userDataReportService,"machineId","111");
    }
    @Nested
    class InitReportDataThreadTest{
        @BeforeEach
        void beforeEach(){
            doCallRealMethod().when(userDataReportService).initReportDataThread();
        }
        @Test
        @DisplayName("test initReportDataThread method when acceptReportData is false")
        void test1(){
            try (MockedStatic<CompletableFuture> mb = Mockito
                    .mockStatic(CompletableFuture.class)) {
                mb.when(() -> CompletableFuture.runAsync(any(Runnable.class),any(Executor.class))).thenReturn(mock(CompletableFuture.class));
                ReflectionTestUtils.setField(userDataReportService,"acceptReportData",false);
                userDataReportService.initReportDataThread();
                mb.verify(() -> CompletableFuture.runAsync(any(Runnable.class),any(Executor.class)), new Times(0));
            }
        }
        @Test
        @SneakyThrows
        @DisplayName("test initReportDataThread method when acceptReportData is true")
        void test2(){
            try (MockedStatic<CompletableFuture> mb = Mockito
                    .mockStatic(CompletableFuture.class)) {
                mb.when(() -> CompletableFuture.runAsync(any(Runnable.class),any(ExecutorService.class))).thenAnswer((invocationOnMock -> {
                    ExecutorService actual = invocationOnMock.getArgument(1, ExecutorService.class);
                    assertEquals(1, ((ThreadPoolExecutor)actual).getCorePoolSize());
                    assertEquals(1, ((ThreadPoolExecutor)actual).getMaximumPoolSize());
                    assertEquals(0, ((ThreadPoolExecutor)actual).getKeepAliveTime(TimeUnit.MILLISECONDS));
                    return null;
                }));
                ReflectionTestUtils.setField(userDataReportService,"acceptReportData",true);
                userDataReportService.initReportDataThread();
                mb.verify(() -> CompletableFuture.runAsync(any(Runnable.class),any(Executor.class)), new Times(1));
            }
        }
    }
    @Nested
    class ConsumeDataTest{
        @BeforeEach
        void beforeEach(){
            doCallRealMethod().when(userDataReportService).consumeData(any(Object.class));
        }
        @Test
        @DisplayName("test consumeData method for RunsNumBatch")
        void test1(){
            RunsNumBatch runsNumBatch = new RunsNumBatch();
            userDataReportService.consumeData(runsNumBatch);
            verify(userDataReportService,new Times(1)).runsNum(runsNumBatch);
        }
        @Test
        @DisplayName("test consumeData method for RunDaysBatch")
        void test2(){
            RunDaysBatch runDaysBatch = new RunDaysBatch();
            userDataReportService.consumeData(runDaysBatch);
            verify(userDataReportService,new Times(1)).runDays(runDaysBatch);
        }
        @Test
        @DisplayName("test consumeData method for UniqueInstallBatch")
        void test3(){
            UniqueInstallBatch uniqueInstallBatch = new UniqueInstallBatch();
            userDataReportService.consumeData(uniqueInstallBatch);
            verify(userDataReportService,new Times(1)).uniqueInstall(uniqueInstallBatch);
        }
        @Test
        @DisplayName("test consumeData method for ConfigureSourceBatch")
        void test4(){
            ConfigureSourceBatch configureSourceBatch = new ConfigureSourceBatch();
            userDataReportService.consumeData(configureSourceBatch);
            verify(userDataReportService,new Times(1)).configureDatasource(configureSourceBatch);
        }
        @Test
        @DisplayName("test consumeData method for TasksNumBatch")
        void test5(){
            TasksNumBatch tasksNumBatch = new TasksNumBatch();
            userDataReportService.consumeData(tasksNumBatch);
            verify(userDataReportService,new Times(1)).tasksNum(tasksNumBatch);
        }
        @Test
        @DisplayName("test consumeData method for Object")
        void test6(){
            userDataReportService.consumeData(mock(Object.class));
            verify(userDataReportService,new Times(0)).runsNum(any(RunsNumBatch.class));
            verify(userDataReportService,new Times(0)).runDays(any(RunDaysBatch.class));
            verify(userDataReportService,new Times(0)).uniqueInstall(any(UniqueInstallBatch.class));
            verify(userDataReportService,new Times(0)).configureDatasource(any(ConfigureSourceBatch.class));
            verify(userDataReportService,new Times(0)).tasksNum(any(TasksNumBatch.class));
        }
    }
    @Nested
    class ProduceDataTest{
        private LinkedBlockingQueue reportQueue;
        private Object obj;
        @BeforeEach
        void beforeEach(){
            reportQueue = spy(LinkedBlockingQueue.class);
            ReflectionTestUtils.setField(userDataReportService,"reportQueue",reportQueue);
            obj = mock(Object.class);
            doCallRealMethod().when(userDataReportService).produceData(obj);
        }
        @Test
        @SneakyThrows
        @DisplayName("test produceData method when acceptReportData is false")
        void test1(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",false);
            userDataReportService.produceData(obj);
            verify(reportQueue,never()).offer(obj, 100, TimeUnit.MILLISECONDS);
        }
        @Test
        @SneakyThrows
        @DisplayName("test produceData method when acceptReportData is true")
        void test2(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",true);
            userDataReportService.produceData(obj);
            verify(reportQueue,new Times(1)).offer(obj, 100, TimeUnit.MILLISECONDS);
            Object actual = reportQueue.poll();
            assertEquals(obj, actual);
        }
        @Test
        @SneakyThrows
        @DisplayName("test produceData method when offer occur exception")
        void test3(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",true);
            doThrow(InterruptedException.class).when(reportQueue).offer(obj,100,TimeUnit.MILLISECONDS);
            userDataReportService.produceData(obj);
            verify(reportQueue,new Times(1)).offer(obj, 100, TimeUnit.MILLISECONDS);
            Object actual = reportQueue.poll();
            assertNull(actual);
        }
    }
    @Nested
    class RunsNumTest{
        @BeforeEach
        void beforeEach(){
            doCallRealMethod().when(userDataReportService).runsNum(any(RunsNumBatch.class));
        }
        @Test
        @DisplayName("test runsNum method when acceptReportData is false")
        void test1(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",false);
            userDataReportService.runsNum(mock(RunsNumBatch.class));
            verify(reportPlatform,new Times(0)).sendRequest(anyString(),anyString());
        }
        @Test
        @DisplayName("test runsNum method when acceptReportData is true")
        void test2(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",true);
            userDataReportService.runsNum(mock(RunsNumBatch.class));
            verify(reportPlatform,new Times(1)).sendRequest(anyString(),anyString());
        }
    }
    @Nested
    class RunDaysTest{
        @BeforeEach
        void beforeEach(){
            doCallRealMethod().when(userDataReportService).runDays(any(RunDaysBatch.class));
        }
        @Test
        @DisplayName("test runDays method when acceptReportData is false")
        void test1(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",false);
            userDataReportService.runDays(mock(RunDaysBatch.class));
            verify(reportPlatform,new Times(0)).sendRequest(anyString(),anyString());
        }
        @Test
        @DisplayName("test runDays method when acceptReportData is true")
        void test2(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",true);
            userDataReportService.runDays(mock(RunDaysBatch.class));
            verify(reportPlatform,new Times(1)).sendRequest(anyString(),anyString());
        }
    }
    @Nested
    class UniqueInstallTest{
        @BeforeEach
        void beforeEach(){
            doCallRealMethod().when(userDataReportService).uniqueInstall(any(UniqueInstallBatch.class));
        }
        @Test
        @DisplayName("test uniqueInstall method when acceptReportData is false")
        void test1(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",false);
            userDataReportService.uniqueInstall(mock(UniqueInstallBatch.class));
            verify(reportPlatform,new Times(0)).sendRequest(anyString(),anyString());
        }
        @Test
        @DisplayName("test uniqueInstall method when acceptReportData is true")
        void test2(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",true);
            userDataReportService.uniqueInstall(mock(UniqueInstallBatch.class));
            verify(reportPlatform,new Times(1)).sendRequest(anyString(),anyString());
        }
    }
    @Nested
    class ConfigureDatasourceTest{
        @BeforeEach
        void beforeEach(){
            doCallRealMethod().when(userDataReportService).configureDatasource(any(ConfigureSourceBatch.class));
        }
        @Test
        @DisplayName("test configureDatasource method when acceptReportData is false")
        void test1(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",false);
            userDataReportService.configureDatasource(mock(ConfigureSourceBatch.class));
            verify(reportPlatform,new Times(0)).sendRequest(anyString(),anyString());
        }
        @Test
        @DisplayName("test configureDatasource method when acceptReportData is true")
        void test2(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",true);
            userDataReportService.configureDatasource(mock(ConfigureSourceBatch.class));
            verify(reportPlatform,new Times(1)).sendRequest(anyString(),anyString());
        }
    }
    @Nested
    class TasksNumTest{
        @BeforeEach
        void beforeEach(){
            doCallRealMethod().when(userDataReportService).tasksNum(any(TasksNumBatch.class));
        }
        @Test
        @DisplayName("test tasksNum method when acceptReportData is false")
        void test1(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",false);
            userDataReportService.tasksNum(mock(TasksNumBatch.class));
            verify(reportPlatform,new Times(0)).sendRequest(anyString(),anyString());
        }
        @Test
        @DisplayName("test tasksNum method when acceptReportData is true")
        void test2(){
            ReflectionTestUtils.setField(userDataReportService,"acceptReportData",true);
            userDataReportService.tasksNum(mock(TasksNumBatch.class));
            verify(reportPlatform,new Times(1)).sendRequest(anyString(),anyString());
        }
    }
}
