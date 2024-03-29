package io.tapdata.observable.logging.appender;


import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import lombok.SneakyThrows;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class AppenderFactoryTest {
    @Test
    void testGetInstance(){
        assertDoesNotThrow(()->{
            AppenderFactory instance=AppenderFactory.getInstance();
            Object cacheLogsQueue = ReflectionTestUtils.getField(instance, "cacheLogsQueue");
            Object cycle = ReflectionTestUtils.getField(instance, "cycle");
            assertNotNull(cacheLogsQueue);
            assertNotNull(cycle);
        });
    }
    @Nested
    class DeleteFileIfLessThanCurrentCycleTest{
        @DisplayName("test DeleteFileIfLessThanCurrentCycle when cycle less than current cycle")
        @Test
        void test1(){
            try(MockedStatic<FileUtils> fileUtilsMockedStatic = mockStatic(FileUtils.class)){
                File file = mock(File.class);
                when(FileUtils.deleteQuietly(file)).thenReturn(true);
                AppenderFactory instance = AppenderFactory.getInstance();
                ReflectionTestUtils.setField(instance,"cycle",10);
                instance.deleteFileIfLessThanCurrentCycle(4,file);
                long cycle = (long)ReflectionTestUtils.getField(instance, "cycle");
                assertEquals(4,cycle);
            }
        }
        @DisplayName("test DeleteFileIfLessThanCurrentCycle when cycle Greater than current cycle")
        @Test
        void test2(){
            File file = mock(File.class);
            AppenderFactory instance = AppenderFactory.getInstance();
            ReflectionTestUtils.setField(instance,"cycle",10);
            instance.deleteFileIfLessThanCurrentCycle(11,file);
            long cycle = (long)ReflectionTestUtils.getField(instance, "cycle");
            assertEquals(11,cycle);
        }
    }
    @Nested
    class ReadMesaageTest{
        @DisplayName("test ReadMessage when tailer read Ducument success")
        @Test
        void test1(){
            MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();
            try(MockedStatic<MonitoringLogsDto> monitoringLogsDtoMockedStatic = mockStatic(MonitoringLogsDto.class);){
                when(MonitoringLogsDto.builder()).thenReturn(builder);
                AppenderFactory instance = mock(AppenderFactory.class);
                ExcerptTailer tailer = mock(ExcerptTailer.class);
                doCallRealMethod().when(instance).readMessageFromCacheQueue(tailer,"fileAppender");
                doNothing().when(instance).appenderAppendLog(any(),anyString());
                when(tailer.readDocument(any())).thenReturn(true);
                instance.readMessageFromCacheQueue(tailer,"fileAppender");
                verify(instance,times(1)).appenderAppendLog(builder,"fileAppender");
            }
        }
        @DisplayName("test ReadMessage when tailer read Ducument error")
        @Test
        void test2(){
            MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();
            try(MockedStatic<MonitoringLogsDto> monitoringLogsDtoMockedStatic = mockStatic(MonitoringLogsDto.class);){
                Logger logger = mock(Logger.class);
                when(MonitoringLogsDto.builder()).thenReturn(builder);
                AppenderFactory instance = mock(AppenderFactory.class);
                ReflectionTestUtils.setField(instance,"logger",logger);
                ExcerptTailer tailer = mock(ExcerptTailer.class);
                doCallRealMethod().when(instance).readMessageFromCacheQueue(tailer,"fileAppender");
                when(tailer.readDocument(any())).thenThrow(new RuntimeException("read Error"));
                instance.readMessageFromCacheQueue(tailer,"fileAppender");
                verify(logger,times(1)).warn(anyString(),any(),any());
            }
        }
        @DisplayName("test ReadMessage when the queue is empty,will waiting")
        @SneakyThrows
        @Test
        void test3(){
            MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();
            try(MockedStatic<MonitoringLogsDto> monitoringLogsDtoMockedStatic = mockStatic(MonitoringLogsDto.class);){
                when(MonitoringLogsDto.builder()).thenReturn(builder);
                AppenderFactory instance = mock(AppenderFactory.class);
                Semaphore semaphore = mock(Semaphore.class);
                ReflectionTestUtils.setField(instance,"emptyWaiting",semaphore);
                ExcerptTailer tailer = mock(ExcerptTailer.class);
                doCallRealMethod().when(instance).readMessageFromCacheQueue(tailer,"fileAppender");
                when(tailer.readDocument(any())).thenReturn(false);
                instance.readMessageFromCacheQueue(tailer,"fileAppender");
                verify(semaphore,times(1)).tryAcquire(1, 200, TimeUnit.MILLISECONDS);
            }
        }
    }
    @Nested
    class AppendersAppendLogTest{
        private MonitoringLogsDto.MonitoringLogsDtoBuilder builder;
        private AppenderFactory appenderFactory;
        Map<String, List<Appender<MonitoringLogsDto>>> appenderMap;
        public static final String FILE_APPENDER_TAILER_ID = "FILE_APPENDER_TAILER";
        public static final String TM_APPENDER_TAILER_ID= "TM_APPENDER_TAILER";
        @BeforeEach
        void setUp(){
            builder = MonitoringLogsDto.builder();
            builder.taskId("123");
            appenderFactory = mock(AppenderFactory.class);
            appenderMap = new ConcurrentHashMap<>();

        }
        @DisplayName("test AppenderAppendLog when task appenders is empty")
        @Test
        void test1(){
            appenderMap.put("123",new ArrayList<>());
            ReflectionTestUtils.setField(appenderFactory,"appenderMap",appenderMap);
            doCallRealMethod().when(appenderFactory).appenderAppendLog(builder,"FILE_APPENDER_TAILER");
            assertDoesNotThrow(()->{appenderFactory.appenderAppendLog(builder,"FILE_APPENDER_TAILER");});
        }
        @DisplayName("test AppenderAppendLog when task appenders is not empty")
        @Test
        void test2(){
            List<Appender<MonitoringLogsDto>> appenders=new ArrayList<>();
            Appender appender = mock(FileAppender.class);
            appenders.add(appender);
            appenderMap.put("123",appenders);
            ReflectionTestUtils.setField(appenderFactory,"appenderMap",appenderMap);
            doCallRealMethod().when(appenderFactory).appenderAppendLog(builder,FILE_APPENDER_TAILER_ID);
            appenderFactory.appenderAppendLog(builder,FILE_APPENDER_TAILER_ID);
            doAnswer(invocationOnMock -> {
                MonitoringLogsDto monitoringLogsDto = (MonitoringLogsDto) invocationOnMock.getArgument(0);
                assertEquals("123",monitoringLogsDto.getTaskId());
                return null;
            }).when(appender).append(any());
        }
        @DisplayName("test appenderAppendLog when task appender is null")
        @Test
        void test3(){
            List<Appender<MonitoringLogsDto>> appenders=new ArrayList<>();
            appenders.add(null);
            appenderMap.put("123",appenders);
            ReflectionTestUtils.setField(appenderFactory,"appenderMap",appenderMap);
            doCallRealMethod().when(appenderFactory).appenderAppendLog(builder,FILE_APPENDER_TAILER_ID);
            assertDoesNotThrow(()->{appenderFactory.appenderAppendLog(builder,FILE_APPENDER_TAILER_ID);});
        }
        @DisplayName("test appenderAppendLog when appender is obsHttpAppender")
        @Test
        void test4(){
            List<Appender<MonitoringLogsDto>> appenders=new ArrayList<>();
            Appender appender = mock(ObsHttpTMAppender.class);
            appenders.add(appender);
            appenderMap.put("123",appenders);
            ReflectionTestUtils.setField(appenderFactory,"appenderMap",appenderMap);
            doCallRealMethod().when(appenderFactory).appenderAppendLog(builder,TM_APPENDER_TAILER_ID);
            appenderFactory.appenderAppendLog(builder,TM_APPENDER_TAILER_ID);
            doAnswer(invocationOnMock -> {
                MonitoringLogsDto monitoringLogsDto = (MonitoringLogsDto) invocationOnMock.getArgument(0);
                assertEquals("123",monitoringLogsDto.getTaskId());
                return null;
            }).when(appender).append(any());
        }
        @DisplayName("test appenderAppendLog when appender is ")
        @Test
        void test5(){
            List<Appender<MonitoringLogsDto>> appenders=new ArrayList<>();
            Appender appender = mock(ScriptNodeProcessNodeAppender.class);
            appenders.add(appender);
            appenderMap.put("123",appenders);
            ReflectionTestUtils.setField(appenderFactory,"appenderMap",appenderMap);
            doCallRealMethod().when(appenderFactory).appenderAppendLog(builder,TM_APPENDER_TAILER_ID);
            appenderFactory.appenderAppendLog(builder,TM_APPENDER_TAILER_ID);
            doAnswer(invocationOnMock -> {
                MonitoringLogsDto monitoringLogsDto = (MonitoringLogsDto) invocationOnMock.getArgument(0);
                assertEquals("123",monitoringLogsDto.getTaskId());
                return null;
            }).when(appender).append(any());
        }
    }
    @Nested
    class AppendLogTest{
        @DisplayName("test Append log when value is null")
        @Test
        void test1(){
            MonitoringLogsDto monitoringLogsDto = MonitoringLogsDto.builder().taskId("123").build();
            AppenderFactory instance = AppenderFactory.getInstance();
            instance.appendLog(monitoringLogsDto);
            assertDoesNotThrow(()->{instance.appendLog(monitoringLogsDto);});
        }
    }


}
