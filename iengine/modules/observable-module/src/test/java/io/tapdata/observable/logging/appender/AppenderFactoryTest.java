package io.tapdata.observable.logging.appender;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.tapdata.constant.JSONUtil;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import lombok.SneakyThrows;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.ValueIn;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;
import java.io.File;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class AppenderFactoryTest {

    public static final String PATH_NAME = "." + File.separator + "test-ChronicleQueue";

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

        private ChronicleQueue chronicleQueue;
        private AppenderFactory appenderFactory;

        @BeforeEach
        void setUp() {
            chronicleQueue = ChronicleQueue.singleBuilder(PATH_NAME).build();
            appenderFactory = mock(AppenderFactory.class);
            ReflectionTestUtils.setField(appenderFactory, "cacheLogsQueue", chronicleQueue);
            doCallRealMethod().when(appenderFactory).appendLog(any(MonitoringLogsDto.class));
            Semaphore emptyWaiting = new Semaphore(1);
            ReflectionTestUtils.setField(appenderFactory, "emptyWaiting", emptyWaiting);
            Logger logger = mock(Logger.class);
            ReflectionTestUtils.setField(appenderFactory, "logger", logger);
        }

        @SneakyThrows
		@AfterEach
        void tearDown() {
            FileUtils.deleteDirectory(new File(PATH_NAME));
        }

        @DisplayName("test Append log when value is null")
        @Test
        void test1(){
            MonitoringLogsDto monitoringLogsDto = MonitoringLogsDto.builder().taskId("123").build();
            AppenderFactory instance = AppenderFactory.getInstance();
            assertDoesNotThrow(() -> instance.appendLog(monitoringLogsDto));
        }

        @Test
        @DisplayName("test main process")
        void test2() {
            MonitoringLogsDto monitoringLogsDto = MonitoringLogsDto.builder()
                    .date(new Date())
                    .level("INFO")
                    .errorStack("error stack")
                    .message("message")
                    .taskId("task id")
                    .taskRecordId("task record id")
                    .timestamp(System.currentTimeMillis())
                    .taskName("task name")
                    .nodeId("node id")
                    .nodeName("node name")
                    .errorCode("11001")
                    .fullErrorCode("TAP11001")
                    .dynamicDescriptionParameters(new String[]{"test", Instant.now().toString()})
                    .build();
            ExcerptTailer tailer = chronicleQueue.createTailer();
            appenderFactory.appendLog(monitoringLogsDto);
            boolean read = tailer.readDocument(r -> {
                ValueIn valueIn = r.getValueIn();
                assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(monitoringLogsDto.getDate()), valueIn.readString());
                assertEquals(monitoringLogsDto.getLevel(), valueIn.readString());
                assertEquals(monitoringLogsDto.getErrorStack(), valueIn.readString());
                assertEquals(monitoringLogsDto.getMessage(), valueIn.readString());
                assertEquals(monitoringLogsDto.getTaskId(), valueIn.readString());
                assertEquals(monitoringLogsDto.getTaskRecordId(), valueIn.readString());
                assertEquals(monitoringLogsDto.getTimestamp(), valueIn.readLong());
                assertEquals(monitoringLogsDto.getTaskName(), valueIn.readString());
                assertEquals(monitoringLogsDto.getNodeId(), valueIn.readString());
                assertEquals(monitoringLogsDto.getNodeName(), valueIn.readString());
                assertEquals(monitoringLogsDto.getErrorCode(), valueIn.readString());
                assertEquals(monitoringLogsDto.getFullErrorCode(), valueIn.readString());
                assertDoesNotThrow(() -> assertArrayEquals(monitoringLogsDto.getDynamicDescriptionParameters(), JSONUtil.json2POJO(valueIn.readString(), String[].class)));
            });
            assertTrue(read);
        }

        @Test
        @DisplayName("test dynamicDescriptionParameters is null")
        void test3() {
            MonitoringLogsDto monitoringLogsDto = MonitoringLogsDto.builder()
                    .date(new Date())
                    .level("INFO")
                    .errorStack("error stack")
                    .message("message")
                    .taskId("task id")
                    .taskRecordId("task record id")
                    .timestamp(System.currentTimeMillis())
                    .taskName("task name")
                    .nodeId("node id")
                    .nodeName("node name")
                    .errorCode("11001")
                    .fullErrorCode("TAP11001")
                    .dynamicDescriptionParameters(null)
                    .build();
            ExcerptTailer tailer = chronicleQueue.createTailer();
            appenderFactory.appendLog(monitoringLogsDto);
            boolean read = tailer.readDocument(r -> {
                ValueIn valueIn = r.getValueIn();
                assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(monitoringLogsDto.getDate()), valueIn.readString());
                assertEquals(monitoringLogsDto.getLevel(), valueIn.readString());
                assertEquals(monitoringLogsDto.getErrorStack(), valueIn.readString());
                assertEquals(monitoringLogsDto.getMessage(), valueIn.readString());
                assertEquals(monitoringLogsDto.getTaskId(), valueIn.readString());
                assertEquals(monitoringLogsDto.getTaskRecordId(), valueIn.readString());
                assertEquals(monitoringLogsDto.getTimestamp(), valueIn.readLong());
                assertEquals(monitoringLogsDto.getTaskName(), valueIn.readString());
                assertEquals(monitoringLogsDto.getNodeId(), valueIn.readString());
                assertEquals(monitoringLogsDto.getNodeName(), valueIn.readString());
                assertEquals(monitoringLogsDto.getErrorCode(), valueIn.readString());
                assertEquals(monitoringLogsDto.getFullErrorCode(), valueIn.readString());
                assertEquals("[]", valueIn.readString());
            });
            assertTrue(read);
        }

        @Test
        @DisplayName("test dynamicDescriptionParameters is empty array")
        void test4() {
            MonitoringLogsDto monitoringLogsDto = MonitoringLogsDto.builder()
                    .date(new Date())
                    .level("INFO")
                    .errorStack("error stack")
                    .message("message")
                    .taskId("task id")
                    .taskRecordId("task record id")
                    .timestamp(System.currentTimeMillis())
                    .taskName("task name")
                    .nodeId("node id")
                    .nodeName("node name")
                    .errorCode("11001")
                    .fullErrorCode("TAP11001")
                    .dynamicDescriptionParameters(new String[]{})
                    .build();
            ExcerptTailer tailer = chronicleQueue.createTailer();
            appenderFactory.appendLog(monitoringLogsDto);
            boolean read = tailer.readDocument(r -> {
                ValueIn valueIn = r.getValueIn();
                assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(monitoringLogsDto.getDate()), valueIn.readString());
                assertEquals(monitoringLogsDto.getLevel(), valueIn.readString());
                assertEquals(monitoringLogsDto.getErrorStack(), valueIn.readString());
                assertEquals(monitoringLogsDto.getMessage(), valueIn.readString());
                assertEquals(monitoringLogsDto.getTaskId(), valueIn.readString());
                assertEquals(monitoringLogsDto.getTaskRecordId(), valueIn.readString());
                assertEquals(monitoringLogsDto.getTimestamp(), valueIn.readLong());
                assertEquals(monitoringLogsDto.getTaskName(), valueIn.readString());
                assertEquals(monitoringLogsDto.getNodeId(), valueIn.readString());
                assertEquals(monitoringLogsDto.getNodeName(), valueIn.readString());
                assertEquals(monitoringLogsDto.getErrorCode(), valueIn.readString());
                assertEquals(monitoringLogsDto.getFullErrorCode(), valueIn.readString());
                assertEquals("[]", valueIn.readString());
            });
            assertTrue(read);
        }

        @Test
        @DisplayName("test json parse dynamicDescriptionParameters error")
        void test5() {
            try (
                    MockedStatic<JSONUtil> jsonUtilMockedStatic = mockStatic(JSONUtil.class)
            ) {
                JsonProcessingException jsonProcessingException = mock(JsonProcessingException.class);
                jsonUtilMockedStatic.when(() -> JSONUtil.obj2Json(any())).thenThrow(jsonProcessingException);
                MonitoringLogsDto monitoringLogsDto = MonitoringLogsDto.builder()
                        .date(new Date())
                        .level("INFO")
                        .errorStack("error stack")
                        .message("message")
                        .taskId("task id")
                        .taskRecordId("task record id")
                        .timestamp(System.currentTimeMillis())
                        .taskName("task name")
                        .nodeId("node id")
                        .nodeName("node name")
                        .errorCode("11001")
                        .fullErrorCode("TAP11001")
                        .dynamicDescriptionParameters(new String[]{"xxx"})
                        .build();
                ExcerptTailer tailer = chronicleQueue.createTailer();
                appenderFactory.appendLog(monitoringLogsDto);
                boolean read = tailer.readDocument(r -> {
                    ValueIn valueIn = r.getValueIn();
                    assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(monitoringLogsDto.getDate()), valueIn.readString());
                    assertEquals(monitoringLogsDto.getLevel(), valueIn.readString());
                    assertEquals(monitoringLogsDto.getErrorStack(), valueIn.readString());
                    assertEquals(monitoringLogsDto.getMessage(), valueIn.readString());
                    assertEquals(monitoringLogsDto.getTaskId(), valueIn.readString());
                    assertEquals(monitoringLogsDto.getTaskRecordId(), valueIn.readString());
                    assertEquals(monitoringLogsDto.getTimestamp(), valueIn.readLong());
                    assertEquals(monitoringLogsDto.getTaskName(), valueIn.readString());
                    assertEquals(monitoringLogsDto.getNodeId(), valueIn.readString());
                    assertEquals(monitoringLogsDto.getNodeName(), valueIn.readString());
                    assertEquals(monitoringLogsDto.getErrorCode(), valueIn.readString());
                    assertEquals(monitoringLogsDto.getFullErrorCode(), valueIn.readString());
                    assertEquals("[]", valueIn.readString());
                });
                assertTrue(read);
            }
        }
    }

    @Nested
    @DisplayName("Method decodeFromWireIn test")
    class decodeFromWireInTest {
        private ChronicleQueue chronicleQueue;
        private AppenderFactory appenderFactory;

        @BeforeEach
        void setUp() {
            chronicleQueue = ChronicleQueue.singleBuilder(PATH_NAME).build();
            appenderFactory = mock(AppenderFactory.class);
            ReflectionTestUtils.setField(appenderFactory, "cacheLogsQueue", chronicleQueue);
            doCallRealMethod().when(appenderFactory).appendLog(any(MonitoringLogsDto.class));
            Semaphore emptyWaiting = new Semaphore(1);
            ReflectionTestUtils.setField(appenderFactory, "emptyWaiting", emptyWaiting);
            Logger logger = mock(Logger.class);
            ReflectionTestUtils.setField(appenderFactory, "logger", logger);
            doCallRealMethod().when(appenderFactory).decodeFromWireIn(any(ValueIn.class), any(MonitoringLogsDto.MonitoringLogsDtoBuilder.class));
        }

        @SneakyThrows
        @AfterEach
        void tearDown() {
            FileUtils.deleteDirectory(new File(PATH_NAME));
        }

        @Test
        @DisplayName("test main process")
        void test1() {
            MonitoringLogsDto monitoringLogsDto = MonitoringLogsDto.builder()
                    .date(new Date())
                    .level("INFO")
                    .errorStack("error stack")
                    .message("message")
                    .taskId("task id")
                    .taskRecordId("task record id")
                    .timestamp(System.currentTimeMillis())
                    .taskName("task name")
                    .nodeId("node id")
                    .nodeName("node name")
                    .errorCode("11001")
                    .fullErrorCode("TAP11001")
                    .dynamicDescriptionParameters(new String[]{"test", Instant.now().toString()})
                    .build();
            ExcerptTailer tailer = chronicleQueue.createTailer();
            appenderFactory.appendLog(monitoringLogsDto);
            MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();
            tailer.readDocument(w -> {
                ValueIn valueIn = w.getValueIn();
                appenderFactory.decodeFromWireIn(valueIn, builder);
            });
            MonitoringLogsDto result = builder.build();
            assertEquals(monitoringLogsDto.getDate().toString(), result.getDate().toString());
            assertEquals(monitoringLogsDto.getLevel(), result.getLevel());
            assertEquals(monitoringLogsDto.getErrorStack(), result.getErrorStack());
            assertEquals(monitoringLogsDto.getMessage(), result.getMessage());
            assertEquals(monitoringLogsDto.getTaskId(), result.getTaskId());
            assertEquals(monitoringLogsDto.getTaskRecordId(), result.getTaskRecordId());
            assertEquals(monitoringLogsDto.getTimestamp(), result.getTimestamp());
            assertEquals(monitoringLogsDto.getTaskName(), result.getTaskName());
            assertEquals(monitoringLogsDto.getNodeId(), result.getNodeId());
            assertEquals(monitoringLogsDto.getNodeName(), result.getNodeName());
            assertEquals(monitoringLogsDto.getErrorCode(), result.getErrorCode());
            assertEquals(monitoringLogsDto.getFullErrorCode(), result.getFullErrorCode());
            assertArrayEquals(monitoringLogsDto.getDynamicDescriptionParameters(), result.getDynamicDescriptionParameters());
        }
    }
}
