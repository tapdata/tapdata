package io.tapdata.observable.logging;

import com.tapdata.constant.BeanUtil;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.observable.logging.appender.Appender;
import io.tapdata.observable.logging.appender.AppenderFactory;
import io.tapdata.observable.logging.appender.FileAppender;
import io.tapdata.observable.logging.appender.ObsHttpTMAppender;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TriggeringPolicy;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TaskLoggerTest {
    @DisplayName("test Refresh FileAppender")
    @Test
    void test1(){
        try(MockedStatic<AppenderFactory> appenderFactoryMockedStatic = mockStatic(AppenderFactory.class)){
            List<io.tapdata.observable.logging.appender.Appender<?>> tapObsAppenders = new ArrayList<>();
            LogConfiguration logConfiguration = LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(100).build();
            AppenderFactory appenderFactory = mock(AppenderFactory.class);
            when(AppenderFactory.getInstance()).thenReturn(appenderFactory);
            TaskLogger taskLogger = mock(TaskLogger.class);
            TimeBasedTriggeringPolicy timeBasedTriggeringPolicy = TimeBasedTriggeringPolicy.newBuilder().withInterval(1).withModulate(true).build();
            RollingFileAppender rollingFileAppender = RollingFileAppender.newBuilder()
                    .setName("rollingFileAppender")
                    .withFileName("./tapdata-agent.log")
                    .withFilePattern("./tapdata-agent-%i.log.%d{yyyyMMdd}.gz")
                    .withPolicy(timeBasedTriggeringPolicy)
                    .build();
            FileAppender fileAppender = mock(FileAppender.class);
            ReflectionTestUtils.setField(fileAppender,"rollingFileAppender",rollingFileAppender);
            doCallRealMethod().when(fileAppender).getRollingFileAppender();
            tapObsAppenders.add(fileAppender);
            ReflectionTestUtils.setField(taskLogger,"tapObsAppenders",tapObsAppenders);
            ReflectionTestUtils.setField(taskLogger,"logAppendFactory",appenderFactory);
            doCallRealMethod().when(taskLogger).refreshFileAppender(logConfiguration);
            taskLogger.refreshFileAppender(logConfiguration);
            assertEquals("./tapdata-agent.log",rollingFileAppender.getFileName());
            assertEquals("rollingFileAppender",rollingFileAppender.getName());
            TriggeringPolicy triggeringPolicy = rollingFileAppender.getTriggeringPolicy();
            boolean updateSuccess= triggeringPolicy instanceof CompositeTriggeringPolicy;
            assertEquals(true,updateSuccess);
        }

    }

    @DisplayName("test appenders list no have FileAppender ")
    @Test
    void test2() {
        LogConfiguration logConfiguration = LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(100).build();
        TaskLogger taskLogger = mock(TaskLogger.class);
        List<io.tapdata.observable.logging.appender.Appender<?>> tapObsAppenders = new ArrayList<>();
        ObsHttpTMAppender obsHttpTMAppender = mock(ObsHttpTMAppender.class);
        tapObsAppenders.add(obsHttpTMAppender);
        ReflectionTestUtils.setField(taskLogger, "tapObsAppenders", tapObsAppenders);
        doCallRealMethod().when(taskLogger).refreshFileAppender(logConfiguration);
        assertEquals(1, tapObsAppenders.size());
        assertEquals(true, obsHttpTMAppender == tapObsAppenders.get(0));
    }

    @Nested
    class testTaskLogger {
        @Test
        void testEnableDebug() {
            TaskLogger taskLogger = mock(TaskLogger.class);
            doCallRealMethod().when(taskLogger).withTaskLogSetting(anyString(), anyLong(), anyLong());
            doCallRealMethod().when(taskLogger).isDebugEnabled();
            doCallRealMethod().when(taskLogger).noNeedLog(any());

            taskLogger.withTaskLogSetting("INFO", 1L, 1L);
            Assertions.assertFalse((Boolean) ReflectionTestUtils.getField(taskLogger, "enableDebugLogger"));

            taskLogger.withTaskLogSetting("DEBUG", 1000L, 1000L);
            Assertions.assertTrue((Boolean)ReflectionTestUtils.getField(taskLogger, "enableDebugLogger"));
        }

        @Test
        void testNoNeedLog() {
            TaskLogger taskLogger = mock(TaskLogger.class);
            doCallRealMethod().when(taskLogger).withTaskLogSetting(anyString(), anyLong(), anyLong());
            doCallRealMethod().when(taskLogger).noNeedLog(any());

            taskLogger.withTaskLogSetting("DEBUG", 10L, 60L);

            for (int i = 0; i < 100; i++) {
                Assertions.assertFalse(taskLogger.noNeedLog("TRACE"));
            }
            for (int i = 0; i < 10; i++) {
                Assertions.assertFalse(taskLogger.noNeedLog("DEBUG"));
            }
            Assertions.assertTrue(taskLogger.noNeedLog("DEBUG"));
        }

        @Test
        void testTrace() {
            TaskLogger taskLogger = mock(TaskLogger.class);
            doCallRealMethod().when(taskLogger).withTaskLogSetting(anyString(), anyLong(), anyLong());
            doCallRealMethod().when(taskLogger).noNeedLog(any());
            doCallRealMethod().when(taskLogger).trace(any(), anyString(), any());

            AppenderFactory logAppendFactory = mock(AppenderFactory.class);
            ReflectionTestUtils.setField(taskLogger, "logAppendFactory", logAppendFactory);

            taskLogger.trace(() -> MonitoringLogsDto.builder().level(LogLevel.INFO.getLevel()), "test {}", 1);

            verify(logAppendFactory, times(1)).appendLog(any());
        }

        @Test
        void testObsLogger() {
            AtomicInteger counter = new AtomicInteger(0);
            ObsLogger obsLogger = new ObsLogger() {
                @Override
                public MonitoringLogsDto.MonitoringLogsDtoBuilder logBaseBuilder() {
                    return null;
                }

                @Override
                public void trace(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {
                    counter.incrementAndGet();
                }

                @Override
                public void debug(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {

                }

                @Override
                public void info(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {

                }

                @Override
                public void warn(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {

                }

                @Override
                public void error(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable, String message, Object... params) {

                }

                @Override
                public void fatal(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable, String message, Object... params) {

                }

                @Override
                public boolean isEnabled(LogLevel logLevel) {
                    return false;
                }

                @Override
                public boolean isInfoEnabled() {
                    return false;
                }

                @Override
                public boolean isWarnEnabled() {
                    return false;
                }

                @Override
                public boolean isErrorEnabled() {
                    return false;
                }

                @Override
                public boolean isDebugEnabled() {
                    return false;
                }

                @Override
                public boolean isFatalEnabled() {
                    return false;
                }
            };
            obsLogger.trace("test {}", 1);
            Assertions.assertEquals(1, counter.get());
            obsLogger.trace("test {}", 2);
            Assertions.assertEquals(2, counter.get());
        }

        @Test
        void testWithTaskLogSetting() {
            TaskLogger taskLogger = mock(TaskLogger.class);
            doCallRealMethod().when(taskLogger).withTaskLogSetting(anyString(), anyLong(), anyLong());
            doCallRealMethod().when(taskLogger).isDebugEnabled();
            doCallRealMethod().when(taskLogger).noNeedLog(any());
            ReflectionTestUtils.setField(taskLogger, "enableDebugLogger",true);
            taskLogger.withTaskLogSetting("INFO", 1L, 1L);
            verify(taskLogger, times(1)).closeCatchData();
            Assertions.assertFalse((Boolean) ReflectionTestUtils.getField(taskLogger, "enableDebugLogger"));

        }
    }

    @Test
    void testFilterDebugFileAppender() {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        taskDto.setName("taskName");
        TaskLogger taskLogger = mock(TaskLogger.class);
        ReflectionTestUtils.setField(taskLogger, "taskId", taskDto.getId().toHexString());

        try (MockedStatic<BeanUtil> beanUtilMock = mockStatic(BeanUtil.class)) {

            beanUtilMock.when(() -> BeanUtil.getBean(any())).thenAnswer(answer -> {
                Class<?> cls = answer.getArgument(0);
                return mock(cls);
            });

            doCallRealMethod().when(taskLogger).filterDebugFileAppender(any());
            doCallRealMethod().when(taskLogger).getDebugFileAppenderName(any());
            doCallRealMethod().when(taskLogger).getTaskId();
            when(taskLogger.getRecordCeiling()).thenCallRealMethod();
            when(taskLogger.getIntervalCeiling()).thenCallRealMethod();
            when(taskLogger.isEnableDebugLogger()).thenCallRealMethod();

            List<Appender<?>> tapObsAppenders = new ArrayList<>();
            tapObsAppenders.add(FileAppender.create("test", taskLogger.getDebugFileAppenderName(taskDto.getId().toHexString())));
            ReflectionTestUtils.setField(taskLogger, "tapObsAppenders", tapObsAppenders);

            AtomicReference<Appender<?>> appender = new AtomicReference<>();
            taskLogger.filterDebugFileAppender(appender::set);

            Assertions.assertNotNull(appender.get());
            Assertions.assertNull(taskLogger.getRecordCeiling());
            Assertions.assertNull(taskLogger.getIntervalCeiling());
            Assertions.assertFalse(taskLogger.isEnableDebugLogger());

        } catch (Exception e) {
            e.printStackTrace(System.err);
        }

    }
}