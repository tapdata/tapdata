package io.tapdata.Schedule.Watcher;

import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.TaskLogger;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TaskLogConfigurationWatcherTest {
    @Test
    void testGetLogConf(){
        TaskLogConfigurationWatcher taskLogConfigurationWatcher = new TaskLogConfigurationWatcher();
        try(MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class)){
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            when(ObsLoggerFactory.getInstance()).thenReturn(obsLoggerFactory);
            LogConfiguration logConfiguration = new LogConfiguration(180, 100, 10);
            when(obsLoggerFactory.getLogConfiguration("task")).thenReturn(logConfiguration);
            LogConfiguration logConf = taskLogConfigurationWatcher.getLogConfig();
            assertEquals(logConfiguration,logConf);
        }
    }
    @DisplayName("test updateConfigTest")
    @Test
    void updateConfigTest1(){
        TaskLogConfigurationWatcher taskLogConfigurationWatcher = new TaskLogConfigurationWatcher();
        LogConfiguration logConfiguration = new LogConfiguration(180, 10, 100);
        try(MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class)){
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            when(ObsLoggerFactory.getInstance()).thenReturn(obsLoggerFactory);
            Map<String, TaskLogger> taskLoggerMap=new HashMap<>();
            TaskLogger mock1 = mock(TaskLogger.class);
            TaskLogger mock2 = mock(TaskLogger.class);
            taskLoggerMap.put("test1",mock1);
            taskLoggerMap.put("test2",mock2);
            when(obsLoggerFactory.getTaskLoggersMap()).thenReturn(taskLoggerMap);
            taskLogConfigurationWatcher.updateConfig(logConfiguration);
            verify(mock1,times(1)).refreshFileAppender(logConfiguration);
            verify(mock2,times(1)).refreshFileAppender(logConfiguration);
        }
    }
}
