package io.tapdata.Schedule.Watcher;

import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
 class AbstractLogConfigurationWatcherTest {

    @DisplayName("test check logConfiguration modify")
    @Test
    void checkIsModifyTest1(){
        LogConfiguration logConfiguration= LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(100).build();
        AbstractLogConfigurationWatcher logConfigurationWatcher=new TaskLogConfigurationWatcher(logConfiguration);
        LogConfiguration logConfiguration1= LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(1).build();
        boolean modify = logConfigurationWatcher.checkIsModify(logConfiguration1);
        assertEquals(true,modify);
    }
    @DisplayName("test check logConfiguration no modify")
    @Test
    void checkIsModifyTest2(){
        LogConfiguration logConfiguration= LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(100).build();
        AbstractLogConfigurationWatcher logConfigurationWatcher=new TaskLogConfigurationWatcher(logConfiguration);
        LogConfiguration logConfiguration1= LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(100).build();
        boolean modify = logConfigurationWatcher.checkIsModify(logConfiguration1);
        assertEquals(false,modify);
    }
    @DisplayName("test check modify by null args")
    @Test
    void checkIsModifyTest3(){
        LogConfiguration logConfiguration= LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(100).build();
        AbstractLogConfigurationWatcher logConfigurationWatcher=new TaskLogConfigurationWatcher(logConfiguration);
        boolean modify = logConfigurationWatcher.checkIsModify(null);
        assertEquals(false,modify);
    }
    @DisplayName("test onCheck when watcher logConf is null")
    @Test
    void testOnCheckTest1(){
        AbstractLogConfigurationWatcher logConfigurationWatcher=mock(TaskLogConfigurationWatcher.class);
        LogConfiguration logConfiguration= LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(100).build();
        doCallRealMethod().when(logConfigurationWatcher).checkIsModify(logConfiguration);
        doCallRealMethod().when(logConfigurationWatcher).onCheck();
        when(logConfigurationWatcher.getLogConfig()).thenReturn(logConfiguration);
        logConfigurationWatcher.onCheck();
        verify(logConfigurationWatcher,times(1)).updateConfig(logConfiguration);
        assertEquals(logConfiguration,logConfigurationWatcher.logConfiguration);
    }
    @Test
    @DisplayName("test onCheck when logConf modify")
    void testOnCheckTest2(){
        LogConfiguration logConfiguration= LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(100).build();
        LogConfiguration logConfigurationArgs = LogConfiguration.builder().logSaveTime(190).logSaveSize(10).logSaveCount(100).build();
        AbstractLogConfigurationWatcher logConfigurationWatcher=mock(TaskLogConfigurationWatcher.class);
        when(logConfigurationWatcher.getLogConfig()).thenReturn(logConfigurationArgs);
        doCallRealMethod().when(logConfigurationWatcher).onCheck();
        doCallRealMethod().when(logConfigurationWatcher).checkIsModify(logConfigurationArgs);
        ReflectionTestUtils.setField(logConfigurationWatcher,"logConfiguration",logConfiguration);
        logConfigurationWatcher.onCheck();
        verify(logConfigurationWatcher,times(1)).updateConfig(logConfigurationArgs);
        assertEquals(logConfigurationArgs,logConfigurationWatcher.logConfiguration);
    }
}
