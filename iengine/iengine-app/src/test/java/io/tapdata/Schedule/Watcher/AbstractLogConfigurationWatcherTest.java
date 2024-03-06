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
        LogConfiguration logConfiguration=new LogConfiguration(180,10,100);
        AbstractLogConfigurationWatcher logConfigurationWatcher=new AgentLogConfigurationWatcher(logConfiguration);
        boolean modify = logConfigurationWatcher.checkIsModify(new LogConfiguration(180, 10, 1));
        assertEquals(true,modify);
    }
    @DisplayName("test check logConfiguration no modify")
    @Test
    void checkIsModifyTest2(){
        LogConfiguration logConfiguration=new LogConfiguration(180,10,100);
        AbstractLogConfigurationWatcher logConfigurationWatcher=new AgentLogConfigurationWatcher(logConfiguration);
        boolean modify = logConfigurationWatcher.checkIsModify(new LogConfiguration(180, 10, 100));
        assertEquals(false,modify);
    }
    @DisplayName("test check modify by null args")
    @Test
    void checkIsModifyTest3(){
        LogConfiguration logConfiguration=new LogConfiguration(180,10,100);
        AbstractLogConfigurationWatcher logConfigurationWatcher=new AgentLogConfigurationWatcher(logConfiguration);
        boolean modify = logConfigurationWatcher.checkIsModify(null);
        assertEquals(false,modify);
    }
    @DisplayName("test onCheck when watcher logConf is null")
    @Test
    void testOnCheckTest1(){
        AbstractLogConfigurationWatcher logConfigurationWatcher=mock(AgentLogConfigurationWatcher.class);
        doCallRealMethod().when(logConfigurationWatcher).onCheck();
        LogConfiguration logConfiguration = new LogConfiguration(180, 10, 100);
        when(logConfigurationWatcher.getLogConfig()).thenReturn(logConfiguration);
        logConfigurationWatcher.onCheck();
        verify(logConfigurationWatcher,times(1)).updateConfig(logConfiguration);
        assertEquals(logConfiguration,logConfigurationWatcher.logConfiguration);
    }
    @Test
    @DisplayName("test onCheck when logConf modify")
    void testOnCheckTest2(){
        LogConfiguration logConfiguration = new LogConfiguration(180, 10, 100);
        LogConfiguration logConfigurationArgs = new LogConfiguration(190, 10, 100);
        AbstractLogConfigurationWatcher logConfigurationWatcher=mock(AgentLogConfigurationWatcher.class);
        when(logConfigurationWatcher.getLogConfig()).thenReturn(logConfigurationArgs);
        doCallRealMethod().when(logConfigurationWatcher).onCheck();
        doCallRealMethod().when(logConfigurationWatcher).checkIsModify(logConfigurationArgs);
        ReflectionTestUtils.setField(logConfigurationWatcher,"logConfiguration",logConfiguration);
        logConfigurationWatcher.onCheck();
        verify(logConfigurationWatcher,times(1)).updateConfig(logConfigurationArgs);
        assertEquals(logConfigurationArgs,logConfigurationWatcher.logConfiguration);
    }
}
