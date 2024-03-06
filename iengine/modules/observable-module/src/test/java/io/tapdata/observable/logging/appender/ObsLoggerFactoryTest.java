package io.tapdata.observable.logging.appender;

import io.tapdata.common.SettingService;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class ObsLoggerFactoryTest {
    @DisplayName("test Get Task Log Configuration ")
    @Test
    void test1(){
        ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
        SettingService settingService = mock(SettingService.class);
        ReflectionTestUtils.setField(obsLoggerFactory,"settingService",settingService);
        String prefix="task";
        when(settingService.getInt(prefix+"_log_file_save_time", 180)).thenReturn(180);
        when(settingService.getInt(prefix+"_log_file_save_size",10)).thenReturn(10);
        when(settingService.getInt(prefix + "_log_file_save_count", 100)).thenReturn(100);
        doCallRealMethod().when(obsLoggerFactory).getLogConfiguration(prefix);
        LogConfiguration logConfiguration = obsLoggerFactory.getLogConfiguration(prefix);
        assertEquals(180,logConfiguration.getLogSaveTime());
        assertEquals(10,logConfiguration.getLogSaveSize());
        assertEquals(100,logConfiguration.getLogSaveCount());
    }
    @DisplayName("test Get null Log Configuration")
    @Test
    void test2(){
        ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
        SettingService settingService = mock(SettingService.class);
        ReflectionTestUtils.setField(obsLoggerFactory, "settingService", settingService);
        String prefix = null;
        when(settingService.getInt(prefix + "_log_file_save_time", 180)).thenReturn(180);
        when(settingService.getInt(prefix + "_log_file_save_size", 10)).thenReturn(10);
        when(settingService.getInt(prefix + "_log_file_save_count", 100)).thenReturn(100);
        doCallRealMethod().when(obsLoggerFactory).getLogConfiguration(prefix);
        LogConfiguration logConfiguration = obsLoggerFactory.getLogConfiguration(prefix);
        assertEquals(180, logConfiguration.getLogSaveTime());
        assertEquals(10, logConfiguration.getLogSaveSize());
        assertEquals(100, logConfiguration.getLogSaveCount());
    }
}
