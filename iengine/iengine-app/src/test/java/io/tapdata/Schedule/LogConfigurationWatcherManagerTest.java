package io.tapdata.Schedule;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LogConfigurationWatcherManagerTest {
    @Test
    void test1(){
        LogConfigurationWatcherManager logConfigurationWatcherManager = new LogConfigurationWatcherManager();
        assertDoesNotThrow(()->{
            logConfigurationWatcherManager.start();
        });
    }
}
