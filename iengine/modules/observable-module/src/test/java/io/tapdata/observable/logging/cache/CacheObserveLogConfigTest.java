package io.tapdata.observable.logging.cache;

import io.tapdata.common.SettingService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CacheObserveLogConfigTest {

    @Test
    void shouldReadValidSettings() {
        SettingService settingService = mock(SettingService.class);
        when(settingService.getString(CacheObserveLogConfig.MAX_FILE_SIZE_MB_KEY, "100"))
                .thenReturn("10");
        when(settingService.getString(CacheObserveLogConfig.MAX_BACKUP_INDEX_KEY, "5"))
                .thenReturn("2");

        CacheObserveLogConfig config = CacheObserveLogConfig.from(settingService);

        assertEquals(10L * 1024L * 1024L, config.getMaxFileSizeBytes());
        assertEquals(2, config.getMaxBackupIndex());
    }

    @Test
    void shouldUseFiniteDefaultsForInvalidSettings() {
        SettingService settingService = mock(SettingService.class);
        when(settingService.getString(CacheObserveLogConfig.MAX_FILE_SIZE_MB_KEY, "100"))
                .thenReturn("-1");
        when(settingService.getString(CacheObserveLogConfig.MAX_BACKUP_INDEX_KEY, "5"))
                .thenReturn("invalid");

        CacheObserveLogConfig config = CacheObserveLogConfig.from(settingService);

        assertEquals(100L * 1024L * 1024L, config.getMaxFileSizeBytes());
        assertEquals(5, config.getMaxBackupIndex());
    }

    @Test
    void shouldUseFiniteDefaultWhenMegabytesOverflowBytes() {
        SettingService settingService = mock(SettingService.class);
        when(settingService.getString(CacheObserveLogConfig.MAX_FILE_SIZE_MB_KEY, "100"))
                .thenReturn(String.valueOf(Long.MAX_VALUE));
        when(settingService.getString(CacheObserveLogConfig.MAX_BACKUP_INDEX_KEY, "5"))
                .thenReturn("5");

        CacheObserveLogConfig config = CacheObserveLogConfig.from(settingService);

        assertEquals(100L * 1024L * 1024L, config.getMaxFileSizeBytes());
        assertEquals(5, config.getMaxBackupIndex());
    }
}
