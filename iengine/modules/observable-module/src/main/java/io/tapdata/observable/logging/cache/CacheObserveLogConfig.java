package io.tapdata.observable.logging.cache;

import io.tapdata.common.SettingService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CacheObserveLogConfig {
    static final String MAX_FILE_SIZE_MB_KEY = "observe.log.cache.maxFileSizeMB";
    static final String MAX_BACKUP_INDEX_KEY = "observe.log.cache.maxBackupIndex";
    static final int DEFAULT_MAX_FILE_SIZE_MB = 100;
    static final int DEFAULT_MAX_BACKUP_INDEX = 2;
    private static final long BYTES_PER_MB = 1024L * 1024L;
    private static final Logger LOGGER = LogManager.getLogger(CacheObserveLogConfig.class);

    private final long maxFileSizeBytes;
    private final int maxBackupIndex;

    CacheObserveLogConfig(long maxFileSizeBytes, int maxBackupIndex) {
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.maxBackupIndex = maxBackupIndex;
    }

    static CacheObserveLogConfig from(SettingService settingService) {
        String maxFileSizeMbValue = settingService == null
                ? String.valueOf(DEFAULT_MAX_FILE_SIZE_MB)
                : settingService.getString(MAX_FILE_SIZE_MB_KEY, String.valueOf(DEFAULT_MAX_FILE_SIZE_MB));
        String maxBackupIndexValue = settingService == null
                ? String.valueOf(DEFAULT_MAX_BACKUP_INDEX)
                : settingService.getString(MAX_BACKUP_INDEX_KEY, String.valueOf(DEFAULT_MAX_BACKUP_INDEX));
        long maxFileSizeMb = positiveLong(
                MAX_FILE_SIZE_MB_KEY,
                maxFileSizeMbValue,
                DEFAULT_MAX_FILE_SIZE_MB);
        int maxBackupIndex = nonNegativeInt(
                MAX_BACKUP_INDEX_KEY,
                maxBackupIndexValue,
                DEFAULT_MAX_BACKUP_INDEX);

        long maxFileSizeBytes;
        try {
            maxFileSizeBytes = Math.multiplyExact(maxFileSizeMb, BYTES_PER_MB);
        } catch (ArithmeticException e) {
            maxFileSizeBytes = DEFAULT_MAX_FILE_SIZE_MB * BYTES_PER_MB;
            LOGGER.error("CacheObserveLogs setting {} is too large, use safe default {}",
                    MAX_FILE_SIZE_MB_KEY, maxFileSizeBytes);
        }
        return new CacheObserveLogConfig(maxFileSizeBytes, maxBackupIndex);
    }

    private static long positiveLong(String key, String value, long defaultValue) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed > 0L) {
                return parsed;
            }
        } catch (NumberFormatException ignored) {
            // Report the invalid setting once below.
        }
        LOGGER.error("Invalid CacheObserveLogs setting {}, use safe default {}", key, defaultValue);
        return defaultValue;
    }

    private static int nonNegativeInt(String key, String value, int defaultValue) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed >= 0) {
                return parsed;
            }
        } catch (NumberFormatException ignored) {
            // Report the invalid setting once below.
        }
        LOGGER.error("Invalid CacheObserveLogs setting {}, use safe default {}", key, defaultValue);
        return defaultValue;
    }

    long getMaxFileSizeBytes() {
        return maxFileSizeBytes;
    }

    int getMaxBackupIndex() {
        return maxBackupIndex;
    }
}
