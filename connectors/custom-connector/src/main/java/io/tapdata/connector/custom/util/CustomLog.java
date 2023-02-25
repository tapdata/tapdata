package io.tapdata.connector.custom.util;

import io.tapdata.entity.logger.TapLogger;

public class CustomLog {

    private static final String TAG = CustomLog.class.getSimpleName();

    public CustomLog() {

    }

    public void debug(String msg, Object... params) {
        TapLogger.debug(TAG, msg, params);
    }

    public void info(String msg, Object... params) {
        TapLogger.info(TAG, msg, params);
    }

    public void info(Long spendTime, String msg, Object... params) {
        TapLogger.info(TAG, spendTime, msg, params);
    }

    public void infoWithData(String dataType, String data, String msg, Object... params) {
        TapLogger.info(TAG, dataType, data, msg, params);
    }

    public void warn(String msg, Object... params) {
        TapLogger.warn(TAG, msg, params);
    }

    public void error(String msg, Object... params) {
        TapLogger.error(TAG, msg, params);
    }

    public void fatal(String msg, Object... params) {
        TapLogger.fatal(TAG, msg, params);
    }

    public void memory(String msg, Object... params) {
        TapLogger.memory(TAG, msg, params);
    }
}
