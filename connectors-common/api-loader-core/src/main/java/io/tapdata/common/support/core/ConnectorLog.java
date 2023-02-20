package io.tapdata.common.support.core;

import io.tapdata.entity.logger.TapLogger;

import java.util.Objects;
import java.util.Optional;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class ConnectorLog {

    private static final String TAG = ConnectorLog.class.getSimpleName();

    public ConnectorLog() {

    }

    public void debug(String msg, Object... params) {
        TapLogger.debug(TAG, msg, this.args(params));
    }

    public void info(String msg, Object... params) {
        TapLogger.info(TAG, msg, this.args(params));
    }

    public void info(Long spendTime, String msg, Object... params) {
        TapLogger.info(TAG, spendTime, msg, this.args(params));
    }

    public void infoWithData(String dataType, String data, String msg, Object... params) {
        TapLogger.info(TAG, dataType, data, msg, this.args(params));
    }

    public void warn(String msg, Object... params) {
        TapLogger.warn(TAG, msg, this.args(params));
    }

    public void error(String msg, Object... params) {
        TapLogger.error(TAG, msg, this.args(params));
    }

    public void fatal(String msg, Object... params) {
        TapLogger.fatal(TAG, msg, this.args(params));
    }

    public void memory(String msg, Object... params) {
        TapLogger.memory(TAG, msg, this.args(params));
    }

    protected Object args(Object... params) {
        if (Objects.nonNull(params) && params.length > 0) {
            String[] args = new String[params.length];
            for (int index = 0; index < params.length; index++) {
                Object param = Optional.ofNullable(params[index]).orElse("null");
                args[index] = param instanceof String ? (String) param : toJson(param);
            }
            return args;
        }
        return new String[0];
    }
}