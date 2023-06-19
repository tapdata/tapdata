package io.tapdata.http.util;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.logger.TapLogger;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
                args[index] = param instanceof String ? (String) param : this.covertData(param);
            }
            return args;
        }
        return new String[0];
    }

    protected String covertData(Object apply) {
        if (Objects.isNull(apply)) {
            return null;
        } else if (apply instanceof Map) {
            return ConnectorBase.toJson(apply);
        } else if (apply instanceof Collection) {
            try {
                return ConnectorBase.toJson(apply);
            } catch (Exception e) {
                String toString = apply.toString();
                if (toString.matches("\\(([0-9]+)\\)\\[.*]")) {
                    toString = toString.replaceFirst("\\(([0-9]+)\\)", "");
                }
                return toString;
            }
        } else if(apply.getClass().isArray()){
            return ConnectorBase.toJson(apply);
        }else {
            return String.valueOf(apply);
        }
    }
}