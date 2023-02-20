package io.tapdata.js.connector.base;

import io.tapdata.common.support.core.ConnectorLog;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.logger.TapLogger;

import java.util.Objects;

public class TapConnectorLog extends ConnectorLog {
    private static final String TAG = ConnectorLog.class.getSimpleName();
    private Log log;
    public TapConnectorLog() {
    }
    public TapConnectorLog(Log log) {
        this.log = log;
    }

    public void debug(String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.debug(TAG, msg, this.args(params));
        }else {
            log.debug(msg,this.args(params));
        }
    }

    public void info(String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.info(TAG, msg, this.args(params));
        }else {
            log.info(msg,this.args(params));
        }
    }

    public void info(Long spendTime, String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.info(TAG, spendTime, msg, this.args(params));
        }else {
            log.info(msg,spendTime,this.args(params));
        }
    }

    public void infoWithData(String dataType, String data, String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.info(TAG, dataType, data, msg, this.args(params));
        }else {
            log.info(msg,dataType,this.args(params));
        }
    }

    public void warn(String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.warn(TAG, msg, this.args(params));
        }else {
            log.warn(msg,this.args(params));
        }
    }

    public void error(String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.error(TAG, msg, this.args(params));
        }else {
            log.error(msg,this.args(params));
        }
    }

    public void fatal(String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.fatal(TAG, msg, this.args(params));
        }else {
            log.fatal(msg,this.args(params));
        }
    }

    public void memory(String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.memory(TAG, msg, this.args(params));
        }else {

        }
    }
}
