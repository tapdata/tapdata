package io.tapdata.http.util;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;

import java.util.Objects;

public class TapConnectorLog extends ConnectorLog {
    private static final String TAG = TapConnectorLog.class.getSimpleName();
    private Log log;

    public TapConnectorLog() {
    }

    public TapConnectorLog(Log log) {
        this.log = log;
    }

    public void debug(String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.debug(TAG, msg);
        } else {
            log.debug(msg);
        }
    }

    public void info(String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.info(TAG, msg);
        } else {
            log.info(msg);
        }
    }

//    public void info(Long spendTime, String msg, Object... params) {
//        if (Objects.isNull(this.log)) {
//            TapLogger.info(TAG, spendTime, msg, this.args(params));
//        }else {
//            log.info(msg,spendTime,this.args(params));
//        }
//    }

//    public void infoWithData(String dataType, String data, String msg, Object... params) {
//        if (Objects.isNull(this.log)) {
//            TapLogger.info(TAG, dataType, data, msg, this.args(params));
//        }else {
//            log.info(msg,dataType,this.args(params));
//        }
//    }

    public void warn(String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.warn(TAG, msg);
        } else {
            log.warn(msg);
        }
    }

    public void error(String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.error(TAG, msg);
        } else {
            log.error(msg);
        }
    }

    public void fatal(String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.fatal(TAG, msg);
        } else {
            log.fatal(msg);
        }
    }

    public void memory(String msg, Object... params) {
        if (Objects.isNull(this.log)) {
            TapLogger.memory(TAG, msg);
        }
    }
}