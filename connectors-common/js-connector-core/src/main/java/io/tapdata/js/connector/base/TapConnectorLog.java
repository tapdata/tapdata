package io.tapdata.js.connector.base;

import io.tapdata.common.support.core.ConnectorLog;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;

import java.util.*;

import static io.tapdata.base.ConnectorBase.toJson;

public class TapConnectorLog extends ConnectorLog {
    private static final String TAG = ConnectorLog.class.getSimpleName();
    private Log log;

    public TapConnectorLog() {
    }

    public TapConnectorLog(Log log) {
        this.log = log;
    }

    public void debug(Object... params) {
        LogEntity logEntity = new LogEntity(params);
        if (Objects.isNull(this.log)) {
            TapLogger.debug(TAG, logEntity.msg(), this.args(logEntity.params()));
        } else {
            log.debug(logEntity.msg(), this.args(logEntity.params()));
        }
    }

    public void info(Object... params) {
        LogEntity logEntity = new LogEntity(params);
        if (Objects.isNull(this.log)) {
            TapLogger.info(TAG, logEntity.msg(), this.args(logEntity.params()));
        } else {
            log.info(logEntity.msg(), this.args(logEntity.params()));
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
        LogEntity logEntity = new LogEntity(params);
        if (Objects.isNull(this.log)) {
            TapLogger.warn(TAG, logEntity.msg(), this.args(logEntity.params()));
        } else {
            log.warn(logEntity.msg(), this.args(logEntity.params()));
        }
    }

    public void error(Object... params) {
        LogEntity logEntity = new LogEntity(params);
        if (Objects.isNull(this.log)) {
            TapLogger.error(TAG, logEntity.msg(), this.args(logEntity.params()));
        } else {
            log.error(logEntity.msg(), this.args(logEntity.params()));
        }
    }

    public void fatal(Object... params) {
        LogEntity logEntity = new LogEntity(params);
        if (Objects.isNull(this.log)) {
            TapLogger.fatal(TAG, logEntity.msg(), this.args(logEntity.params()));
        } else {
            log.fatal(logEntity.msg(), this.args(logEntity.params()));
        }
    }

    public void memory(Object... params) {
        LogEntity logEntity = new LogEntity(params);
        if (Objects.isNull(this.log)) {
            TapLogger.memory(TAG, logEntity.msg(), this.args(logEntity.params()));
        }
    }
}

class LogEntity {
    String msg;
    Object[] params = new Object[]{};

    public LogEntity msg(String msg) {
        this.msg = msg;
        return this;
    }

    public LogEntity params(Objects... params) {
        this.params = params;
        return this;
    }

    public String msg() {
        return this.msg;
    }

    public Object[] params() {
        return this.params;
    }

    public LogEntity(Object... logEntities) {
        Object[] todo = this.todo(logEntities);
        List<Object> o = (List<Object>) todo[0];
        if (o.size()>0) {
            this.msg = String.valueOf(o.get(0));
            if (o.size() > 1) {
                this.params = o.subList(1, o.size()).toArray();
            }
        }
        //if (Objects.nonNull(logEntities) && logEntities.length > 0) {
       //    Object msg = logEntities[0];
       //    this.msg = msg instanceof String ? (String) msg : toJson(msg);
       //    if (logEntities.length > 1) {
       //        this.params = new Object[logEntities.length - 1];
       //        for (int i = 1; i < logEntities.length; i++) {
       //            this.params[i - 1] = logEntities[i];
       //        }
       //    }
       //}
    }

    private Object[] todo(Object ... params){
        Object[] obj = new Object[params.length];
        for (int index = 0; index < params.length; index++) {
            Iterator<Object> l = ((List<Object>) params[index]).iterator();
            List<Object> value = new ArrayList<>();
            while (l.hasNext()) {
                Object next = l.next();
                value.add(next);
            }
            obj[index] = value;
        }
        return obj;
    }
}