package io.tapdata.js.connector.base;

import io.tapdata.common.support.core.ConnectorLog;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.js.utils.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

//    public LogEntity(Object... logEntities) {
//        if (Objects.nonNull(logEntities) && logEntities.length > 0) {
//            Object msg = logEntities[0];
//            this.msg = msg instanceof String ? (String) msg : toJson(msg);
//            if (logEntities.length > 1) {
//                this.params = new Object[logEntities.length - 1];
//                for (int i = 1; i < logEntities.length; i++) {
//                    this.params[i - 1] = logEntities[i];
//                }
//            }
//        }
//    }

    public LogEntity(Object... logEntities) {
        Object[] todo = (Object[]) Collector.convertObj(logEntities);
        List<Object> o = (List<Object>) todo[0];
        if (o.size() > 0) {
            this.msg = String.valueOf(o.get(0));
            if (o.size() > 1) {
                this.params = o.subList(1, o.size()).toArray();
            }
        }
    }
//    public LogEntity(Object... logEntities) {
//        Object[] todo = this.todo(logEntities);
//        List<Object> o = (List<Object>) todo[0];
//        if (o.size() > 0) {
//            this.msg = String.valueOf(o.get(0));
//            if (o.size() > 1) {
//                this.params = o.subList(1, o.size()).toArray();
//            }
//        }
//    }

    private Object[] todo(Object... params) {
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