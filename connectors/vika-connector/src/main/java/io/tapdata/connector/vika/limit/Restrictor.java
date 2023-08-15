package io.tapdata.connector.vika.limit;

import io.tapdata.connector.vika.VikaConnector;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;

/**
 * @author GavinXiao
 * @description Restrictor create by Gavin
 * @create 2023/6/27 16:02
 **/
public interface Restrictor {
    int wait_time_once = 250;
    public static Object limitRule(Restrictor limit, Log log) {
        synchronized (VikaConnector.apiLock) {
            if (null == limit) return null;
            return retry(limit, log);
        }
    }
    public static void limitRule0(Restrictor0 limit, Log log) {
        synchronized (VikaConnector.apiLock) {
            if (null == limit) return;
            retry(limit, log);
        }
    }

    /**
     * @deprecated use limitRule
     * */
    public static Object retry(Restrictor limit, Log log){
        try {
            long limitStart = System.currentTimeMillis();
            Object obj = limit.limit();
            long payTime = (System.currentTimeMillis() - limitStart) / 1000;
            try {
                Thread.sleep(wait_time_once - payTime);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return obj;
        } catch (Throwable e) {
            if (null != e.getMessage() && (
                    e.getMessage().contains("code:429")
                            || e.getMessage().contains("\"code\":429")
            )) {
                if (null == log) {
                    TapLogger.warn("API-LIMIT", "API limited, retry again, msg: {}", e.getMessage());
                } else {
                    log.warn("API limited, retry again, msg: {}", e.getMessage());
                }
                try {
                    Thread.sleep(1000L);
                }catch (Exception ignore) {}
                return retry(limit, log);
            } else {
                throw e;
            }
        }
    }

    /**
     * @deprecated use limitRule0
     * */
    public static void retry(Restrictor0 limit, Log log){
        try {
            long limitStart = System.currentTimeMillis();
            limit.limit();
            long payTime = (System.currentTimeMillis() - limitStart) / 1000;
            try {
                Thread.sleep(wait_time_once - payTime);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Throwable e) {
            if (null != e.getMessage() && (
                    e.getMessage().contains("code:429")
                            || e.getMessage().contains("\"code\":429")
            )) {
                if (null == log) {
                    TapLogger.warn("API-LIMIT", "API limited, retry again, msg: {}", e.getMessage());
                } else {
                    log.warn("API limited, retry again, msg: {}", e.getMessage());
                }
                try {
                    Thread.sleep(1000L);
                } catch (Exception ignore) {}
                retry(limit, log);
            } else {
                throw e;
            }
        }
    }

    public Object limit();
}


