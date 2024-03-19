package io.tapdata.loglistener;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import jdk.jfr.internal.LogTag;

import java.util.concurrent.Callable;

public interface TapLogger {
    void info(String message, Object... params);
    void warn(String message, Object... params);
    void error(String message, Object... params);
    void debug(String message, Object... params);
    boolean isDebugEnabled();


}
