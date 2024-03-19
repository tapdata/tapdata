package io.tapdata.loglistener;



public interface TapLogger {
    void info(String message, Object... params);
    void warn(String message, Object... params);
    void error(String message, Object... params);
    void debug(String message, Object... params);
    boolean isDebugEnabled();


}
