package io.tapdata.flow.engine.V2.script;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.processor.ScriptLogger;
import io.tapdata.entity.logger.TapLogger;

public class TapScriptLogger implements ScriptLogger {
    private String taskId;

    public  TapScriptLogger(String taskId) {
        this.taskId = taskId;
    }

    @Override
    public void debug(String message, Object... params) {
        Log4jUtil.setThreadContext(taskId);
        TapLogger.debug("", message, params);
    }

    @Override
    public void info(String message, Object... params) {
        Log4jUtil.setThreadContext(taskId);
        TapLogger.info("", message, params);
    }

    @Override
    public void warn(String message, Object... params) {
        Log4jUtil.setThreadContext(taskId);
        TapLogger.info("", message, params);

    }

    @Override
    public void error(String message, Object... params) {
        Log4jUtil.setThreadContext(taskId);
        TapLogger.info("", message, params);

    }

    @Override
    public void error(String message, Throwable throwable) {
        Log4jUtil.setThreadContext(taskId);
        TapLogger.error("", message, throwable);

    }

    @Override
    public void fatal(String message, Object... params) {
        Log4jUtil.setThreadContext(taskId);
        TapLogger.info("", message, params);

    }
}
