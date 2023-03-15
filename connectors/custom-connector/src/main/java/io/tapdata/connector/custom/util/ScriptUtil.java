package io.tapdata.connector.custom.util;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.exception.StopException;
import io.tapdata.kit.EmptyKit;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.HashMap;

public class ScriptUtil {

    /**
     * script
     */
    public static final String FUNCTION_PREFIX = "function ";
    public static final String SOURCE_FUNCTION_NAME = "requestData";
    public static final String BEFORE_FUNCTION_NAME = "before";
    public static final String AFTER_FUNCTION_NAME = "after";
    public static final String TARGET_FUNCTION_NAME = "onData";
    public static final String FUNCTION_SUFFIX = "\n}";
    public static final String INITIAL_FUNCTION_IN_PARAM = " () {\n";
    public static final String CDC_FUNCTION_IN_PARAM = " (ctx) {\n";
    public static final String TARGET_FUNCTION_IN_PARAM = " (data) {\n";

    private static final String TAG = ScriptUtil.class.getSimpleName();

    public static String appendSourceFunctionScript(String script, boolean isInitial) {
        script = FUNCTION_PREFIX +
                SOURCE_FUNCTION_NAME +
                (isInitial ? INITIAL_FUNCTION_IN_PARAM : CDC_FUNCTION_IN_PARAM) +
                script +
                FUNCTION_SUFFIX;
        return script;
    }

    public static String appendTargetFunctionScript(String script) {
        script = FUNCTION_PREFIX +
                TARGET_FUNCTION_NAME +
                TARGET_FUNCTION_IN_PARAM +
                script +
                FUNCTION_SUFFIX;
        return script;
    }

    public static String appendBeforeFunctionScript(String script) {
        script = FUNCTION_PREFIX +
                BEFORE_FUNCTION_NAME +
                INITIAL_FUNCTION_IN_PARAM +
                (EmptyKit.isEmpty(script) ? "" : script) +
                FUNCTION_SUFFIX;
        return script;
    }

    public static String appendAfterFunctionScript(String script) {
        script = FUNCTION_PREFIX +
                AFTER_FUNCTION_NAME +
                INITIAL_FUNCTION_IN_PARAM +
                script +
                FUNCTION_SUFFIX;
        return script;
    }

    public static Runnable createScriptRunnable(ScriptEngine scriptEngine, String function) {
        if (scriptEngine != null) {
            return () -> executeScript(scriptEngine, function, new HashMap<>());
        } else {
            return null;
        }
    }

    public static Runnable createScriptRunnable(ScriptEngine scriptEngine, String function, Object data) {
        if (scriptEngine != null) {
            return () -> executeScript(scriptEngine, function, data);
        } else {
            return null;
        }
    }

    public static void executeScript(ScriptEngine scriptEngine, String function) {
        executeScript(scriptEngine, function, new HashMap<>());
    }

    public static void executeScript(ScriptEngine scriptEngine, String function, Object data) {
        if (scriptEngine != null) {
            Invocable invocable = (Invocable) scriptEngine;
            try {
                invocable.invokeFunction(function, data);
            } catch (StopException e) {
                TapLogger.info(TAG, "Get data and stop script.");
                throw new RuntimeException(e);
            } catch (ScriptException | NoSuchMethodException | RuntimeException e) {
                TapLogger.error(TAG, "Run script error, message: {}", e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }
}

