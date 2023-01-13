package io.tapdata.js.connector.iengine.typed;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleBindings;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.function.Function;

import static io.tapdata.base.ConnectorBase.fromJson;

public class GraalEngine extends AbstractEngine<GraalEngine> {
    public static final String TAG = NashornEngine.class.getSimpleName();

    @Override
    public Object invoker(String functionName, Object... params) {
        if (Objects.isNull(functionName)) return null;
        if (Objects.isNull(this.scriptEngine)) return null;
        Function<Object[], Object> polyglotMapAndFunction;
        try {
            polyglotMapAndFunction = (Function<Object[], Object>) this.scriptEngine.get(functionName);

            Object apply = polyglotMapAndFunction.apply(params);
            if (Objects.isNull(apply)) {
                return null;
            } else if (apply instanceof Map || apply instanceof Collection) {
                try {
                    String toString = apply.toString();
                    if (toString.matches("\\(([0-9]+)\\)\\[.*]")) {
                        toString = toString.replaceFirst("\\(([0-9]+)\\)", "");
                    }
                    return ConnectorBase.fromJsonArray(toString);
                } catch (Exception e) {
                    try {
                        String string = apply.toString();
                        return "{}".equals(string) ? new HashMap<>() : fromJson(string);
                    } catch (Exception error) {
                        TapLogger.warn(TAG, "function named " + functionName + " exec failed, function return value is: " + apply.toString() + "error cast java Object.");
                        return null;
                    }
                }
            } else {
                return apply;
            }
        } catch (Exception e) {
            TapLogger.warn(TAG, "Not function named " + functionName + " can be found.");
            return null;
        }
    }

    @Override
    public Object covertData(Object apply) {
        if (Objects.isNull(apply)) {
            return null;
        } else if (apply instanceof Map || apply instanceof Collection) {
            try {
                String toString = apply.toString();
                if (toString.matches("\\(([0-9]+)\\)\\[.*]")) {
                    toString = toString.replaceFirst("\\(([0-9]+)\\)", "");
                }
                return ConnectorBase.fromJsonArray(toString);
            } catch (Exception e) {
                try {
                    String string = apply.toString();
                    return "{}".equals(string) ? new HashMap<>() : fromJson(string);
                } catch (Exception error) {
                    //TapLogger.warn(TAG, "function named " + functionName + " exec failed, function return value is: " + apply.toString() + "error cast java Object.");
                    return null;
                }
            }
        } else {
            return apply;
        }
    }

    @Override
    protected Map.Entry<String, EngineHandel> load(URL url) {
        ScriptEngineManager engineManager = new ScriptEngineManager();
        this.scriptEngine = engineManager.getEngineByName(EngineHandel.GRAAL_ENGINE);
        try {
            List<Map.Entry<InputStream, File>> files = javaScriptFiles(url);
            for (Map.Entry<InputStream, File> file : files) {
                String path = file.getValue().getPath().replaceAll("\\\\", "/");
                SimpleBindings simpleBindings = new SimpleBindings();
                scriptEngine.getBindings(ScriptContext.GLOBAL_SCOPE).put(ScriptEngine.FILENAME, path);
                simpleBindings.putAll(scriptEngine.getBindings(ScriptContext.GLOBAL_SCOPE));
                scriptEngine.eval(new InputStreamReader(file.getKey()), simpleBindings);
            }
        } catch (Exception error) {
            throw new CoreException("Error java script code, message: " + error.getMessage());
        }
        return new AbstractMap.SimpleEntry<String, EngineHandel>("", this);
    }
}
