package io.tapdata.js.connector.iengine.typed;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.logger.TapLogger;

import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static io.tapdata.base.ConnectorBase.fromJson;

public class NashornEngine extends AbstractEngine<NashornEngine> {
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
            //scriptException.set(e);
            TapLogger.warn(TAG, "Not function named " + functionName + " can be found.");
            return null;
        }
    }

    @Override
    public Object covertData(Object data) {
        return null;
    }

    @Override
    protected Map.Entry<String, EngineHandel> load(URL url) {
        return null;
    }
}
