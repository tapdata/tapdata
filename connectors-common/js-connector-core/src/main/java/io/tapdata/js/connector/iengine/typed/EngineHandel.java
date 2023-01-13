package io.tapdata.js.connector.iengine.typed;

import java.net.URL;
import java.util.Enumeration;
import java.util.Map;

public interface EngineHandel {
    public static final String NASHORN_ENGINE = "nashorn";
    public static final String GRAAL_ENGINE = "graal.js";

    public Object invoker(String functionName, Object... params);

    public Map<String, EngineHandel> load(String jarFilePath, String flooder, Enumeration<URL> resources);

    public Object covertData(Object data);
}
