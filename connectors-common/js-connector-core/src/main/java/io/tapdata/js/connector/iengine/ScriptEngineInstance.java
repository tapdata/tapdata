package io.tapdata.js.connector.iengine;

import io.tapdata.entity.error.CoreException;
import io.tapdata.js.connector.JSConnector;

import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ScriptEngineInstance {
    public static final String JS_FLOODER = "connectors-javascript";

    public LoadJavaScripter script;
    public static final Map<String,LoadJavaScripter> scriptContext = new ConcurrentHashMap<>();

    private static ScriptEngineInstance instance;

    public static ScriptEngineInstance instance() {
        if (Objects.isNull(instance)) {
            synchronized (ScriptEngineInstance.class) {
                if (Objects.isNull(instance)) {
                    instance = new ScriptEngineInstance();
                }
            }
        }
        return instance;//= new ScriptEngineInstance();
    }

    private ScriptEngineInstance() {
        this.scriptInstance();
    }

    private void scriptInstance() {
        String threadName = Thread.currentThread().getName();
        this.script = ScriptEngineInstance.scriptContext.get(threadName);
        if (Objects.isNull(this.script)){
            this.script = LoadJavaScripter.loader("", JS_FLOODER);
            ScriptEngineInstance.scriptContext.put(threadName,this.script);
        }
    }

    public LoadJavaScripter script() {
        return this.script;
    }

    public void loadScript() {
        try {
            ClassLoader classLoader = JSConnector.class.getClassLoader();
            Enumeration<URL> resources = classLoader.getResources(JS_FLOODER + "/");
            this.script.load(resources);
        } catch (Exception error) {
            throw new CoreException(error.getMessage());
        }
    }
}
