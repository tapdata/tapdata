package io.tapdata.js.connector.iengine;

import io.tapdata.js.connector.JSConnector;

import java.net.URL;
import java.util.Enumeration;
import java.util.Objects;

public class ScriptEngineInstance {
    public static final String JS_FLOODER = "connectors-javascript";

    public LoadJavaScripter script;

    private static ScriptEngineInstance instance;

    public static ScriptEngineInstance instance(){
        if (Objects.isNull(instance)){
            synchronized (ScriptEngineInstance.class){
                if (Objects.isNull(instance)){
                    instance = new ScriptEngineInstance();
                }
            }
        }
        return instance;
    }

    private ScriptEngineInstance(){
        this.scriptInstance();
    }

    private void scriptInstance(){
        try {
            ClassLoader classLoader = JSConnector.class.getClassLoader();
            Enumeration<URL> resources = classLoader.getResources(JS_FLOODER+"/");
            this.script = LoadJavaScripter.loader("", JS_FLOODER);
            //this.script = LoadJavaScripter.loader(jarFilePath, flooder);
            this.script.load(resources);
        }catch (Exception ignored){

        }
    }
    public LoadJavaScripter script(){
        return this.script;
    }
}
