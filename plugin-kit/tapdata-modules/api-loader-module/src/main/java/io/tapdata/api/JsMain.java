package io.tapdata.api;

import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.list;

public class JsMain {
    public static void main(String[] args) {
        StreamReadConsumer consumer = new StreamReadConsumer();
        try {
            streamRead(
                    list("TestCodingIssue"),
                    new Object(),
                    10,
                    consumer
            );
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    public static void streamRead(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        ScriptEngineManager engineManager = new ScriptEngineManager();
        ScriptEngine scriptEngine = engineManager.getEngineByName("nashorn");
        scriptEngine.eval("load('D:\\GavinData\\kitSpace\\tapdata\\plugin-kit\\tapdata-modules\\api-loader-module\\src\\main\\java\\io\\tapdata\\api\\apiJs\\connector.js');");
        //scriptEngine.put("core", scriptCore);
        //scriptEngine.put("log", new CustomLog());
        AtomicReference<Throwable> scriptException = new AtomicReference<>();
        Runnable runnable = () -> {
            Invocable invocable = (Invocable) scriptEngine;
            try {
                invocable.invokeFunction("test");
            } catch (Exception e) {
                scriptException.set(e);
            }
        };
        new Thread(runnable).start();
    }
}
