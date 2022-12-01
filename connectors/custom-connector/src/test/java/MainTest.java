import io.tapdata.connector.custom.CustomEventMessage;
import io.tapdata.connector.custom.core.ScriptCore;
import io.tapdata.connector.custom.util.CustomLog;
import io.tapdata.connector.custom.util.ScriptUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.Test;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.list;

public class MainTest {
    @Test
    public void testScript() {
        exec(new String[]{"src/main/resources/test.js","requestData"});
    }
    @Test
    public void testScriptJs() {
        exec();
    }

    @Test
    public void testScripts(){
        String[][] script = {
                {"src/main/resources/apiJs/test.js","requestData"},
                {"src/main/resources/apiJs/batchRead.js","batchRead"}
        };
        for (String[] js : script) {
            exec(js);
        }
    }

    public void exec(String[] script){
        ScriptEngineManager engineManager = new ScriptEngineManager();
        ScriptEngine scriptEngine = engineManager.getEngineByName("nashorn");
        try {
            scriptEngine.eval("load('" + script[0] + "');");
            Invocable inv = (Invocable) scriptEngine;
            String retValue = (String) inv.invokeFunction(script[1], new MainTest());
            System.out.println(script[0] + "@" + script[1] + " returned " + retValue);
        } catch (ScriptException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }
    public void exec(){
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

    private void streamRead( List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        ScriptCore scriptCore = new ScriptCore(tableList.get(0));
        AtomicReference<Object> contextMap = new AtomicReference<>(offsetState);

        //ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class);
        //ScriptEngine scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName("nashorn"));//customConfig.getJsEngineName()

        ScriptEngineManager engineManager = new ScriptEngineManager();
        ScriptEngine scriptEngine = engineManager.getEngineByName("nashorn");

        scriptEngine.eval(
                "load('src/main/resources/apiJs/test.js');" +
                "load('src/main/resources/apiJs/batchRead.js');"
        );
//        scriptEngine.eval("load('src/main/resources/apiJs/batchRead.js');");
        scriptEngine.put("core", scriptCore);
        scriptEngine.put("log", new CustomLog());
        AtomicReference<Throwable> scriptException = new AtomicReference<>();
        Runnable runnable = () -> {
            Invocable invocable = (Invocable) scriptEngine;
            try {
                while (isAlive()) {
                    invocable.invokeFunction("requestData", contextMap.get());
                    invocable.invokeFunction("batchRead", null,null,null,"Issues");
                    Thread.sleep(1000);
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                scriptException.set(e);
            }
        };
        Thread t = new Thread(runnable);
        t.start();
        consumer.streamReadStarted();
        List<TapEvent> eventList = new ArrayList<>();
        Object lastContextMap = null;
        while (isAlive() && t.isAlive()) {
            CustomEventMessage message = null;
            try {
                message = scriptCore.getEventQueue().poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
            if (EmptyKit.isNotNull(message)) {
                eventList.add(message.getTapEvent());
                lastContextMap = message.getContextMap();
                if (eventList.size() == recordSize) {
                    consumer.accept(eventList, lastContextMap);
                    contextMap.set(lastContextMap);
                    eventList = new ArrayList<>();
                }
            }
        }
        if (EmptyKit.isNotNull(scriptException.get())) {
            throw scriptException.get();
        }
        if (isAlive() && EmptyKit.isNotEmpty(eventList)) {
            consumer.accept(eventList, lastContextMap);
            contextMap.set(lastContextMap);
        }
        if (t.isAlive()) {
            t.stop();
        }
        consumer.streamReadEnded();
    }

    boolean isAlive(){
        return true;
    }
}