package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.js.connector.base.CustomEventMessage;
import io.tapdata.js.connector.base.ScriptCore;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.js.connector.server.sender.StreamReadSender;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;

import javax.script.ScriptEngine;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class JSStreamReadFunction extends FunctionBase implements FunctionSupport<StreamReadFunction> {
    private AtomicBoolean isAlive = new AtomicBoolean(true);
    private final Object lock = new Object();
    private static final long STREAM_READ_DELAY_SEC = 1 * 60 * 1000L;

    public JSStreamReadFunction isAlive(AtomicBoolean isAlive) {
        this.isAlive = isAlive;
        return this;
    }

    JSStreamReadFunction() {
        super();
        super.functionName = JSFunctionNames.StreamReadFunction;
    }

    @Override
    public StreamReadFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return this::streamRead;
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        if (Objects.isNull(nodeContext)) {
            throw new CoreException("TapConnectorContext must not be null or not be empty.");
        }
        if (Objects.isNull(tableList)) {
            throw new CoreException("Table lists must not be null or not be empty.");
        }
        ScriptCore scriptCore = new ScriptCore(tableList.get(0));
        AtomicReference<Object> contextMap = new AtomicReference<>(offsetState);
        ScriptEngine scriptEngine = javaScripter.scriptEngine();
        scriptEngine.put("core", scriptCore);
        AtomicReference<Throwable> scriptException = new AtomicReference<>();
        StreamReadSender sender = new StreamReadSender().core(scriptCore);
        final Object finalOffset = offsetState;
        Runnable runnable = () -> {
            try {
                while (this.isAlive.get()) {
                    super.javaScripter.invoker(
                            JSFunctionNames.StreamReadFunction.jsName(),
                            nodeContext.getConfigContext(),
                            nodeContext.getNodeConfig(),
                            finalOffset,
                            tableList,
                            recordSize,
                            sender
                    );
                    synchronized (this.lock) {
                        this.lock.wait(JSStreamReadFunction.STREAM_READ_DELAY_SEC);
                    }
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
        long ts = System.currentTimeMillis();
        while (this.isAlive.get() && t.isAlive()) {
            CustomEventMessage message = null;
            try {
                message = scriptCore.getEventQueue().poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
            if (EmptyKit.isNotNull(message)) {
                eventList.add(message.getTapEvent());
                lastContextMap = message.getContextMap();
                if (eventList.size() == recordSize || (System.currentTimeMillis() - ts) >= 3000) {
                    consumer.accept(eventList, lastContextMap);
                    contextMap.set(lastContextMap);
                    eventList = new ArrayList<>();
                    ts = System.currentTimeMillis();
                }
            }
        }
        if (EmptyKit.isNotNull(scriptException.get())) {
            throw scriptException.get();
        }
        if (this.isAlive.get() && EmptyKit.isNotEmpty(eventList)) {
            consumer.accept(eventList, lastContextMap);
            contextMap.set(lastContextMap);
        }
        if (t.isAlive() || this.isAlive.get()) {
            synchronized (this.lock){
                this.lock.notifyAll();
            }
            t.stop();
        }
        consumer.streamReadEnded();
    }

    public static StreamReadFunction create(LoadJavaScripter loadJavaScripter, AtomicBoolean isAlive) {
        return new JSStreamReadFunction().isAlive(isAlive).function(loadJavaScripter);
    }
}
