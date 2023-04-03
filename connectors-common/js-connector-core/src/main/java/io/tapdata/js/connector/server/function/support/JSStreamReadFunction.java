package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.js.connector.JSConnector;
import io.tapdata.js.connector.base.CustomEventMessage;
import io.tapdata.js.connector.base.ScriptCore;
import io.tapdata.js.connector.base.TapConfigContext;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.ExecuteConfig;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.js.connector.server.sender.StreamReadSender;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;

import javax.script.ScriptEngine;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class JSStreamReadFunction extends FunctionBase implements FunctionSupport<StreamReadFunction> {
    private AtomicBoolean isAlive = new AtomicBoolean(true);
    private final Object lock = new Object();

    ExecuteConfig config;

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
            throw new CoreException("TapConnectorContext cannot not be empty.");
        }
        if (Objects.isNull(this.config)) {
            this.config = ExecuteConfig.contextConfig(nodeContext);
        }
        if (Objects.isNull(tableList)) {
            throw new CoreException("Table lists cannot not be empty.");
        }
        ScriptCore scriptCore = new ScriptCore();
        AtomicReference<Object> contextMap = new AtomicReference<>(offsetState);
        ScriptEngine scriptEngine = super.javaScripter.scriptEngine();
        scriptEngine.put("core", scriptCore);
        AtomicReference<Throwable> scriptException = new AtomicReference<>();
        StreamReadSender sender = new StreamReadSender().core(scriptCore);
        AtomicBoolean streamReadFinished = new AtomicBoolean(false);
        final long waitTime = ((TapConfigContext) Optional.ofNullable(this.javaScripter.scriptEngine().get("tapConfig")).orElse(new TapConfigContext())).getStreamReadIntervalSeconds();
        Runnable runnable = () -> {
            try {
                while (this.isAlive.get()) {
                    synchronized (this.lock) {
                        this.lock.wait(waitTime);//JSStreamReadFunction.STREAM_READ_DELAY_SEC
                    }
                    synchronized (JSConnector.execLock) {
                        super.javaScripter.invoker(
                                JSFunctionNames.StreamReadFunction.jsName(),
                                Optional.ofNullable(nodeContext.getConnectionConfig()).orElse(new DataMap()),
                                Optional.ofNullable(nodeContext.getNodeConfig()).orElse(new DataMap()),
                                contextMap.get(),
                                tableList,
                                recordSize,
                                sender
                        );
                    }
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                scriptException.set(e);
            } finally {
                streamReadFinished.set(true);
            }
        };
        Thread t = new Thread(runnable);
        consumer.streamReadStarted();
        t.start();
        List<TapEvent> eventList = new ArrayList<>();
        Object lastContextMap = null;
        long ts = System.currentTimeMillis();
        while (this.isAlive.get()) {
            CustomEventMessage message = null;
            try {
                message = scriptCore.getEventQueue().poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
            if (EmptyKit.isNotNull(message)) {
                lastContextMap = message.getContextMap();
                TapEvent tapEvent = message.getTapEvent();
                if (Objects.nonNull(lastContextMap)) {
                    contextMap.set(lastContextMap);
                } else {
                    throw new CoreException("The breakpoint offset cannot be empty. Please carry the offset when submitting the event data.");
                }
                if (Objects.isNull(tapEvent)) {
                    continue;
                }
                eventList.add(tapEvent);
                if (eventList.size() == recordSize || (System.currentTimeMillis() - ts) >= 3000) {
                    consumer.accept(eventList, lastContextMap);
                    eventList = new ArrayList<>();
                    ts = System.currentTimeMillis();
                }
            }
            if (streamReadFinished.get() && scriptCore.getEventQueue().isEmpty())
                break;
        }
        if (EmptyKit.isNotNull(scriptException.get())) {
            throw scriptException.get();
        }
        if (this.isAlive.get() && EmptyKit.isNotEmpty(eventList)) {
            if (Objects.isNull(lastContextMap)) {
                throw new CoreException("The breakpoint offset cannot be empty. Please carry the offset when submitting the event data.");
            }
            consumer.accept(eventList, lastContextMap);
            contextMap.set(lastContextMap);
        }
        if (t.isAlive() || this.isAlive.get()) {
            synchronized (this.lock) {
                this.lock.notifyAll();
            }
        }
        consumer.streamReadEnded();
    }

    public static StreamReadFunction create(LoadJavaScripter loadJavaScripter, AtomicBoolean isAlive) {
        return new JSStreamReadFunction().isAlive(isAlive).function(loadJavaScripter);
    }

    public static class Config extends ExecuteConfig {
        private LoadJavaScripter javaScripter;
        private AtomicBoolean isAlive;

        public Config javaScripter(LoadJavaScripter javaScripter) {
            this.javaScripter = javaScripter;
            return this;
        }

        public LoadJavaScripter javaScripter() {
            return this.javaScripter;
        }

        public Config isAlive(AtomicBoolean isAlive) {
            this.isAlive = isAlive;
            return this;
        }

        public AtomicBoolean isAlive() {
            return this.isAlive;
        }

        public static Config config(TapConnectionContext context, LoadJavaScripter javaScripter, AtomicBoolean isAlive) {
            return new Config(context)
                    .javaScripter(javaScripter)
                    .isAlive(isAlive);
        }

        protected Config(TapConnectionContext context) {
            super(context);

        }
    }
}
