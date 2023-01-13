package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.js.connector.base.CustomEventMessage;
import io.tapdata.js.connector.base.ScriptCore;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.js.connector.server.sender.BatchReadSender;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;

import javax.script.ScriptEngine;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class JSBatchReadFunction extends FunctionBase implements FunctionSupport<BatchReadFunction> {
    AtomicBoolean isAlive = new AtomicBoolean(true);

    public JSBatchReadFunction isAlive(AtomicBoolean isAlive) {
        this.isAlive = isAlive;
        return this;
    }

    private JSBatchReadFunction() {
        super();
        super.functionName = JSFunctionNames.BatchReadFunction;
    }

    @Override
    public BatchReadFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return this::batchRead;
    }

    private void batchRead(TapConnectorContext context, TapTable table, Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        if (Objects.isNull(context)) {
            throw new CoreException("TapConnectorContext must not be null or not be empty.");
        }
        if (Objects.isNull(table)) {
            throw new CoreException("TapTable must not be null or not be empty.");
        }
        ScriptEngine scriptEngine = javaScripter.scriptEngine();
        ScriptCore scriptCore = new ScriptCore(table.getId());
        scriptEngine.put("core", scriptCore);
        AtomicReference<Throwable> scriptException = new AtomicReference<>();
        BatchReadSender sender = new BatchReadSender().core(scriptCore);
        final Object finalOffset = offset;
        Runnable runnable = () -> {
            try {
                super.javaScripter.invoker(
                        JSFunctionNames.BatchReadFunction.jsName(),
                        context.getConfigContext(),
                        context.getNodeConfig(),
                        finalOffset,
                        table.getId(),
                        batchCount,
                        sender
                );
            } catch (Exception e) {
                scriptException.set(e);
            }
        };
        Thread t = new Thread(runnable);
        t.start();
        List<TapEvent> eventList = new ArrayList<>();
        while (isAlive.get() && t.isAlive()) {
            try {
                CustomEventMessage message = null;
                try {
                    message = scriptCore.getEventQueue().poll(1, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
                if (EmptyKit.isNotNull(message)) {
                    eventList.add(message.getTapEvent());
                    if (eventList.size() == batchCount) {
                        eventsOffsetConsumer.accept(eventList, offset);
                        eventList = new ArrayList<>();
                    }
                }
            } catch (Exception e) {
                break;
            }
        }
        if (EmptyKit.isNotNull(scriptException.get())) {
            throw new RuntimeException(scriptException.get());
        }
        if (isAlive.get() && EmptyKit.isNotEmpty(eventList)) {
            eventsOffsetConsumer.accept(eventList, offset);
        }
        if (t.isAlive()) {
            t.stop();
        }
    }

    public static BatchReadFunction create(LoadJavaScripter loadJavaScripter, AtomicBoolean isAlive) {
        return new JSBatchReadFunction().isAlive(isAlive).function(loadJavaScripter);
    }
}
