package com.tapdata.processor;

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.processor.constant.JSEngineEnum;
import com.tapdata.tm.commons.workflow.WorkflowScriptRequest;
import com.tapdata.tm.commons.workflow.WorkflowScriptResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.graalvm.polyglot.proxy.ProxyObject;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Workflow JS：与任务 JS 处理器共用 {@link ScriptUtil} 内置函数（rest/mongo/tcp/httpUtil 等）。
 * 每次 Attempt 新建引擎；超时关闭引擎 + cancel Future。
 * ScriptExecutorsManager 由 Engine 侧 {@link HostBindings} 注入。
 */
@Slf4j
public class WorkflowScriptEngine {

    public interface HostBindings extends AutoCloseable {
        void install(ScriptEngine engine, WorkflowScriptRequest request);

        @Override
        void close();
    }

    @FunctionalInterface
    public interface HostBindingsFactory {
        HostBindings create(WorkflowScriptRequest request);
    }

    private static volatile HostBindingsFactory hostBindingsFactory;

    public static void setHostBindingsFactory(HostBindingsFactory factory) {
        hostBindingsFactory = factory;
    }

    private static final AtomicInteger SEQ = new AtomicInteger();
    private static final ExecutorService POOL = Executors.newFixedThreadPool(2, r -> {
        Thread t = new Thread(r);
        t.setName("workflow-js-" + SEQ.incrementAndGet());
        t.setDaemon(true);
        return t;
    });

    public WorkflowScriptResult execute(WorkflowScriptRequest request) {
        WorkflowScriptResult result = new WorkflowScriptResult();
        long timeout = request == null ? 10_000L : Math.min(Math.max(1L, request.getTimeoutMs()), 25_000L);
        int maxBytes = request == null || request.getMaxOutputBytes() <= 0 ? 65_536 : request.getMaxOutputBytes();
        AtomicReference<ScriptEngine> engineRef = new AtomicReference<>();
        HostBindings host = null;
        HostBindingsFactory factory = hostBindingsFactory;
        if (factory != null) {
            host = factory.create(request);
        }
        HostBindings hostRef = host;
        Future<WorkflowScriptResult> future = POOL.submit(() -> eval(request, maxBytes, engineRef, hostRef));
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeoutException) {
            future.cancel(true);
            result.setSuccess(false);
            result.setTimeout(true);
            result.setErrorCode("Workflow.ScriptTimeout");
            result.setErrorMessage("script timeout");
            return result;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            result.setSuccess(false);
            result.setErrorMessage("interrupted");
            return result;
        } catch (ExecutionException ex) {
            result.setSuccess(false);
            result.setErrorMessage(formatExecutionError(ex));
            return result;
        } finally {
            closeEngine(engineRef.get());
            if (hostRef != null) {
                try {
                    hostRef.close();
                } catch (RuntimeException closeEx) {
                    log.warn("Close workflow js host bindings failed", closeEx);
                }
            }
        }
    }

    private WorkflowScriptResult eval(WorkflowScriptRequest request, int maxBytes,
                                      AtomicReference<ScriptEngine> engineRef, HostBindings host) throws ScriptException {
        WorkflowScriptResult result = new WorkflowScriptResult();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        ClassLoader saved = Thread.currentThread().getContextClassLoader();
        if (saved == null) {
            Thread.currentThread().setContextClassLoader(WorkflowScriptEngine.class.getClassLoader());
        }
        ScriptEngine scriptEngine = ScriptUtil.getScriptEngine(JSEngineEnum.GRAALVM_JS.getEngineName(), out, err);
        engineRef.set(scriptEngine);
        try {
            scriptEngine.eval(ScriptUtil.initBuildInMethod(null, null, null, false));
            String script = request == null || request.getScript() == null ? "" : request.getScript();
            scriptEngine.eval(script);
            scriptEngine.put("log", new Log4jScriptLogger(LogManager.getLogger(WorkflowScriptEngine.class)));
            if (host != null) {
                host.install(scriptEngine, request);
            }
            Map<String, Object> ctx = request == null || request.getContext() == null
                    ? Map.of() : request.getContext();
            Object output = invokeMain((Invocable) scriptEngine, scriptEngine, ctx);
            byte[] json = String.valueOf(output).getBytes(StandardCharsets.UTF_8);
            if (json.length > maxBytes) {
                result.setSuccess(false);
                result.setOutputTooLarge(true);
                result.setErrorCode("Workflow.ScriptOutputTooLarge");
                result.setErrorMessage("output too large");
                return result;
            }
            result.setOutput(asOutput(output));
            result.setSuccess(true);
            return result;
        } finally {
            Thread.currentThread().setContextClassLoader(saved);
        }
    }

    private static Object invokeMain(Invocable invocable, ScriptEngine scriptEngine, Map<String, Object> ctx)
            throws ScriptException {
        try {
            if (scriptEngine instanceof GraalJSScriptEngine) {
                return invocable.invokeFunction("main", ProxyObject.fromMap(new LinkedHashMap<>(ctx)));
            }
            return invocable.invokeFunction("main", ctx);
        } catch (NoSuchMethodException ignored) {
            return ctx;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asOutput(Object output) {
        if (output == null) {
            return new LinkedHashMap<>();
        }
        if (output instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            map.forEach((k, v) -> out.put(String.valueOf(k), v));
            return out;
        }
        Map<String, Object> wrapped = new LinkedHashMap<>();
        wrapped.put("value", output);
        return wrapped;
    }

    static String formatExecutionError(Throwable error) {
        Throwable t = error instanceof ExecutionException && error.getCause() != null ? error.getCause() : error;
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        while (t != null && depth++ < 8) {
            String msg = t.getMessage();
            if (msg == null || msg.isBlank()) {
                msg = t.getClass().getSimpleName();
            }
            if (sb.indexOf(msg) < 0) {
                if (sb.length() > 0) {
                    sb.append(": ");
                }
                sb.append(msg);
            }
            t = t.getCause();
        }
        return sb.length() == 0 ? String.valueOf(error) : sb.toString();
    }

    private static void closeEngine(ScriptEngine scriptEngine) {
        if (scriptEngine instanceof GraalJSScriptEngine graal) {
            try {
                graal.close();
            } catch (RuntimeException closeEx) {
                log.warn("Close workflow js engine failed", closeEx);
            }
        }
    }
}
