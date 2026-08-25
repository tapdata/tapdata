package com.tapdata.processor;

import com.tapdata.tm.commons.workflow.WorkflowScriptRequest;
import com.tapdata.tm.commons.workflow.WorkflowScriptResult;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.IOAccess;

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

/**
 * Workflow JS：每次 Attempt 新建并关闭 Context，超时关闭 Context + cancel Future。
 */
@Slf4j
public class WorkflowScriptEngine {

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
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        Context context = Context.newBuilder("js")
                .allowAllAccess(false)
                .allowHostAccess(ScriptUtil.SANDBOX_HOST_ACCESS)
                .allowHostClassLookup(name -> ScriptUtil.isAllowedHostClass(name, null, ScriptSandboxPolicy.WORKFLOW))
                .allowNativeAccess(false)
                .allowCreateProcess(false)
                .allowIO(IOAccess.NONE)
                .allowPolyglotAccess(PolyglotAccess.NONE)
                .out(out)
                .err(err)
                .build();
        Future<WorkflowScriptResult> future = POOL.submit(() -> eval(context, request, maxBytes));
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
            result.setErrorMessage(String.valueOf(ex.getCause()));
            return result;
        } finally {
            try {
                context.close(true);
            } catch (RuntimeException closeEx) {
                log.warn("Close workflow js context failed", closeEx);
            }
        }
    }

    private WorkflowScriptResult eval(Context context, WorkflowScriptRequest request, int maxBytes) {
        WorkflowScriptResult result = new WorkflowScriptResult();
        String script = request == null || request.getScript() == null ? "" : request.getScript();
        // Do not bind a local `var main`: it hoists and shadows the user-defined main(),
        // so typeof main is always 'undefined' and the fallback returns ctx.
        context.eval("js", script);
        context.eval("js", "function __workflowMainWrapper(ctx){"
                + "var impl = (typeof main === 'function') ? main : function(c){ return c; };"
                + "return impl(ctx);"
                + "}");
        Value bindings = context.getBindings("js");
        Value fn = bindings.getMember("__workflowMainWrapper");
        Value jsResult = fn.execute(request == null ? Map.of() : request.getContext());
        Object output = jsResult == null || jsResult.isNull() ? Map.of() : jsResult.as(Object.class);
        byte[] json = String.valueOf(output).getBytes(StandardCharsets.UTF_8);
        if (json.length > maxBytes) {
            result.setSuccess(false);
            result.setOutputTooLarge(true);
            result.setErrorCode("Workflow.ScriptOutputTooLarge");
            result.setErrorMessage("output too large");
            return result;
        }
        if (output instanceof Map) {
            result.setOutput(new LinkedHashMap<>((Map<String, Object>) output));
        } else {
            Map<String, Object> wrapped = new LinkedHashMap<>();
            wrapped.put("value", output);
            result.setOutput(wrapped);
        }
        result.setSuccess(true);
        return result;
    }
}
