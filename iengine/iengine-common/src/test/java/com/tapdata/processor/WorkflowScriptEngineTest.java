package com.tapdata.processor;

import com.tapdata.tm.commons.workflow.WorkflowScriptRequest;
import com.tapdata.tm.commons.workflow.WorkflowScriptResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowScriptEngineTest {

    private final WorkflowScriptEngine engine = new WorkflowScriptEngine();

    @AfterEach
    void resetHost() {
        WorkflowScriptEngine.setHostBindingsFactory(null);
    }

    @Test
    void mainReturnValueIsScriptOutputNotInputContext() {
        WorkflowScriptRequest request = new WorkflowScriptRequest();
        request.setScript("function main(ctx) { return { action: 'alarm' }; }");
        request.setContext(Map.of("trigger", Map.of("taskId", "t1")));
        request.setTimeoutMs(5_000L);
        request.setMaxOutputBytes(65_536);

        WorkflowScriptResult result = engine.execute(request);

        assertTrue(result.isSuccess());
        assertEquals("alarm", result.getOutput().get("action"));
        assertFalse(result.getOutput().containsKey("trigger"));
    }

    @Test
    void missingMainFallsBackToContext() {
        WorkflowScriptRequest request = new WorkflowScriptRequest();
        request.setScript("var ignored = 1;");
        request.setContext(Map.of("trigger", Map.of("taskId", "t1")));
        request.setTimeoutMs(5_000L);
        request.setMaxOutputBytes(65_536);

        WorkflowScriptResult result = engine.execute(request);

        assertTrue(result.isSuccess());
        assertEquals("t1", ((Map<?, ?>) result.getOutput().get("trigger")).get("taskId"));
    }

    @Test
    void exposesTaskJsBuiltinsRestMongoHttpUtilTcpAndUuid() {
        WorkflowScriptRequest request = new WorkflowScriptRequest();
        request.setScript("function main(ctx) { return {"
                + "rest: typeof rest !== 'undefined',"
                + "mongo: typeof mongo !== 'undefined',"
                + "httpUtil: typeof httpUtil !== 'undefined',"
                + "tcp: typeof tcp !== 'undefined',"
                + "uuid: typeof uuid !== 'undefined',"
                + "MD5: typeof MD5 === 'function'"
                + "}; }");
        request.setTimeoutMs(5_000L);
        request.setMaxOutputBytes(65_536);

        WorkflowScriptResult result = engine.execute(request);

        assertTrue(result.isSuccess(), () -> String.valueOf(result.getErrorMessage()));
        assertEquals(true, result.getOutput().get("rest"));
        assertEquals(true, result.getOutput().get("mongo"));
        assertEquals(true, result.getOutput().get("httpUtil"));
        assertEquals(true, result.getOutput().get("tcp"));
        assertEquals(true, result.getOutput().get("uuid"));
        assertEquals(true, result.getOutput().get("MD5"));
    }

    @Test
    void javaTypeHttpUtilIsAllowedLikeTaskJs() {
        WorkflowScriptRequest request = new WorkflowScriptRequest();
        request.setScript("function main(ctx) { try { Java.type('com.tapdata.http.HttpUtil'); return { blocked: false }; } catch (e) { return { blocked: true, error: String(e) }; } }");
        request.setTimeoutMs(5_000L);
        request.setMaxOutputBytes(65_536);

        WorkflowScriptResult result = engine.execute(request);

        assertTrue(result.isSuccess(), () -> String.valueOf(result.getErrorMessage()));
        assertEquals(false, result.getOutput().get("blocked"));
    }

    @Test
    void bindsScriptExecutorsManagerFromHost() {
        AtomicBoolean closed = new AtomicBoolean(false);
        Object dummy = new Object();
        WorkflowScriptEngine.setHostBindingsFactory(req -> new WorkflowScriptEngine.HostBindings() {
            @Override
            public void install(ScriptEngine scriptEngine, WorkflowScriptRequest request) {
                scriptEngine.put("ScriptExecutorsManager", dummy);
            }

            @Override
            public void close() {
                closed.set(true);
            }
        });
        WorkflowScriptRequest request = new WorkflowScriptRequest();
        request.setScript("function main(ctx) { return { present: typeof ScriptExecutorsManager !== 'undefined' }; }");
        request.setTimeoutMs(5_000L);
        request.setMaxOutputBytes(65_536);

        WorkflowScriptResult result = engine.execute(request);

        assertTrue(result.isSuccess(), () -> String.valueOf(result.getErrorMessage()));
        assertEquals(true, result.getOutput().get("present"));
        assertTrue(closed.get());
    }

    @Test
    void formatExecutionErrorIncludesNestedCause() {
        RuntimeException nested = new RuntimeException("script execute error",
                new RuntimeException("duplicate key value violates unique constraint \"workflow_audit_pkey\""));
        ExecutionException wrapped = new ExecutionException(new ScriptException(nested));

        String message = WorkflowScriptEngine.formatExecutionError(wrapped);

        assertTrue(message.contains("duplicate key value violates unique constraint"), message);
        assertTrue(message.contains("script execute error"), message);
    }
}
