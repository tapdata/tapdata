package com.tapdata.processor;

import com.tapdata.tm.commons.workflow.WorkflowScriptRequest;
import com.tapdata.tm.commons.workflow.WorkflowScriptResult;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowScriptEngineTest {

    private final WorkflowScriptEngine engine = new WorkflowScriptEngine();

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
}
