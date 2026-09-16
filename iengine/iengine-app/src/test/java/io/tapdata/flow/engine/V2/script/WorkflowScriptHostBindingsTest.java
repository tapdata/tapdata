package io.tapdata.flow.engine.V2.script;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.workflow.WorkflowScriptRequest;
import io.tapdata.entity.logger.Log;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngine;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class WorkflowScriptHostBindingsTest {

    @Test
    void installPutsScriptExecutorsManager() {
        ClientMongoOperator operator = mock(ClientMongoOperator.class);
        HazelcastInstance hazelcast = mock(HazelcastInstance.class);
        Log log = mock(Log.class);
        ScriptEngine engine = mock(ScriptEngine.class);
        WorkflowScriptRequest request = new WorkflowScriptRequest();
        request.setRunId("run-1");
        request.setStepId("js");
        WorkflowScriptHostBindings bindings = new WorkflowScriptHostBindings(operator, hazelcast, log, "run-1", "js");

        bindings.install(engine, request);

        verify(engine).put(eq("ScriptExecutorsManager"), org.mockito.ArgumentMatchers.any(ScriptExecutorsManager.class));
        bindings.close();
    }
}
