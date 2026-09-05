package io.tapdata.flow.engine.V2.script;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.Log4jScriptLogger;
import com.tapdata.processor.WorkflowScriptEngine;
import com.tapdata.tm.commons.workflow.WorkflowScriptRequest;
import io.tapdata.entity.logger.Log;
import org.apache.logging.log4j.LogManager;

import javax.script.ScriptEngine;

/**
 * 把任务 JS 同款 {@link ScriptExecutorsManager} 绑到 Workflow JS。
 */
public class WorkflowScriptHostBindings implements WorkflowScriptEngine.HostBindings {

    private final ClientMongoOperator clientMongoOperator;
    private final HazelcastInstance hazelcastInstance;
    private final Log scriptLogger;
    private final String runId;
    private final String stepId;
    private ScriptExecutorsManager manager;

    WorkflowScriptHostBindings(ClientMongoOperator clientMongoOperator,
                               HazelcastInstance hazelcastInstance,
                               Log scriptLogger,
                               String runId,
                               String stepId) {
        this.clientMongoOperator = clientMongoOperator;
        this.hazelcastInstance = hazelcastInstance;
        this.scriptLogger = scriptLogger;
        this.runId = runId;
        this.stepId = stepId;
    }

    static WorkflowScriptEngine.HostBindings create(WorkflowScriptRequest request) {
        ClientMongoOperator operator = BeanUtil.getBean(ClientMongoOperator.class);
        if (operator == null) {
            throw new IllegalStateException("Workflow JS ScriptExecutorsManager requires ClientMongoOperator");
        }
        HazelcastInstance hazelcast = HazelcastUtil.getInstance();
        Log log = new Log4jScriptLogger(LogManager.getLogger(WorkflowScriptEngine.class));
        String runId = request == null || request.getRunId() == null ? "workflow" : request.getRunId();
        String stepId = request == null || request.getStepId() == null ? "js" : request.getStepId();
        return new WorkflowScriptHostBindings(operator, hazelcast, log, runId, stepId);
    }

    @Override
    public void install(ScriptEngine engine, WorkflowScriptRequest request) {
        if (engine == null) {
            throw new IllegalArgumentException("script engine is required");
        }
        this.manager = new ScriptExecutorsManager(
                scriptLogger,
                clientMongoOperator,
                hazelcastInstance,
                runId,
                stepId,
                false);
        engine.put("ScriptExecutorsManager", manager);
    }

    @Override
    public void close() {
        if (manager != null) {
            manager.close();
            manager = null;
        }
    }
}
