package io.tapdata.flow.engine.V2.script;

import com.tapdata.processor.WorkflowScriptEngine;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

@Component
public class WorkflowScriptHostRegistrar {

    @PostConstruct
    public void register() {
        WorkflowScriptEngine.setHostBindingsFactory(WorkflowScriptHostBindings::create);
    }
}
