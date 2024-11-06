package com.tapdata.tm.statemachine;

import com.tapdata.tm.statemachine.config.TaskStateMachineConfig;
import com.tapdata.tm.statemachine.configuration.StateMachineBuilder;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.TaskState;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class TaskStateMachineConfigTest {
    @Test
    void ConfigureTest() {
        TaskStateMachineConfig config = new TaskStateMachineConfig();
        StateMachineBuilder builder = spy(StateMachineBuilder.class);
        config.configure(builder);
        verify(builder, new Times(1)).transition(TaskState.RENEW_FAILED, TaskState.RENEW_FAILED, DataFlowEvent.CONFIRM);
    }
}
