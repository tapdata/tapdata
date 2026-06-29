package com.tapdata.tm.statemachine;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.statemachine.config.TaskStateMachineConfig;
import com.tapdata.tm.statemachine.configuration.StateMachineBuilder;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.TaskState;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.data.mongodb.core.query.Update;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    private Document setOperTimeUpdate(String status) throws Exception {
        TaskStateMachineConfig config = new TaskStateMachineConfig();
        Update update = new Update();
        Method m = TaskStateMachineConfig.class.getDeclaredMethod("setOperTime", String.class, Update.class);
        m.setAccessible(true);
        m.invoke(config, status, update);
        return update.getUpdateObject().get("$set", Document.class);
    }

    @Test
    void setOperTimeSeedsPingTimeForRunning() throws Exception {
        // A freshly-running task must carry a fresh pingTime so the overtime watchdog
        // (engineRestartNeedStartTask) cannot demote it on a stale pre-restart pingTime baseline.
        Document set = setOperTimeUpdate(TaskDto.STATUS_RUNNING);
        assertNotNull(set);
        assertTrue(set.containsKey("runningTime"));
        assertTrue(set.containsKey("pingTime"), "RUNNING transition must seed pingTime");
        assertTrue(set.get("pingTime") instanceof Long, "pingTime must be epoch-millis long to match the engine writer");
    }

    @Test
    void setOperTimeSeedsPingTimeForWaitRun() throws Exception {
        Document set = setOperTimeUpdate(TaskDto.STATUS_WAIT_RUN);
        assertNotNull(set);
        assertTrue(set.containsKey("scheduledTime"));
        assertTrue(set.containsKey("pingTime"), "WAIT_RUN transition must seed pingTime");
        assertTrue(set.get("pingTime") instanceof Long, "pingTime must be epoch-millis long to match the engine writer");
    }
}
