package io.tapdata.task;

import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.impl.TaskSessionClassHolder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.*;

public class TaskSessionClassHolderTest {

    @Test
    void testEnsureTaskSessionStopped(){

        TaskSessionClassHolder taskSessionClassHolder = spy(new TaskSessionClassHolder());
        TaskStopAspect taskStopAspect = new TaskStopAspect();
        Map aspectTaskMap = spy(ConcurrentHashMap.class);
        aspectTaskMap.put("66618104c016c632a6612322",taskStopAspect);
        String taskId = "66618104c016c632a6612325";
        ReflectionTestUtils.setField(taskSessionClassHolder,"aspectTaskMap",aspectTaskMap);
        taskSessionClassHolder.ensureTaskSessionStopped(taskId,taskStopAspect);
        Assertions.assertTrue(aspectTaskMap.size()==1);

    }
}
