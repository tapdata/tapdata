package com.tapdata.taskinspect;

import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.taskinspect.TaskInspectConfig;
import com.tapdata.tm.taskinspect.TaskInspectMode;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/14 15:01 Create
 */
class TaskInspectTest {
    TaskDto taskDto;
    String taskId;
    ClientMongoOperator mongoOperator;
    TaskInspectContext context;

    @BeforeEach
    void setUp() {
        taskDto = new TaskDto();
        taskDto.setId(ObjectId.get());
        taskId = taskDto.getId().toHexString();
        mongoOperator = Mockito.mock(ClientMongoOperator.class);
        context = new TaskInspectContext(taskDto, mongoOperator);
    }

    @Nested
    class InitTest {
        @Test
        void testInit() throws Exception {
            TaskInspectConfig config = new TaskInspectConfig();
            config.setMode(TaskInspectMode.CLOSE);

            IOperator spy = Mockito.mock(IOperator.class);
            Mockito.doReturn(config).when(spy).getConfig(Mockito.eq(taskId));
            try (TaskInspect ins = new TaskInspect(context, spy)) {

            }
        }
    }
}
