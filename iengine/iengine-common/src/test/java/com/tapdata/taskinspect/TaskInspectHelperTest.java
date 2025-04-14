package com.tapdata.taskinspect;

import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.utils.UnitTestUtils;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.CALLS_REAL_METHODS;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/3 14:50 Create
 */
class TaskInspectHelperTest {

    @BeforeEach
    void setUp() {
        UnitTestUtils.injectField(TaskInspectHelper.class, null, "operator", null);
    }

    @Test
    void testLifecycle() {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(ObjectId.get());
        String taskId = taskDto.getId().toHexString();

        ClientMongoOperator mongoOperator = Mockito.mock(ClientMongoOperator.class);
        TaskInspectContext context = new TaskInspectContext(taskDto, mongoOperator);

        try (TaskInspect first = TaskInspectHelper.create(context)) {
            Assertions.assertEquals(10000, context.getReportInterval());
            Assertions.assertFalse(context.isStopping());
            Assertions.assertEquals(taskDto, context.getTask());
            Assertions.assertEquals(taskId, context.getTaskId());
            Assertions.assertEquals(mongoOperator, context.getClientMongoOperator());
            Assertions.assertEquals(first, TaskInspectHelper.get(taskId));

            try (TaskInspect second = TaskInspectHelper.create(context)) {
                Assertions.assertNotEquals(first, second);
                Assertions.assertEquals(second, TaskInspectHelper.get(taskId));
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }

        Assertions.assertNull(TaskInspectHelper.get(taskId));
    }

    @Test
    void testGetOperator() {
        try (MockedStatic<TaskInspectHelper> helper = Mockito.mockStatic(TaskInspectHelper.class, CALLS_REAL_METHODS)) {
            IOperator mockOperator = Mockito.mock(IOperator.class);
            helper.when(() -> TaskInspectHelper.insForName(Mockito.anyString())).thenReturn(mockOperator);
            Assertions.assertEquals(mockOperator, TaskInspectHelper.getOperator());
        }
    }

    @Test
    void testInsForName() {
        try {
            Object o = TaskInspectHelper.insForName(String.class.getName());
            Assertions.assertInstanceOf(String.class, o);
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    void testCloseWithExistsException() throws Exception {
        String taskId = "test-task-id";
        TaskInspect taskInspect = Mockito.mock(TaskInspect.class);
        try (MockedStatic<TaskInspectHelper> mockStatic = Mockito.mockStatic(TaskInspectHelper.class, CALLS_REAL_METHODS)) {
            Mockito.doThrow(new Exception("test")).when(taskInspect).close();
            mockStatic.when(()-> TaskInspectHelper.remove(Mockito.eq(taskId))).thenReturn(taskInspect);
            TaskInspectHelper.closeWithExists(taskId);
        }
    }
}
