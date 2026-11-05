package io.tapdata.flow.engine.V2.task.cleaner;

import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskResetEventDto;
import io.tapdata.observable.logging.appender.AppenderFactory;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskCleanerTest {

    @Test
    void shouldDeleteTaskCacheEvenWhenDagIsEmpty() throws TaskCleanerException {
        ClientMongoOperator operator = mock(ClientMongoOperator.class);
        TaskDto task = task();
        when(operator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(task);
        AppenderFactory appenderFactory = mock(AppenderFactory.class);

        try (MockedStatic<AppenderFactory> factory = mockStatic(AppenderFactory.class)) {
            factory.when(AppenderFactory::getInstance).thenReturn(appenderFactory);

            new TaskDeleteCleaner(new TaskCleanerContext(task.getId().toHexString(), operator)).clean();
        }

        verify(appenderFactory).deleteTaskCache(task.getId().toHexString());
        List<TaskResetEventDto> events = events(operator);
        assertEquals(TaskResetEventDto.ResetStatusEnum.TASK_SUCCEED,
                events.get(events.size() - 1).getStatus());
    }

    @Test
    void shouldNotFailTaskDeletionWhenCacheDeletionFails() throws TaskCleanerException {
        ClientMongoOperator operator = mock(ClientMongoOperator.class);
        TaskDto task = task();
        when(operator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(task);
        AppenderFactory appenderFactory = mock(AppenderFactory.class);
        doThrow(new IllegalStateException("delete failed"))
                .when(appenderFactory)
                .deleteTaskCache(task.getId().toHexString());

        try (MockedStatic<AppenderFactory> factory = mockStatic(AppenderFactory.class)) {
            factory.when(AppenderFactory::getInstance).thenReturn(appenderFactory);

            new TaskDeleteCleaner(
                    new TaskCleanerContext(task.getId().toHexString(), operator)).clean();
        }

        List<TaskResetEventDto> events = events(operator);
        assertEquals(TaskResetEventDto.ResetStatusEnum.TASK_SUCCEED,
                events.get(events.size() - 1).getStatus());
    }

    private TaskDto task() {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setName("task");
        task.setDag(null);
        return task;
    }

    private List<TaskResetEventDto> events(ClientMongoOperator operator) {
        ArgumentCaptor<TaskResetEventDto> captor = ArgumentCaptor.forClass(TaskResetEventDto.class);
        verify(operator, org.mockito.Mockito.atLeastOnce())
                .insertOne(captor.capture(), anyString());
        return captor.getAllValues();
    }
}
