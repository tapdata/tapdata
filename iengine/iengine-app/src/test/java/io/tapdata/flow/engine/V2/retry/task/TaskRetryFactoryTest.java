package io.tapdata.flow.engine.V2.retry.task;

import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.MockTaskUtil;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryContext;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryFactory;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TaskRetryFactoryTest {
    @Nested
    class TestGetTaskRetryService {
        private HazelcastBaseNode mockHazelcastBaseNode;
        private TaskRetryFactory taskRetryFactory=TaskRetryFactory.getInstance();
        private ProcessorBaseContext processorBaseContext;
        TaskDto taskDto;

        @BeforeEach
        void beforeSetUp() {
//            mockHazelcastBaseNode=spy(hazelcastBaseNode);
            processorBaseContext = mock(ProcessorBaseContext.class);
            taskDto=MockTaskUtil.setUpTaskDtoByJsonFile();
            when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
        }

        @DisplayName("Test Get RetryService When RetryTimeSecond is 900 and IntervalSecond is 60")
        @Test
        void test1() {
            TaskConfig taskConfig = new TaskConfig();
            when(processorBaseContext.getTaskConfig()).thenReturn(taskConfig);
            TaskRetryConfig taskRetryConfig = new TaskRetryConfig();
            taskRetryConfig.maxRetryTimeSecond(900L);
            taskRetryConfig.retryIntervalSecond(60L);
            taskConfig.taskRetryConfig(taskRetryConfig);
            TaskRetryService taskRetryService = taskRetryFactory.getTaskRetryService(processorBaseContext);
            TaskRetryContext retryContext = (TaskRetryContext) taskRetryService.getRetryContext();
            assertEquals(900000, retryContext.getRetryDurationMs());
            assertEquals(60000, retryContext.getRetryIntervalMs());
            assertEquals(15L, retryContext.getMethodRetryTime());
        }

        @DisplayName("Test Get RetryService When RetryTimeSecond is 900 and IntervalSecond is 60")
        @Test
        void test2() {
            taskDto.setId(new ObjectId());
            TaskConfig taskConfig = new TaskConfig();
            when(processorBaseContext.getTaskConfig()).thenReturn(taskConfig);
            TaskRetryConfig taskRetryConfig = new TaskRetryConfig();
            taskRetryConfig.maxRetryTimeSecond(0L);
            taskRetryConfig.retryIntervalSecond(0L);
            taskConfig.taskRetryConfig(taskRetryConfig);
            TaskRetryService taskRetryService = taskRetryFactory.getTaskRetryService(processorBaseContext);
            TaskRetryContext retryContext = (TaskRetryContext) taskRetryService.getRetryContext();
            assertEquals(0, retryContext.getRetryDurationMs());
            assertEquals(60000, retryContext.getRetryIntervalMs());
            assertEquals(15L, retryContext.getMethodRetryTime());
        }
    }
}
