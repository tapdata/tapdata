package io.tapdata.metric.collector;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/1/2 10:49 Create
 */
public class ISyncMetricCollectorTest {

    @Nested
    class TestInit {
        TaskDto taskDto;
        DataProcessorContext dataProcessorContext;

        @BeforeEach
        void afterEach() {
            taskDto = Mockito.mock(TaskDto.class);
            dataProcessorContext = Mockito.mock(DataProcessorContext.class);
        }

        @Test
        void testMetricEnable() {
            Mockito.when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
            Mockito.when(taskDto.getEnableSyncMetricCollector()).thenReturn(true);
            Assertions.assertInstanceOf(SyncMetricCollector.class, ISyncMetricCollector.init(dataProcessorContext));
        }

        @Test
        void testMetricDisable() {
            // null context
            Assertions.assertInstanceOf(NoneSyncMetricCollector.class, ISyncMetricCollector.init(null));
            // null taskDto
            Assertions.assertInstanceOf(NoneSyncMetricCollector.class, ISyncMetricCollector.init(dataProcessorContext));
            // null config
            Mockito.when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
            Assertions.assertInstanceOf(NoneSyncMetricCollector.class, ISyncMetricCollector.init(dataProcessorContext));
            // disable config
            Mockito.when(taskDto.getEnableSyncMetricCollector()).thenReturn(false);
            Assertions.assertInstanceOf(NoneSyncMetricCollector.class, ISyncMetricCollector.init(dataProcessorContext));
        }
    }
}
