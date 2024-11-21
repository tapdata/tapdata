package io.tapdata.observable.logging;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.util.CollectionUtils;

import java.util.Collections;

import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/30 11:43
 */
public class TaskLoggerNodeProxyTest {

    @Test
    public void testBaseBuilder() {

        TaskLogger taskLogger = mock(TaskLogger.class);
        when(taskLogger.getTaskId()).thenReturn("taskId");
        when(taskLogger.getTaskName()).thenReturn("taskName");
        when(taskLogger.getTaskRecordId()).thenReturn("taskRecordId");

        TaskLoggerNodeProxy logProxy = new TaskLoggerNodeProxy()
                .withTaskLogger(taskLogger)
                .withNode("NodeId", "NodeName")
                .withTags(Collections.singletonList("tag1"));

        MonitoringLogsDto dto = logProxy.logBaseBuilder().build();

        Assertions.assertNotNull(dto);
        Assertions.assertEquals("taskId", dto.getTaskId());
        Assertions.assertNotNull(dto.getLogTags());
        Assertions.assertEquals(1, dto.getLogTags().size());

        logProxy = new TaskLoggerNodeProxy()
                .withTaskLogger(taskLogger)
                .withNode("NodeId", "NodeName");

        dto = logProxy.logBaseBuilder().build();

        Assertions.assertNotNull(dto);
        Assertions.assertEquals("taskId", dto.getTaskId());
        Assertions.assertTrue(CollectionUtils.isEmpty(dto.getLogTags()));
    }

    @Test
    void testTrace() {
        TaskLogger taskLogger = mock(TaskLogger.class);
        TaskLoggerNodeProxy logProxy = new TaskLoggerNodeProxy()
                .withTaskLogger(taskLogger)
                .withNode("NodeId", "NodeName")
                .withTags(Collections.singletonList("tag1"));

        logProxy.trace(MonitoringLogsDto::builder, "test {}", 1);

        verify(taskLogger, times(1)).trace(any(), anyString(), any());
    }
}
