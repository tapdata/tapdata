package io.tapdata.observable.metric.handler;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.observable.metric.TaskSampleRetriever;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskSampleHandlerTest {
    private final TaskSampleRetriever taskSampleRetriever = TaskSampleRetriever.getInstance();
    private Object previousOperator;
    private boolean operatorReplaced;

    @AfterEach
    void restoreRetriever() {
        if (operatorReplaced) {
            ReflectionTestUtils.setField(taskSampleRetriever, "operator", previousOperator);
        }
    }

    @Test
    void addsStableEngineAndPhysicalNodeTagsWithoutChangingTaskIdentity() {
        Map<String, String> baseTags = Map.of(
                "type", "task",
                "taskId", "task-1",
                "taskRecordId", "record-1");

        Map<String, String> tags = TaskSampleHandler.createTaskTags(baseTags, " engine-a ", " node-a ");

        assertEquals("task", tags.get("type"));
        assertEquals("task-1", tags.get("taskId"));
        assertEquals("record-1", tags.get("taskRecordId"));
        assertEquals("engine-a", tags.get("engineId"));
        assertEquals("node-a", tags.get("physicalNodeId"));
        assertFalse(baseTags.containsKey("physicalNodeId"));
    }

    @Test
    void omitsMissingAttributionTagsForLegacyEngines() {
        Map<String, String> baseTags = Map.of("type", "task", "taskId", "task-1", "taskRecordId", "record-1");

        Map<String, String> tags = TaskSampleHandler.createTaskTags(baseTags, " ", null);

        assertEquals(baseTags, tags);
    }

    @Test
    void taskHandlerUsesRecoveryUrlWithoutAddingIdentityToPersistedTags() {
        previousOperator = ReflectionTestUtils.getField(taskSampleRetriever, "operator");
        operatorReplaced = true;
        HttpClientMongoOperator operator = mock(HttpClientMongoOperator.class);
        ReflectionTestUtils.setField(taskSampleRetriever, "operator", operator);
        TaskSampleRetriever.Samples emptySamples = new TaskSampleRetriever.Samples();
        emptySamples.setRetriever(new ArrayList<>());
        TaskSampleRetriever.SampleResponse response = new TaskSampleRetriever.SampleResponse();
        response.setSamples(emptySamples);
        String recoveryResource = ConnectorConstant.SAMPLE_STATISTIC_COLLECTION + "/query/v2?taskCounterRecovery=true";
        when(operator.postOne(anyMap(), eq(recoveryResource),
                eq(TaskSampleRetriever.SampleResponse.class))).thenReturn(response);

        TaskDto task = mock(TaskDto.class);
        when(task.getId()).thenReturn(new ObjectId("0123456789abcdef01234567"));
        when(task.getTaskRecordId()).thenReturn("record-1");
        when(task.getStartTime()).thenReturn(new Date(60_000L));
        TaskSampleHandler handler = new TaskSampleHandler(task);

        assertEquals("task", handler.tags().get("type"));
        assertEquals("record-1", handler.recoveryTags().get("taskRecordId"));
        assertFalse(handler.recoveryTags().containsKey("physicalNodeId"));
        assertTrue(handler.retrieve().isEmpty());

        verify(operator).postOne(anyMap(), eq(recoveryResource),
                eq(TaskSampleRetriever.SampleResponse.class));
    }
}
