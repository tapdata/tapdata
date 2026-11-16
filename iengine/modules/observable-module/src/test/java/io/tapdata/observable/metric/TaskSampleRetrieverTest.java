package io.tapdata.observable.metric;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.metrics.MetricCons;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.ArgumentCaptor;

class TaskSampleRetrieverTest {
    private static final String RECOVERY_RESOURCE = ConnectorConstant.SAMPLE_STATISTIC_COLLECTION
            + "/query/v2?taskCounterRecovery=true";

    private final TaskSampleRetriever retriever = TaskSampleRetriever.getInstance();
    private HttpClientMongoOperator operator;
    private Object previousOperator;

    @BeforeEach
    void setUp() {
        previousOperator = ReflectionTestUtils.getField(retriever, "operator");
        operator = mock(HttpClientMongoOperator.class);
        ReflectionTestUtils.setField(retriever, "operator", operator);
    }

    @AfterEach
    void restoreOperator() {
        ReflectionTestUtils.setField(retriever, "operator", previousOperator);
    }

    @Test
    void taskRecoveryAddsOnlyTheUrlParameterAndPreservesBodyTags() {
        Map<String, String> tags = Map.of("type", "task", "taskId", "task-1", "taskRecordId", "record-1");
		List<String> fields = coreCounterFields();
        when(operator.postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class)))
				.thenReturn(response(List.of(counterValues(tags, 200L)), 1));

        Map<String, Number> result = retriever.retrieveTaskCounterRecoveryWithRetry(120_000L, tags, fields);

        assertEquals(200L, result.get("inputInsertTotal").longValue());
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> body = ArgumentCaptor.forClass(Map.class);
        verify(operator).postOne(body.capture(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class));
        assertEquals(60_000L, ((Number) body.getValue().get("startAt")).longValue());
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> samples = (Map<String, Map<String, Object>>) body.getValue().get("samples");
        assertEquals(tags, samples.get("retriever").get("tags"));
        assertEquals(fields, samples.get("retriever").get("fields"));
        assertEquals("instant", samples.get("retriever").get("type"));
    }

    @Test
    void ordinaryInstantQueryKeepsTheOriginalUrlAndTags() {
        Map<String, String> tags = Map.of("type", "task", "taskId", "task-1", "taskRecordId", "record-1");
        List<String> fields = List.of("inputInsertTotal");
        when(operator.postOne(anyMap(), eq(ConnectorConstant.SAMPLE_STATISTIC_COLLECTION + "/query/v2"),
                eq(TaskSampleRetriever.SampleResponse.class))).thenReturn(response(List.of()));

        retriever.retrieveRaw(60_000L, tags, fields);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> body = ArgumentCaptor.forClass(Map.class);
        verify(operator).postOne(body.capture(), eq(ConnectorConstant.SAMPLE_STATISTIC_COLLECTION + "/query/v2"),
                eq(TaskSampleRetriever.SampleResponse.class));
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> samples = (Map<String, Map<String, Object>>) body.getValue().get("samples");
        assertEquals(tags, samples.get("retriever").get("tags"));
    }

    @Test
    void emptyRecoveryResultIsValidAndHttpFailureIsRetriedThenSurfaced() {
        Map<String, String> tags = Map.of("type", "task", "taskId", "task-1", "taskRecordId", "record-1");
        when(operator.postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class)))
				.thenReturn(response(List.of(), 1));

        assertTrue(retriever.retrieveTaskCounterRecoveryWithRetry(
				60_000L, tags, coreCounterFields()).isEmpty());

        when(operator.postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class)))
                .thenThrow(new IllegalStateException("network failure"));
        assertThrows(RuntimeException.class, () -> retriever.retrieveTaskCounterRecoveryWithRetry(
				60_000L, tags, coreCounterFields()));
        verify(operator, times(4)).postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class));
    }

    @Test
    void malformedRecoveryResponseIsRetriedInsteadOfTreatedAsEmptyHistory() {
        Map<String, String> tags = Map.of("type", "task", "taskId", "task-1", "taskRecordId", "record-1");
        when(operator.postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class)))
                .thenReturn(new TaskSampleRetriever.SampleResponse());

        assertThrows(RuntimeException.class, () -> retriever.retrieveTaskCounterRecoveryWithRetry(
				60_000L, tags, coreCounterFields()));
        verify(operator, times(3)).postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class));
    }

    @Test
    void nullNanAndNonFiniteCounterValuesAreRetriedInsteadOfResetToZero() {
        Map<String, String> tags = Map.of("type", "task", "taskId", "task-1", "taskRecordId", "record-1");
        for (Object invalidValue : List.<Object>of("NaN", Double.NaN, Double.POSITIVE_INFINITY, "not-a-number")) {
			Map<String, Object> item = counterValues(tags, 200L);
			item.put(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL, invalidValue);
            when(operator.postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class)))
                    .thenReturn(response(List.of(item)));

            assertThrows(RuntimeException.class, () -> retriever.retrieveTaskCounterRecoveryWithRetry(
					60_000L, tags, coreCounterFields()));

            verify(operator, times(3)).postOne(anyMap(), eq(RECOVERY_RESOURCE),
                    eq(TaskSampleRetriever.SampleResponse.class));
            clearInvocations(operator);
        }
		Map<String, Object> nullValueItem = counterValues(tags, 200L);
		nullValueItem.put(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL, null);
        when(operator.postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class)))
                .thenReturn(response(List.of(nullValueItem)));

        assertThrows(RuntimeException.class, () -> retriever.retrieveTaskCounterRecoveryWithRetry(
				60_000L, tags, coreCounterFields()));
        verify(operator, times(3)).postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class));
    }

	@Test
	void optionalNullMetricsAreIgnoredWhileCoreCountersAreRequired() {
		Map<String, String> tags = Map.of("type", "task", "taskId", "task-1", "taskRecordId", "record-1");
		Map<String, Object> item = counterValues(tags, 200L);
		item.put(MetricCons.SS.VS.F_SNAPSHOT_START_AT, null);
		item.put(MetricCons.SS.VS.F_INPUT_QPS, null);
		item.put(MetricCons.SS.VS.F_OUTPUT_QPS, "NaN");
		when(operator.postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class)))
				.thenReturn(response(List.of(item)));

		Map<String, Number> result = retriever.retrieveTaskCounterRecoveryWithRetry(
				60_000L, tags, coreCounterFields());

		assertEquals(200L, result.get(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL).longValue());
		assertFalse(result.containsKey(MetricCons.SS.VS.F_SNAPSHOT_START_AT));
		assertFalse(result.containsKey(MetricCons.SS.VS.F_INPUT_QPS));
		assertEquals(0D, result.get(MetricCons.SS.VS.F_OUTPUT_QPS));
	}

	@Test
	void legacyNoHistorySyntheticRowPreservesOptionalSnapshotState() {
		Map<String, String> tags = Map.of("type", "task", "taskId", "task-1", "taskRecordId", "record-1");
		Map<String, Object> item = new HashMap<>();
		item.put("tags", tags);
		item.put(MetricCons.SS.VS.F_CURR_EVENT_TS, 456L);
		item.put(MetricCons.SS.VS.F_SNAPSHOT_DONE_AT, 123L);
		item.put(MetricCons.SS.VS.F_SNAPSHOT_TABLE_TOTAL, 8L);
		when(operator.postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class)))
				.thenReturn(response(List.of(item)));

		Map<String, Number> result = retriever.retrieveTaskCounterRecoveryWithRetry(
				60_000L, tags, coreCounterFields());

		assertEquals(456L, result.get(MetricCons.SS.VS.F_CURR_EVENT_TS).longValue());
		assertEquals(123L, result.get(MetricCons.SS.VS.F_SNAPSHOT_DONE_AT).longValue());
		assertEquals(8L, result.get(MetricCons.SS.VS.F_SNAPSHOT_TABLE_TOTAL).longValue());
		assertFalse(result.containsKey(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL));
	}

	@Test
	void partialCoreCounterRowIsNotTreatedAsLegacyNoHistory() {
		Map<String, String> tags = Map.of("type", "task", "taskId", "task-1", "taskRecordId", "record-1");
		Map<String, Object> item = new HashMap<>();
		item.put("tags", tags);
		item.put(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL, 12L);
		when(operator.postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class)))
				.thenReturn(response(List.of(item)));

		assertThrows(RuntimeException.class, () -> retriever.retrieveTaskCounterRecoveryWithRetry(
				60_000L, tags, coreCounterFields()));
		verify(operator, times(3)).postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class));
	}

    @Test
    void acknowledgedRecoveryRequiresCoreCountersAndRejectsUnknownSchemaVersions() {
        Map<String, String> tags = Map.of("type", "task", "taskId", "task-1", "taskRecordId", "record-1");
        Map<String, Object> partial = new HashMap<>();
        partial.put("tags", tags);
        partial.put(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL, 12L);
        when(operator.postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class)))
                .thenReturn(response(List.of(partial), 1));

        assertThrows(RuntimeException.class, () -> retriever.retrieveTaskCounterRecoveryWithRetry(
                60_000L, tags, coreCounterFields()));
        verify(operator, times(3)).postOne(anyMap(), eq(RECOVERY_RESOURCE),
                eq(TaskSampleRetriever.SampleResponse.class));

        clearInvocations(operator);
        when(operator.postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class)))
                .thenReturn(response(List.of(counterValues(tags, 12L)), 2));

        assertThrows(RuntimeException.class, () -> retriever.retrieveTaskCounterRecoveryWithRetry(
                60_000L, tags, coreCounterFields()));
        verify(operator, times(3)).postOne(anyMap(), eq(RECOVERY_RESOURCE),
                eq(TaskSampleRetriever.SampleResponse.class));
    }

    @Test
    void acknowledgedSyntheticNoHistoryRowIsRejected() {
        Map<String, String> tags = Map.of("type", "task", "taskId", "task-1", "taskRecordId", "record-1");
        Map<String, Object> synthetic = new HashMap<>();
        synthetic.put("tags", tags);
        synthetic.put(MetricCons.SS.VS.F_SNAPSHOT_DONE_AT, 123L);
        when(operator.postOne(anyMap(), eq(RECOVERY_RESOURCE), eq(TaskSampleRetriever.SampleResponse.class)))
                .thenReturn(response(List.of(synthetic), 1));

        assertThrows(RuntimeException.class, () -> retriever.retrieveTaskCounterRecoveryWithRetry(
                60_000L, tags, coreCounterFields()));
        verify(operator, times(3)).postOne(anyMap(), eq(RECOVERY_RESOURCE),
                eq(TaskSampleRetriever.SampleResponse.class));
    }

	private List<String> coreCounterFields() {
		return List.of(MetricCons.SS.VS.F_INPUT_DDL_TOTAL, MetricCons.SS.VS.F_INPUT_INSERT_TOTAL,
				MetricCons.SS.VS.F_INPUT_UPDATE_TOTAL, MetricCons.SS.VS.F_INPUT_DELETE_TOTAL,
				MetricCons.SS.VS.F_INPUT_OTHERS_TOTAL, MetricCons.SS.VS.F_OUTPUT_DDL_TOTAL,
				MetricCons.SS.VS.F_OUTPUT_INSERT_TOTAL, MetricCons.SS.VS.F_OUTPUT_UPDATE_TOTAL,
				MetricCons.SS.VS.F_OUTPUT_DELETE_TOTAL, MetricCons.SS.VS.F_OUTPUT_OTHERS_TOTAL);
	}

	private Map<String, Object> counterValues(Map<String, String> tags, long inputInsertTotal) {
		Map<String, Object> values = new HashMap<>();
		values.put("tags", tags);
		for (String field : coreCounterFields()) {
			values.put(field, 0L);
		}
		values.put(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL, inputInsertTotal);
		return values;
	}

    private TaskSampleRetriever.SampleResponse response(List<Map<String, Object>> items) {
        return response(items, null);
    }

    private TaskSampleRetriever.SampleResponse response(List<Map<String, Object>> items, Integer schemaVersion) {
        TaskSampleRetriever.Samples samples = new TaskSampleRetriever.Samples();
        samples.setRetriever(items);
        TaskSampleRetriever.SampleResponse response = new TaskSampleRetriever.SampleResponse();
        response.setSamples(samples);
        response.setTaskCounterRecoverySchemaVersion(schemaVersion);
        return response;
    }
}
