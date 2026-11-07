package io.tapdata.observable.metric;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.metrics.MetricCons;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Dexter
 */
public class TaskSampleRetriever {
	private static final int MAX_RETRIEVE_ATTEMPT = 3;
	private static final String QUERY_V2_RESOURCE = ConnectorConstant.SAMPLE_STATISTIC_COLLECTION + "/query/v2";
	private static final String TASK_COUNTER_RECOVERY_RESOURCE = QUERY_V2_RESOURCE + "?taskCounterRecovery=true";
	private static final Set<String> CORE_COUNTER_FIELDS = Set.of(
			MetricCons.SS.VS.F_INPUT_DDL_TOTAL,
			MetricCons.SS.VS.F_INPUT_INSERT_TOTAL,
			MetricCons.SS.VS.F_INPUT_UPDATE_TOTAL,
			MetricCons.SS.VS.F_INPUT_DELETE_TOTAL,
			MetricCons.SS.VS.F_INPUT_OTHERS_TOTAL,
			MetricCons.SS.VS.F_OUTPUT_DDL_TOTAL,
			MetricCons.SS.VS.F_OUTPUT_INSERT_TOTAL,
			MetricCons.SS.VS.F_OUTPUT_UPDATE_TOTAL,
			MetricCons.SS.VS.F_OUTPUT_DELETE_TOTAL,
			MetricCons.SS.VS.F_OUTPUT_OTHERS_TOTAL);
	private static final Set<String> LEGACY_NO_HISTORY_FIELDS = Set.of(
			MetricCons.SS.VS.F_CURR_EVENT_TS,
			MetricCons.SS.VS.F_SNAPSHOT_DONE_AT,
			MetricCons.SS.VS.F_SNAPSHOT_TABLE_TOTAL);
	private static final TaskSampleRetriever INSTANCE = new TaskSampleRetriever();
	private final Logger logger = LogManager.getLogger(TaskSampleRetriever.class);

	public static TaskSampleRetriever getInstance() {
		return INSTANCE;
	}

	private TaskSampleRetriever() {
	}

	private HttpClientMongoOperator operator;

	public void start(HttpClientMongoOperator operator) {
		if (this.operator == null) {
			this.operator = operator;
		}
	}

	public SampleResponse retrieveRaw(long startTime, Map<String, String> tags, List<String> fields) {
		if (operator == null) {
			throw new RuntimeException("TaskSampleRetrieverV2 should be call start() first.");
		}
		return operator.postOne(buildRequest(startTime, tags, fields), QUERY_V2_RESOURCE, SampleResponse.class);
	}

	public SampleResponse retrieveTaskCounterRecoveryRaw(long startTime, Map<String, String> tags, List<String> fields) {
		if (operator == null) {
			throw new RuntimeException("TaskSampleRetrieverV2 should be call start() first.");
		}
		return operator.postOne(buildRequest(startTime, tags, fields), TASK_COUNTER_RECOVERY_RESOURCE, SampleResponse.class);
	}

	private Map<String, Object> buildRequest(long startTime, Map<String, String> tags, List<String> fields) {
		Map<String, Object> sample = new HashMap<>();
		sample.put("tags", tags);
		sample.put("fields", fields);
		sample.put("type", "instant");
		Map<String, Object> request = new HashMap<>();
		request.put("startAt", startTime);
		request.put("endAt", System.currentTimeMillis());
		request.put("samples", Map.of("retriever", sample));
		return request;
	}

	public Map<String, Number> retrieve(long startTime, Map<String, String> tags, List<String> fields) {
		SampleResponse response = null;
		try {
			response = retrieveRaw(startTime, tags, fields);
		} catch (Exception e) {
			logger.warn("Failed to retrieve old metric data with tags {} and fields {} with error {}", tags, fields, e.getMessage());
		}

		Map<String, Number> samples = new HashMap<>();
		if (response != null && response.getSamples() != null) {
			for (Map<String, Object> item : response.getSamples().getRetriever()) {
				for (Map.Entry<String, Object> entry : item.entrySet()) {
					String key = entry.getKey();
					if (!key.equals("tags")) {
						if(entry.getValue() instanceof String && ((String) entry.getValue()).equals("NaN")){
							samples.put(key, 0.0);
						}else{
							samples.put(key, (Number) entry.getValue());
						}

					}
				}
			}
		}

		return samples;
	}

	public Map<String, Number> retrieveWithRetry(long startTime,Map<String, String> tags, List<String> fields) {
		int attempt = 1;
		Throwable error = null;
		while (attempt <= MAX_RETRIEVE_ATTEMPT) {
			try {
				return retrieve(startTime - 60000, tags, fields);
			} catch (Throwable throwable) {
				error = throwable;
			}
			attempt += 1;
		}

		String message = "Failed to retrieve old metric data with tags %s and fields %s after attempt for %s times " +
				"with error %s, the old metric data will be used as the initial value for the new metric data, if " +
				"error happens, new metric data won't be init or reported.";
		throw new RuntimeException(String.format(message, tags, fields, MAX_RETRIEVE_ATTEMPT, error.getMessage()), error);
	}

	public Map<String, Number> retrieveTaskCounterRecoveryWithRetry(long startTime,
			Map<String, String> tags, List<String> fields) {
		if (fields == null || !fields.containsAll(CORE_COUNTER_FIELDS)) {
			throw new IllegalArgumentException("Task counter recovery requires all core cumulative counter fields");
		}
		Throwable error = null;
		for (int attempt = 1; attempt <= MAX_RETRIEVE_ATTEMPT; attempt++) {
			try {
				SampleResponse response = retrieveTaskCounterRecoveryRaw(startTime - 60000L, tags, fields);
				return parseTaskCounterRecoveryResponse(response, tags);
			} catch (Exception e) {
				error = e;
				logger.warn("Failed task counter recovery query attempt {} of {} for tags {}: {}",
						attempt, MAX_RETRIEVE_ATTEMPT, tags, e.getMessage());
			}
		}
		String message = "Failed to retrieve task counter history for tags %s after %s attempts: %s";
		throw new RuntimeException(String.format(message, tags, MAX_RETRIEVE_ATTEMPT,
				error == null ? "unknown error" : error.getMessage()), error);
	}

	private Map<String, Number> parseTaskCounterRecoveryResponse(SampleResponse response,
			Map<String, String> expectedTags) {
		if (response == null || response.getSamples() == null || response.getSamples().getRetriever() == null) {
			throw new IllegalStateException("Task counter recovery response is missing its samples list");
		}
		Integer schemaVersion = response.getTaskCounterRecoverySchemaVersion();
		if (schemaVersion != null && schemaVersion != 1) {
			throw new IllegalStateException("Unsupported task counter recovery schema version " + schemaVersion);
		}
		if (response.getSamples().getRetriever().isEmpty()) {
			return new HashMap<>();
		}
		if (response.getSamples().getRetriever().size() != 1) {
			throw new IllegalStateException("Task counter recovery response contains ambiguous sample rows");
		}
		Map<String, Number> values = new HashMap<>();
		Map<String, Object> item = response.getSamples().getRetriever().get(0);
		if (item == null || !(item.get("tags") instanceof Map<?, ?> responseTags)) {
			throw new IllegalStateException("Task counter recovery response is missing task tags");
		}
		validateResponseTags(responseTags, expectedTags);
		Set<String> returnedFields = new HashSet<>(item.keySet());
		returnedFields.remove("tags");
		boolean hasCoreCounter = returnedFields.stream().anyMatch(CORE_COUNTER_FIELDS::contains);
		boolean acknowledged = schemaVersion != null;
		if (!acknowledged && !hasCoreCounter && isLegacyNoHistoryRow(returnedFields)) {
			parseOptionalValues(item, values);
			return values;
		}
		if (!returnedFields.containsAll(CORE_COUNTER_FIELDS)) {
			throw new IllegalStateException("Task counter recovery response is missing core cumulative counter values");
		}
		for (Map.Entry<String, Object> entry : item.entrySet()) {
			if ("tags".equals(entry.getKey())) {
				continue;
			}
			if (CORE_COUNTER_FIELDS.contains(entry.getKey())) {
				values.put(entry.getKey(), requireFiniteCoreCounter(entry.getKey(), entry.getValue()));
			} else {
				parseOptionalValue(entry.getKey(), entry.getValue(), values);
			}
		}
		return values;
	}

	private boolean isLegacyNoHistoryRow(Set<String> returnedFields) {
		return LEGACY_NO_HISTORY_FIELDS.containsAll(returnedFields);
	}

	private void validateResponseTags(Map<?, ?> responseTags, Map<String, String> expectedTags) {
		for (String key : List.of(MetricCons.Tags.F_TYPE, MetricCons.Tags.F_TASK_ID, MetricCons.Tags.F_TASK_RECORD_ID)) {
			if (!java.util.Objects.equals(expectedTags.get(key), responseTags.get(key))) {
				throw new IllegalStateException("Task counter recovery response tags do not match the requested task");
			}
		}
	}

	private Number requireFiniteCoreCounter(String field, Object value) {
		if (!(value instanceof Number)) {
			throw new IllegalStateException("Task counter recovery response contains an invalid core counter for " + field);
		}
		Number number = (Number) value;
		if (!Double.isFinite(number.doubleValue())) {
			throw new IllegalStateException("Task counter recovery response contains an invalid core counter for " + field);
		}
		return number;
	}

	private void parseOptionalValues(Map<String, Object> item, Map<String, Number> values) {
		for (String field : LEGACY_NO_HISTORY_FIELDS) {
			parseOptionalValue(field, item.get(field), values);
		}
	}

	private void parseOptionalValue(String field, Object value, Map<String, Number> values) {
		if (value == null) {
			return;
		}
		if (value instanceof String stringValue && "NaN".equals(stringValue)) {
			values.put(field, 0D);
		} else if (value instanceof Number number && Double.isFinite(number.doubleValue())) {
			values.put(field, number);
		}
	}

	@Data
	public static class SampleResponse {
		private Integer taskCounterRecoverySchemaVersion;
		private Samples samples;
	}

	@Data
	public static class Samples {
		private List<Map<String, Object>> retriever;
	}
}
