package io.tapdata.observable.metric;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.RestTemplateOperator;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Dexter
 */
public class TaskSampleRetriever {
	private static final int MAX_RETRIEVE_ATTEMPT = 3;
	private static final TaskSampleRetriever INSTANCE = new TaskSampleRetriever();

	public static TaskSampleRetriever getInstance() {
		return INSTANCE;
	}

	private TaskSampleRetriever() {
	}

	private RestTemplateOperator operator;

	public void start(RestTemplateOperator operator) {
		if (this.operator == null) {
			this.operator = operator;
		}
	}

	public SampleResponse retrieveRaw(long startTime, Map<String, String> tags, List<String> fields) {
		if (operator == null) {
			throw new RuntimeException("TaskSampleRetrieverV2 should be call start() first.");
		}

		Map<String, Object> request = new HashMap<>();
		long now = System.currentTimeMillis();
		request.put("startAt", startTime);
		request.put("endAt", now);
		request.put("samples", new HashMap<String, Map<String, Object>>() {{
			put("retriever", new HashMap<String, Object>() {{
				put("tags", tags);
				put("fields", fields);
				put("type", "instant");
			}});
		}});

		return operator.postOne(request, ConnectorConstant.SAMPLE_STATISTIC_COLLECTION + "/query/v2", SampleResponse.class);
	}

	public Map<String, Number> retrieve(long startTime, Map<String, String> tags, List<String> fields) {
		SampleResponse response = retrieveRaw(startTime, tags, fields);

		Map<String, Number> samples = new HashMap<>();
		if (response != null && response.getSamples() != null) {
			for (Map<String, Object> item : response.getSamples().getRetriever()) {
				for (Map.Entry<String, Object> entry : item.entrySet()) {
					String key = entry.getKey();
					if (!key.equals("tags")) {
						samples.put(key, (Number) entry.getValue());
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
				return retrieve(startTime, tags, fields);
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

	@Data
	public static class SampleResponse {
		private Samples samples;
	}

	@Data
	public static class Samples {
		private List<Map<String, Object>> retriever;
	}
}

