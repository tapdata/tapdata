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

	public SampleResponse retrieveRaw(Map<String, String> tags, List<String> fields) {
		if (operator == null) {
			throw new RuntimeException("TaskSampleRetrieverV2 should be call start() first.");
		}

		Map<String, Object> request = new HashMap<>();
		long now = System.currentTimeMillis();
		request.put("startAt", now);
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

	public Map<String, Number> retrieve(Map<String, String> tags, List<String> fields) {
		SampleResponse response = retrieveRaw(tags, fields);

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

	@Data
	public static class SampleResponse {
		private Samples samples;
	}

	@Data
	public static class Samples {
		private List<Map<String, Object>> retriever;
	}
}

