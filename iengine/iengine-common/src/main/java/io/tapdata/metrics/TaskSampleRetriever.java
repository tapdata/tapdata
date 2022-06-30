package io.tapdata.metrics;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.RestTemplateOperator;

import java.util.*;

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

	public Map<String, Number> retrieve(Map<String, String> tags, List<String> fields) {
		if (operator == null) {
			throw new RuntimeException("TaskSampleRetriever should be call start() first.");
		}
		Map<String, Object> request = new HashMap<>();
		request.put("samples", new ArrayList<Map<String, Object>>() {{
			add(new HashMap<String, Object>() {{
				put("tags", tags);
				put("fields", fields);
				put("limit", 1);
			}});
		}});
		SampleResponse response = operator.postOne(request, ConnectorConstant.SAMPLE_STATISTIC_COLLECTION + "/query", SampleResponse.class);

		Map<String, Number> samples = new HashMap<>();
		if (response != null && response.samples != null) {
			for (Map<String, List<Number>> sample : response.samples) {
				for (Map.Entry<String, List<Number>> entry : sample.entrySet()) {
					if (entry.getValue() != null && entry.getValue().size() > 0) {
						samples.put(entry.getKey(), entry.getValue().get(0));
					}
				}
			}
		}

		return samples;
	}

	public static class SampleResponse {
		private List<Map<String, List<Number>>> statistics;
		private List<Map<String, List<Number>>> samples;

		public List<Map<String, List<Number>>> getStatistics() {
			return statistics;
		}

		public void setStatistics(List<Map<String, List<Number>>> statistics) {
			this.statistics = statistics;
		}

		public List<Map<String, List<Number>>> getSamples() {
			return samples;
		}

		public void setSamples(List<Map<String, List<Number>>> samples) {
			this.samples = samples;
		}
	}
}

