package io.tapdata.metrics;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.sample.BulkReporter;
import io.tapdata.common.sample.request.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * @author Dexter
 */
public class TaskSampleReporter implements BulkReporter {
	private final Logger logger = LogManager.getLogger(TaskSampleReporter.class);

	private final ClientMongoOperator operator;

	public TaskSampleReporter(ClientMongoOperator operator) {
		this.operator = operator;
	}

	@Override
	public void execute(BulkRequest bulkRequest) {
		handleSumOverNode(bulkRequest);
		report(bulkRequest);
	}

	private void handleSumOverNode(BulkRequest bulkRequest) {
		// handle samples
		Map<String, Map<String, List<Number>>> taskSamples = new HashMap<>();
		Map<String, Map<String, String>> taskSampleTags = new HashMap<>();
		Map<String, Date> taskSampleDate = new HashMap<>();
		for (SampleRequest sample : bulkRequest.getSamples()) {
			String subTaskId = sample.getTags().getOrDefault("subTaskId", null);
			if (subTaskId == null) {
				continue;
			}

			Map<String, List<Number>> samplesMap = taskSamples.computeIfAbsent(subTaskId, s -> new HashMap<>());
			if (!taskSampleTags.containsKey(subTaskId)) {
				Map<String, String> tags = new HashMap<>(sample.getTags());
				tags.remove("nodeId");
				tags.put("type", "subTask");
				taskSampleTags.put(subTaskId, tags);
			}

			Date date = taskSampleDate.getOrDefault(subTaskId, null);
			if (date == null || date.before(sample.getSample().getDate())) {
				taskSampleDate.put(subTaskId, sample.getSample().getDate());
			}
			for (Map.Entry<String, Number> entry : sample.getSample().getVs().entrySet()) {
				List<Number> samplesListByKey = samplesMap.computeIfAbsent(entry.getKey(), s -> new ArrayList<>());
				samplesListByKey.add(entry.getValue());
			}
		}

		for (String subTaskId : taskSamples.keySet()) {
			SampleRequest sampleRequest = new SampleRequest();
			sampleRequest.setTags(taskSampleTags.get(subTaskId));
			Sample sample = new Sample();
			sample.setDate(taskSampleDate.get(subTaskId));
			Map<String, Number> sampleValues = new HashMap<>();
			for (Map.Entry<String, List<Number>> entry : taskSamples.get(subTaskId).entrySet()) {
				Number value = calculateByName(entry.getKey(), entry.getValue());
				if (value != null) {
					sampleValues.put(entry.getKey(), value);
				}
			}
			sample.setVs(sampleValues);
			sampleRequest.setSample(sample);
			bulkRequest.addSampleRequest(sampleRequest);
		}

		// handle statistics
		Map<String, Map<String, List<Number>>> taskStatistics = new HashMap<>();
		Map<String, Map<String, String>> taskStatisticTags = new HashMap<>();
		Map<String, Date> taskStaticticDate = new HashMap<>();
		Map<String, Boolean> taskStatisticShouldInc = new HashMap<>();
		for (StatisticRequest statistic : bulkRequest.getStatistics()) {
			String subTaskId = statistic.getTags().get("subTaskId");
			if (subTaskId == null) {
				continue;
			}
			List<String> incFields = statistic.getStatistic().getIncFields();
			Map<String, List<Number>> statisticsMap = taskStatistics.computeIfAbsent(subTaskId, s -> new HashMap<>());
			if (!taskStatisticTags.containsKey(subTaskId)) {
				Map<String, String> tags = new HashMap<>(statistic.getTags());
				tags.remove("nodeId");
				tags.put("type", "subTask");
				taskStatisticTags.put(subTaskId, tags);
			}

			Date date = taskStaticticDate.getOrDefault(subTaskId, null);
			if (date == null || date.before(statistic.getStatistic().getDate())) {
				taskSampleDate.put(subTaskId, statistic.getStatistic().getDate());
			}

			for (Map.Entry<String, Number> entry : statistic.getStatistic().getValues().entrySet()) {
				if (incFields != null && incFields.contains(entry.getKey())) {
					taskStatisticShouldInc.putIfAbsent(entry.getKey(), true);
				}
				List<Number> statisticsListByKey = statisticsMap.computeIfAbsent(entry.getKey(), s -> new ArrayList<>());
				statisticsListByKey.add(entry.getValue());
			}
		}

		for (String subTaskId : taskStatistics.keySet()) {
			StatisticRequest statisticRequest = new StatisticRequest();
			statisticRequest.setTags(taskStatisticTags.get(subTaskId));
			Statistic statistic = new Statistic();
			statistic.setDate(taskSampleDate.get(subTaskId));
			Map<String, Number> statisticValues = new HashMap<>();
			List<String> incFields = new ArrayList<>();
			for (Map.Entry<String, List<Number>> entry : taskStatistics.get(subTaskId).entrySet()) {
				Number value = calculateByName(entry.getKey(), entry.getValue());
				if (value != null) {
					statisticValues.put(entry.getKey(), calculateByName(entry.getKey(), entry.getValue()));
					if (taskStatisticShouldInc.getOrDefault(entry.getKey(), false)) {
						incFields.add(entry.getKey());
					}
				}
			}
			statistic.setValues(statisticValues);
			statistic.setIncFields(incFields);
			statisticRequest.setStatistic(statistic);
			bulkRequest.addStatisticRequest(statisticRequest);
		}
	}

	private Number calculateByName(String name, List<Number> values) {
		switch (name) {
			case "inputTotal":
			case "outputTotal":
			case "insertedTotal":
			case "updatedTotal":
			case "deletedTotal":
				return values.stream().mapToLong(Number::longValue).sum();
			case "inputQPS":
			case "outputQPS":
				return values.stream().mapToDouble(Number::longValue).sum();
			case "timeCostAvg":
				double total = values.stream().mapToDouble(Number::longValue).sum();
				return total / values.size();
			case "initialTime":
			case "cdcTime":
				OptionalDouble valueMin = values.stream().mapToDouble(Number::longValue).min();
				return !valueMin.isPresent() ? 0 : valueMin.getAsDouble();
			case "replicateLag":
			case "initialWrite":
			case "initialTotal":
				OptionalDouble valueMax = values.stream().mapToDouble(Number::longValue).max();
				return !valueMax.isPresent() ? 0 : valueMax.getAsDouble();
			default:
				return null;
		}
	}

	private void report(BulkRequest bulkRequest) {
		if (bulkRequest == null) {
			logger.warn("The bulk request is null, checkout if the data si not set.");
			return;
		}
		if (bulkRequest.getSamples().isEmpty() && bulkRequest.getStatistics().isEmpty()) {
			logger.info("The bulk request is empty, skip report process.");
			return;
		}

		try {
			operator.insertOne(bulkRequest, ConnectorConstant.SAMPLE_STATISTIC_COLLECTION + "/points");
		} catch (Exception e) {
			logger.warn("Failed to report task samples and statistics, will retry...");
		}
	}


}
