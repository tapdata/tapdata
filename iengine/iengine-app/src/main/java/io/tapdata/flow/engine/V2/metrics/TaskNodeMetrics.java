package io.tapdata.flow.engine.V2.metrics;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Stats;
import com.tapdata.entity.dataflow.RuntimeThroughput;
import com.tapdata.entity.dataflow.StageRuntimeStats;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.metrics.Metrics;
import com.tapdata.tm.commons.metrics.MetricsLabel;
import com.tapdata.tm.commons.metrics.TaskMetricsLabel;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-12-10 15:14
 **/
public class TaskNodeMetrics extends TaskMetrics {

	private NodeType nodeType;
	private Stats stats;
	private Node<?> node;
	private Double previousSubTaskTotalInput = 0d;
	private Double previousSubTaskTotalOutput = 0d;

	public TaskNodeMetrics(TaskDto taskDto, ClientMongoOperator clientMongoOperator, BaseMetrics.NodeType nodeType, Stats stats, Node<?> node, ConfigurationCenter configurationCenter,
						   HazelcastInstance hazelcastInstance) {
		super(clientMongoOperator, taskDto, configurationCenter, hazelcastInstance);
		checkInput(nodeType, stats, node);
		this.nodeType = nodeType;
		this.stats = stats;
		this.node = node;
	}

	@Override
	public void doStats() {
		try {
			switch (nodeType) {
				case SOURCE:
					subTaskNodeTotalOutput();
					subTaskTotalOutputQps();
//          insertTotalCount();
//          insertValidateStats();
					break;
				case TARGET:
					subTaskNodeTotalInput();
					subTaskNodeTotalInputQps();
					subTaskNodeTotalInsert();
					subTaskNodeTotalUpdate();
					subTaskNodeTotalDelete();
//          insertTotalStats();
//          insertStageRuntimeStats();
					break;
				default:
					break;
			}

			if (CollectionUtils.isNotEmpty(metricsList)) {
				insertMetrics();
			}
		} catch (Exception e) {
			errorLog(e);
		}
	}

	private void subTaskNodeTotalInput() {
		subTaskNodeTotal("processed", "total_input");
	}

	private void subTaskNodeTotalOutput() {
		subTaskNodeTotal("source_received", "total_output");
	}

	private void subTaskNodeTotalInsert() {
		subTaskNodeTotal("target_inserted", "total_insert");
	}

	private void subTaskNodeTotalUpdate() {
		subTaskNodeTotal("target_updated", "total_update");
	}

	private void subTaskNodeTotalDelete() {
		subTaskNodeTotal("total_deleted", "total_delete");
	}

	private void subTaskNodeTotal(String key, String metricsName) {
		Map<String, Long> total = stats.getTotal();
		if (total == null) {
			return;
		}
		Long value = total.getOrDefault(key, 0L);
		metricsName = SUB_TASK_NODE_METRICS_PREFIX + metricsName;
		MetricsLabel metricsLabel = new TaskMetricsLabel(taskDto.getId().toHexString(), node.getId());
		Metrics metrics = Metrics.counter(metricsName, Double.valueOf(value.toString()), null, metricsLabel);
		insertMetrics(metrics);
	}

	private void subTaskNodeTotalInputQps() {
		Map<String, Long> total = stats.getTotal();
		MetricsLabel metricsLabel = new TaskMetricsLabel(taskDto.getId().toHexString(), node.getId());
		String name = SUB_TASK_NODE_METRICS_PREFIX + "total_input_qps";
		double subTaskTotalInputQps = 0d;
		if (total != null) {
			Double processed = Double.parseDouble(total.getOrDefault("processed", 0L).toString());
			subTaskTotalInputQps = new BigDecimal(processed - previousSubTaskTotalInput).divide(new BigDecimal(METRICS_INTERVAL / 1000), RoundingMode.HALF_UP).doubleValue();
			previousSubTaskTotalInput = processed;
		}
		Metrics metrics = Metrics.gauge(name, subTaskTotalInputQps, null, metricsLabel);
		insertMetrics(metrics);
	}

	private void subTaskTotalOutputQps() {
		Map<String, Long> total = stats.getTotal();
		MetricsLabel metricsLabel = new TaskMetricsLabel(taskDto.getId().toHexString(), node.getId());
		String name = SUB_TASK_NODE_METRICS_PREFIX + "total_output_qps";
		Double subTaskTotalOutputQps = 0d;
		if (total != null) {
			Double sourceReceived = Double.parseDouble(total.getOrDefault("source_received", 0L).toString());
			subTaskTotalOutputQps = new BigDecimal(sourceReceived - previousSubTaskTotalOutput).divide(new BigDecimal(METRICS_INTERVAL / 1000), RoundingMode.HALF_UP).doubleValue();
			previousSubTaskTotalOutput = sourceReceived;
		}
		Metrics metrics = Metrics.gauge(name, subTaskTotalOutputQps, null, metricsLabel);
		insertMetrics(metrics);
	}

	private void insertTotalCount() {
		List<Map<String, Object>> totalCount = stats.getTotalCount();
		if (CollectionUtils.isEmpty(totalCount)) {
			return;
		}
		for (Map<String, Object> totalCountMap : totalCount) {
			if (MapUtils.isEmpty(totalCountMap)) {
				continue;
			}
			if (!totalCountMap.containsKey("stageId") || !totalCountMap.containsKey("dataCount")) {
				continue;
			}
			String stageId = (String) totalCountMap.get("stageId");
			Double dataCount = Double.valueOf(totalCountMap.get("dataCount").toString());
			String name = "totalCount-" + stageId;
			MetricsLabel metricsLabel = new TaskMetricsLabel(taskDto.getId().toHexString(), stageId);
			Metrics metrics = Metrics.counter(name, dataCount, "", metricsLabel);
			try {
				insertMetrics(metrics);
			} catch (Exception e) {
				throw new RuntimeException("Insert task total count failed; Total count: " + totalCountMap + "; " + e.getMessage(), e);
			}
		}
	}

	private void insertTotalStats() {
		Map<String, Long> total = stats.getTotal();
		try {
			insertMapStats(total, "total-");
		} catch (Exception e) {
			throw new RuntimeException("Insert task total stats failed; Total: " + total + "; " + e.getMessage(), e);
		}
	}

	private void insertStageRuntimeStats() {
		List<StageRuntimeStats> stageRuntimeStats = stats.getStageRuntimeStats();
		String namePrefix = "stage-runtime-stats-";
		for (StageRuntimeStats stageRuntimeStat : stageRuntimeStats) {
			String stageId = stageRuntimeStat.getStageId();

			RuntimeThroughput input = stageRuntimeStat.getInput();
			String name = namePrefix + "input";
			insertRuntimeThroughput(input, name, stageId);

			RuntimeThroughput output = stageRuntimeStat.getOutput();
			name = namePrefix + "output";
			insertRuntimeThroughput(output, name, stageId);

			RuntimeThroughput insert = stageRuntimeStat.getInsert();
			name = namePrefix + "insert";
			insertRuntimeThroughput(insert, name, stageId);

			RuntimeThroughput update = stageRuntimeStat.getUpdate();
			name = namePrefix + "update";
			insertRuntimeThroughput(update, name, stageId);

			RuntimeThroughput delete = stageRuntimeStat.getDelete();
			name = namePrefix + "delete";
			insertRuntimeThroughput(delete, name, stageId);

			long replicationLag = stageRuntimeStat.getReplicationLag();
			name = namePrefix + "replicationLag";
			try {
				insertLongValue(name, replicationLag, stageId);
			} catch (Exception e) {
				throw new RuntimeException("Insert node runtime replicationLag failed; " + e.getMessage(), e);
			}

			long transmissionTime = stageRuntimeStat.getTransmissionTime();
			name = namePrefix + "transmissionTime";
			try {
				insertLongValue(name, transmissionTime, stageId);
			} catch (Exception e) {
				throw new RuntimeException("Insert node runtime transmissionTime failed; " + e.getMessage(), e);
			}

			long transTimeAvg = stageRuntimeStat.getTransTimeAvg();
			name = namePrefix + "transTimeAvg";
			try {
				insertLongValue(name, transTimeAvg, stageId);
			} catch (Exception e) {
				throw new RuntimeException("Insert node runtime transTimeAvg failed; " + e.getMessage(), e);
			}
		}
	}

	private void insertValidateStats() {
		Map<String, Long> validateStats = stats.getValidate_stats();
		try {
			insertMapStats(validateStats, "validateStats-");
		} catch (Exception e) {
			throw new RuntimeException("Insert validate stats failed; " + e.getMessage(), e);
		}
	}

	private void insertMapStats(Map<String, Long> map, String namePrefix) {
		if (MapUtils.isEmpty(map)) {
			return;
		}
		for (Map.Entry<String, Long> entry : map.entrySet()) {
			String key = entry.getKey();
			long value = entry.getValue() == null ? 0L : entry.getValue();
			String name = namePrefix + key;
			insertLongValue(name, value);
		}
	}

	private void insertLongValue(String name, long value) {
		MetricsLabel metricsLabel = new TaskMetricsLabel(taskDto.getId().toHexString());
		Metrics metrics = Metrics.counter(name, (double) value, "", metricsLabel);
		insertMetrics(metrics);
	}

	private void insertLongValue(String name, long value, String nodeId) {
		MetricsLabel metricsLabel = new TaskMetricsLabel(taskDto.getId().toHexString(), nodeId);
		Metrics metrics = Metrics.counter(name, (double) value, "", metricsLabel);
		insertMetrics(metrics);
	}

	private void insertRuntimeThroughput(RuntimeThroughput runtimeThroughput, String name, String nodeId) {
		long rows = runtimeThroughput.getRows();
		try {
			insertLongValue(name, rows, nodeId);
		} catch (Exception e) {
			throw new RuntimeException("Insert node runtime throughput " + name + " failed; Runtime throughput: " + runtimeThroughput + "; " + e.getMessage(), e);
		}
	}

	private void checkInput(NodeType nodeType, Stats stats, Node<?> node) {
		if (node == null) {
			throw new IllegalArgumentException("Node cannot be null");
		}
		if (StringUtils.isBlank(node.getId())) {
			throw new IllegalArgumentException("Node id cannot be empty");
		}
		if (nodeType == null) {
			throw new IllegalArgumentException("Node type cannot be null");
		}
		if (stats == null) {
			throw new IllegalArgumentException("Stats cannot be null");
		}
	}

	@Override
	protected void insertMetrics(Metrics metrics) {
		super.insertMetrics(metrics);

		if (hazelcastInstance != null) {
			// put node metrics in IMap
			IMap<String, Map<String, Map<String, Metrics>>> subTaskMetricsMap = hazelcastInstance.getMap(IMAP_NAME);
			Map<String, Map<String, Metrics>> metricsMap;
			Map<String, Metrics> nodeMetricsMap;
			if (subTaskMetricsMap.containsKey(taskDto.getId().toHexString())) {
				metricsMap = subTaskMetricsMap.get(taskDto.getId().toHexString());
			} else {
				metricsMap = new HashMap<>();
			}
			if (null == metrics) {
				return;
			}
			if (metricsMap.containsKey(metrics.getName())) {
				nodeMetricsMap = metricsMap.get(metrics.getName());
			} else {
				nodeMetricsMap = new HashMap<>();
			}
			nodeMetricsMap.put(node.getId(), metrics);
			metricsMap.put(metrics.getName(), nodeMetricsMap);
			subTaskMetricsMap.put(taskDto.getId().toHexString(), metricsMap);
		}
	}

	@Override
	public String toString() {
		return "TaskMetrics{" +
				", nodeType=" + nodeType +
				"subTaskDto=" + taskDto.getId() + ", " + taskDto.getName() +
				'}';
	}
}
