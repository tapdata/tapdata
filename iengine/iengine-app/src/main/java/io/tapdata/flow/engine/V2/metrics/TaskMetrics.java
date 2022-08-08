package io.tapdata.flow.engine.V2.metrics;

import com.google.common.util.concurrent.AtomicDouble;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.metrics.Metrics;
import com.tapdata.tm.commons.metrics.MetricsLabel;
import com.tapdata.tm.commons.metrics.TaskMetricsLabel;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-12-24 11:22
 **/
public class TaskMetrics extends BaseMetrics {

	protected SubTaskDto subTaskDto;

	public TaskMetrics(ClientMongoOperator clientMongoOperator, SubTaskDto subTaskDto, ConfigurationCenter configurationCenter, HazelcastInstance hazelcastInstance) {
		super(clientMongoOperator, configurationCenter, hazelcastInstance);
		checkInput(subTaskDto);
		this.subTaskDto = subTaskDto;
	}

	private void checkInput(SubTaskDto subTaskDto) {
		if (subTaskDto == null) {
			throw new IllegalArgumentException("Sub task dto cannot be null");
		}
		if (subTaskDto.getId() == null) {
			throw new IllegalArgumentException("Sub task id cannot be empty");
		}
	}

	@Override
	public void close() {
		doStats();
		getSubTaskNodeMetricsMap().remove(subTaskDto.getId().toHexString());
		super.close();
	}

	@Override
	public void doStats() {
		IMap<String, Map<String, Map<String, Metrics>>> subTaskMetricsMap = hazelcastInstance.getMap(IMAP_NAME);
		if (!subTaskMetricsMap.containsKey(subTaskDto.getId().toHexString())) {
			return;
		}
		Map<String, Map<String, Metrics>> metricsMap = subTaskMetricsMap.get(subTaskDto.getId().toHexString());
		for (String metricsName : metricsMap.keySet()) {
			Map<String, Metrics> nodeMetricsMap = metricsMap.get(metricsName);
			Collection<Metrics> metricsList = nodeMetricsMap.values();
			String name;
			if (CollectionUtils.isEmpty(metricsList)) {
				continue;
			}
			if (StringUtils.startsWith(metricsName, SUB_TASK_NODE_METRICS_PREFIX)) {
				name = SUB_TASK_METRICS_PREFIX + StringUtils.removeStart(metricsName, SUB_TASK_NODE_METRICS_PREFIX);
			} else {
				continue;
			}
			Metrics firstMetrics = metricsList.iterator().next();
			Metrics.MetricsType metricsType = Metrics.MetricsType.fromType(firstMetrics.getType());
			AtomicDouble sumValue = new AtomicDouble(0d);
			metricsList.stream().map(Metrics::getValue).forEach(sumValue::getAndAdd);
			MetricsLabel metricsLabel = new TaskMetricsLabel(subTaskDto.getId().toHexString());
			Metrics metrics = null;
			switch (metricsType) {
				case COUNTER:
					metrics = Metrics.counter(name, sumValue.get(), null, metricsLabel);
					break;
				case GAUGE:
					metrics = Metrics.gauge(name, sumValue.get(), null, metricsLabel);
					break;
				default:
					break;
			}
			if (metrics != null) {
				insertMetrics(metrics);
			}
		}

		if (CollectionUtils.isNotEmpty(metricsList)) {
			insertMetrics();
		}


	}
}
