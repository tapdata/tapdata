package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.micrometer.core.instrument.Metrics;
import io.tapdata.common.sample.sampler.*;
import io.tapdata.firedome.MultiTaggedGauge;
import io.tapdata.firedome.PrometheusName;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Dexter
 */
public abstract class AbstractNodeSampleHandler extends AbstractHandler {
	static final String SAMPLE_TYPE_NODE = "node";

	final Node<?> node;
	protected String taskId;
	protected String taskName;
	protected String nodeId;
	protected String nodeName;
	protected String nodeType;
	protected String taskType;

	CounterSampler inputInsertCounter;
	CounterSampler inputUpdateCounter;
	CounterSampler inputDeleteCounter;
	CounterSampler inputDdlCounter;
	CounterSampler inputOthersCounter;

	CounterSampler outputInsertCounter;
	CounterSampler outputUpdateCounter;
	CounterSampler outputDeleteCounter;
	CounterSampler outputDdlCounter;
	CounterSampler outputOthersCounter;

	SpeedSampler inputSpeed;
	SpeedSampler outputSpeed;
	AverageSampler timeCostAverage;
	protected transient MultiTaggedGauge nodeProcessDataGauge;
	@Getter
	ResetSampler replicateLag;
	@Getter
	NumberSampler<Long> currentEventTimestamp;
	protected MultiTaggedGauge timeCostAvgGauge;

	@Override
	public void close() {
		super.close();
	}

	AbstractNodeSampleHandler(TaskDto task, Node<?> node) {
		super(task);
		this.node = node;
	}

	@Override
	String type() {
		return SAMPLE_TYPE_NODE;
	}

	@Override
	public Map<String, String> tags() {
		Map<String, String> tags = super.tags();
		tags.put("nodeId", node.getId());
		tags.put("nodeType", node.getType());

		return tags;
	}

	@Override
	List<String> samples() {
		return Arrays.asList(
				MetricCons.SS.VS.F_CURR_EVENT_TS,
				MetricCons.SS.VS.F_INPUT_SIZE_QPS,
				MetricCons.SS.VS.F_OUTPUT_SIZE_QPS,
				MetricCons.SS.VS.F_QPS_TYPE,
				MetricCons.SS.VS.F_INPUT_DDL_TOTAL,
				MetricCons.SS.VS.F_INPUT_INSERT_TOTAL,
				MetricCons.SS.VS.F_INPUT_UPDATE_TOTAL,
				MetricCons.SS.VS.F_INPUT_DELETE_TOTAL,
				MetricCons.SS.VS.F_INPUT_OTHERS_TOTAL,
				MetricCons.SS.VS.F_OUTPUT_DDL_TOTAL,
				MetricCons.SS.VS.F_OUTPUT_INSERT_TOTAL,
				MetricCons.SS.VS.F_OUTPUT_UPDATE_TOTAL,
				MetricCons.SS.VS.F_OUTPUT_DELETE_TOTAL,
				MetricCons.SS.VS.F_OUTPUT_OTHERS_TOTAL,
				MetricCons.SS.VS.F_INPUT_QPS,
				MetricCons.SS.VS.F_OUTPUT_QPS,
				MetricCons.SS.VS.F_TIME_COST_AVG,
				MetricCons.SS.VS.F_REPLICATE_LAG,
				MetricCons.SS.VS.F_CURR_EVENT_TS
		);
	}

	void doInit(Map<String, Number> values) {
		super.doInit(values);
		initPrometheusReporter();
		inputDdlCounter = getCounterSampler(values, MetricCons.SS.VS.F_INPUT_DDL_TOTAL);
		inputInsertCounter = getCounterSampler(values, MetricCons.SS.VS.F_INPUT_INSERT_TOTAL);
		inputUpdateCounter = getCounterSampler(values, MetricCons.SS.VS.F_INPUT_UPDATE_TOTAL);
		inputDeleteCounter = getCounterSampler(values, MetricCons.SS.VS.F_INPUT_DELETE_TOTAL);
		inputOthersCounter = getCounterSampler(values, MetricCons.SS.VS.F_INPUT_OTHERS_TOTAL);

		outputDdlCounter = getCounterSampler(values, MetricCons.SS.VS.F_OUTPUT_DDL_TOTAL);
		outputInsertCounter = getCounterSampler(values, MetricCons.SS.VS.F_OUTPUT_INSERT_TOTAL);
		outputUpdateCounter = getCounterSampler(values, MetricCons.SS.VS.F_OUTPUT_UPDATE_TOTAL);
		outputDeleteCounter = getCounterSampler(values, MetricCons.SS.VS.F_OUTPUT_DELETE_TOTAL);
		outputOthersCounter = getCounterSampler(values, MetricCons.SS.VS.F_OUTPUT_OTHERS_TOTAL);

		inputSpeed = collector.getSpeedSampler(MetricCons.SS.VS.F_INPUT_QPS);
		outputSpeed = collector.getSpeedSampler(MetricCons.SS.VS.F_OUTPUT_QPS);

		Number currentEventTimestampInitial = values.getOrDefault(MetricCons.SS.VS.F_CURR_EVENT_TS, null);
		currentEventTimestamp = collector.getNumberCollector(MetricCons.SS.VS.F_CURR_EVENT_TS, Long.class,
				null == currentEventTimestampInitial ? null : currentEventTimestampInitial.longValue());
		replicateLag = collector.getResetSampler(MetricCons.SS.VS.F_REPLICATE_LAG);
	}


	private void initPrometheusReporter() {
		taskId = Optional.ofNullable(task.getId()).map(Object::toString).orElse("");
		taskName = Optional.ofNullable(task.getName()).orElse("");
		nodeId = Optional.ofNullable(node.getId()).orElse("");
		nodeName = Optional.ofNullable(node.getName()).orElse("");
		nodeType = Optional.ofNullable(node.getType()).orElse("");
		taskType = Optional.ofNullable(task.getSyncType()).orElse("");
		timeCostAvgGauge = new MultiTaggedGauge(PrometheusName.TASK_NODE_PROCESS_DATA_MS, Metrics.globalRegistry, "node_id", "node_name", "node_type", "task_id", "task_name", "task_type");
	}
}
