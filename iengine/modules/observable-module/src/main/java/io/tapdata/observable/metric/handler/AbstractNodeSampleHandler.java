package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.sampler.*;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Dexter
 */
abstract class AbstractNodeSampleHandler extends AbstractHandler {
    static final String SAMPLE_TYPE_NODE    = "node";

    final Node<?> node;

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
    @Getter
    ResetSampler replicateLag;
    @Getter
    NumberSampler<Long> currentEventTimestamp;

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
                Constants.INPUT_DDL_TOTAL,
                Constants.INPUT_INSERT_TOTAL,
                Constants.INPUT_UPDATE_TOTAL,
                Constants.INPUT_DELETE_TOTAL,
                Constants.INPUT_OTHERS_TOTAL,
                Constants.OUTPUT_DDL_TOTAL,
                Constants.OUTPUT_INSERT_TOTAL,
                Constants.OUTPUT_UPDATE_TOTAL,
                Constants.OUTPUT_DELETE_TOTAL,
                Constants.OUTPUT_OTHERS_TOTAL,
                Constants.INPUT_QPS,
                Constants.OUTPUT_QPS,
                Constants.TIME_COST_AVG,
                Constants.REPLICATE_LAG,
                Constants.CURR_EVENT_TS
        );
    }

    void doInit(Map<String, Number> values) {
        inputDdlCounter = getCounterSampler(values, Constants.INPUT_DDL_TOTAL);
        inputInsertCounter = getCounterSampler(values, Constants.INPUT_INSERT_TOTAL);
        inputUpdateCounter = getCounterSampler(values, Constants.INPUT_UPDATE_TOTAL);
        inputDeleteCounter = getCounterSampler(values, Constants.INPUT_DELETE_TOTAL);
        inputOthersCounter = getCounterSampler(values, Constants.INPUT_OTHERS_TOTAL);

        outputDdlCounter = getCounterSampler(values, Constants.OUTPUT_DDL_TOTAL);
        outputInsertCounter = getCounterSampler(values,Constants.OUTPUT_INSERT_TOTAL);
        outputUpdateCounter = getCounterSampler(values,Constants.OUTPUT_UPDATE_TOTAL);
        outputDeleteCounter = getCounterSampler(values,Constants.OUTPUT_DELETE_TOTAL);
        outputOthersCounter = getCounterSampler(values,Constants.OUTPUT_OTHERS_TOTAL);

        inputSpeed = collector.getSpeedSampler(Constants.INPUT_QPS);
        outputSpeed = collector.getSpeedSampler(Constants.OUTPUT_QPS);
        timeCostAverage = collector.getAverageSampler(Constants.TIME_COST_AVG);

        Number currentEventTimestampInitial = values.getOrDefault(Constants.CURR_EVENT_TS, null);
        currentEventTimestamp = collector.getNumberCollector(Constants.CURR_EVENT_TS, Long.class,
                null == currentEventTimestampInitial ? null : currentEventTimestampInitial.longValue());
        replicateLag = collector.getResetSampler(Constants.REPLICATE_LAG);
    }
}
