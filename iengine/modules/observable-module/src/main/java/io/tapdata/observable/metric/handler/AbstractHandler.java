package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.observable.metric.TaskSampleRetriever;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Dexter
 */
public abstract class AbstractHandler {
    final TaskDto task;
    @Getter
    SampleCollector collector;
    protected SpeedSampler inputSizeSpeed;
    protected SpeedSampler outputSizeSpeed;
    private Double outputSizeQpsMax;
    private Double outputSizeQpsAvg;
    protected int qpsType = MetricCons.QpsType.MEMORY.code();

    AbstractHandler(TaskDto task) {
        this.task = task;
    }

    Map<String, String> tags() {
        Map<String, String> hashMap = new HashMap<>();
        hashMap.put(MetricCons.Tags.F_TYPE, type());
        hashMap.put(MetricCons.Tags.F_TASK_ID, task.getId().toHexString());
        hashMap.put(MetricCons.Tags.F_TASK_RECORD_ID, task.getTaskRecordId());
        return hashMap;
    }

    public void init() {
        Map<String, String> tags = tags();
        collector = CollectorFactory.getInstance("v2").getSampleCollectorByTags("nodeSampler", tags);
        // init the samplers with the retrieved values
        doInit(retrieve());
        // cache the initial sample value
        CollectorFactory.getInstance("v2").recordCurrentValueByTag(tags);
    }

    public void close() {
        Map<String, String> tags = tags();
        // cache the last sample value
        CollectorFactory.getInstance("v2").recordCurrentValueByTag(tags);
        CollectorFactory.getInstance("v2").removeSampleCollectorByTags(tags);
    }

    public Map<String, Number> retrieve() {
        return TaskSampleRetriever.getInstance().retrieveWithRetry(task.getStartTime().getTime(), tags(), samples());
    }


    abstract String type();
    abstract List<String> samples();
    void doInit(Map<String, Number> values){
        inputSizeSpeed = collector.getSpeedSampler(MetricCons.SS.VS.F_INPUT_SIZE_QPS);
        outputSizeSpeed = collector.getSpeedSampler(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS);
        collector.addSampler(MetricCons.SS.VS.F_QPS_TYPE, () -> qpsType);
        collector.addSampler(MetricCons.SS.VS.F_INPUT_SIZE_QPS, () -> inputSizeSpeed.value());
        collector.addSampler(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS, () -> outputSizeSpeed.value());
        collector.addSampler(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS_MAX, () -> {
            outputSizeQpsMax = Optional.ofNullable(outputSizeSpeed)
                .map(SpeedSampler::getMaxValue)
                .orElse(outputSizeQpsMax);
            return outputSizeQpsMax;
        });
        collector.addSampler(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS_AVG, () -> {
            outputSizeQpsAvg = Optional.ofNullable(outputSizeSpeed)
                .map(SpeedSampler::getAvgValue)
                .orElse(outputSizeQpsAvg);
            return outputSizeQpsAvg;
        });
    }

    CounterSampler getCounterSampler(Map<String, Number> values, String name) {
        return collector.getCounterSampler(name, values.getOrDefault(name, 0).longValue());
    }
}
