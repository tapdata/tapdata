package io.tapdata.observable.metric.handler;

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
    protected int qpsType = Constants.QPS_TYPE_MEMORY;//memory(内存) / count(数量)

    AbstractHandler(TaskDto task) {
        this.task = task;
    }

    Map<String, String> tags() {
        return new HashMap<String, String>() {{
            put("type", type());
            put("taskId", task.getId().toHexString());
            put("taskRecordId", task.getTaskRecordId());
        }};
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
        inputSizeSpeed = collector.getSpeedSampler(Constants.INPUT_SIZE_QPS);
        outputSizeSpeed = collector.getSpeedSampler(Constants.OUTPUT_SIZE_QPS);
        collector.addSampler(Constants.QPS_TYPE, () -> qpsType);
        collector.addSampler(Constants.INPUT_SIZE_QPS, () -> inputSizeSpeed.value());
        collector.addSampler(Constants.OUTPUT_SIZE_QPS, () -> outputSizeSpeed.value());
        collector.addSampler(Constants.OUTPUT_SIZE_QPS_MAX, () -> {
            Optional.ofNullable(outputSizeSpeed).ifPresent(speed -> outputSizeQpsMax = speed.getMaxValue());
            return outputSizeQpsMax;
        });
        collector.addSampler(Constants.OUTPUT_SIZE_QPS_AVG, () -> {
            Optional.ofNullable(outputSizeSpeed).ifPresent(speed ->  outputSizeQpsAvg = speed.getAvgValue());
            return outputSizeQpsAvg;
        });
    }

    CounterSampler getCounterSampler(Map<String, Number> values, String name) {
        return collector.getCounterSampler(name, values.getOrDefault(name, 0).longValue());
    }
}
