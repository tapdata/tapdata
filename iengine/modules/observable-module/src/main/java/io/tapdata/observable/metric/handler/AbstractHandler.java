package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.observable.metric.TaskSampleRetriever;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Dexter
 */
public abstract class AbstractHandler {
    final TaskDto task;
    @Getter
    SampleCollector collector;

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
        return TaskSampleRetriever.getInstance().retrieveWithRetry(tags(), samples());
    }


    abstract String type();
    abstract List<String> samples();
    abstract void doInit(Map<String, Number> values);

    CounterSampler getCounterSampler(Map<String, Number> values, String name) {
        return collector.getCounterSampler(name, values.getOrDefault(name, 0).longValue());
    }
}
