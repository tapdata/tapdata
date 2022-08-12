package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.NumberSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.observable.metric.TaskSampleRetriever;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Dexter
 */
public class ProcessorNodeSampleHandler extends AbstractNodeSampleHandler {
    public ProcessorNodeSampleHandler(TaskDto task) {
        super(task);
    }


    private final Map<String, SampleCollector> collectors = new HashMap<>();

    private final Map<String, CounterSampler> inputInsertCounters = new HashMap<>();
    private final Map<String, CounterSampler> inputUpdateCounters = new HashMap<>();
    private final Map<String, CounterSampler> inputDeleteCounters = new HashMap<>();
    private final Map<String, CounterSampler> inputDdlCounters = new HashMap<>();
    private final Map<String, CounterSampler> inputOthersCounters = new HashMap<>();

    private final Map<String, CounterSampler> outputInsertCounters = new HashMap<>();
    private final Map<String, CounterSampler> outputUpdateCounters = new HashMap<>();
    private final Map<String, CounterSampler> outputDeleteCounters = new HashMap<>();
    private final Map<String, CounterSampler> outputDdlCounters = new HashMap<>();
    private final Map<String, CounterSampler> outputOthersCounters = new HashMap<>();


    private final Map<String, SpeedSampler> inputSpeeds = new HashMap<>();
    private final Map<String, SpeedSampler> outputSpeeds = new HashMap<>();

    private final Map<String, AverageSampler> timeCostAverages = new HashMap<>();

    private final Map<String, NumberSampler<Long>>  currentEventTimestamps = new HashMap<>();


    public void init(Node<?> node) {
        Map<String, String> tags = nodeTags(node);
        Map<String, Number> values = TaskSampleRetriever.getInstance().retrieve(tags, Arrays.asList(
                "inputInsertTotal", "inputUpdateTotal", "inputDeleteTotal", "inputDdlTotal", "inputOthersTotal",
                "outputInsertTotal", "outputUpdateTotal", "outputDeleteTotal", "outputDdlTotal", "outputOthersTotal"
        ));

        String nodeId = node.getId();
        SampleCollector collector = CollectorFactory.getInstance("v2").getSampleCollectorByTags("nodeSamplers", tags);
        collectors.put(nodeId, collector);

        inputInsertCounters.put(nodeId, collector.getCounterSampler("inputInsertTotal",
                values.getOrDefault("inputInsertTotal", 0).longValue()));
        inputUpdateCounters.put(nodeId, collector.getCounterSampler("inputUpdateTotal",
                values.getOrDefault("inputUpdateTotal", 0).longValue()));
        inputDeleteCounters.put(nodeId, collector.getCounterSampler("inputDeleteTotal",
                values.getOrDefault("inputDeleteTotal", 0).longValue()));
        inputDdlCounters.put(nodeId, collector.getCounterSampler("inputDdlTotal",
                values.getOrDefault("inputDdlTotal", 0).longValue()));
        inputOthersCounters.put(nodeId, collector.getCounterSampler("inputOthersTotal",
                values.getOrDefault("inputOthersTotal", 0).longValue()));

        outputInsertCounters.put(nodeId, collector.getCounterSampler("outputInsertTotal",
                values.getOrDefault("outputInsertTotal", 0).longValue()));
        outputUpdateCounters.put(nodeId, collector.getCounterSampler("outputUpdateTotal",
                values.getOrDefault("outputUpdateTotal", 0).longValue()));
        outputDeleteCounters.put(nodeId, collector.getCounterSampler("outputDeleteTotal",
                values.getOrDefault("outputDeleteTotal", 0).longValue()));
        outputDdlCounters.put(nodeId, collector.getCounterSampler("outputDdlTotal",
                values.getOrDefault("outputDdlTotal", 0).longValue()));
        outputOthersCounters.put(nodeId, collector.getCounterSampler("outputOthersTotal",
                values.getOrDefault("outputOthersTotal", 0).longValue()));

        inputSpeeds.put(nodeId, collector.getSpeedSampler("inputQps"));
        outputSpeeds.put(nodeId, collector.getSpeedSampler("outputQps"));
        timeCostAverages.put(nodeId, collector.getAverageSampler("timeCostAvg"));

        Number currentEventTimestampInitial = values.getOrDefault("currentEventTimestamp", null);
        currentEventTimestamps.put(nodeId, collector.getNumberCollector("currentEventTimestamp", Long.class,
                null == currentEventTimestampInitial ? null : currentEventTimestampInitial.longValue()));

        // cache the initial sample value
        CollectorFactory.getInstance("v2").recordCurrentValueByTag(tags);
    }

    public void close(Node<?> node) {
        String nodeId = node.getId();
        Optional.ofNullable(collectors.get(nodeId)).ifPresent(collector -> {
            Map<String, String> tags = collector.getTags();
            // cache the last sample value
            CollectorFactory.getInstance("v2").recordCurrentValueByTag(tags);
            CollectorFactory.getInstance("v2").removeSampleCollectorByTags(tags);
        });
    }

    public void handleProcessStart(String nodeId, HandlerUtil.EventTypeRecorder recorder) {
        Optional.ofNullable(inputInsertCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getInsertTotal()));
        Optional.ofNullable(inputUpdateCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getUpdateTotal()));
        Optional.ofNullable(inputDeleteCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getDeleteTotal()));
        Optional.ofNullable(inputDdlCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getDdlTotal()));
        Optional.ofNullable(inputOthersCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getOthersTotal()));
        Optional.ofNullable(inputSpeeds.get(nodeId)).ifPresent(SpeedSampler::add);
    }

    public void handleProcessAccept(String nodeId, HandlerUtil.EventTypeRecorder recorder, Long newestEventTimestamp) {
        Optional.ofNullable(outputInsertCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getInsertTotal()));
        Optional.ofNullable(outputUpdateCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getUpdateTotal()));
        Optional.ofNullable(outputDeleteCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getDeleteTotal()));
        Optional.ofNullable(outputDdlCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getDdlTotal()));
        Optional.ofNullable(outputOthersCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getOthersTotal()));
        Optional.ofNullable(outputSpeeds.get(nodeId)).ifPresent(speed -> speed.add(recorder.getTotal()));
        Optional.ofNullable(currentEventTimestamps.get(nodeId)).ifPresent(number -> number.setValue(newestEventTimestamp));
    }

    public void handleProcessEnd(String nodeId, Long startAt, Long endAt, long total) {
        Optional.ofNullable(timeCostAverages.get(nodeId)).ifPresent(average ->
                average.add(total, endAt - startAt));
    }
}
