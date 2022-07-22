package io.tapdata.observable.handler;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.observable.TaskSampleRetriever;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Dexter
 */
public class ProcessorNodeSampleHandler extends AbstractNodeSampleHandler {
    public ProcessorNodeSampleHandler(SubTaskDto task) {
        super(task);
    }


    private final Map<String, SampleCollector> collectors = new HashMap<>();

    private final Map<String, CounterSampler> inputCounters = new HashMap<>();
    private final Map<String, CounterSampler> inputInsertCounters = new HashMap<>();
    private final Map<String, CounterSampler> inputUpdateCounters = new HashMap<>();
    private final Map<String, CounterSampler> inputDeleteCounters = new HashMap<>();
    private final Map<String, CounterSampler> inputDdlCounters = new HashMap<>();
    private final Map<String, CounterSampler> inputOthersCounters = new HashMap<>();

    private final Map<String, CounterSampler> outputCounters = new HashMap<>();
    private final Map<String, CounterSampler> outputInsertCounters = new HashMap<>();
    private final Map<String, CounterSampler> outputUpdateCounters = new HashMap<>();
    private final Map<String, CounterSampler> outputDeleteCounters = new HashMap<>();
    private final Map<String, CounterSampler> outputDdlCounters = new HashMap<>();
    private final Map<String, CounterSampler> outputOthersCounters = new HashMap<>();


    private final Map<String, SpeedSampler> inputSpeeds = new HashMap<>();
    private final Map<String, SpeedSampler> outputSpeeds = new HashMap<>();

    private final Map<String, AverageSampler> timeCostAverages = new HashMap<>();


    public void init(Node<?> node) {
        Map<String, String> tags = nodeTags(node);
        Map<String, Number> values = TaskSampleRetriever.getInstance().retrieve(tags, Arrays.asList(
                "inputTotal", "inputInsertTotal", "inputUpdateTotal", "inputDeleteTotal", "inputDdlTotal", "inputOthersTotal",
                "outputTotal", "outputInsertTotal", "outputUpdateTotal", "outputDeleteTotal", "outputDdlTotal", "outputOthersTotal"
        ));

        String nodeId = node.getId();
        SampleCollector collector = CollectorFactory.getInstance().getSampleCollectorByTags("nodeSamplers", tags);
        collectors.put(nodeId, collector);

        inputCounters.put(nodeId, collector.getCounterSampler("inputTotal",
                values.getOrDefault("inputTotal", 0).longValue()));
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

        outputCounters.put(nodeId, collector.getCounterSampler("outputTotal",
                values.getOrDefault("outputTotal", 0).longValue()));
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

        inputSpeeds.put(nodeId, collector.getSpeedSampler("inputQPS"));
        outputSpeeds.put(nodeId, collector.getSpeedSampler("outputQPS"));
        timeCostAverages.put(nodeId, collector.getAverageSampler("timeCostAvg"));

        // cache the initial sample value
        CollectorFactory.getInstance().recordCurrentValueByTag(tags);
    }

    public void close(Node<?> node) {
        String nodeId = node.getId();
        Optional.ofNullable(collectors.get(nodeId)).ifPresent(collector -> {
            Map<String, String> tags = collector.getTags();
            // cache the last sample value
            CollectorFactory.getInstance().recordCurrentValueByTag(tags);
            CollectorFactory.getInstance().removeSampleCollectorByTags(tags);
        });
    }

    public void handleProcessStart(String nodeId, Long startAt) {
        Optional.ofNullable(inputCounters.get(nodeId)).ifPresent(CounterSampler::inc);
        Optional.ofNullable(inputSpeeds.get(nodeId)).ifPresent(SpeedSampler::add);
    }

    public void handleProcessEnd(String nodeId, Long startAt, Long endAt, long total) {
        Optional.ofNullable(outputCounters.get(nodeId)).ifPresent(counter -> counter.inc(total));
        Optional.ofNullable(outputSpeeds.get(nodeId)).ifPresent(speed -> speed.add(total));
        Optional.ofNullable(timeCostAverages.get(nodeId)).ifPresent(average ->
                average.add(total, endAt - startAt));
    }
}
