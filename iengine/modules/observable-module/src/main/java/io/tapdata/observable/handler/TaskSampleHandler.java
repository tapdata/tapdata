package io.tapdata.observable.handler;

import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.entity.event.dml.*;
import io.tapdata.observable.TaskSampleRetriever;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.*;

/**
 * @author Dexter
 */
public class TaskSampleHandler extends AbstractHandler {
    public TaskSampleHandler(SubTaskDto task) {
        super(task);
    }

    public Map<String, String> taskTags() {
        return baseTags(SAMPLE_TYPE_TASK);
    }

    private final Set<String> taskTables = new HashSet<>();
    public void addTable(String... tables) {
        taskTables.addAll(Arrays.asList(tables));
    }

    private SampleCollector collector;
    private CounterSampler createTableTotal;
    private CounterSampler snapshotTableTotal;
    private CounterSampler inputInsertTotal;
    private CounterSampler inputUpdateTotal;
    private CounterSampler inputDeleteTotal;
    private CounterSampler inputOthersTotal;
    private CounterSampler inputDdlTotal;
    private SpeedSampler inputQps;

    private CounterSampler outputInsertTotal;
    private CounterSampler outputUpdateTotal;
    private CounterSampler outputDeleteTotal;
    private CounterSampler outputOthersTotal;
    private CounterSampler outputDdlTotal;
    private SpeedSampler outputQps;

    private AverageSampler timeCostAvg;

    public void init() {
        Map<String, String> tags = taskTags();
        Map<String, Number> values = TaskSampleRetriever.getInstance().retrieve(tags, Arrays.asList(
                "createTableTotal", "snapshotTableTotal",
                "inputInsertTotal", "inputUpdateTotal", "inputDeleteTotal", "inputDdlTotal", "inputOthersTotal",
                "outputInsertTotal", "outputUpdateTotal", "outputDeleteTotal", "outputDdlTotal", "outputOthersTotal"
        ));

        collector = CollectorFactory.getInstance().getSampleCollectorByTags("taskSamplers", tags);

        collector.addSampler("tableTotal", taskTables::size);

        createTableTotal = collector.getCounterSampler("createTableTotal",
                values.getOrDefault("createTableTotal", 0).longValue());
        snapshotTableTotal = collector.getCounterSampler("snapshotTableTotal",
                values.getOrDefault("createTableTotal", 0).longValue());

        inputInsertTotal = collector.getCounterSampler("inputInsertTotal",
                values.getOrDefault("inputInsertTotal", 0).longValue());
        inputUpdateTotal = collector.getCounterSampler("inputUpdateTotal",
                values.getOrDefault("inputUpdateTotal", 0).longValue());
        inputDeleteTotal = collector.getCounterSampler("inputDeleteTotal",
                values.getOrDefault("inputDeleteTotal", 0).longValue());
        inputOthersTotal = collector.getCounterSampler("inputOthersTotal",
                values.getOrDefault("inputOthersTotal", 0).longValue());
        inputDdlTotal = collector.getCounterSampler("inputDdlTotal",
                values.getOrDefault("inputDdlTotal", 0).longValue());

        outputInsertTotal = collector.getCounterSampler("outputInsertTotal",
                values.getOrDefault("outputInsertTotal", 0).longValue());
        outputUpdateTotal = collector.getCounterSampler("outputUpdateTotal",
                values.getOrDefault("outputUpdateTotal", 0).longValue());
        outputDeleteTotal = collector.getCounterSampler("outputDeleteTotal",
                values.getOrDefault("outputDeleteTotal", 0).longValue());
        outputOthersTotal = collector.getCounterSampler("outputOthersTotal",
                values.getOrDefault("outputOthersTotal", 0).longValue());
        outputDdlTotal = collector.getCounterSampler("outputDdlTotal",
                values.getOrDefault("outputDdlTotal", 0).longValue());

        inputQps = collector.getSpeedSampler("inputQps");
        outputQps = collector.getSpeedSampler("outputQps");

        timeCostAvg = collector.getAverageSampler("timeCostAvg");

        // cache the initial sample value
        CollectorFactory.getInstance().recordCurrentValueByTag(tags);
    }

    public void close() {
        Optional.ofNullable(collector).ifPresent(collector -> {
            Map<String, String> tags = collector.tags();
            // cache the last sample value
            CollectorFactory.getInstance().recordCurrentValueByTag(tags);
            CollectorFactory.getInstance().removeSampleCollectorByTags(tags);
        });
    }

    public void handleCreateTableEnd() {
        createTableTotal.inc();
    }

    public void handleBatchReadAccept(long size) {
        inputInsertTotal.inc(size);
    }

    public void handleBatchReadFuncEnd() {
        snapshotTableTotal.inc();
    }

    public void handleStreamReadAccept(HandlerUtil.EventTypeRecorder recorder) {
        inputInsertTotal.inc(recorder.getInsertTotal());
        inputUpdateTotal.inc(recorder.getUpdateTotal());
        inputDeleteTotal.inc(recorder.getDeleteTotal());
        inputDdlTotal.inc(recorder.getDdlTotal());
        inputOthersTotal.inc(recorder.getOthersTotal());

        inputQps.add(recorder.getTotal());
    }

    public void handleWriteRecordAccept(WriteListResult<TapRecordEvent> result) {
        long inserted = result.getInsertedCount();
        long updated = result.getModifiedCount();
        long deleted = result.getRemovedCount();
        long total = inserted + updated + deleted;

        outputInsertTotal.inc(inserted);
        outputUpdateTotal.inc(updated);
        outputDeleteTotal.inc(deleted);
        outputQps.add(total);
    }
}
