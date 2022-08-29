package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.observable.metric.TaskSampleRetriever;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.*;

/**
 * @author Dexter
 */
public class TaskSampleHandler extends AbstractHandler {
    private static final String TAG = TaskSampleHandler.class.getSimpleName();

    public TaskSampleHandler(TaskDto task) {
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
    private CounterSampler snapshotRowTotal;
    private CounterSampler snapshotInsertRowTotal;
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

    private Long snapshotDoneAt = null;

    public void init() {
        Map<String, String> tags = taskTags();
        Map<String, Number> values = TaskSampleRetriever.getInstance().retrieve(tags, Arrays.asList(
                "createTableTotal", "snapshotTableTotal", "snapshotRowTotal", "snapshotInsertRowTotal", "snapshotDoneAt",
                "inputInsertTotal", "inputUpdateTotal", "inputDeleteTotal", "inputDdlTotal", "inputOthersTotal",
                "outputInsertTotal", "outputUpdateTotal", "outputDeleteTotal", "outputDdlTotal", "outputOthersTotal"
        ));

        collector = CollectorFactory.getInstance("v2").getSampleCollectorByTags("taskSamplers", tags);

        collector.addSampler("tableTotal", taskTables::size);

        createTableTotal = collector.getCounterSampler("createTableTotal",
                values.getOrDefault("createTableTotal", 0).longValue());
        snapshotTableTotal = collector.getCounterSampler("snapshotTableTotal",
                values.getOrDefault("createTableTotal", 0).longValue());
        snapshotRowTotal = collector.getCounterSampler("snapshotRowTotal",
                values.getOrDefault("snapshotRowTotal", 0).longValue());
        snapshotInsertRowTotal = collector.getCounterSampler("snapshotInsertRowTotal",
                values.getOrDefault("snapshotInsertRowTotal", 0).longValue());

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

        Number retrieveSnapshotDoneAt = values.getOrDefault("snapshotDoneAt", null);
        if (retrieveSnapshotDoneAt != null) {
            snapshotDoneAt = retrieveSnapshotDoneAt.longValue();
        }
        collector.addSampler("snapshotDoneAt", () -> snapshotDoneAt);

        // cache the initial sample value
        CollectorFactory.getInstance("v2").recordCurrentValueByTag(tags);
    }

    public void close() {
        Optional.ofNullable(collector).ifPresent(collector -> {
            Map<String, String> tags = collector.tags();
            // cache the last sample value
            CollectorFactory.getInstance("v2").recordCurrentValueByTag(tags);
            CollectorFactory.getInstance("v2").removeSampleCollectorByTags(tags);
        });
    }

    public void handleTableCountAccept(long count) {
        snapshotRowTotal.inc(count);
    }

    public void handleCreateTableEnd() {
        // if task tables size = 0, must be error stops the table adder, stop the
        // creating table counter
        if (taskTables.size() != 0) {
            createTableTotal.inc();
        }
    }

    public void handleDdlStart() {
        inputDdlTotal.inc();
    }

    public void handleDdlEnd() {
        outputDdlTotal.inc();
    }

    public void handleBatchReadAccept(long size) {
        inputInsertTotal.inc(size);
        inputQps.add(size);

        snapshotInsertRowTotal.inc(size);
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

    public void handleWriteRecordAccept(WriteListResult<TapRecordEvent> result, List<TapRecordEvent> events) {
        long current = System.currentTimeMillis();

        long inserted = result.getInsertedCount();
        long updated = result.getModifiedCount();
        long deleted = result.getRemovedCount();
        long total = inserted + updated + deleted;

        outputInsertTotal.inc(inserted);
        outputUpdateTotal.inc(updated);
        outputDeleteTotal.inc(deleted);
        outputQps.add(total);

        long timeCostTotal = 0L;
        for (TapRecordEvent event : events) {
            Long time = event.getTime();
            if (null == time) {
                TapLogger.warn(TAG, "event from task {} does have time field.", task.getId().toHexString());
                break;
            }
            timeCostTotal += (current - time);
        }
        timeCostAvg.add(total, timeCostTotal);
    }

    public void handleSnapshotDone(Long time) {
        snapshotDoneAt = time;
    }

    public void handleSourceDynamicTableAdd(List<String> tables) {
        if (null == tables || tables.isEmpty()) {
            return;
        }

        taskTables.addAll(tables);
        inputDdlTotal.inc();
    }

    public void handleSourceDynamicTableRemove(List<String> tables) {
        if (null == tables || tables.isEmpty()) {
            return;
        }
        inputDdlTotal.inc();
    }
}
