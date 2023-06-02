package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Dexter
 */
public class TaskSampleHandler extends AbstractHandler {
    private static final String TAG = TaskSampleHandler.class.getSimpleName();
    static final String SAMPLE_TYPE_TASK                      = "task";

    static final String TABLE_TOTAL                           = "tableTotal";
    static final String CREATE_TABLE_TOTAL                    = "createTableTotal";
    static final String SNAPSHOT_TABLE_TOTAL                  = "snapshotTableTotal";
    static final String SNAPSHOT_ROW_TOTAL                    = "snapshotRowTotal";
    static final String SNAPSHOT_INSERT_ROW_TOTAL             = "snapshotInsertRowTotal";
    static final String SNAPSHOT_START_AT                     = "snapshotStartAt";
    static final String SNAPSHOT_DONE_AT                      = "snapshotDoneAt";
    static final String SNAPSHOT_DONE_COST                    = "snapshotDoneCost";
    static final String CURR_SNAPSHOT_TABLE                   = "currentSnapshotTable";
    static final String CURR_SNAPSHOT_TABLE_ROW_TOTAL         = "currentSnapshotTableRowTotal";
    static final String CURR_SNAPSHOT_TABLE_INSERT_ROW_TOTAL  = "currentSnapshotTableInsertRowTotal";
    static final String OUTPUT_QPS_MAX                        = "outputQpsMax";
    static final String OUTPUT_QPS_AVG                        = "outputQpsAvg";


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

    private CounterSampler createTableTotal;
    private CounterSampler snapshotTableTotal;
    private CounterSampler snapshotRowTotal;
    private CounterSampler snapshotInsertRowTotal;
    private Long snapshotStartAt = null;
    private Long snapshotDoneAt = null;
    private Long snapshotDoneCost = null;
    private String currentSnapshotTable = null;
    private final Map<String, Long> currentSnapshotTableRowTotalMap = new HashMap<>();
    private Long currentSnapshotTableInsertRowTotal = 0L;
    private Long currentSnapshotTableRowTotal = null;
    private Double outputQpsMax;
    private Double outputQpsAvg;

    private final Set<String> taskTables = new HashSet<>();

    private final HashMap<String, DataNodeSampleHandler> targetNodeHandlers = new HashMap<>();
    private final HashMap<String, DataNodeSampleHandler> sourceNodeHandlers = new HashMap<>();

    public TaskSampleHandler(TaskDto task) {
        super(task);
    }

    @Override
    String type() {
        return SAMPLE_TYPE_TASK;
    }

    @Override
    public Map<String, String> tags() {
        return super.tags();
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
                Constants.CURR_EVENT_TS,
                CREATE_TABLE_TOTAL,
                SNAPSHOT_TABLE_TOTAL,
                SNAPSHOT_ROW_TOTAL,
                SNAPSHOT_INSERT_ROW_TOTAL,
                SNAPSHOT_START_AT,
                SNAPSHOT_DONE_AT,
                SNAPSHOT_DONE_COST,
                CURR_SNAPSHOT_TABLE,
                CURR_SNAPSHOT_TABLE_ROW_TOTAL,
                CURR_SNAPSHOT_TABLE_INSERT_ROW_TOTAL,
                OUTPUT_QPS_MAX,
                OUTPUT_QPS_AVG,
                TABLE_TOTAL
        );
    }

    public void doInit(Map<String, Number> values) {
        Number tableTotal = values.getOrDefault(TABLE_TOTAL, null);
        collector.addSampler(TABLE_TOTAL, () -> {
            if (Objects.nonNull(tableTotal)) {
                return tableTotal.longValue();
            } else if (CollectionUtils.isNotEmpty(taskTables)) {
                if (Objects.nonNull(snapshotTableTotal.value())) {
                    return Math.max(snapshotTableTotal.value().longValue(), taskTables.size());
                } else {
                    return taskTables.size();
                }
            } else {
                return null;
            }
        });

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

        collector.addSampler(Constants.CURR_EVENT_TS, () -> {
            AtomicReference<Long> currentEventTimestampRef = new AtomicReference<>();
            Stream.concat(sourceNodeHandlers.values().stream(), targetNodeHandlers.values().stream()).forEach(h -> {
                Optional.ofNullable(h.getCurrentEventTimestamp()).ifPresent(sampler -> {
                    Number value = sampler.value();
                    if (null == value) return;
                    long v = value.longValue();
                    if (null == currentEventTimestampRef.get() || currentEventTimestampRef.get() > v) {
                        currentEventTimestampRef.set(v);
                    }
                });
            });
            return currentEventTimestampRef.get();
        });
        collector.addSampler(Constants.REPLICATE_LAG, () -> {
            AtomicReference<Long> replicateLagRef = new AtomicReference<>(null);
            Stream.concat(sourceNodeHandlers.values().stream(), targetNodeHandlers.values().stream())
                    .forEach(h -> {
                        Optional.ofNullable(h.getReplicateLag()).ifPresent(sampler -> {
                            Number value = sampler.value();
                            if (Objects.nonNull(value)) {
                                long v = value.longValue();
                                if (null == replicateLagRef.get() || replicateLagRef.get() < v) {
                                    replicateLagRef.set(v);
                                }
                            }
                        });
                    });

            return replicateLagRef.get();
        });

        createTableTotal = getCounterSampler(values, CREATE_TABLE_TOTAL);
        snapshotTableTotal = getCounterSampler(values, SNAPSHOT_TABLE_TOTAL);
        snapshotRowTotal = getCounterSampler(values, SNAPSHOT_ROW_TOTAL);
        snapshotInsertRowTotal = getCounterSampler(values, SNAPSHOT_INSERT_ROW_TOTAL);

        Number retrieveSnapshotStartAt = values.getOrDefault(SNAPSHOT_START_AT, null);
        if (retrieveSnapshotStartAt != null) {
            snapshotStartAt = retrieveSnapshotStartAt.longValue();
        }
        collector.addSampler(SNAPSHOT_START_AT, () -> snapshotStartAt);

        Number retrieveSnapshotDoneAt = values.getOrDefault(SNAPSHOT_DONE_AT, null);
        if (retrieveSnapshotDoneAt != null) {
            snapshotDoneAt = retrieveSnapshotDoneAt.longValue();
        }

        collector.addSampler(SNAPSHOT_DONE_AT, () -> {
            if (Objects.isNull(snapshotDoneAt)) {
                long allSourceSize = sourceNodeHandlers.values().size();
                long completeSize = sourceNodeHandlers.values().stream()
                        .filter(h -> Objects.nonNull(h.getSnapshotDoneAt())).count();
                if (allSourceSize == completeSize) {
                    List<Long> collect = sourceNodeHandlers.values().stream()
                            .map(DataNodeSampleHandler::getSnapshotDoneAt)
                            .filter(Objects::nonNull).collect(Collectors.toList());
                    if (!collect.isEmpty()) {
                        return Collections.max(collect);
                    }
                }
                return null;
            }
            return snapshotDoneAt;
        });

        collector.addSampler(SNAPSHOT_DONE_COST, () -> {
            if (Objects.nonNull(snapshotDoneAt) && Objects.nonNull(snapshotStartAt)) {
                snapshotDoneCost = snapshotDoneAt - snapshotStartAt;
            }
            return snapshotDoneCost;
        });

        collector.addSampler(CURR_SNAPSHOT_TABLE_ROW_TOTAL, () -> {
            if (null != currentSnapshotTable) {
                currentSnapshotTableRowTotal = currentSnapshotTableRowTotalMap.get(currentSnapshotTable);
            }
            return currentSnapshotTableRowTotal;
        });
        collector.addSampler(CURR_SNAPSHOT_TABLE_INSERT_ROW_TOTAL, () -> {
            if (Objects.nonNull(snapshotTableTotal.value()) && CollectionUtils.isNotEmpty(taskTables) &&
            snapshotTableTotal.value().intValue() == taskTables.size() && Objects.nonNull(currentSnapshotTable)) {
                currentSnapshotTableInsertRowTotal = currentSnapshotTableRowTotalMap.get(currentSnapshotTable);
            }
            return currentSnapshotTableInsertRowTotal;
        });
        collector.addSampler(OUTPUT_QPS_MAX, () -> {
            Optional.ofNullable(outputSpeed).ifPresent(speed -> {
                outputQpsMax = speed.getMaxValue();
            });
            return outputQpsMax;
        });
        collector.addSampler(OUTPUT_QPS_AVG, () -> {
            Optional.ofNullable(outputSpeed).ifPresent(speed -> {
                outputQpsAvg = speed.getAvgValue();
            });
            return outputQpsAvg;
        });
    }

    public void close() {
        Optional.ofNullable(collector).ifPresent(collector -> {
            Map<String, String> tags = collector.tags();
            // cache the last sample value
            CollectorFactory.getInstance("v2").recordCurrentValueByTag(tags);
            CollectorFactory.getInstance("v2").removeSampleCollectorByTags(tags);
        });
    }

    public void addTable(String... tables) {
        taskTables.addAll(Arrays.asList(tables));
    }

    public void handleTableCountAccept(String table, long count) {
        snapshotRowTotal.inc(count);
        currentSnapshotTableRowTotalMap.putIfAbsent(table, count);
    }

    public void handleCreateTableEnd() {
        // if task tables size = 0, must be error stops the table adder, stop the
        // creating table counter
        if (taskTables.size() != 0) {
            createTableTotal.inc();
        }
    }

    public void handleDdlStart() {
        inputDdlCounter.inc();
    }

    public void handleDdlEnd() {
        outputDdlCounter.inc();
    }

    AtomicBoolean firstBatchRead = new AtomicBoolean(true);
    public void handleBatchReadStart(String table) {
        currentSnapshotTable = table;
        currentSnapshotTableInsertRowTotal = 0L;
//        if (firstBatchRead.get()) {
//            if (Objects.nonNull(snapshotTableTotal)) {
//                snapshotTableTotal.reset();
//            }
//            firstBatchRead.set(false);
//        }
    }

    public void handleBatchReadAccept(long size) {
        inputInsertCounter.inc(size);
        inputSpeed.add(size);
//        currentSnapshotTableInsertRowTotal += size;

//        snapshotInsertRowTotal.inc(size);
    }

    public void handleBatchReadFuncEnd() {
        snapshotTableTotal.inc();
    }

    public void handleStreamReadStart(List<String> tables) {
        for(String table : tables) {
            addTable(table);
        }
    }

    public void handleStreamReadAccept(HandlerUtil.EventTypeRecorder recorder) {
        inputInsertCounter.inc(recorder.getInsertTotal());
        inputUpdateCounter.inc(recorder.getUpdateTotal());
        inputDeleteCounter.inc(recorder.getDeleteTotal());
        inputDdlCounter.inc(recorder.getDdlTotal());
        inputOthersCounter.inc(recorder.getOthersTotal());

        inputSpeed.add(recorder.getTotal());
    }

    public void addTargetNodeHandler(String nodeId, DataNodeSampleHandler handler) {
        targetNodeHandlers.putIfAbsent(nodeId, handler);
    }

    public void addSourceNodeHandler(String nodeId, DataNodeSampleHandler handler) {
        sourceNodeHandlers.putIfAbsent(nodeId, handler);
    }

    public void handleWriteRecordAccept(WriteListResult<TapRecordEvent> result, List<TapRecordEvent> events) {
        long current = System.currentTimeMillis();

        long inserted = result.getInsertedCount();
        long updated = result.getModifiedCount();
        long deleted = result.getRemovedCount();
        long total = inserted + updated + deleted;

        outputInsertCounter.inc(inserted);
        outputUpdateCounter.inc(updated);
        outputDeleteCounter.inc(deleted);
        outputSpeed.add(total);

        snapshotInsertRowTotal.inc(total);
        if (Objects.isNull(currentSnapshotTableInsertRowTotal)) {
            currentSnapshotTableInsertRowTotal = total;
        } else {
            currentSnapshotTableInsertRowTotal += total;
        }

        long timeCostTotal = 0L;
        for (TapRecordEvent event : events) {
            Long time = event.getTime();
            if (null == time) {
                TapLogger.warn(TAG, "event from task {} does have time field.", task.getId().toHexString());
                break;
            }
            timeCostTotal += (current - time);
        }
        timeCostAverage.add(total, timeCostTotal);
    }

    public void handleSnapshotStart(Long time) {
        snapshotStartAt = time;
    }

    public void handleSnapshotDone(Long time) {
        snapshotDoneAt = time;
    }

    public void handleSourceDynamicTableAdd(List<String> tables) {
        if (null == tables || tables.isEmpty()) {
            return;
        }

        taskTables.addAll(tables);
        inputDdlCounter.inc(tables.size());
    }

    public void handleSourceDynamicTableRemove(List<String> tables) {
        if (null == tables || tables.isEmpty()) {
            return;
        }
        inputDdlCounter.inc(tables.size());
    }

    public Long getSnapshotDone() {
        return snapshotDoneAt;
    }
}
