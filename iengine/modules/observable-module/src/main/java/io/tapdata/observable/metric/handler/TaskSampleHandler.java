package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.micrometer.core.instrument.Metrics;
import io.tapdata.aspect.CpuMemUsageAspect;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SamplerPrometheus;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.firedome.MultiTaggedGauge;
import io.tapdata.firedome.PrometheusName;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Dexter
 */
public class TaskSampleHandler extends AbstractHandler {
    private static final String TAG = TaskSampleHandler.class.getSimpleName();
    static final String SAMPLE_TYPE_TASK = MetricCons.SampleType.TASK.code();

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

    protected CounterSampler createTableTotal;
    protected CounterSampler snapshotTableTotal;
    protected CounterSampler snapshotRowTotal;
    protected CounterSampler snapshotInsertRowTotal;
    protected Long snapshotStartAt = null;
    protected Long snapshotDoneAt = null;
    protected Long snapshotDoneCost = null;
    protected String currentSnapshotTable = null;
    protected final Map<String, Long> currentSnapshotTableRowTotalMap = new HashMap<>();
    protected Long currentSnapshotTableInsertRowTotal = 0L;
    protected Long currentSnapshotTableRowTotal = null;
    protected Double outputQpsMax;
    protected Double outputQpsAvg;

    private final Set<String> taskTables = new HashSet<>();

    // Prometheus metrics reporting
    private transient ScheduledExecutorService prometheusScheduler;
    private transient ScheduledFuture<?> prometheusFuture;
    private transient MultiTaggedGauge replicateLagGauge;

    private final HashMap<String, DataNodeSampleHandler> targetNodeHandlers = new HashMap<>();
    private final HashMap<String, DataNodeSampleHandler> sourceNodeHandlers = new HashMap<>();
    private String taskId;
    private String taskName;

    protected AtomicReference<Double> taskCpuUsage = new AtomicReference<>(null);
    protected AtomicReference<Long> taskMemUsage = new AtomicReference<>(null);

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
            MetricCons.SS.VS.F_CURR_EVENT_TS,
            MetricCons.SS.VS.F_CREATE_TABLE_TOTAL,
            MetricCons.SS.VS.F_SNAPSHOT_TABLE_TOTAL,
            MetricCons.SS.VS.F_SNAPSHOT_ROW_TOTAL,
            MetricCons.SS.VS.F_SNAPSHOT_INSERT_ROW_TOTAL,
            MetricCons.SS.VS.F_SNAPSHOT_START_AT,
            MetricCons.SS.VS.F_SNAPSHOT_DONE_AT,
            MetricCons.SS.VS.F_SNAPSHOT_DONE_COST,
            MetricCons.SS.VS.F_CURR_SNAPSHOT_TABLE,
            MetricCons.SS.VS.F_CURR_SNAPSHOT_TABLE_ROW_TOTAL,
            MetricCons.SS.VS.F_CURR_SNAPSHOT_TABLE_INSERT_ROW_TOTAL,
            MetricCons.SS.VS.F_OUTPUT_QPS_MAX,
            MetricCons.SS.VS.F_OUTPUT_QPS_AVG,
            MetricCons.SS.VS.F_TABLE_TOTAL,
            MetricCons.SS.VS.F_INPUT_SIZE_QPS,
            MetricCons.SS.VS.F_OUTPUT_SIZE_QPS,
            MetricCons.SS.VS.F_QPS_TYPE,
            MetricCons.SS.VS.F_OUTPUT_SIZE_QPS_MAX,
            MetricCons.SS.VS.F_OUTPUT_SIZE_QPS_AVG,
            MetricCons.SS.VS.F_CPU_USAGE,
            MetricCons.SS.VS.F_MEMORY_USAGE
        );
    }

    private void initPrometheusReporter() {
        try {
            // base tags
            taskId = Optional.ofNullable(task.getId()).map(Object::toString).orElse("");
            taskName = Optional.ofNullable(task.getName()).orElse("");

            // build gauge for REPLICATE_LAG -> task_cdc_delay_seconds
            replicateLagGauge = new MultiTaggedGauge(PrometheusName.TASK_CDC_DELAY_MS, Metrics.globalRegistry,
                    "task_id", "task_name", "task_type");
        } catch (Throwable ignore) {
        }
    }

    public void doInit(Map<String, Number> values) {
        super.doInit(values);
        // init Prometheus metrics reporter
        initPrometheusReporter();

        collector.addSampler(MetricCons.SS.VS.F_TABLE_TOTAL, () -> {
            if (taskTables.isEmpty() && snapshotTableTotal.value().longValue() > 0) {
                return Math.max(snapshotTableTotal.value().longValue(), values.get(MetricCons.SS.VS.F_TABLE_TOTAL).longValue());
            }
            if (Objects.nonNull(snapshotTableTotal.value())) {
                return Math.max(snapshotTableTotal.value().longValue(), taskTables.size());
            } else {
                return taskTables.size();
            }
        });

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
        timeCostAverage = collector.getAverageSampler(MetricCons.SS.VS.F_TIME_COST_AVG);

        collector.addSampler(MetricCons.SS.VS.F_CURR_EVENT_TS, () -> {
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
        collector.addSampler(MetricCons.SS.VS.F_REPLICATE_LAG, new SamplerPrometheus() {
            @Override
            public Number value() {
                AtomicReference<Long> replicateLagRef = new AtomicReference<>(null);
                Stream.concat(sourceNodeHandlers.values().stream(), targetNodeHandlers.values().stream())
                        .forEach(h -> Optional.ofNullable(h.getReplicateLag()).ifPresent(sampler -> {
							Number value = sampler.value();
							if (Objects.nonNull(value)) {
								long v = value.longValue();
								if (null == replicateLagRef.get() || replicateLagRef.get() < v) {
									replicateLagRef.set(v);
								}
							}
						}));

                return replicateLagRef.get();
            }

            @Override
            public String[] tagValues() {
                return new String[]{taskId, taskName, task.getSyncType()};
            }

            @Override
            public MultiTaggedGauge multiTaggedGauge() {
                return replicateLagGauge;
            }
        });

        createTableTotal = getCounterSampler(values, MetricCons.SS.VS.F_CREATE_TABLE_TOTAL);
        snapshotTableTotal = getCounterSampler(values, MetricCons.SS.VS.F_SNAPSHOT_TABLE_TOTAL);
        snapshotRowTotal = getCounterSampler(values, MetricCons.SS.VS.F_SNAPSHOT_ROW_TOTAL);
        snapshotInsertRowTotal = getCounterSampler(values, MetricCons.SS.VS.F_SNAPSHOT_INSERT_ROW_TOTAL);

        Number retrieveSnapshotStartAt = values.getOrDefault(MetricCons.SS.VS.F_SNAPSHOT_START_AT, null);
        if (retrieveSnapshotStartAt != null) {
            snapshotStartAt = retrieveSnapshotStartAt.longValue();
        }
        collector.addSampler(MetricCons.SS.VS.F_SNAPSHOT_START_AT, () -> snapshotStartAt);

        Number retrieveSnapshotDoneAt = values.getOrDefault(MetricCons.SS.VS.F_SNAPSHOT_DONE_AT, null);
        if (retrieveSnapshotDoneAt != null) {
            snapshotDoneAt = retrieveSnapshotDoneAt.longValue();
        }

        collector.addSampler(MetricCons.SS.VS.F_SNAPSHOT_DONE_AT, () -> {
            if (Objects.isNull(snapshotDoneAt)) {
                long allSourceSize = sourceNodeHandlers.values().size();
                long completeSize = sourceNodeHandlers.values().stream()
                        .filter(h -> Objects.nonNull(h.getSnapshotDoneAt())).count();
                if (allSourceSize != 0 && allSourceSize == completeSize) {
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

        collector.addSampler(MetricCons.SS.VS.F_SNAPSHOT_DONE_COST, () -> {
            if (Objects.nonNull(snapshotDoneAt) && Objects.nonNull(snapshotStartAt)) {
                snapshotDoneCost = snapshotDoneAt - snapshotStartAt;
            }
            return snapshotDoneCost;
        });

        collector.addSampler(MetricCons.SS.VS.F_CURR_SNAPSHOT_TABLE_ROW_TOTAL, () -> {
            if (null != currentSnapshotTable) {
                currentSnapshotTableRowTotal = currentSnapshotTableRowTotalMap.get(currentSnapshotTable);
            }
            return currentSnapshotTableRowTotal;
        });
        collector.addSampler(MetricCons.SS.VS.F_CURR_SNAPSHOT_TABLE_INSERT_ROW_TOTAL, () -> {
            if (Objects.nonNull(snapshotTableTotal.value()) && CollectionUtils.isNotEmpty(taskTables) &&
            snapshotTableTotal.value().intValue() == taskTables.size() && Objects.nonNull(currentSnapshotTable)) {
                currentSnapshotTableInsertRowTotal = currentSnapshotTableRowTotalMap.get(currentSnapshotTable);
            }
            return currentSnapshotTableInsertRowTotal;
        });
        collector.addSampler(MetricCons.SS.VS.F_OUTPUT_QPS_MAX, () -> {
            Optional.ofNullable(outputSpeed).ifPresent(speed -> {
                outputQpsMax = speed.getMaxValue();
            });
            return outputQpsMax;
        });
        collector.addSampler(MetricCons.SS.VS.F_OUTPUT_QPS_AVG, () -> {
            Optional.ofNullable(outputSpeed).ifPresent(speed -> {
                outputQpsAvg = speed.getAvgValue();
            });
            return outputQpsAvg;
        });
        collector.addSampler(MetricCons.SS.VS.F_CPU_USAGE, () -> taskCpuUsage.get());
        collector.addSampler(MetricCons.SS.VS.F_MEMORY_USAGE, this::getTaskMem);
    }

    protected Long getTaskMem() {
        Long mem = taskMemUsage.get();
        return null == mem || mem < 0L ? null : mem;
    }

    public void close() {
        Optional.ofNullable(collector).ifPresent(collector -> {
            Map<String, String> tags = collector.tags();
            // cache the last sample value
            CollectorFactory.getInstance("v2").recordCurrentValueByTag(tags);
            CollectorFactory.getInstance("v2").removeSampleCollectorByTags(tags);
        });
        // stop Prometheus reporter when closing
        if (prometheusFuture != null) {
            try { prometheusFuture.cancel(true); } catch (Throwable ignore) {}
            prometheusFuture = null;
        }
        if (prometheusScheduler != null) {
            try { prometheusScheduler.shutdownNow(); } catch (Throwable ignore) {}
            prometheusScheduler = null;
        }
    }

    public void addTable(String... tables) {
        taskTables.addAll(Arrays.asList(tables));
    }

    public void handleTableCountAccept(String table, long count) {
        snapshotRowTotal.inc(count);
        currentSnapshotTableRowTotalMap.put(table, count);
    }
        // stop Prometheus reporter when closing


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

    public void handleBatchReadStart(String table) {
        currentSnapshotTable = table;
        currentSnapshotTableInsertRowTotal = 0L;
    }

    public void handleBatchReadAccept(HandlerUtil.EventTypeRecorder recorder) {
        long size = recorder.getInsertTotal();
        inputSizeSpeed.add(recorder.getMemorySize());
        inputInsertCounter.inc(size);
        inputSpeed.add(size);
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
        inputSizeSpeed.add(recorder.getMemorySize());
        inputSpeed.add(recorder.getTotal());
    }

    public void addTargetNodeHandler(String nodeId, DataNodeSampleHandler handler) {
        targetNodeHandlers.putIfAbsent(nodeId, handler);
    }

    public void addSourceNodeHandler(String nodeId, DataNodeSampleHandler handler) {
        sourceNodeHandlers.putIfAbsent(nodeId, handler);
    }

    public void handleWriteRecordAccept(WriteListResult<TapRecordEvent> result, List<TapRecordEvent> events, HandlerUtil.EventTypeRecorder eventTypeRecorder) {        long current = System.currentTimeMillis();

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
        outputSizeSpeed.add(eventTypeRecorder.getMemorySize());
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

    public Void handleCpuMemUsage(CpuMemUsageAspect aspect) {
        taskCpuUsage.set(aspect.getUsage().getCpuUsage());
        taskMemUsage.set(aspect.getUsage().getHeapMemoryUsage());
        return null;
    }
}
