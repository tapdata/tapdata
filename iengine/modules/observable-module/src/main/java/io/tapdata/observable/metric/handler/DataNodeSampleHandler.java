package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.executor.ExecutorsManager;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.NumberSampler;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.observable.metric.aspect.ConnectionPingAspect;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckFunction;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import lombok.extern.slf4j.Slf4j;
import io.tapdata.pdk.apis.functions.PDKMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Dexter
 */
@Slf4j
public class DataNodeSampleHandler extends AbstractNodeSampleHandler {
    private final Logger logger = LogManager.getLogger(DataNodeSampleHandler.class);

    static final String TABLE_TOTAL                        = "tableTotal";
    static final String SNAPSHOT_TABLE_TOTAL               = "snapshotTableTotal";
    static final String SNAPSHOT_ROW_TOTAL                 = "snapshotRowTotal";
    static final String SNAPSHOT_INSERT_ROW_TOTAL          = "snapshotInsertRowTotal";
    static final String SNAPSHOT_SOURCE_READ_TIME_COST_AVG = "snapshotSourceReadTimeCostAvg";
    static final String INCR_SOURCE_READ_TIME_COST_AVG     = "incrementalSourceReadTimeCostAvg";
    static final String TARGET_WRITE_TIME_COST_AVG         = "targetWriteTimeCostAvg";

    static final String CURR_SNAPSHOT_TABLE                   = "currentSnapshotTable";
    static final String CURR_SNAPSHOT_TABLE_ROW_TOTAL         = "currentSnapshotTableRowTotal";
    static final String CURR_SNAPSHOT_TABLE_INSERT_ROW_TOTAL  = "currentSnapshotTableInsertRowTotal";

    public DataNodeSampleHandler(TaskDto task, Node<?> node) {
        super(task, node);
    }

    private CounterSampler snapshotTableCounter;
    private CounterSampler snapshotRowCounter;
    private CounterSampler snapshotInsertRowCounter;
    private AverageSampler snapshotSourceReadTimeCostAvg;
    private AverageSampler incrementalSourceReadTimeCostAvg;
    private AverageSampler targetWriteTimeCostAvg;

    private final Set<String> nodeTables = new HashSet<>();

    private String currentSnapshotTable = null;
    private final Map<String, Long> currentSnapshotTableRowTotalMap = new HashMap<>();
    private Long currentSnapshotTableInsertRowTotal = null;

    @Override
    List<String> samples() {
        List<String> sampleNames = new ArrayList<>(super.samples());
        sampleNames.add(TABLE_TOTAL);
        sampleNames.add(SNAPSHOT_TABLE_TOTAL);
        sampleNames.add(SNAPSHOT_ROW_TOTAL);
        sampleNames.add(SNAPSHOT_INSERT_ROW_TOTAL);
        sampleNames.add(CURR_SNAPSHOT_TABLE);
        sampleNames.add(CURR_SNAPSHOT_TABLE_ROW_TOTAL);
        sampleNames.add(CURR_SNAPSHOT_TABLE_INSERT_ROW_TOTAL);

        return sampleNames;
    }

    @Override
    void doInit(Map<String, Number> values) {
        super.doInit(values);

        // table samples for node
        collector.addSampler(TABLE_TOTAL, nodeTables::size);
        snapshotTableCounter = getCounterSampler(values, SNAPSHOT_TABLE_TOTAL);
        snapshotRowCounter = getCounterSampler(values, SNAPSHOT_ROW_TOTAL);
        snapshotInsertRowCounter = getCounterSampler(values, SNAPSHOT_INSERT_ROW_TOTAL);
        snapshotSourceReadTimeCostAvg = collector.getAverageSampler(SNAPSHOT_SOURCE_READ_TIME_COST_AVG);
        targetWriteTimeCostAvg = collector.getAverageSampler(TARGET_WRITE_TIME_COST_AVG);

        // TODO(dexter): find a way to record the current table name
        collector.addSampler(CURR_SNAPSHOT_TABLE, () -> -1);
        collector.addSampler(CURR_SNAPSHOT_TABLE_ROW_TOTAL, () -> {
            if (null == currentSnapshotTable) return null;
            return currentSnapshotTableRowTotalMap.get(currentSnapshotTable);
        });
        collector.addSampler(CURR_SNAPSHOT_TABLE_INSERT_ROW_TOTAL, () -> currentSnapshotTableInsertRowTotal);
    }

    private Long batchAcceptLastTs;
    private Long batchProcessStartTs;

    public void handleBatchReadFuncStart(String table, Long startAt) {
        batchAcceptLastTs = startAt;
        currentSnapshotTable = table;
        currentSnapshotTableInsertRowTotal = 0L;
    }

    public void handleBatchReadReadComplete(Long readCompleteAt, long size) {
        // batch read only has insert events
        currentSnapshotTableInsertRowTotal += size;
        Optional.ofNullable(inputInsertCounter).ifPresent(counter -> counter.inc(size));
        Optional.ofNullable(inputSpeed).ifPresent(speed -> speed.add(size));
        Optional.ofNullable(snapshotSourceReadTimeCostAvg).ifPresent(
                avg -> avg.add(size, readCompleteAt - batchAcceptLastTs));

        batchProcessStartTs = readCompleteAt;
    }

    public void handleBatchReadProcessComplete(Long processCompleteAt, HandlerUtil.EventTypeRecorder recorder) {
        long size = recorder.getInsertTotal();
        Optional.ofNullable(outputInsertCounter).ifPresent(counter -> counter.inc(size));
        Optional.ofNullable(outputSpeed).ifPresent(speed -> speed.add(size));

        if (null != recorder.getNewestEventTimestamp()) {
            Optional.ofNullable(currentEventTimestamp).ifPresent(number -> number.setValue(recorder.getNewestEventTimestamp()));
            Optional.ofNullable(timeCostAverage).ifPresent(average -> {
                average.add(recorder.getInsertTotal(), processCompleteAt - batchProcessStartTs);
            });
        }

        // snapshot related
        Optional.ofNullable(snapshotInsertRowCounter).ifPresent(counter -> counter.inc(size));
    }

    public void handleBatchReadEnqueued(Long enqueuedTime) {
        batchAcceptLastTs = enqueuedTime;
    }

    public void handleBatchReadFuncEnd() {
        Optional.ofNullable(snapshotTableCounter).ifPresent(CounterSampler::inc);
        currentSnapshotTable = null;
        currentSnapshotTableInsertRowTotal = null;
    }


    private Long streamAcceptLastTs;
    private Long streamReferenceTimeLastTs;
    private Long streamProcessStartTs;
    public void handleStreamReadStreamStart(Long startAt) {
        incrementalSourceReadTimeCostAvg = collector.getAverageSampler(INCR_SOURCE_READ_TIME_COST_AVG);
        streamAcceptLastTs = startAt;
    }

    public void handleStreamReadReadComplete(Long readCompleteAt, HandlerUtil.EventTypeRecorder recorder) {
        long total = recorder.getTotal();

        Optional.ofNullable(inputDdlCounter).ifPresent(counter -> counter.inc(recorder.getDdlTotal()));
        Optional.ofNullable(inputInsertCounter).ifPresent(counter -> counter.inc(recorder.getInsertTotal()));
        Optional.ofNullable(inputUpdateCounter).ifPresent(counter -> counter.inc(recorder.getUpdateTotal()));
        Optional.ofNullable(inputDeleteCounter).ifPresent(counter -> counter.inc(recorder.getDeleteTotal()));
        Optional.ofNullable(inputOthersCounter).ifPresent(counter -> counter.inc(recorder.getOthersTotal()));
        Optional.ofNullable(inputSpeed).ifPresent(speed -> speed.add(total));
        Optional.ofNullable(incrementalSourceReadTimeCostAvg).ifPresent(
                avg -> {
                    if (null == recorder.getOldestEventTimestamp() || null == recorder.getNewestEventTimestamp()) {
                        logger.warn("No reference time found in tap events of incremental sync, skip the " +
                                "incremental source read time cost calculation");
                        return;
                    }

                    if (null == streamAcceptLastTs) {
                        streamAcceptLastTs = readCompleteAt;
                    }
                    if (null == streamReferenceTimeLastTs) {
                        streamReferenceTimeLastTs = recorder.getNewestEventTimestamp();
                    }

                    // ### Case1
                    // Condition: newestRefTs > lastEnqueueTs && oldestRefTs > lastEnqueueTs
                    //
                    // lastNewestRefTs
                    //  || lastReadCompleteTs    oldestRefTs    newestRefTs
                    //  ||  || lastEnqueueTs         ||             || readCompleteTs
                    //  \/  \/    \/                 \/             \/     \/
                    // |_*___*_____*__________________*__*__*___*____*______*_____|
                    //                                |_____________________|
                    //                                           \/
                    //                              incrementalSourceReadTimeCost
                    //
                    // ### Case2
                    // Condition: newestRefTs > lastEnqueueTs && oldestRefTs < lastEnqueueTs
                    //
                    // lastNewestRefTs
                    //  || oldestRefTs          lastEnqueueTs
                    //  ||    || lastReadCompleteTs  || newestRefTs
                    //  ||    ||    ||               ||     || readCompleteTs
                    //  \/    \/    \/               \/     \/     \/
                    // |_*_____*__*__*__*__*___*______*______*______*___|
                    //                                       |______|
                    //                                          \/
                    //                               incrementalSourceReadTimeCost
                    //
                    // ### Case3 && Case4
                    // Condition: newestRefTs < lastEnqueueTs && oldestRefTs < lastEnqueueTs
                    //
                    // lastNewestRefTs           lastReadCompleteTs
                    //  ||     oldestRefTs  newestRefTs  || lastEnqueueTs
                    //  ||         ||           ||       ||     ||   readCompleteTs
                    //  \/         \/           \/       \/     \/      \/
                    // |_*__________*__*__*__*___*________*______*______*____|
                    //                                           |______|
                    //                                              \/
                    //                                   incrementalSourceReadTimeCost
                    //
                    // lastNewestRefTs
                    //  || oldestRefTs          lastEnqueueTs
                    //  ||    || lastReadCompleteTs  ||
                    //  ||    ||    ||  newestRefTs  || readCompleteTs
                    //  \/    \/    \/        \/     \/     \/
                    // |_*_____*__*__*__*__*___*______*______*____|
                    //                                |______|
                    //                                   \/
                    //                            incrementalSourceReadTimeCost
                    long oldestRefTs = recorder.getOldestEventTimestamp();
                    long newestRefTs = recorder.getNewestEventTimestamp();
                    if (newestRefTs >= streamAcceptLastTs && oldestRefTs >= streamAcceptLastTs) {
                        avg.add(total, readCompleteAt - oldestRefTs);
                    } else if (newestRefTs > streamAcceptLastTs && oldestRefTs < streamAcceptLastTs) {
                        avg.add(total, readCompleteAt - newestRefTs);
                    } else if (newestRefTs < streamAcceptLastTs && oldestRefTs < streamAcceptLastTs) {
                        avg.add(total, readCompleteAt - streamAcceptLastTs);
                    } else {
                        logger.warn("Another condition happens when calculate incrementalSourceReadTimeCost, " +
                                "oldestRef: {}, newestRef:{}, lastEnqueueTs: {}, readCompleteTs: {}", oldestRefTs,
                                newestRefTs, readCompleteAt, streamAcceptLastTs);
                    }
                }
        );

        streamProcessStartTs = readCompleteAt;
        streamReferenceTimeLastTs = recorder.getNewestEventTimestamp();
    }

    public void handleStreamReadProcessComplete(Long processCompleteAt, HandlerUtil.EventTypeRecorder recorder) {
        long total = recorder.getTotal();

        Optional.ofNullable(outputDdlCounter).ifPresent(counter -> counter.inc(recorder.getDdlTotal()));
        Optional.ofNullable(outputInsertCounter).ifPresent(counter -> counter.inc(recorder.getInsertTotal()));
        Optional.ofNullable(outputUpdateCounter).ifPresent(counter -> counter.inc(recorder.getUpdateTotal()));
        Optional.ofNullable(outputDeleteCounter).ifPresent(counter -> counter.inc(recorder.getDeleteTotal()));
        Optional.ofNullable(outputOthersCounter).ifPresent(counter -> counter.inc(recorder.getOthersTotal()));
        Optional.ofNullable(outputSpeed).ifPresent(speed -> speed.add(total));

        Optional.ofNullable(currentEventTimestamp).ifPresent(number -> number.setValue(recorder.getNewestEventTimestamp()));
        Optional.ofNullable(replicateLag).ifPresent(speed -> {
            if (null != recorder.getReplicateLagTotal()) {
                speed.add(recorder.getTotal(), recorder.getReplicateLagTotal());
            }
        });

        Optional.ofNullable(timeCostAverage).ifPresent(average -> {
            average.add(total, processCompleteAt - streamProcessStartTs);
        });
    }


    public void handleStreamReadEnqueued(Long enqueuedTime) {
        streamAcceptLastTs = enqueuedTime;
    }

    private Long writeRecordAcceptLastTs;

    public void handleWriteRecordStart(Long startAt, HandlerUtil.EventTypeRecorder recorder) {
        writeRecordAcceptLastTs = startAt;

        Optional.ofNullable(inputDdlCounter).ifPresent(counter -> counter.inc(recorder.getDdlTotal()));
        Optional.ofNullable(inputInsertCounter).ifPresent(counter -> counter.inc(recorder.getInsertTotal()));
        Optional.ofNullable(inputUpdateCounter).ifPresent(counter -> counter.inc(recorder.getUpdateTotal()));
        Optional.ofNullable(inputDeleteCounter).ifPresent(counter -> counter.inc(recorder.getDeleteTotal()));
        Optional.ofNullable(inputOthersCounter).ifPresent(counter -> counter.inc(recorder.getOthersTotal()));
        Optional.ofNullable(inputSpeed).ifPresent(speed -> speed.add(recorder.getTotal()));

    }

    public void handleWriteRecordAccept(Long acceptTime, WriteListResult<TapRecordEvent> result, HandlerUtil.EventTypeRecorder recorder) {
        long inserted = result.getInsertedCount();
        long updated = result.getModifiedCount();
        long deleted = result.getRemovedCount();
        long total = inserted + updated + deleted;

        Optional.ofNullable(outputInsertCounter).ifPresent(counter -> counter.inc(inserted));
        Optional.ofNullable(outputUpdateCounter).ifPresent(counter -> counter.inc(updated));
        Optional.ofNullable(outputDeleteCounter).ifPresent(counter -> counter.inc(deleted));
        Optional.ofNullable(outputSpeed).ifPresent(speed -> speed.add(total));

        Optional.ofNullable(targetWriteTimeCostAvg).ifPresent(average -> {
            average.add(total, acceptTime - writeRecordAcceptLastTs);
            writeRecordAcceptLastTs = acceptTime;
        });

        Optional.ofNullable(currentEventTimestamp).ifPresent(number -> number.setValue(recorder.getNewestEventTimestamp()));
        Optional.ofNullable(replicateLag).ifPresent(speed -> {
            if (null != recorder.getReplicateLagTotal()) {
                speed.add(recorder.getTotal(), recorder.getReplicateLagTotal());
            }
        });
    }

    AtomicBoolean firstTableCount = new AtomicBoolean(true);
    public void handleTableCountAccept(String table, long count) {
        if (firstTableCount.get()) {
            Optional.ofNullable(snapshotRowCounter).ifPresent(CounterSampler::reset);
            firstTableCount.set(false);
        }

        currentSnapshotTableRowTotalMap.put(table, count);
        Optional.ofNullable(snapshotRowCounter).ifPresent(counter -> counter.inc(count));
    }

    public void handleDdlStart() {
        Optional.ofNullable(inputDdlCounter).ifPresent(CounterSampler::inc);
    }

    public void handleDdlEnd() {
        Optional.ofNullable(outputDdlCounter).ifPresent(CounterSampler::inc);
    }

    public void handleSourceDynamicTableAdd(List<String> tables) {
        if (null == tables || tables.isEmpty()) {
            return;
        }
        nodeTables.addAll(tables);
        Optional.ofNullable(inputDdlCounter).ifPresent(counter -> counter.inc(tables.size()));
        Optional.ofNullable(outputDdlCounter).ifPresent(counter -> counter.inc(tables.size()));
    }

    public void handleSourceDynamicTableRemove(List<String> tables) {
        if (null == tables || tables.isEmpty()) {
            return;
        }
        Optional.ofNullable(inputDdlCounter).ifPresent(counter -> counter.inc(tables.size()));
        Optional.ofNullable(outputDdlCounter).ifPresent(counter -> counter.inc(tables.size()));
    }

    static class HealthCheckRunner {
        private static final String TAG = HealthCheckRunner.class.getSimpleName();
        private static final int PERIOD_SECOND = 5;
        private ScheduledExecutorService scheduleExecutorService;
        private Map<String, Node<?>> nodeMap;
        private Map<String, ConnectorNode> connectorNodeMap;
        private Map<String, NumberSampler<Long>> tcpPingNumbers;
        private Map<String, NumberSampler<Long>> connectPingNumbers;

        private boolean running = true;

        public boolean isRunning() {
            return running;
        }

        public void setRunning(boolean running) {
            this.running = running;
        }

        private void runHealthCheck(TaskDto task, SampleCollector collector, Node<?> node, String associateId) {
            String nodeId = node.getId();
            ConnectorNode connectorNode = ConnectorNodeService.getInstance().getConnectorNode(associateId);
            // if the data source does not implement the function, does not init samples or thread
            if (null == connectorNode || null == connectorNode.getConnectorFunctions()
                    || null == connectorNode.getConnectorFunctions().getConnectionCheckFunction()) {
                return;
            }

            tcpPingNumbers = new HashMap<>();
            tcpPingNumbers.put(nodeId, collector.getNumberCollector("tcpPing", Long.class));
            connectPingNumbers = new HashMap<>();
            connectPingNumbers.put(nodeId, collector.getNumberCollector("connectPing", Long.class));
            nodeMap = new HashMap<>();
            nodeMap.putIfAbsent(nodeId, node);
            connectorNodeMap = new HashMap<>();
            connectorNodeMap.putIfAbsent(nodeId, connectorNode);
            // start thread to get the tcp ping and connect ping
            if (null == scheduleExecutorService) {
                String name = String.format("Task data node health check %s-%s", task.getName(), task.getId());
                scheduleExecutorService = ExecutorsManager.getInstance().newSingleThreadScheduledExecutor(name);
                AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();
                ScheduledFuture<?> future = scheduleExecutorService.scheduleAtFixedRate(() -> {
                    if (!running) {
                        futureRef.get().cancel(true);
                        scheduleExecutorService.shutdown();
                    }
                    for (String id : connectorNodeMap.keySet()) {
                        ConnectionCheckFunction function = connectorNode.getConnectorFunctions().getConnectionCheckFunction();
                        if (null == function) {
                            continue;
                        }
                        ConnectionPingAspect connectionPingAspect = new ConnectionPingAspect().node(nodeMap.get(id));
                        PDKInvocationMonitor.invoke(connectorNodeMap.get(id), PDKMethod.CONNECTION_CHECK, () -> {
                            function.check(
                                    connectorNode.getConnectorContext(),
                                    Arrays.asList(
                                            ConnectionCheckItem.ITEM_PING,
                                            ConnectionCheckItem.ITEM_CONNECTION
                                    ),
                                    item -> {
                                        Long value;
                                        // connection check failed, use -1 as value
                                        if (item.getResult() == ConnectionCheckItem.RESULT_FAILED) {
                                            value = -1L;
                                        } else {
                                            value = item.getTakes();
                                        }

                                        NumberSampler<Long> sampler = null;
                                        switch (item.getItem()) {
                                            case ConnectionCheckItem.ITEM_PING:
                                                sampler = tcpPingNumbers.get(id);
                                                connectionPingAspect.tcpPing(value);
                                                break;
                                            case ConnectionCheckItem.ITEM_CONNECTION:
                                                sampler = connectPingNumbers.get(id);
                                                connectionPingAspect.connectPing(value);
                                                break;
                                        }
                                        Optional.ofNullable(sampler).ifPresent(s -> {
                                            s.setValue(Optional.ofNullable(value).orElse(-1L));
                                        });
                                    }
                            );
                        }, TAG);
                        AspectUtils.executeAspect(connectionPingAspect);
                    }
                }, 0L, PERIOD_SECOND, TimeUnit.SECONDS);
                futureRef.set(future);
            }
        }
    }
}
