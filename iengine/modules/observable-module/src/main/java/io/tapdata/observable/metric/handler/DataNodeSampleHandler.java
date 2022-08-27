package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.executor.ExecutorsManager;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.NumberSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.observable.metric.TaskSampleRetriever;
import io.tapdata.observable.metric.aspect.ConnectionPingAspect;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckFunction;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
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
public class DataNodeSampleHandler extends AbstractNodeSampleHandler {
    private final Logger logger = LogManager.getLogger(DataNodeSampleHandler.class);
    private static final String TAG = DataNodeSampleHandler.class.getSimpleName();
    public DataNodeSampleHandler(TaskDto task) {
        super(task);
    }

    private boolean running = true;

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    private final Map<String, SampleCollector> collectors = new HashMap<>();
    private final Map<String, CounterSampler> snapshotTableCounters = new HashMap<>();
    private final Map<String, CounterSampler> snapshotRowCounters = new HashMap<>();
    private final Map<String, CounterSampler> snapshotInsertRowCounters = new HashMap<>();

    // event related metrics
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

    private final Map<String, SpeedSampler> inputQpsSpeeds = new HashMap<>();
    private final Map<String, SpeedSampler> outputQpsSpeeds = new HashMap<>();

    private final Map<String, AverageSampler> timeCostAverages = new HashMap<>();

    private final Map<String, NumberSampler<Long>>  currentEventTimestamps = new HashMap<>();

    private final Map<String, Set<String>> nodeTables = new HashMap<>();
    public void init(Node<?> node, String associateId, Set<String> tables) {
        Map<String, String> tags = nodeTags(node);
        Map<String, Number> values = TaskSampleRetriever.getInstance().retrieve(tags, Arrays.asList(
                "inputInsertTotal", "inputUpdateTotal", "inputDeleteTotal", "inputDdlTotal", "inputOthersTotal",
                "outputInsertTotal", "outputUpdateTotal", "outputDeleteTotal", "outputDdlTotal", "outputOthersTotal",
                "snapshotTableTotal", "snapshotRowTotal", "snapshotInsertRowTotal",
                "currentEventTimestamp", "createTableTotal"
        ));
        String nodeId = node.getId();
        SampleCollector collector = CollectorFactory.getInstance("v2").getSampleCollectorByTags("nodeSamplers", tags);
        collectors.put(nodeId, collector);

        // table samples for node
        nodeTables.putIfAbsent(nodeId, new HashSet<>());
        nodeTables.get(nodeId).addAll(tables);
        collector.addSampler("tableTotal", () ->  nodeTables.get(nodeId).size());
        snapshotTableCounters.put(nodeId, collector.getCounterSampler("snapshotTableTotal",
                values.getOrDefault("snapshotTableTotal", 0).longValue()));
        snapshotRowCounters.put(nodeId, collector.getCounterSampler("snapshotRowTotal",
                values.getOrDefault("snapshotRowTotal", 0).longValue()));
        snapshotInsertRowCounters.put(nodeId, collector.getCounterSampler("snapshotInsertRowTotal",
                values.getOrDefault("snapshotInsertRowTotal", 0).longValue()));


        // only data nodes have events metrics
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

        inputQpsSpeeds.put(nodeId, collector.getSpeedSampler("inputQps"));
        outputQpsSpeeds.put(nodeId, collector.getSpeedSampler("outputQps"));

        timeCostAverages.put(nodeId, collector.getAverageSampler("timeCostAvg"));

        Number currentEventTimestampInitial = values.getOrDefault("currentEventTimestamp", null);
        currentEventTimestamps.put(nodeId, collector.getNumberCollector("currentEventTimestamp", Long.class,
                null == currentEventTimestampInitial ? null : currentEventTimestampInitial.longValue()));

        if (null != node.getCatalog() && node.getCatalog() == Node.NodeCatalog.data) {
            // run health check
            runHealthCheck(node, associateId);
        }

        // cache the initial sample value
        CollectorFactory.getInstance("v2").recordCurrentValueByTag(tags);

    }

    public void close(Node<?> node) {
        String nodeId = node.getId();
        Optional.ofNullable(collectors.get(nodeId)).ifPresent(collector -> {
            Map<String, String> tags = collector.tags();
            // cache the last sample value
            CollectorFactory.getInstance("v2").recordCurrentValueByTag(tags);
            CollectorFactory.getInstance("v2").removeSampleCollectorByTags(tags);
        });
    }

    private final Map<String, Long> batchAcceptLastTs = new HashMap<>();
    public void handleBatchReadStart(String nodeId, Long startAt) {
        batchAcceptLastTs.put(nodeId, startAt);
    }

    public void handleBatchReadReadComplete(String nodeId, Long acceptTime, long size, Long newestEventTimestamp) {
        // batch read only has insert events
        Optional.ofNullable(inputInsertCounters.get(nodeId)).ifPresent(counter -> counter.inc(size));
        Optional.ofNullable(inputQpsSpeeds.get(nodeId)).ifPresent(speed -> speed.add(size));

        Optional.ofNullable(outputInsertCounters.get(nodeId)).ifPresent(counter -> counter.inc(size));
        Optional.ofNullable(outputQpsSpeeds.get(nodeId)).ifPresent(speed -> speed.add(size));

        Optional.ofNullable(currentEventTimestamps.get(nodeId)).ifPresent(number -> number.setValue(newestEventTimestamp));
        Optional.ofNullable(timeCostAverages.get(nodeId)).ifPresent(average -> {
            average.add(size, acceptTime - batchAcceptLastTs.get(nodeId));
        });

        // snapshot related
        Optional.ofNullable(snapshotInsertRowCounters.get(nodeId)).ifPresent(counter -> counter.inc(size));
    }

    public void handleBatchReadEnqueued(String nodeId, Long enqueuedTime) {
        batchAcceptLastTs.put(nodeId, enqueuedTime);
    }

    public void handleBatchReadFuncEnd(String nodeId) {
        Optional.ofNullable(snapshotTableCounters.get(nodeId)).ifPresent(CounterSampler::inc);
    }

    private final Map<String, Long> streamAcceptLastTs = new HashMap<>();
    public void handleStreamReadStreamStart(String nodeId, Long startAt) {
        streamAcceptLastTs.put(nodeId, startAt);
    }

    public void handleStreamReadReadComplete(String nodeId, Long acceptTime, HandlerUtil.EventTypeRecorder recorder, Long newestEventTimestamp) {
        long total = recorder.getTotal();

        Optional.ofNullable(inputInsertCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getInsertTotal()));
        Optional.ofNullable(inputUpdateCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getUpdateTotal()));
        Optional.ofNullable(inputDeleteCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getDeleteTotal()));
        Optional.ofNullable(inputDdlCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getDdlTotal()));
        Optional.ofNullable(inputOthersCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getOthersTotal()));
        Optional.ofNullable(inputQpsSpeeds.get(nodeId)).ifPresent(speed -> speed.add(total));

        Optional.ofNullable(outputInsertCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getInsertTotal()));
        Optional.ofNullable(outputUpdateCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getUpdateTotal()));
        Optional.ofNullable(outputDeleteCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getDeleteTotal()));
        Optional.ofNullable(outputDdlCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getDdlTotal()));
        Optional.ofNullable(outputOthersCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getOthersTotal()));
        Optional.ofNullable(outputQpsSpeeds.get(nodeId)).ifPresent(speed -> speed.add(total));

        Optional.ofNullable(currentEventTimestamps.get(nodeId)).ifPresent(sampler -> sampler.setValue(newestEventTimestamp));
        Optional.ofNullable(timeCostAverages.get(nodeId)).ifPresent(average -> {
            average.add(total, acceptTime - streamAcceptLastTs.get(nodeId));
        });
    }

    public void handleStreamReadEnqueued(String nodeId, Long enqueuedTime) {
        streamAcceptLastTs.put(nodeId, enqueuedTime);
    }

    private final Map<String, Long> writeRecordAcceptLastTs = new HashMap<>();

    public void handleWriteRecordStart(String nodeId, Long startAt, HandlerUtil.EventTypeRecorder recorder) {
        writeRecordAcceptLastTs.put(nodeId, startAt);

        Optional.ofNullable(inputInsertCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getInsertTotal()));
        Optional.ofNullable(inputUpdateCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getUpdateTotal()));
        Optional.ofNullable(inputDeleteCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getDeleteTotal()));
        Optional.ofNullable(inputDdlCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getDdlTotal()));
        Optional.ofNullable(inputOthersCounters.get(nodeId)).ifPresent(counter -> counter.inc(recorder.getOthersTotal()));
        Optional.ofNullable(inputQpsSpeeds.get(nodeId)).ifPresent(speed -> speed.add(recorder.getTotal()));

    }

    public void handleWriteRecordAccept(String nodeId, Long acceptTime, WriteListResult<TapRecordEvent> result, Long newestEventTimestamp) {
        long inserted = result.getInsertedCount();
        long updated = result.getModifiedCount();
        long deleted = result.getRemovedCount();
        long total = inserted + updated + deleted;

        Optional.ofNullable(outputInsertCounters.get(nodeId)).ifPresent(counter -> counter.inc(inserted));
        Optional.ofNullable(outputUpdateCounters.get(nodeId)).ifPresent(counter -> counter.inc(updated));
        Optional.ofNullable(outputDeleteCounters.get(nodeId)).ifPresent(counter -> counter.inc(deleted));
        Optional.ofNullable(outputQpsSpeeds.get(nodeId)).ifPresent(speed -> speed.add(total));

        Optional.ofNullable(timeCostAverages.get(nodeId)).ifPresent(average -> {
            average.add(total, acceptTime - writeRecordAcceptLastTs.get(nodeId));
            writeRecordAcceptLastTs.put(nodeId, acceptTime);
        });

        Optional.ofNullable(currentEventTimestamps.get(nodeId)).ifPresent(sampler -> sampler.setValue(newestEventTimestamp));
    }

    AtomicBoolean firstTableCount = new AtomicBoolean(true);
    public void handleTableCountAccept(String nodeId, long count) {
        if (firstTableCount.get()) {
            Optional.ofNullable(snapshotRowCounters.get(nodeId)).ifPresent(CounterSampler::reset);
            firstTableCount.set(false);
        }

        Optional.ofNullable(snapshotRowCounters.get(nodeId)).ifPresent(counter -> counter.inc(count));
    }

    public void handleDdlStart(String nodeId) {
        Optional.ofNullable(inputDdlCounters.get(nodeId)).ifPresent(CounterSampler::inc);
    }

    public void handleDdlEnd(String nodeId) {
        Optional.ofNullable(outputDdlCounters.get(nodeId)).ifPresent(CounterSampler::inc);
    }

    private static final int PERIOD_SECOND = 5;
    private ScheduledExecutorService scheduleExecutorService;
    private Map<String, Node<?>> nodeMap;
    private Map<String, ConnectorNode> connectorNodeMap;
    private Map<String, NumberSampler<Long>> tcpPingNumbers;
    private Map<String, NumberSampler<Long>> connectPingNumbers;
    private void runHealthCheck(Node<?> node, String associateId) {
        String nodeId = node.getId();
        ConnectorNode connectorNode = ConnectorNodeService.getInstance().getConnectorNode(associateId);
        // if the data source does not implement the function, does not init samples or thread
        if (null == connectorNode || null == connectorNode.getConnectorFunctions()
                || null == connectorNode.getConnectorFunctions().getConnectionCheckFunction()) {
            return;
        }

        tcpPingNumbers = new HashMap<>();
        tcpPingNumbers.put(nodeId, collectors.get(nodeId).getNumberCollector("tcpPing", Long.class));
        connectPingNumbers = new HashMap<>();
        connectPingNumbers.put(nodeId, collectors.get(nodeId).getNumberCollector("connectPing", Long.class));
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
