package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.async.master.JobContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePartitionReadDataNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.PDKSourceContext;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.ReadPartitionContext;
import io.tapdata.modules.api.storage.TapStorageFactory;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.LoggerUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static io.tapdata.entity.simplify.TapSimplify.insertRecordEvent;
import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author GavinXiao
 * @description ReadPartitionUnKVStorageHandler create by Gavin
 * @create 2023/4/6 14:25
 **/
public class ReadPartitionUnKVStorageHandler extends PartitionFieldParentHandler implements ReadPartitionKVStorage {
    private final PDKSourceContext pdkSourceContext;
    private final ReadPartition readPartition;
    private final HazelcastSourcePartitionReadDataNode sourcePdkDataNode;
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final LongAdder sentEventCount = new LongAdder();

    public ReadPartitionUnKVStorageHandler(PDKSourceContext pdkSourceContext, TapTable tapTable, ReadPartition readPartition, HazelcastSourcePartitionReadDataNode sourcePdkDataNode) {
        super(tapTable);
        this.readPartition = readPartition;
        this.pdkSourceContext = pdkSourceContext;
        this.sourcePdkDataNode = sourcePdkDataNode;
    }

    public void writeIntoKVStorage(Map<String, Object> key, Map<String, Object> after, TapRecordEvent recordEvent) {

    }

    public void deleteFromKVStorage(Map<String, Object> key) {

    }

    public void justDeleteFromKVStorage(Map<String, Object> key) {

    }

    public JobContext handleStartCachingStreamData(JobContext jobContext1) {
        return null;
    }

    public JobContext handleReadPartition(JobContext jobContext) {
        ReadPartitionContext readPartitionContext = jobContext.getContext(ReadPartitionContext.class);
        sourcePdkDataNode.getObsLogger().info("Start storing partition {} into local, batchSize {} ", readPartition, sourcePdkDataNode.batchSize);
        QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = sourcePdkDataNode.getConnectorNode().getConnectorFunctions().getQueryByAdvanceFilterFunction();
        if(queryByAdvanceFilterFunction != null) {
            long time = System.currentTimeMillis();
            LongAdder storageTakes = new LongAdder();
            LongAdder counter = new LongAdder();
            TapPartitionFilter partitionFilter = readPartition.getPartitionFilter();
            TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create()
                    .match(partitionFilter.getMatch())
                    .op(partitionFilter.getLeftBoundary())
                    .op(partitionFilter.getRightBoundary())
                    .batchSize(sourcePdkDataNode.batchSize);
            PDKMethodInvoker pdkMethodInvoker = sourcePdkDataNode.createPdkMethodInvoker();
            try {
                PDKInvocationMonitor.invoke(sourcePdkDataNode.getConnectorNode(), PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
                        pdkMethodInvoker.runnable(
                                () -> queryByAdvanceFilterFunction.query(sourcePdkDataNode.getConnectorNode().getConnectorContext(), tapAdvanceFilter, readPartitionContext.getTable(), filterResults -> {
                                    Optional.ofNullable(filterResults.getResults()).ifPresent(results -> {
                                        long storageTime;
                                        for (Map<String, Object> result : results) {
                                            counter.increment();
                                            Map<String, Object> value = reviseData(result);
                                            Map<String, Object> key = getKeyFromData(value);
                                            storageTime = System.currentTimeMillis();
                                            storageTakes.add(System.currentTimeMillis() - storageTime);
                                        }
                                    });
                                })
                        ));
            } finally {
                sourcePdkDataNode.removePdkMethodInvoker(pdkMethodInvoker);
                sourcePdkDataNode.getObsLogger().info("Stored the readPartition {}, takes {}, storage takes {}, total {}", readPartition, (System.currentTimeMillis() - time), storageTakes.longValue(), counter.longValue());
            }

        }
        return null;
    }

    public JobContext handleSendingDataFromPartition(JobContext jobContext) {
        ReadPartitionContext readPartitionContext = jobContext.getContext(ReadPartitionContext.class);
        sourcePdkDataNode.getObsLogger().info("Start sending data from partition {}, batchSize {}", readPartition, sourcePdkDataNode.batchSize);
        BatchReadFuncAspect batchReadFuncAspect = readPartitionContext.getBatchReadFuncAspect();
        List<TapEvent> events = new ArrayList<>();
        long time = System.currentTimeMillis();
        AtomicReference<List<TapEvent>> reference = new AtomicReference<>(events);
        enqueueTapEvents(batchReadFuncAspect, reference.get());
        sourcePdkDataNode.getObsLogger().info("Consumer sequence events {} takes {}", sentEventCount.longValue(), (System.currentTimeMillis() - time));
        List<TapEvent> newInsertEvents = new ArrayList<>();
        AtomicReference<List<TapEvent>> newInsertReference = new AtomicReference<>(newInsertEvents);
        enqueueTapEvents(batchReadFuncAspect, newInsertReference.get());
        sourcePdkDataNode.getObsLogger().info("Send {} events to next node for read partition {} takes {}", sentEventCount.longValue(), readPartition, (System.currentTimeMillis() - time));
        return null;
    }

    public JobContext handleFinishedPartition(JobContext jobContext) {
        synchronized (pdkSourceContext) {
            PartitionTableOffset partitionTableOffset = (PartitionTableOffset) ((Map<?, ?>) sourcePdkDataNode.getSyncProgress().getBatchOffsetObj()).get(table);
            if(partitionTableOffset == null) {
                partitionTableOffset = new PartitionTableOffset();
                ((Map<String, PartitionTableOffset>) sourcePdkDataNode.getSyncProgress().getBatchOffsetObj()).put(table, partitionTableOffset);
            }
            Map<String, Long> completedPartitions = partitionTableOffset.getCompletedPartitions();
            if(completedPartitions == null) {
                completedPartitions = new ConcurrentHashMap<>();
                completedPartitions.put(readPartition.getId(), sentEventCount.longValue());
                partitionTableOffset.setCompletedPartitions(completedPartitions);
            } else {
                completedPartitions.put(readPartition.getId(), sentEventCount.longValue());
            }
            sourcePdkDataNode.getObsLogger().info("Finished partition {} completedPartitions {}", readPartition, completedPartitions.size());
        }
        return null;
    }

    public void passThrough(TapEvent event) {
        if(finished.get())
            sourcePdkDataNode.handleStreamEventsReceived(list(event), null);
    }

    public Map<String, Object> getExistDataFromKVMap(Map<String, Object> key) {
        return null;
    }

    public boolean isFinished() {
        return finished.get();
    }

    public void finish() {
        finished.set(true);
    }

    private void enqueueTapEvents(BatchReadFuncAspect batchReadFuncAspect, List<TapEvent> events) {
        if(events == null || events.isEmpty())
            return;
        if (batchReadFuncAspect != null)
            AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_READ_COMPLETE).getReadCompleteConsumers(), events);
        if (sourcePdkDataNode.logger.isDebugEnabled()) {
            sourcePdkDataNode.logger.debug("Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourcePdkDataNode.getConnectorNode()));
        }
        List<TapdataEvent> tapdataEvents = sourcePdkDataNode.wrapTapdataEvent(events);
        if (batchReadFuncAspect != null)
            AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_PROCESS_COMPLETE).getProcessCompleteConsumers(), tapdataEvents);

        if (CollectionUtil.isNotEmpty(tapdataEvents)) {
            tapdataEvents.forEach(sourcePdkDataNode::enqueue);
            if (batchReadFuncAspect != null)
                AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_ENQUEUED).getEnqueuedConsumers(), tapdataEvents);
        }
    }
}
