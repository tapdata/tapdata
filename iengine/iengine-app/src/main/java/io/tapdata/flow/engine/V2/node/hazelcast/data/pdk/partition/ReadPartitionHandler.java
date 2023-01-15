package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.async.master.JobContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePartitionReadDataNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.PDKSourceContext;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.ReadPartitionContext;
import io.tapdata.modules.api.storage.TapKVStorage;
import io.tapdata.modules.api.storage.TapSequenceStorage;
import io.tapdata.modules.api.storage.TapStorageFactory;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.schema.TapTableMap;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static io.tapdata.entity.simplify.TapSimplify.*;

/**
 * @author aplomb
 */
public class ReadPartitionHandler extends PartitionFieldParentHandler {
	private final PDKSourceContext pdkSourceContext;
	private final ReadPartition readPartition;
//	private final TypeSplitterMap typeSplitterMap;
	private final TapStorageFactory storageFactory;
	private final String kvStorageId;
	private final String kvStorageDuringSendingId;
	private final String sequenceStorageId;

	private volatile TapKVStorage kvStorage;
	private volatile TapKVStorage kvStorageDuringSending;
	private volatile TapKVStorage sequenceStorage;
	private final HazelcastSourcePartitionReadDataNode sourcePdkDataNode;
	private final AtomicBoolean finished = new AtomicBoolean(false);
	private final LongAdder sentEventCount = new LongAdder();

	public ReadPartitionHandler(PDKSourceContext pdkSourceContext, TapTable tapTable, ReadPartition readPartition, HazelcastSourcePartitionReadDataNode sourcePdkDataNode) {
		super(tapTable);
		this.readPartition = readPartition;
		this.pdkSourceContext = pdkSourceContext;
		this.sourcePdkDataNode = sourcePdkDataNode;

		this.storageFactory = InstanceFactory.instance(TapStorageFactory.class);
		storageFactory.init(TapStorageFactory.StorageOptions.create().disableJavaSerializable(false).rootPath("./partition_storage"));
		String taskId = pdkSourceContext.getSourcePdkDataNode().getNode().getTaskId();

		kvStorageId = "stream_" + taskId + "_" + readPartition.getId();
		kvStorageDuringSendingId = "stream_" + taskId + "_" + readPartition.getId() + "_during_sending";
		sequenceStorageId = "batch_" + taskId + "_" + readPartition.getId();
	}
	public void writeIntoKVStorage(Map<String, Object> key, Map<String, Object> after, TapRecordEvent recordEvent) {
		if(kvStorageDuringSending != null)
			kvStorageDuringSending.put(key, recordEvent);
		else
			kvStorage.put(key, after);
	}

	public void deleteFromKVStorage(Map<String, Object> key) {
		if(kvStorageDuringSending != null)
			kvStorageDuringSending.remove(key);
		else
			kvStorage.remove(key);
	}

	public JobContext handleStartCachingStreamData(JobContext jobContext1) {
		if(kvStorage == null) {
			synchronized (this) {
				if(kvStorage == null) {
					storageFactory.deleteKVStorage(kvStorageDuringSendingId);
					storageFactory.deleteKVStorage(kvStorageId);
					kvStorage = storageFactory.getKVStorage(kvStorageId);
					kvStorage.setClassLoader(sourcePdkDataNode.getConnectorNode().getConnectorClassLoader());
					kvStorage.setPath(sourcePdkDataNode.getNode().getId());
//					sourcePdkDataNode.getObsLogger().info("Prepared kv storage file {} for partition {}", kvStorageId, readPartition);
				}
			}
		}

		return null;
	}

	public JobContext handleReadPartition(JobContext jobContext) {
		if(sequenceStorage == null) {
			synchronized (this) {
				if(sequenceStorage == null) {
					storageFactory.deleteKVStorage(sequenceStorageId);
					sequenceStorage = storageFactory.getKVStorage(sequenceStorageId);
					sequenceStorage.setClassLoader(sourcePdkDataNode.getConnectorNode().getConnectorClassLoader());
					sequenceStorage.setPath(sourcePdkDataNode.getNode().getId());
//					sourcePdkDataNode.getObsLogger().info("Prepared sequence storage file {} for partition {}", sequenceStorageId, readPartition);
				}
			}
		}
		ReadPartitionContext readPartitionContext = jobContext.getContext(ReadPartitionContext.class);
		sourcePdkDataNode.getObsLogger().info("Start storing partition {} into local, batchSize {}, sequenceStorageId {}", readPartition, sourcePdkDataNode.batchSize, sequenceStorageId);
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
											sequenceStorage.put(key, value);
											storageTakes.add(System.currentTimeMillis() - storageTime);
										}
									});
								})
						));
			} finally {
				sourcePdkDataNode.removePdkMethodInvoker(pdkMethodInvoker);
				sourcePdkDataNode.getObsLogger().info("Stored the readPartition {} into local {}, takes {}, storage takes {}, total {}", readPartition, sequenceStorageId, (System.currentTimeMillis() - time), storageTakes.longValue(), counter.longValue());
			}

		}


		return null;
	}

	public JobContext handleSendingDataFromPartition(JobContext jobContext) {
		ReadPartitionContext readPartitionContext = jobContext.getContext(ReadPartitionContext.class);
		if(kvStorageDuringSending == null) {
			synchronized (this) {
				if(kvStorageDuringSending == null) {
					kvStorageDuringSending = storageFactory.getKVStorage(kvStorageDuringSendingId);
					kvStorageDuringSending.setClassLoader(sourcePdkDataNode.getConnectorNode().getConnectorClassLoader());
					kvStorageDuringSending.setPath(sourcePdkDataNode.getNode().getId());
//					sourcePdkDataNode.getObsLogger().info("Prepared kv storage during sending file {} for partition {}", kvStorageDuringSendingId, readPartition);
				}
			}
		}
		sourcePdkDataNode.getObsLogger().info("Start sending data from partition {}, batchSize {}, sequenceStorageId {}", readPartition, sourcePdkDataNode.batchSize, sequenceStorageId);
		BatchReadFuncAspect batchReadFuncAspect = readPartitionContext.getBatchReadFuncAspect();
		List<TapEvent> events = new ArrayList<>();
		long time = System.currentTimeMillis();

		AtomicReference<List<TapEvent>> reference = new AtomicReference<>(events);
		sequenceStorage.foreachValues((value, value1) -> {
			Map<String, Object> record = (Map<String, Object>) value;
			Map<String, Object> dataFromCDC = (Map<String, Object>) value1;
			if(dataFromCDC != null) {
				record.putAll(dataFromCDC);
			}
			reference.get().add(insertRecordEvent(record, table));
			sentEventCount.increment();
			if(reference.get().size() >= sourcePdkDataNode.batchSize) {
//				long theTime = System.currentTimeMillis();
				enqueueTapEvents(batchReadFuncAspect, reference.get());
//				sourcePdkDataNode.getObsLogger().info("enqueueTapEvents sequence events {} takes {}", reference.get().size(), (System.currentTimeMillis() - theTime));
				reference.set(new ArrayList<>());
			}
			return null;
		}, kvStorage::removeAndGet);
//		sequenceStorage.foreach((key1, value) -> {
//			Map<String, Object> record = (Map<String, Object>) value;
//			Map<String, Object> key = (Map<String, Object>) key1;
//			Map<String, Object> dataFromCDC = (Map<String, Object>) kvStorage.removeAndGet(key);
//			if(dataFromCDC != null) {
//				record.putAll(dataFromCDC);
//			}
//			reference.get().add(insertRecordEvent(record, table));
//			sentEventCount.increment();
//			if(reference.get().size() >= sourcePdkDataNode.batchSize) {
////				long theTime = System.currentTimeMillis();
//				enqueueTapEvents(batchReadFuncAspect, reference.get());
////				sourcePdkDataNode.getObsLogger().info("enqueueTapEvents sequence events {} takes {}", reference.get().size(), (System.currentTimeMillis() - theTime));
//				reference.set(new ArrayList<>());
//			}
//			return null;
//		});
		long theTime = System.currentTimeMillis();
		enqueueTapEvents(batchReadFuncAspect, reference.get());
		sourcePdkDataNode.getObsLogger().info("enqueueTapEvents last sequence events {} takes {}", reference.get().size(), (System.currentTimeMillis() - theTime));

		sourcePdkDataNode.getObsLogger().info("Consumer sequence events {} takes {}", sentEventCount.longValue(), (System.currentTimeMillis() - time));

		theTime = System.currentTimeMillis();
		List<TapEvent> newInsertEvents = new ArrayList<>();
		AtomicReference<List<TapEvent>> newInsertReference = new AtomicReference<>(newInsertEvents);
		kvStorage.foreachValues((value) -> {
			jobContext.checkJobStoppedOrNot();
			newInsertReference.get().add(insertRecordEvent((Map<String, Object>) value, table));
			sentEventCount.increment();
			if(newInsertReference.get().size() >= sourcePdkDataNode.batchSize) {
				enqueueTapEvents(batchReadFuncAspect, newInsertReference.get());
				newInsertReference.set(new ArrayList<>());
			}
			return null;
		});
		enqueueTapEvents(batchReadFuncAspect, newInsertReference.get());
		sourcePdkDataNode.getObsLogger().info("Consumer rest cdc events {} takes {}", sentEventCount.longValue(), (System.currentTimeMillis() - theTime));

		sourcePdkDataNode.getObsLogger().info("Send {} events to next node for read partition {} takes {}", sentEventCount.longValue(), readPartition, (System.currentTimeMillis() - time));
		return null;
	}

	private void enqueueTapEvents(BatchReadFuncAspect batchReadFuncAspect, List<TapEvent> events) {
		if(events == null || events.isEmpty())
			return;
//		long time = System.currentTimeMillis();
		if (batchReadFuncAspect != null)
			AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_READ_COMPLETE).getReadCompleteConsumers(), events);
//		sourcePdkDataNode.getObsLogger().info("STATE_READ_COMPLETE events {} takes {}", events.size(), (System.currentTimeMillis() - time));

		if (sourcePdkDataNode.logger.isDebugEnabled()) {
			sourcePdkDataNode.logger.debug("Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourcePdkDataNode.getConnectorNode()));
		}
//					((Map<String, Object>) syncProgress.getBatchOffsetObj()).put(tapTable.getId(), offsetObject);
//		time = System.currentTimeMillis();
		List<TapdataEvent> tapdataEvents = sourcePdkDataNode.wrapTapdataEvent(events);
//		sourcePdkDataNode.getObsLogger().info("wrapTapdataEvent events {} takes {}", events.size(), (System.currentTimeMillis() - time));

		if (batchReadFuncAspect != null)
			AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_PROCESS_COMPLETE).getProcessCompleteConsumers(), tapdataEvents);

		if (CollectionUtil.isNotEmpty(tapdataEvents)) {
//			long time = System.currentTimeMillis();
			tapdataEvents.forEach(sourcePdkDataNode::enqueue);
//			sourcePdkDataNode.getObsLogger().info("enqueue events {} takes {}", tapdataEvents.size(), (System.currentTimeMillis() - time));

//			time = System.currentTimeMillis();
			if (batchReadFuncAspect != null)
				AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_ENQUEUED).getEnqueuedConsumers(), tapdataEvents);
//			sourcePdkDataNode.getObsLogger().info("STATE_ENQUEUED events {} takes {}", tapdataEvents.size(), (System.currentTimeMillis() - time));
		}
	}

	public JobContext handleFinishedPartition(JobContext jobContext) {
		synchronized (this) {
			if(finished.compareAndSet(false, true)) {
				long time = System.currentTimeMillis();
				LongAdder counter = new LongAdder();

				List<TapEvent> list = new ArrayList<>();
				AtomicReference<List<TapEvent>> eventListReference = new AtomicReference<>(list);
				kvStorageDuringSending.foreachValues((value) -> {
					jobContext.checkJobStoppedOrNot();
//					eventListReference.get().add(insertRecordEvent((Map<String, Object>) value, table.getId()));
					eventListReference.get().add((TapRecordEvent)value);
					counter.increment();
					if(eventListReference.get().size() >= sourcePdkDataNode.batchSize) {
						sourcePdkDataNode.handleStreamEventsReceived(eventListReference.get(), null);
						eventListReference.set(new ArrayList<>());
					}
					return null;
				});
				sourcePdkDataNode.handleStreamEventsReceived(eventListReference.get(), null);
				sourcePdkDataNode.getObsLogger().info("Read partition {} finished, takes {}, event(during sending) count {}", readPartition, (System.currentTimeMillis() - time), counter.longValue());
			}
		}
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
		storageFactory.deleteKVStorage(kvStorageDuringSendingId);
		storageFactory.deleteKVStorage(kvStorageId);
		storageFactory.deleteKVStorage(sequenceStorageId);
		return null;
	}

	public void passThrough(TapEvent event) {
		if(finished.get())
			sourcePdkDataNode.handleStreamEventsReceived(list(event), null);
	}

	public Map<String, Object> getExistDataFromKVMap(Map<String, Object> key) {
		return (Map<String, Object>) kvStorage.get(key);
	}

	public boolean isFinished() {
		return finished.get();
	}

	public void finish() {
		finished.set(true);
	}
}
