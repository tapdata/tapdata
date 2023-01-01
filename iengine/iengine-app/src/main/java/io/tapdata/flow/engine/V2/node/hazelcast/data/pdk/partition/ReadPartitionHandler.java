package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.async.master.JobContext;
import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.ControlEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndexEx;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.simplify.pretty.TypeHandlers;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePdkDataNodeEx1;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.PDKSourceContext;
import io.tapdata.modules.api.storage.TapKVStorage;
import io.tapdata.modules.api.storage.TapSequenceStorage;
import io.tapdata.modules.api.storage.TapStorageFactory;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.LoggerUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.tapdata.entity.simplify.TapSimplify.insertRecordEvent;
import static io.tapdata.entity.simplify.TapSimplify.map;

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
	private volatile TapSequenceStorage sequenceStorage;
	private final HazelcastSourcePdkDataNodeEx1 sourcePdkDataNode;
	private final Integer batchSize = 2000;

	public ReadPartitionHandler(PDKSourceContext pdkSourceContext, TapTable tapTable, ReadPartition readPartition, HazelcastSourcePdkDataNodeEx1 sourcePdkDataNode) {
		super(tapTable);
		this.readPartition = readPartition;
		this.pdkSourceContext = pdkSourceContext;
		this.sourcePdkDataNode = sourcePdkDataNode;

		this.storageFactory = InstanceFactory.instance(TapStorageFactory.class);
		storageFactory.init(TapStorageFactory.StorageOptions.create().disableJavaSerializable(true).rootPath("./partition_storage"));
		String taskId = pdkSourceContext.getSourcePdkDataNode().getNode().getTaskId();

		kvStorageId = "stream_" + taskId + "_" + readPartition.getId();
		kvStorageDuringSendingId = "stream_" + taskId + "_" + readPartition.getId() + "_during_sending";
		sequenceStorageId = "batch_" + taskId + "_" + readPartition.getId();
	}
	public void writeIntoKVStorage(Map<String, Object> key, Map<String, Object> after) {
		if(kvStorageDuringSending != null)
			kvStorageDuringSending.put(key, after);
		else
			kvStorage.put(key, after);
	}

	public void deleteFromKVStorage(Map<String, Object> key) {
		if(kvStorageDuringSending != null)
			kvStorageDuringSending.remove(key);
		else
			kvStorage.remove(key);
	}


	public JobContext handlePartitionExistOrNot(JobContext jobContext) {
		//TODO check partition id has finished or not from TM.
		if(false)
			return JobContext.create(null).jumpToId("finishedPartition");
		return null;
	}

	public JobContext handleStartCachingStreamData(JobContext jobContext1) {
		if(kvStorage == null) {
			synchronized (this) {
				if(kvStorage == null) {
					storageFactory.deleteKVStorage(kvStorageDuringSendingId);
					storageFactory.deleteKVStorage(kvStorageId);
					kvStorage = storageFactory.getKVStorage(kvStorageId);
				}
			}
		}

		return null;
	}

	private void receivedCDCEvents(List<TapEvent> tapEvents) {
		for(TapEvent event : tapEvents)
			typeHandlers.handle(event);
	}

	public JobContext handleReadPartition(JobContext jobContext) {
		if(sequenceStorage == null) {
			synchronized (this) {
				if(sequenceStorage == null) {
					storageFactory.deleteSequenceStorage(sequenceStorageId);
					sequenceStorage = storageFactory.getSequenceStorage(sequenceStorageId);
				}
			}
		}
		QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = sourcePdkDataNode.getConnectorNode().getConnectorFunctions().getQueryByAdvanceFilterFunction();
		if(queryByAdvanceFilterFunction != null) {
			TapPartitionFilter partitionFilter = readPartition.getPartitionFilter();
			TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create()
					.match(partitionFilter.getMatch())
					.op(partitionFilter.getLeftBoundary())
					.op(partitionFilter.getRightBoundary())
					.batchSize(batchSize);
			PDKMethodInvoker pdkMethodInvoker = sourcePdkDataNode.createPdkMethodInvoker();
			try {
				PDKInvocationMonitor.invoke(sourcePdkDataNode.getConnectorNode(), PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
						pdkMethodInvoker.runnable(
								() -> queryByAdvanceFilterFunction.query(sourcePdkDataNode.getConnectorNode().getConnectorContext(), tapAdvanceFilter, table, filterResults -> {
									Optional.ofNullable(filterResults.getResults()).ifPresent(results -> {
										for (Map<String, Object> result : results) {
											sequenceStorage.add(reviseData(result));
										}
									});
								})
						));
			} finally {
				sourcePdkDataNode.removePdkMethodInvoker(pdkMethodInvoker);
			}

		}


		return null;
	}

	public JobContext handleSendingDataFromPartition(JobContext jobContext) {
		if(kvStorageDuringSending == null) {
			synchronized (this) {
				if(kvStorageDuringSending == null) {
					kvStorageDuringSending = storageFactory.getKVStorage(kvStorageDuringSendingId);
				}
			}
		}

		sourcePdkDataNode.executeDataFuncAspect(BatchReadFuncAspect.class, () -> new BatchReadFuncAspect()
				.eventBatchSize(batchSize)
				.connectorContext(sourcePdkDataNode.getConnectorNode().getConnectorContext())
				.offsetState(null)
				.dataProcessorContext(sourcePdkDataNode.getDataProcessorContext())
				.start()
				.table(table), batchReadFuncAspect -> {

			List<TapEvent> events = new ArrayList<>();
			Iterator<Object> iterator = sequenceStorage.iterator();
			AtomicReference<List<TapEvent>> reference = new AtomicReference<>(events);
			jobContext.foreach(iterator, o -> {
				Map<String, Object> record = (Map<String, Object>) o;
				Map<String, Object> key = getKeyFromData(record);
				Map<String, Object> dataFromCDC = (Map<String, Object>) kvStorage.removeAndGet(key);
				if(dataFromCDC != null) {
					record.putAll(dataFromCDC);
				}
				reference.get().add(insertRecordEvent(record, table.getId()));
				if(reference.get().size() >= batchSize) {
					enqueueTapEvents(batchReadFuncAspect, reference.get());
					reference.set(new ArrayList<>());
				}
				return null;
			});
			enqueueTapEvents(batchReadFuncAspect, reference.get());

			List<TapEvent> newInsertEvents = new ArrayList<>();
			kvStorage.foreach((key, value) -> {
				newInsertEvents.add(insertRecordEvent((Map<String, Object>) value, table.getId()));
				return null;
			});
			enqueueTapEvents(batchReadFuncAspect, newInsertEvents);
		});
		return null;
	}

	private void enqueueTapEvents(BatchReadFuncAspect batchReadFuncAspect, List<TapEvent> events) {
		if(events == null || events.isEmpty())
			return;
		if (batchReadFuncAspect != null)
			AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_READ_COMPLETE).getReadCompleteConsumers(), events);

		if (sourcePdkDataNode.logger.isDebugEnabled()) {
			sourcePdkDataNode.logger.debug("Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourcePdkDataNode.getConnectorNode()));
		}
//					((Map<String, Object>) syncProgress.getBatchOffsetObj()).put(tapTable.getId(), offsetObject);
		List<TapdataEvent> tapdataEvents = sourcePdkDataNode.wrapTapdataEvent(events);

		if (batchReadFuncAspect != null)
			AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_PROCESS_COMPLETE).getProcessCompleteConsumers(), tapdataEvents);

		if (CollectionUtil.isNotEmpty(tapdataEvents)) {
			tapdataEvents.forEach(sourcePdkDataNode::enqueue);

			if (batchReadFuncAspect != null)
				AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_ENQUEUED).getEnqueuedConsumers(), tapdataEvents);
		}
	}

	public JobContext handleFinishedPartition(JobContext jobContext) {
		List<TapEvent> list = new ArrayList<>();
		kvStorageDuringSending.foreach((key, value) -> {
			list.add(insertRecordEvent((Map<String, Object>) value, table.getId()));
			return null;
		});
		sourcePdkDataNode.handleStreamEventsReceived(list, null);

		storageFactory.deleteKVStorage(kvStorageDuringSendingId);
		storageFactory.deleteKVStorage(kvStorageId);
		storageFactory.deleteSequenceStorage(sequenceStorageId);
		return null;
	}

	public Map<String, Object> getExistDataFromKVMap(Map<String, Object> key) {
		return (Map<String, Object>) kvStorage.get(key);
	}
}
