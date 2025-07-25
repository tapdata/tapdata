package io.tapdata.observable.metric;

import com.google.common.collect.HashBiMap;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import io.tapdata.aspect.*;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.aspect.taskmilestones.CDCHeartbeatWriteAspect;
import io.tapdata.aspect.taskmilestones.SnapshotWriteTableCompleteAspect;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.pretty.ClassHandlers;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.module.api.PipelineDelay;
import io.tapdata.observable.metric.handler.*;
import io.tapdata.observable.metric.util.SyncGetMemorySizeHandler;
import io.tapdata.observable.metric.util.TapCompletableFutureEx;
import io.tapdata.observable.metric.util.TapCompletableFutureTaskEx;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_CONN_HEARTBEAT, TaskDto.SYNC_TYPE_LOG_COLLECTOR,TaskDto.SYNC_TYPE_MEM_CACHE})
public class ObservableAspectTask extends AspectTask {
	public static final String TAPCOMPLETABLEFUTUREEX_QUEUE_SIZE_PROP_KEY = String.join("_", TapCompletableFutureEx.TAG, "QUEUE", "SIZE");
	public static final int TAPCOMPLETABLEFUTUREEX_QUEUE_SIZE_DEFAULT_VALUE = 500000;
	public static final String TAPCOMPLETABLEFUTUREEX_JOIN_WATERMARK_PROP_KEY = String.join("_", TapCompletableFutureEx.TAG, "JOIN", "WATERMARK");
	public static final int TAPCOMPLETABLEFUTUREEX_JOIN_WATERMARK_DEFAULT_VALUE = 500000;
	private final ClassHandlers observerClassHandlers = new ClassHandlers();

	private TaskSampleHandler taskSampleHandler;
	private Map<String, TableSampleHandler> tableSampleHandlers;
	private Map<String, DataNodeSampleHandler> dataNodeSampleHandlers;
	private Map<String, ProcessorNodeSampleHandler> processorNodeSampleHandlers;
	private static final String TAG = ObservableAspectTask.class.getSimpleName();

	public ObservableAspectTask() {
		// data node aspects
		observerClassHandlers.register(DataNodeInitAspect.class, this::handleDataNodeInit);
		observerClassHandlers.register(PDKNodeInitAspect.class, this::handlePDKNodeInit);
		observerClassHandlers.register(DataNodeCloseAspect.class, this::handleDataNodeClose);
		// source data node aspects
		observerClassHandlers.register(SourceJoinHeartbeatAspect.class, this::handleSourceJoinHeartbeat);
		observerClassHandlers.register(TableCountFuncAspect.class, this::handleTableCount);
		observerClassHandlers.register(BatchReadFuncAspect.class, this::handleBatchReadFunc);
		observerClassHandlers.register(StreamReadFuncAspect.class, this::handleStreamReadFunc);
		observerClassHandlers.register(SourceStateAspect.class, this::handleSourceState);
		observerClassHandlers.register(SourceDynamicTableAspect.class, this::handleSourceDynamicTable);
		// target data node aspects
		observerClassHandlers.register(CDCHeartbeatWriteAspect.class, this::handleCDCHeartbeatWriteAspect);
		observerClassHandlers.register(WriteRecordFuncAspect.class, this::handleWriteRecordFunc);
		observerClassHandlers.register(SnapshotWriteTableCompleteAspect.class, this::handleSnapshotWriteTableCompleteFunc);
		observerClassHandlers.register(NewFieldFuncAspect.class, this::handleNewFieldFun);
		observerClassHandlers.register(AlterFieldNameFuncAspect.class, this::handleAlterFieldNameFunc);
		observerClassHandlers.register(AlterFieldAttributesFuncAspect.class, this::handleAlterFieldAttributesFunc);
		observerClassHandlers.register(TruncateTableFuncAspect.class,this::handleTruncateTableFuncAspect);
		observerClassHandlers.register(DropFieldFuncAspect.class, this::handleDropFieldFunc);
		observerClassHandlers.register(CreateTableFuncAspect.class, this::handleCreateTableFunc);
		observerClassHandlers.register(DropTableFuncAspect.class, this::handleDropTableFunc);

		// processor node aspects
		observerClassHandlers.register(ProcessorNodeInitAspect.class, this::handleProcessorNodeInit);
		observerClassHandlers.register(ProcessorNodeCloseAspect.class, this::handleProcessorNodeClose);
		observerClassHandlers.register(ProcessorNodeProcessAspect.class, this::handleProcessorNodeProcess);

		observerClassHandlers.register(TaskBatchSplitAspect.class, this::handleWriteBatchSplit);
	}

	CompletableFuture<Void> batchReadFuture;
	CompletableFuture<Void> batchProcessFuture;
	CompletableFuture<Void> streamReadFuture;
	CompletableFuture<Void> streamProcessFuture;
	TapCompletableFutureEx writeRecordFuture;
	/**
	 * The task started
	 */
	@Override
	public void onStart(TaskStartAspect startAspect) {
		taskSampleHandler = new TaskSampleHandler(task);
		taskSampleHandler.init();
		initCompletableFuture();
	}

	protected void initCompletableFuture() {
		batchReadFuture = CompletableFuture.runAsync(()->{});
		batchProcessFuture = CompletableFuture.runAsync(()->{});
		streamReadFuture = CompletableFuture.runAsync(()->{});
		streamProcessFuture = CompletableFuture.runAsync(()->{});
		writeRecordFuture = TapCompletableFutureTaskEx.create(
				CommonUtils.getPropertyInt(TAPCOMPLETABLEFUTUREEX_QUEUE_SIZE_PROP_KEY, TAPCOMPLETABLEFUTUREEX_QUEUE_SIZE_DEFAULT_VALUE),
				CommonUtils.getPropertyInt(TAPCOMPLETABLEFUTUREEX_JOIN_WATERMARK_PROP_KEY, TAPCOMPLETABLEFUTUREEX_JOIN_WATERMARK_DEFAULT_VALUE),
				task
		).start();
	}

	protected void closeCompletableFuture() {
		if (null != batchReadFuture) batchReadFuture.cancel(false);
		if (null != batchProcessFuture) batchProcessFuture.cancel(false);
		if (null != streamReadFuture) streamReadFuture.cancel(false);
		if (null != streamProcessFuture) streamProcessFuture.cancel(false);
		try {
			if (null != writeRecordFuture) writeRecordFuture.stop(15L, TimeUnit.SECONDS);
		}catch (Exception e){
			TapLogger.info(TAG, "Stop writeRecordFuture fail: {}", Log4jUtil.getStackString(e));
		}
	}

	/**
	 * The task stopped
	 */
	@Override
	public void onStop(TaskStopAspect stopAspect) {
		pipelineDelay.clear(stopAspect.getTask().getId().toHexString());
		if (null != tableSampleHandlers) {
			for (TableSampleHandler handler : tableSampleHandlers.values()) {
				handler.close();
			}
		}

		if (null != dataNodeSampleHandlers) {
			for (DataNodeSampleHandler handler : dataNodeSampleHandlers.values()) {
				handler.close();
			}
		}

		if (null != processorNodeSampleHandlers) {
			for (ProcessorNodeSampleHandler handler : processorNodeSampleHandlers.values()) {
				handler.close();
			}
		}

		closeCompletableFuture();
		taskSampleHandler.close();
	}

	// data node related

	public Void handleDataNodeInit(DataNodeInitAspect aspect) {
		if (null == dataNodeSampleHandlers) {
			dataNodeSampleHandlers = new HashMap<>();
		}
		Node<?> node = aspect.getDataProcessorContext().getNode();
		if (node instanceof TableNode && ((TableNode) node).isIgnoreMetrics()) {
			return null;
		}
		String nodeId = node.getId();
		DataNodeSampleHandler handler = new DataNodeSampleHandler(task, node);
		dataNodeSampleHandlers.put(nodeId, handler);
		if (node.getDag().getTargets().stream().anyMatch(n->nodeId.equals(n.getId()))) {
			taskSampleHandler.addTargetNodeHandler(nodeId, handler);
		} else if (node.getDag().getSources().stream().anyMatch(n->nodeId.equals(n.getId()))) {
			taskSampleHandler.addSourceNodeHandler(nodeId, handler);
		}
		handler.init();

		return null;
	}

	public Void handleDataNodeClose(DataNodeCloseAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::close);
//		DataNodeSampleHandler.HealthCheckRunner.getInstance().stopHealthCheck(nodeId);

		return null;
	}

	public Void handlePDKNodeInit(PDKNodeInitAspect aspect) {
//		Node<?> node = aspect.getDataProcessorContext().getNode();
//		DataNodeSampleHandler handler = dataNodeSampleHandlers.get(node.getId());
//		if (null != handler) {
//			DataNodeSampleHandler.HealthCheckRunner.getInstance().runHealthCheck(
//					handler.getCollector(), node,  aspect.getDataProcessorContext().getPdkAssociateId());
//		}

		return null;
	}

	// source data node related

	private final Map<String, Boolean> joinHeartbeatMap = new ConcurrentHashMap<>();
	public Void handleSourceJoinHeartbeat(SourceJoinHeartbeatAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		joinHeartbeatMap.put(node.getId(), aspect.getJoinHeartbeat());
		return null;
	}

	Map<String, Map<String, Number>> taskRetrievedTableValues;
	public Void handleTableCount(TableCountFuncAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		switch (aspect.getState()) {
			case TableCountFuncAspect.STATE_START:
				if (null == taskRetrievedTableValues) {
					taskRetrievedTableValues = TableSampleHandler.retrieveAllTables(task);
				}
				for (String table : aspect.getDataProcessorContext().getTapTableMap().keySet()) {
					Optional.ofNullable(dataNodeSampleHandlers.get(node.getId())).ifPresent(
							dataNodeSampleHandler -> dataNodeSampleHandler.addTable(table)
					);
					taskSampleHandler.addTable(table);
				}
				aspect.tableCountConsumer((table, cnt) -> {
					if (null == tableSampleHandlers) {
						tableSampleHandlers = new HashMap<>();
					}
					TableSampleHandler handler = new TableSampleHandler(task, table, cnt, taskRetrievedTableValues.getOrDefault(table, new HashMap<>()), BigDecimal.ZERO);
					tableSampleHandlers.put(table, handler);
					handler.init();
					handler.setTaskSampleHandler(taskSampleHandler);

					Optional.ofNullable(dataNodeSampleHandlers.get(node.getId())).ifPresent(
							dataNodeSampleHandler -> dataNodeSampleHandler.handleTableCountAccept(table, cnt)
					);
					taskSampleHandler.handleTableCountAccept(table, cnt);
				});
				break;
			case TableCountFuncAspect.STATE_END:
				break;
		}
		return null;
	}

	public Void handleBatchReadFunc(BatchReadFuncAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		String nodeId = node.getId();
		String table = aspect.getTable().getName();

		switch (aspect.getState()) {
			case BatchReadFuncAspect.STATE_START:
				if (node instanceof TableNode && ((TableNode) node).isIgnoreMetrics()) {
					break;
				}
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(handler -> handler.handleBatchReadFuncStart(table, aspect.getTime()));
				taskSampleHandler.addTable(table);
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
						dataNodeSampleHandler -> dataNodeSampleHandler.addTable(table)
				);
				taskSampleHandler.handleBatchReadStart(table);
				final SyncGetMemorySizeHandler syncGetMemorySizeHandler = prepareSyncGetMemorySizeHandler();
				aspect.readCompleteConsumer(e -> ObservableAspectTaskUtil.batchReadComplete(batchReadFuture, e, syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(e), nodeId, dataNodeSampleHandlers, taskSampleHandler, System.currentTimeMillis()));
				aspect.processCompleteConsumer(e -> ObservableAspectTaskUtil.batchReadProcessComplete(batchProcessFuture, e, syncGetMemorySizeHandler.getEventTypeRecorderSyncTapDataEvent(e), nodeId, dataNodeSampleHandlers, System.currentTimeMillis()));
				aspect.enqueuedConsumer(events ->
					Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
							handler -> handler.handleBatchReadEnqueued(System.currentTimeMillis())
				));
				break;
			case BatchReadFuncAspect.STATE_END:
				break;
		}

		return null;
	}

	public Void handleStreamReadFunc(StreamReadFuncAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		String nodeId = node.getId();

		switch (aspect.getState()) {
			case StreamReadFuncAspect.STATE_START:
				if (node instanceof TableNode && ((TableNode) node).isIgnoreMetrics()) {
					break;
				}
				List<String> tables = aspect.getTables().stream().filter(t -> {
					if (Boolean.TRUE.equals(joinHeartbeatMap.get(nodeId))) {
						if (ConnHeartbeatUtils.TABLE_NAME.equals(t)) return false;
					}
					return true;
				}).collect(Collectors.toList());
				taskSampleHandler.handleStreamReadStart(tables);
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
						handler -> handler.handleStreamReadStreamStart(tables, aspect.getStreamStartedTime())
				);
				final SyncGetMemorySizeHandler syncGetMemorySizeHandler = prepareSyncGetMemorySizeHandler();
				aspect.streamingReadCompleteConsumers(e -> ObservableAspectTaskUtil.streamReadComplete(streamReadFuture, e, syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(e), nodeId, dataNodeSampleHandlers, taskSampleHandler, System.currentTimeMillis()));
				aspect.streamingProcessCompleteConsumers(e -> ObservableAspectTaskUtil.streamReadProcessComplete(streamProcessFuture, e, syncGetMemorySizeHandler.getEventTypeRecorderSyncTapDataEvent(e), nodeId, dataNodeSampleHandlers, System.currentTimeMillis()));
				aspect.streamingEnqueuedConsumers(events -> {
					Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
							handler -> handler.handleStreamReadEnqueued(System.currentTimeMillis())
					);
				});
				break;
			case StreamReadFuncAspect.STATE_END:
				break;
		}

		return null;
	}

	public Void handleSourceState(SourceStateAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		switch (aspect.getState()) {
			case SourceStateAspect.STATE_INITIAL_SYNC_START:
				taskSampleHandler.handleSnapshotStart(aspect.getInitialSyncStartTime());
				for(String table : aspect.getDataProcessorContext().getTapTableMap().keySet()) {
					taskSampleHandler.addTable(table);
				}
				break;
			case SourceStateAspect.STATE_INITIAL_SYNC_COMPLETED:
//				taskSampleHandler.handleSnapshotDone(aspect.getInitialSyncCompletedTime());
				break;
			default:
				break;
		}

		return null;
	}

	public Void handleSourceDynamicTable(SourceDynamicTableAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		switch (aspect.getType()) {
			case SourceDynamicTableAspect.DYNAMIC_TABLE_TYPE_ADD:
				taskSampleHandler.handleSourceDynamicTableAdd(aspect.getTables());
				Optional.ofNullable(dataNodeSampleHandlers.get(node.getId())).ifPresent(
						handler -> handler.handleSourceDynamicTableAdd(aspect.getTables())
				);
				break;
			case SourceDynamicTableAspect.DYNAMIC_TABLE_TYPE_REMOVE:
				taskSampleHandler.handleSourceDynamicTableRemove(aspect.getTables());
				Optional.ofNullable(dataNodeSampleHandlers.get(node.getId())).ifPresent(
						handler -> handler.handleSourceDynamicTableRemove(aspect.getTables())
				);
				break;
			default:
				break;
		}

		return null;
	}

	// target data node related

	public Void handleCreateTableFunc(CreateTableFuncAspect aspect) {
		if(aspect.isInit()){
			return null;
		}
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case CreateTableFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case CreateTableFuncAspect.STATE_END:
				taskSampleHandler.handleCreateTableEnd();
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}

	public Void handleDropTableFunc(DropTableFuncAspect aspect) {
		if(aspect.isInit()){
			return null;
		}
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case DropTableFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case DropTableFuncAspect.STATE_END:
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}

	public Void handleNewFieldFun(NewFieldFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case NewFieldFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case NewFieldFuncAspect.STATE_END:
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}

	public Void handleAlterFieldNameFunc(AlterFieldNameFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case AlterFieldNameFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case AlterFieldNameFuncAspect.STATE_END:
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}

	public Void handleAlterFieldAttributesFunc(AlterFieldAttributesFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case AlterFieldAttributesFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case AlterFieldAttributesFuncAspect.STATE_END:
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}
	public Void handleTruncateTableFuncAspect(TruncateTableFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case TruncateTableFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case TruncateTableFuncAspect.STATE_END:
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}

	public Void handleDropFieldFunc(DropFieldFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case DropFieldFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case DropFieldFuncAspect.STATE_END:
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}

	public Void handleCDCHeartbeatWriteAspect(CDCHeartbeatWriteAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		String nodeId = node.getId();
		Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
				handler -> {
					handler.handleCDCHeartbeatWriteAspect(aspect.getTapdataEvents());
				}
		);
		return null;
	}

	private PipelineDelayImpl pipelineDelay = (PipelineDelayImpl) InstanceFactory.instance(PipelineDelay.class);

	public Void handleWriteRecordFunc(WriteRecordFuncAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		switch (aspect.getState()) {
			case WriteRecordFuncAspect.STATE_START:
				String nodeId = node.getId();
				String table = aspect.getTable().getName();
				if (node instanceof TableNode && ((TableNode) node).isSourceAndTarget()) {
					break;
				}
				HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapEvent(aspect.getRecordEvents());
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
						handler -> {
							handler.handleWriteRecordStart(aspect.getTime(), recorder);
						}
				);
				aspect.consumer((events, result) -> {
					try {
						writeRecordFuture.thenRun(() -> {
							if (null == events || events.isEmpty()) {
								return;
							}

							HandlerUtil.EventTypeRecorder inner = HandlerUtil.countTapEvent(events, recorder.getMemorySize());
							Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(handler -> handler.handleWriteRecordAccept(System.currentTimeMillis(), result, inner));

							taskSampleHandler.handleWriteRecordAccept(result, events, inner);

							Optional.ofNullable(tableSampleHandlers)
									.flatMap(handlers -> {
										// source >> target table name maybe change

										String syncType = aspect.getDataProcessorContext().getTaskDto().getSyncType();
										if (TaskDto.SYNC_TYPE_SYNC.equals(syncType) || TaskDto.SYNC_TYPE_CONN_HEARTBEAT.equals(syncType)) {
											return handlers.values().stream().findFirst();
										} else {
											LinkedHashMap<String, String> tableNameRelation = ((DatabaseNode) node).getSyncObjects().get(0).getTableNameRelation();
											String targetTableName = HashBiMap.create(tableNameRelation).inverse().get(table);
											if (handlers.containsKey(targetTableName)) {
												return Optional.ofNullable(handlers.get(targetTableName));
											}
											return Optional.ofNullable(handlers.get(table));

										}
									})
									.ifPresent(handler -> handler.incrTableSnapshotInsertTotal(recorder.getInsertTotal()));

							pipelineDelay.refreshDelay(task.getId().toHexString(), nodeId, inner.getProcessTimeTotal() / inner.getTotal(), inner.getNewestEventTimestamp());
						});
					} catch (Exception e) {
						TapLogger.info("Run async writeRecordFuture fail: {}", Log4jUtil.getStackString(e));
					}
				});
				break;
			case WriteRecordFuncAspect.BATCH_SPLIT:
				writeRecordFuture.thenRun(() -> {
					if (taskSampleHandler instanceof TaskSampleHandlerV2) {
						((TaskSampleHandlerV2) taskSampleHandler).handleWriteBatchSplit();
					}
				});
				break;
			case WriteRecordFuncAspect.STATE_END:
				break;
		}

		return null;
	}

	public Void handleSnapshotWriteTableCompleteFunc(SnapshotWriteTableCompleteAspect aspect) {
		String nodeId = aspect.getSourceNodeId();
		Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
			handler -> handler.handleBatchReadFuncEnd(System.currentTimeMillis())
		);
		tableSampleHandlers.get(aspect.getSourceTableName()).setSnapshotDone();
		taskSampleHandler.handleBatchReadFuncEnd();
		return null;
	}

	// processor node related

	public Void handleProcessorNodeInit(ProcessorNodeInitAspect aspect) {
		if (null == processorNodeSampleHandlers) {
			processorNodeSampleHandlers = new HashMap<>();
		}
		Node<?> node = aspect.getProcessorBaseContext().getNode();
		ProcessorNodeSampleHandler handler = new ProcessorNodeSampleHandler(task, node);
		processorNodeSampleHandlers.put(node.getId(), handler);
		handler.init();

		return null;
	}

	public Void handleProcessorNodeClose(ProcessorNodeCloseAspect aspect) {
		String nodeId = aspect.getProcessorBaseContext().getNode().getId();
		Optional.ofNullable(processorNodeSampleHandlers.get(nodeId)).ifPresent(ProcessorNodeSampleHandler::close);

		return null;
	}

	public Void handleProcessorNodeProcess(ProcessorNodeProcessAspect aspect) {
		String nodeId = aspect.getProcessorBaseContext().getNode().getId();

		switch (aspect.getState()) {
			case ProcessorNodeProcessAspect.STATE_START:
				HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapdataEvent(aspect.getInputEvents());
				Optional.ofNullable(processorNodeSampleHandlers.get(nodeId)).ifPresent(
						handler -> handler.handleProcessStart(recorder)
				);
				aspect.consumer(event -> {
					if (null == event) {
						return;
					}
					HandlerUtil.EventTypeRecorder inner = HandlerUtil.countTapdataEvent(Collections.singletonList(event));
					Optional.ofNullable(processorNodeSampleHandlers.get(nodeId)).ifPresent(
							handler -> handler.handleProcessAccept(inner)
					);
				});
				break;
			case ProcessorNodeProcessAspect.STATE_END:
				Optional.ofNullable(processorNodeSampleHandlers.get(nodeId)).ifPresent(
						handler -> handler.handleProcessEnd(aspect.getTime(), aspect.getEndTime(), aspect.outputCount())
				);
				break;
		}

		return null;
	}

	public Void handleWriteBatchSplit(TaskBatchSplitAspect taskBatchSplitAspect) {
		if (taskSampleHandler instanceof TaskSampleHandlerV2) {
			((TaskSampleHandlerV2) taskSampleHandler).handleWriteBatchSplit();
		}
		return null;
	}

	@Override
	public List<Class<? extends Aspect>> observeAspects() {
		List<Class<? extends Aspect>> aspects = new ArrayList<>();
		for (Class<?> clazz : observerClassHandlers.keyList()) {
			aspects.add((Class<? extends Aspect>) clazz);
		}

		return aspects;
	}

	@Override
	public List<Class<? extends Aspect>> interceptAspects() {
		return null;
	}

	@Override
	public void onObserveAspect(Aspect aspect) {
		observerClassHandlers.handle(aspect);
	}

	@Override
	public AspectInterceptResult onInterceptAspect(Aspect aspect) {
		return null;
	}
	protected SyncGetMemorySizeHandler prepareSyncGetMemorySizeHandler() {
		return new SyncGetMemorySizeHandler(new AtomicLong(-1));
	}
}
